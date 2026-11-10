import { describe, expect, test } from '@jest/globals';
import dgram from 'dgram';
import net from 'net';
import { TcpTransport, Transport, UdpTransport } from '../src/transport';
import { concat, decodeOpaque, encode, encodeCall, encodeString, encodeOpaque } from '../src/shared/xdr';

const TEXT = new TextDecoder();
const BINARY = new TextEncoder();

// Wire format used across this library (see client.ts / server.ts):
//   16B XDR base header + 32B call header + XDR string msgId + XDR opaque body
const MESSAGE_START = 48; // 16 + 32

function u32(data: Uint8Array, offset: number): number {
  return new DataView(data.buffer, data.byteOffset + offset, 4).getUint32(0, false);
}

function callMessage(msgId: string, payload: Uint8Array): Uint8Array {
  return concat(
    encode({ version: 0, type: 0, headerLength: 16, authType: 0 }),
    encodeCall(100003, 2, 7, { version: 0, type: 0, headerLength: 0, authType: 0 }),
    encodeString(msgId),
    encodeOpaque(payload),
  );
}

function replyMessage(msgId: string, payload: Uint8Array): Uint8Array {
  return concat(
    encode({ version: 2, type: 2, headerLength: 16, authType: 0 }),
    encodeCall(0, 0, 0, { version: 0, type: 0, headerLength: 0, authType: 0 }),
    encodeString(msgId),
    encodeOpaque(payload),
  );
}

function extractMsgId(message: Uint8Array): string {
  const len = u32(message, MESSAGE_START);
  return TEXT.decode(message.subarray(MESSAGE_START + 4, MESSAGE_START + 4 + len)).split('\0')[0]!;
}

function extractBody(message: Uint8Array): Uint8Array {
  const msgIdLen = u32(message, MESSAGE_START);
  return decodeOpaque(message.subarray(MESSAGE_START + 4 + msgIdLen));
}

async function startServer(): Promise<TcpTransport> {
  const transport = new TcpTransport({ host: '127.0.0.1', port: 0, isServer: true });
  await transport.connect();
  return transport;
}

async function startClient(port: number): Promise<TcpTransport> {
  const transport = new TcpTransport({ host: '127.0.0.1', port });
  await transport.connect();
  return transport;
}

function onlyConnection(t: Transport): string {
  const ids = t.connectionIds();
  if (ids.length !== 1) throw new Error(`expected exactly one connection, got ${ids.length}`);
  return ids[0]!;
}

function rawSocket(t: TcpTransport, connectionId: string): net.Socket {
  return t.connection(connectionId)!.channel as net.Socket;
}

function rawUdpSocket(t: Transport, connectionId: string): dgram.Socket {
  return t.connection(connectionId)!.channel as dgram.Socket;
}

// Fire-and-await for a raw dgram send (the socket callback signature
// doesn't fit a promise resolver).
function sendUdp(socket: dgram.Socket, data: Uint8Array | Buffer, port: number, host: string): Promise<void> {
  return new Promise<void>((resolve, reject) =>
    socket.send(data, port, host, (err) => (err ? reject(err) : resolve())));
}

async function startUdpServer(): Promise<UdpTransport> {
  const transport = new UdpTransport({ host: '127.0.0.1', port: 0, isServer: true });
  await transport.connect();
  return transport;
}

async function startUdpClient(port: number): Promise<UdpTransport> {
  const transport = new UdpTransport({ host: '127.0.0.1', port });
  await transport.connect();
  return transport;
}

// Wait until the server has opened that many peer connections (first
// datagram from a peer opens it — UDP has no handshake to await).
async function waitForPeers(server: UdpTransport, count: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (server.connectionCount() < count && Date.now() < deadline) await sleep(25);
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

describe('TcpTransport', () => {
  test('routes messages between two concurrent clients', async () => {
    const server = await startServer();
    try {
      const c1 = await startClient(server.actualPort);
      const c2 = await startClient(server.actualPort);
      await server.waitForConnection(3000);
      expect(server.connectionCount()).toBe(2);

      const sid1 = onlyConnection(c1);
      const sid2 = onlyConnection(c2);

      await c1.send(sid1, callMessage('msg-c1', BINARY.encode('hello-c1')));
      await c2.send(sid2, callMessage('msg-c2', BINARY.encode('hello-c2')));

      const r1 = await server.receiveAny(3000);
      const r2 = await server.receiveAny(3000);
      expect(r1).toBeDefined();
      expect(r2).toBeDefined();

      const incoming = new Map<string, { connectionId: string; message: Uint8Array }>();
      for (const r of [r1, r2]) {
        if (r) incoming.set(extractMsgId(r.message), r);
      }
      expect(incoming.has('msg-c1')).toBe(true);
      expect(incoming.has('msg-c2')).toBe(true);

      // Reply to each message on its own connection.
      for (const [msgId, r] of incoming) {
        await server.send(r.connectionId, replyMessage(msgId, BINARY.encode(`echo-${msgId}`)));
      }

      const p1 = await c1.receive(sid1, 3000);
      const p2 = await c2.receive(sid2, 3000);
      expect(extractMsgId(p1)).toBe('msg-c1');
      expect(extractMsgId(p2)).toBe('msg-c2');
      expect(TEXT.decode(extractBody(p1))).toBe('echo-msg-c1');
      expect(TEXT.decode(extractBody(p2))).toBe('echo-msg-c2');

      await c1.closeAll();
      await c2.closeAll();
    } finally {
      await server.closeAll();
    }
  });

  test('reassembles a message split across TCP segments', async () => {
    const server = await startServer();
    try {
      const client = await startClient(server.actualPort);
      await server.waitForConnection(3000);
      const cid = onlyConnection(client);
      const sid = server.connectionIds()[0]!;

      const full = callMessage('split-msg', BINARY.encode('fragmented-payload-payload'));
      const splitAt = Math.floor(full.length / 3);

      const socket = rawSocket(client, cid);
      socket.write(full.subarray(0, splitAt));
      await sleep(120);
      socket.write(full.subarray(splitAt));

      const received = await server.receive(sid, 3000);
      expect(received.length).toBe(full.length);
      expect(extractMsgId(received)).toBe('split-msg');
      expect(TEXT.decode(extractBody(received))).toBe('fragmented-payload-payload');

      await client.closeAll();
    } finally {
      await server.closeAll();
    }
  });

  test('splits two messages that arrive in a single TCP segment', async () => {
    const server = await startServer();
    try {
      const client = await startClient(server.actualPort);
      await server.waitForConnection(3000);
      const cid = onlyConnection(client);
      const sid = server.connectionIds()[0]!;

      const m1 = callMessage('combo-a', BINARY.encode('first'));
      const m2 = callMessage('combo-b', BINARY.encode('second'));

      rawSocket(client, cid).write(Buffer.concat([Buffer.from(m1), Buffer.from(m2)]));

      const first = await server.receive(sid, 3000);
      const second = await server.receive(sid, 3000);
      expect(extractMsgId(first)).toBe('combo-a');
      expect(extractMsgId(second)).toBe('combo-b');
      expect(TEXT.decode(extractBody(first))).toBe('first');
      expect(TEXT.decode(extractBody(second))).toBe('second');

      await client.closeAll();
    } finally {
      await server.closeAll();
    }
  });

  test('fires close event and rejects pending receive when a client goes away', async () => {
    const server = await startServer();
    try {
      const client = await startClient(server.actualPort);
      await server.waitForConnection(3000);
      const sid = server.connectionIds()[0]!;

      const closes: string[] = [];
      const onClose = (connectionId: string) => closes.push(connectionId);
      server.on('close', onClose);

      await client.closeAll();

      const deadline = Date.now() + 3000;
      while (closes.length === 0 && Date.now() < deadline) await sleep(25);
      expect(closes).toEqual(expect.arrayContaining([sid]));

      await expect(server.receive(sid, 300)).rejects.toThrow();
      server.off('close', onClose);
    } finally {
      await server.closeAll();
    }
  });
});

describe('UdpTransport', () => {
  test('routes messages between two concurrent clients', async () => {
    const server = await startUdpServer();
    try {
      const c1 = await startUdpClient(server.actualPort);
      const c2 = await startUdpClient(server.actualPort);

      const cid1 = onlyConnection(c1);
      const cid2 = onlyConnection(c2);

      // The first datagram from each peer opens the server-side
      // connection (UDP has no handshake to await beforehand).
      await c1.send(cid1, callMessage('msg-c1', BINARY.encode('hello-c1')));
      await c2.send(cid2, callMessage('msg-c2', BINARY.encode('hello-c2')));
      await waitForPeers(server, 2, 3000);
      expect(server.connectionCount()).toBe(2);

      const r1 = await server.receiveAny(3000);
      const r2 = await server.receiveAny(3000);
      expect(r1).toBeDefined();
      expect(r2).toBeDefined();

      const incoming = new Map<string, { connectionId: string; message: Uint8Array }>();
      for (const r of [r1, r2]) {
        if (r) incoming.set(extractMsgId(r.message), r);
      }
      expect(incoming.has('msg-c1')).toBe(true);
      expect(incoming.has('msg-c2')).toBe(true);

      // Reply to each message on its own peer connection.
      for (const [msgId, r] of incoming) {
        await server.send(r.connectionId, replyMessage(msgId, BINARY.encode(`echo-${msgId}`)));
      }

      const p1 = await c1.receive(cid1, 3000);
      const p2 = await c2.receive(cid2, 3000);
      expect(extractMsgId(p1)).toBe('msg-c1');
      expect(extractMsgId(p2)).toBe('msg-c2');
      expect(TEXT.decode(extractBody(p1))).toBe('echo-msg-c1');
      expect(TEXT.decode(extractBody(p2))).toBe('echo-msg-c2');

      await c1.closeAll();
      await c2.closeAll();
    } finally {
      await server.closeAll();
    }
  });

  test('reassembles a message split across two datagrams', async () => {
    const server = await startUdpServer();
    try {
      const client = await startUdpClient(server.actualPort);
      const cid = onlyConnection(client);

      const full = callMessage('split-msg', BINARY.encode('fragmented-payload-payload'));
      const splitAt = Math.floor(full.length / 3);

      const socket = rawUdpSocket(client, cid);
      // First half: no complete message yet, but the peer registers.
      await sendUdp(socket, full.subarray(0, splitAt), server.actualPort, '127.0.0.1');
      await waitForPeers(server, 1, 3000);
      const sid = server.connectionIds()[0]!;
      await sendUdp(socket, full.subarray(splitAt), server.actualPort, '127.0.0.1');

      const received = await server.receive(sid, 3000);
      expect(received.length).toBe(full.length);
      expect(extractMsgId(received)).toBe('split-msg');
      expect(TEXT.decode(extractBody(received))).toBe('fragmented-payload-payload');

      await client.closeAll();
    } finally {
      await server.closeAll();
    }
  });

  test('splits two messages that arrive in a single datagram', async () => {
    const server = await startUdpServer();
    try {
      const client = await startUdpClient(server.actualPort);
      const cid = onlyConnection(client);

      const m1 = callMessage('combo-a', BINARY.encode('first'));
      const m2 = callMessage('combo-b', BINARY.encode('second'));

      await sendUdp(rawUdpSocket(client, cid), Buffer.concat([Buffer.from(m1), Buffer.from(m2)]), server.actualPort, '127.0.0.1');

      await waitForPeers(server, 1, 3000);
      const sid = server.connectionIds()[0]!;

      const first = await server.receive(sid, 3000);
      const second = await server.receive(sid, 3000);
      expect(extractMsgId(first)).toBe('combo-a');
      expect(extractMsgId(second)).toBe('combo-b');
      expect(TEXT.decode(extractBody(first))).toBe('first');
      expect(TEXT.decode(extractBody(second))).toBe('second');

      await client.closeAll();
    } finally {
      await server.closeAll();
    }
  });

  test('close fires the event, rejects receive and drops the peer afterwards', async () => {
    // UDP has no FIN: close here is app-driven, and the server
    // blacklists the peer so its later datagrams are dropped.
    const server = await startUdpServer();
    const client = await startUdpClient(server.actualPort);
    try {
      const cid = onlyConnection(client);
      await client.send(cid, callMessage('before-close', BINARY.encode('hi')));
      await waitForPeers(server, 1, 3000);
      const sid = server.connectionIds()[0]!;
      expect(await server.receive(sid, 3000)).toBeDefined();


      const closes: string[] = [];
      const onClose = (connectionId: string) => closes.push(connectionId);
      server.on('close', onClose);

      await server.close(sid);

      // UDP has no FIN, so this fires synchronously — the listener was
      // already attached when close() ran.
      expect(closes).toEqual(expect.arrayContaining([sid]));
      await expect(server.receive(sid, 300)).rejects.toThrow();

      // Late datagrams from the closed peer must not re-register it.
      await client.send(cid, callMessage('after-close', BINARY.encode('late')));
      await sleep(200);
      expect(server.connectionCount()).toBe(0);
      expect(await server.receiveAny(300)).toBeUndefined();
      server.off('close', onClose);
    } finally {
      await client.closeAll();
      await server.closeAll();
    }
  });
});
