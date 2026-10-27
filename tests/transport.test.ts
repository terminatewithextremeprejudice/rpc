import { describe, expect, test } from '@jest/globals';
import net from 'net';
import { TcpTransport } from '../src/transport';
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

function onlyConnection(t: TcpTransport): string {
  const ids = t.connectionIds();
  if (ids.length !== 1) throw new Error(`expected exactly one connection, got ${ids.length}`);
  return ids[0]!;
}

function rawSocket(t: TcpTransport, connectionId: string): net.Socket {
  return t.connection(connectionId)!.channel as net.Socket;
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
