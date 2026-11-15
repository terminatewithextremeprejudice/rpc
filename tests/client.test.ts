import { describe, expect, test } from '@jest/globals';
import { RpcClient, createTcpClient, createUdpClient, createWsClient } from '../src/client';
import { TcpTransport, Transport, UdpTransport, WsTransport } from '../src/transport';
import { encodeString, RpcConstants, decodeString } from '../src/shared/xdr';
import { parseMessage, buildReply } from '../src/shared/message';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

type Protocol = 'tcp' | 'udp' | 'ws';

interface Pair {
  server: Transport;
  client: RpcClient;
  // Resolve the server-side connection id for the client's peer.
  // TCP/WS register it at handshake time; UDP only when the first datagram
  // lands (waitForConnection resolves immediately once one exists, so it is
  // safe for all three — call it after at least one message is in flight).
  serverCid: () => Promise<string>;
}

async function makePair(protocol: Protocol): Promise<Pair> {
  const common = { host: '127.0.0.1' };
  const server = protocol === 'tcp'
    ? new TcpTransport({ ...common, port: 0, isServer: true })
    : protocol === 'udp'
      ? new UdpTransport({ ...common, port: 0, isServer: true })
      : new WsTransport({ ...common, port: 0, isServer: true });
  await server.connect();

  // Default timeout is 5000ms; tests that need a shorter one pass it
  // explicitly to call()/getResponse().
  const client = protocol === 'tcp'
    ? await createTcpClient(100003, 2, '127.0.0.1', server.actualPort)
    : protocol === 'udp'
      ? await createUdpClient(100003, 2, '127.0.0.1', server.actualPort)
      : await createWsClient(100003, 2, '127.0.0.1', server.actualPort);

  return { server, client, serverCid: () => server.waitForConnection(3000) };
}

// The same three RpcClient scenarios run over every supported transport —
// the client is transport-agnostic, so per-transport drift would be a bug.
describe.each(['tcp', 'udp', 'ws'] as const)('RpcClient over real %s', (protocol) => {
  test('synchronous call() round-trips over a real socket', async () => {
    const pair = await makePair(protocol);
    try {
      // Fire async then get response (avoids type narrowing issues)
      await pair.client.fire(9, encodeString('sync'), 'rt-1');

      const cid = await pair.serverCid();
      await sleep(100);

      const received = await pair.server.receive(cid, 3000);
      const parsed = parseMessage(received);
      expect(parsed.type).toBe(RpcConstants.CallTypes.RPC_CALL);
      expect(parsed.proc).toBe(9);
      expect(parsed.msgId).toBe('rt-1');

      // Send reply
      await pair.server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'rt-1', encodeString('reply')));

      await sleep(100);

      const result = await pair.client.getResponse('rt-1', 2000);
      expect(result.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(result.msgId).toBe('rt-1');
      expect(decodeString(result.value!)).toBe('reply');
    } finally {
      await pair.client.disconnect();
      await pair.server.closeAll();
    }
  });

  test('call() rejects on timeout with no reply from peer', async () => {
    const pair = await makePair(protocol);
    try {
      const started = Date.now();
      await expect(pair.client.call(1, encodeString('no-reply'), { timeout: 250 }))
        .rejects.toThrow(/timed out|timeout/i);
      expect(Date.now() - started).toBeLessThan(1000);
    } finally {
      await pair.client.disconnect();
      await pair.server.closeAll();
    }
  });

  test('pipelined calls resolve correctly when replies arrive out of order', async () => {
    const pair = await makePair(protocol);
    try {
      // Fire 3 async calls
      await pair.client.fire(1, encodeString('req-1'), 'po-1');
      await pair.client.fire(2, encodeString('req-2'), 'po-2');
      await pair.client.fire(3, encodeString('req-3'), 'po-3');

      await sleep(200);

      const cid = await pair.serverCid();
      const m1 = await pair.server.receive(cid, 3000);
      const m2 = await pair.server.receive(cid, 3000);
      const m3 = await pair.server.receive(cid, 3000);

      // Verify we received 3 calls
      const receivedMsgIds = [parseMessage(m1).msgId!, parseMessage(m2).msgId!, parseMessage(m3).msgId!];
      expect(receivedMsgIds).toEqual(expect.arrayContaining(['po-1', 'po-2', 'po-3']));

      // Reply OUT OF ORDER: po-3, then po-1, then po-2
      await pair.server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'po-3', encodeString('resp-3')));
      await pair.server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'po-1', encodeString('resp-1')));
      await pair.server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'po-2', encodeString('resp-2')));

      await sleep(100);

      // Get responses by msgId - client matches regardless of arrival order
      const r1 = await pair.client.getResponse('po-1', 2000);
      const r2 = await pair.client.getResponse('po-2', 2000);
      const r3 = await pair.client.getResponse('po-3', 2000);

      // Verify each response matches the correct value
      expect(r1.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(decodeString(r1.value!)).toBe('resp-1');

      expect(r2.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(decodeString(r2.value!)).toBe('resp-2');

      expect(r3.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(decodeString(r3.value!)).toBe('resp-3');
    } finally {
      await pair.client.disconnect();
      await pair.server.closeAll();
    }
  });
});
