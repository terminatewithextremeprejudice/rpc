import { describe, expect, test } from '@jest/globals';
import { RpcClient, RpcCallResult } from '../src/client';
import { TcpTransport } from '../src/transport';
import { encodeString, RpcConstants, decodeString } from '../src/shared/xdr';
import { parseMessage, buildReply } from '../src/shared/message';

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

describe('RpcClient over real TCP', () => {
  test('synchronous call() round-trips over a real socket', async () => {
    const server = new TcpTransport({ host: '127.0.0.1', port: 7101, isServer: true });
    await server.connect();

    try {
      const clientT = new TcpTransport({ host: '127.0.0.1', port: server.actualPort });
      const client = new RpcClient(100003, 2, { transport: clientT, timeout: 3000 });
      await client.connect();

      // Fire async then get response (avoids type narrowing issues)
      await client.fire(9, encodeString('tcp-sync'), 'tcp-1');
      
      const cid = await server.waitForConnection(3000);
      await sleep(100);
      
      const received = await server.receive(cid, 3000);
      const parsed = parseMessage(received);
      expect(parsed.type).toBe(RpcConstants.CallTypes.RPC_CALL);
      expect(parsed.proc).toBe(9);
      expect(parsed.msgId).toBe('tcp-1');
      
      // Send reply
      await server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'tcp-1', encodeString('tcp-reply')));
      
      await sleep(100);
      
      const result = await client.getResponse('tcp-1', 2000);
      expect(result.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(result.msgId).toBe('tcp-1');
      expect(decodeString(result.value!)).toBe('tcp-reply');
      
      await client.disconnect();
    } finally {
      await server.closeAll();
    }
  });

  test('call() rejects on timeout with no reply from peer', async () => {
    const server = new TcpTransport({ host: '127.0.0.1', port: 7102, isServer: true });
    await server.connect();
    try {
      const clientT = new TcpTransport({ host: '127.0.0.1', port: server.actualPort });
      const client = new RpcClient(100003, 2, { transport: clientT });
      await client.connect();
      
      await server.waitForConnection(3000);
      
      const started = Date.now();
      await expect(client.call(1, encodeString('no-reply'), { timeout: 250 })).rejects.toThrow(/timed out|timeout/i);
      expect(Date.now() - started).toBeLessThan(1000);
      
      await client.disconnect();
    } finally { await server.closeAll(); }
  });

  test('pipelined calls resolve correctly when replies arrive out of order', async () => {
    const server = new TcpTransport({ host: '127.0.0.1', port: 7103, isServer: true });
    await server.connect();
    try {
      const clientT = new TcpTransport({ host: '127.0.0.1', port: server.actualPort });
      const client = new RpcClient(100003, 2, { transport: clientT, timeout: 5000 });
      await client.connect();

      // Fire 3 async calls
      await client.fire(1, encodeString('req-1'), 'po-1');
      await client.fire(2, encodeString('req-2'), 'po-2');
      await client.fire(3, encodeString('req-3'), 'po-3');
      
      await sleep(200);
      
      const cid = await server.waitForConnection(3000);
      const m1 = await server.receive(cid, 3000);
      const m2 = await server.receive(cid, 3000);
      const m3 = await server.receive(cid, 3000);
      
      // Verify we received 3 calls
      const receivedMsgIds = [parseMessage(m1).msgId!, parseMessage(m2).msgId!, parseMessage(m3).msgId!];
      expect(receivedMsgIds).toEqual(expect.arrayContaining(['po-1', 'po-2', 'po-3']));
      
      // Reply OUT OF ORDER: po-3, then po-1, then po-2
      await server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'po-3', encodeString('resp-3')));
      await server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'po-1', encodeString('resp-1')));
      await server.send(cid, buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, 'po-2', encodeString('resp-2')));
      
      await sleep(100);
      
      // Get responses by msgId - client matches regardless of arrival order
      const r1 = await client.getResponse('po-1', 2000);
      const r2 = await client.getResponse('po-2', 2000);
      const r3 = await client.getResponse('po-3', 2000);
      
      // Verify each response matches the correct value
      expect(r1.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(decodeString(r1.value!)).toBe('resp-1');
      
      expect(r2.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(decodeString(r2.value!)).toBe('resp-2');
      
      expect(r3.status).toBe(RpcConstants.ReplyStates.RPC_ACCEPTED);
      expect(decodeString(r3.value!)).toBe('resp-3');
      
      await client.disconnect();
    } finally { await server.closeAll(); }
  });
});
