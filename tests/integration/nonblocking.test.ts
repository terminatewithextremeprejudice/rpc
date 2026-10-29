import { describe, expect, test } from '@jest/globals';
import { RpcClient, createTcpClient } from '../../src/client';
import { RpcServer } from '../../src/server';
import { TcpTransport } from '../../src/transport';

let nextPort = 5500;
function getPort(): number {
  return nextPort++;
}

describe('Non-blocking TCP RPC Integration', () => {
  let server: RpcServer;
  let client: RpcClient;
  let port: number;

  beforeEach(async () => {
    port = getPort();
    const transport = new TcpTransport({host: '127.0.0.1', port: port, isServer: true});
    server = new RpcServer({ transport });
    
    server.register(1, 2, (args: Uint8Array) => args);
    server.register(2, 2, (args: Uint8Array) => args);
    
    await server.start(false);
    client = await createTcpClient(100003, 2, '127.0.0.1', port);
  });

  afterEach(async () => {
    if (client && client.isConnected()) {
      await client.disconnect();
    }
    if (server && server.isRunning()) {
      await server.stop();
    }
  });

  test('fire-and-forget returns async handle immediately', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('async1'));
    expect((handle as any)._async).toBe(true);
    expect(handle.msgId).toBeDefined();
  });

  test('receiveNext returns request with connectionId', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('test-msg'));
    
    // Server receives next request - returns connectionId automatically
    const req = await server.receiveNext();
    expect(req).toBeDefined();
    expect(req!.msgId).toEqual(handle.msgId);
    expect(req!.connectionId).toBeDefined();
    expect(req!.proc).toBe(1);
    expect(req!.args).toEqual(new TextEncoder().encode('test-msg'));
  });

  test('queued request is processed and response can be retrieved', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('queued-msg'));
    
    // Server receives next request (auto-queues with correct connectionId)
    const req = await server.receiveNext();
    expect(req?.msgId).toEqual(handle.msgId);
    
    // Server flushes all pending requests (sends responses to correct connections)
    await server.flushResponses();
    
    const result = await client.getResponse(handle.msgId, 2000);
    expect(result.status).toBe(0);
    expect(new TextDecoder().decode(result.value!).replace(/\0/g, '')).toBe('queued-msg');
  });

  test('multiple fire-and-forget requests processed out of order', async () => {
    const h1 = await client.fire(1, new TextEncoder().encode('first'));
    const h2 = await client.fire(2, new TextEncoder().encode('second'));
    const h3 = await client.fire(1, new TextEncoder().encode('third'));
    
    // Server receives all 3 (auto-queues with connectionId)
    await server.receiveNext();
    await server.receiveNext();
    await server.receiveNext();
    
    expect(server.getStats().pendingRequests).toBe(3);
    
    // Server flushes all - responses go to correct connection automatically
    await server.flushResponses();
    expect(server.getStats().bufferedResponses).toBe(0);
    
    const responses = await client.collectResponses(2000);
    expect(responses.size).toBe(3);
    
    const r1 = responses.get(h1.msgId!);
    const r2 = responses.get(h2.msgId!);
    const r3 = responses.get(h3.msgId!);
    
    expect(new TextDecoder().decode(r1!.value!).replace(/\0/g, '')).toBe('first');
    expect(new TextDecoder().decode(r2!.value!).replace(/\0/g, '')).toBe('second');
    expect(new TextDecoder().decode(r3!.value!).replace(/\0/g, '')).toBe('third');
  });

  test('pipeline: multiple requests with burst processing', async () => {
    const requests: Array<{ msgId: string; data: string }> = [];
    
    // Fire 5 requests rapidly
    for (let i = 0; i < 5; i++) {
      const h = await client.fire(1, new TextEncoder().encode(`burst-${i}`));
      requests.push({ msgId: h.msgId!, data: `burst-${i}` });
    }
    
    // Server receives and queues all
    for (let i = 0; i < 5; i++) {
      await server.receiveNext();
    }
    
    expect(server.getStats().pendingRequests).toBe(5);
    
    // Burst process all at once
    await server.flushResponses();
    expect(server.getStats().bufferedResponses).toBe(0);
    
    // Client collects all 5 responses
    const responses = await client.collectResponses(1000);
    expect(responses.size).toBe(5);
    
    // Verify each one
    for (const req of requests) {
      const resp = responses.get(req.msgId);
      expect(resp?.status).toBe(0);
      expect(new TextDecoder().decode(resp?.value!).replace(/\0/g, '')).toBe(req.data);
    }
  });
  });

describe('Non-blocking TCP with concurrent clients', () => {
  let server: RpcServer;
  let port: number;

  beforeEach(async () => {
    port = getPort();
    const transport = new TcpTransport({host: '127.0.0.1', port: port, isServer: true});
    server = new RpcServer({ transport });
    server.register(1, 2, (args: Uint8Array) => args);
    await server.start(false);
  });

  afterEach(async () => {
    if (server && server.isRunning()) {
      await server.stop();
    }
  });

  test('server handles multiple clients with interleaved requests', async () => {
    const client1 = await createTcpClient(100003, 2, '127.0.0.1', port);
    const client2 = await createTcpClient(100003, 2, '127.0.0.1', port);
    
    // Client1 fires first
    const c1_msg1 = await client1.fire(1, new TextEncoder().encode('client1-msg'));
    // Client2 fires second (interleaved in server's receive buffer)
    const c2_msg1 = await client2.fire(1, new TextEncoder().encode('client2-msg'));
    // Server receives requests using receiveNext()
    const req1 = await server.receiveNext();
    const req2 = await server.receiveNext();

    // Verify we received 2 requests from different connections
    expect(req1).toBeDefined();
    expect(req2).toBeDefined();
    expect(req1!.connectionId).not.toBe(req2!.connectionId);
    
    // Server flushes all - each response goes to correct connection
    await server.flushResponses();
    
    // Each client collects its own response
    const c1_result = await client1.getResponse(c1_msg1.msgId!, 2000);
    const c2_result = await client2.getResponse(c2_msg1.msgId!, 2000);
    
    expect(c1_result.status).toBe(0);
    expect(c2_result.status).toBe(0);
    
    // Responses are correctly matched to their respective clients
    expect(new TextDecoder().decode(c1_result.value!).replace(/\0/g, '')).toBe('client1-msg');
    expect(new TextDecoder().decode(c2_result.value!).replace(/\0/g, '')).toBe('client2-msg');
    
    await client1.disconnect();
    await client2.disconnect();
  });

	  test('multi-client pipelined requests processed out of order', async () => {
    const client1 = await createTcpClient(100003, 2, '127.0.0.1', port);
    const client2 = await createTcpClient(100003, 2, '127.0.0.1', port);
    
    // Both clients fire 2 requests each
    const c1_h1 = await client1.fire(1, new TextEncoder().encode('C1-req1'));
    const c1_h2 = await client1.fire(1, new TextEncoder().encode('C1-req2'));
    const c2_h1 = await client2.fire(1, new TextEncoder().encode('C2-req1'));
    const c2_h2 = await client2.fire(1, new TextEncoder().encode('C2-req2'));
    
    // Server receives all 4 requests via receiveNext
    const allReqs: any[] = [];
    for (let i = 0; i < 4; i++) {
      const req = await server.receiveNext();
      if (req) allReqs.push(req);
    }
    
    expect(server.getStats().pendingRequests).toBe(4);
    
    // Process and flush - each response automatically routed to correct connection
    await server.flushResponses();
    
    // Each client collects their own responses
    const c1_responses = await client1.collectResponses(2000);
    const c2_responses = await client2.collectResponses(2000);
    
    expect(c1_responses.size).toBe(2);
    expect(c2_responses.size).toBe(2);
    
    // Verify each client got their own responses
    const c1_r1 = c1_responses.get(c1_h1.msgId!);
    const c1_r2 = c1_responses.get(c1_h2.msgId!);
    const c2_r1 = c2_responses.get(c2_h1.msgId!);
    const c2_r2 = c2_responses.get(c2_h2.msgId!);
    
    expect(c1_r1?.status).toBe(0);
    expect(new TextDecoder().decode(c1_r1!.value!).replace(/\0/g, '')).toBe('C1-req1');
    expect(c1_r2?.status).toBe(0);
    expect(new TextDecoder().decode(c1_r2!.value!).replace(/\0/g, '')).toBe('C1-req2');
    expect(c2_r1?.status).toBe(0);
    expect(new TextDecoder().decode(c2_r1!.value!).replace(/\0/g, '')).toBe('C2-req1');
    expect(c2_r2?.status).toBe(0);
    expect(new TextDecoder().decode(c2_r2!.value!).replace(/\0/g, '')).toBe('C2-req2');
    
    // Verify correct matching - client1 doesn't get client2's responses
    expect(c1_responses.get(c2_h1.msgId!)).toBeUndefined();
    expect(c1_responses.get(c2_h2.msgId!)).toBeUndefined();
    expect(c2_responses.get(c1_h1.msgId!)).toBeUndefined();
    expect(c2_responses.get(c1_h2.msgId!)).toBeUndefined();
    
    await client1.disconnect();
    await client2.disconnect();
  });
});

