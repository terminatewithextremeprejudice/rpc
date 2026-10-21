import { describe, expect, test } from '@jest/globals';
import { RpcClient, createTcpClient } from '../../src/client';
import { RpcServer, startRpcServer } from '../../src/server';
import { TcpTransport } from '../../src/transport';

// Helper to get a free port
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
    
    // Start server in server mode - pass false to skip waitForConnection
    const transport = new TcpTransport('127.0.0.1', port, true);
    server = new RpcServer({ transport });
    
    // Register test handlers
    server.register(1, 2, (args: Uint8Array) => {
      // Echo handler - just returns the args
      return args;
    });
    
    server.register(2, 2, (args: Uint8Array) => {
      // Echo handler with different proc
      return args;
    });
    
    await server.start(false); // Don't wait for connection
    
    // Create client - this will connect to server and resolve waitForConnection internally
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
    // Client sends request non-blocking
    const handle = await client.fire(1, new TextEncoder().encode('async1'));
    
    // Should get back an async handle immediately (not wait for server)
    expect((handle as any)._async).toBe(true);
    expect(handle.msgId).toBeDefined();
  });

  test('queued request is processed and response can be retrieved', async () => {
    // Client fires async request
    const handle = await client.fire(1, new TextEncoder().encode('queued-msg'));
    
    // Server queues the incoming request
    const queuedMsgId = await server.queueIncomingRequest();
    expect(queuedMsgId).toEqual(handle.msgId);
    
    // Server processes and flushes response
    await server.processAndFlush();
    
    // Client retrieves response using msgId
    const result = await client.getResponse(handle.msgId, 2000);
    
    expect(result.status).toBe(0); // RPC_ACCEPTED
    expect(result.msgId).toEqual(handle.msgId);
    
    // Should get back the original data (echo)
    // XDR strings may have null padding, so strip null bytes
    const decoded = new TextDecoder().decode(result.value!).replace(/\0/g, '');
    expect(decoded).toBe('queued-msg');
  });

  test('multiple fire-and-forget requests processed out of order', async () => {
    // Client fires 3 requests in sequence
    const h1 = await client.fire(1, new TextEncoder().encode('first'));
    const h2 = await client.fire(2, new TextEncoder().encode('second'));
    const h3 = await client.fire(1, new TextEncoder().encode('third'));
    
    // Server queues all 3
    await server.queueIncomingRequest();
    await server.queueIncomingRequest();
    await server.queueIncomingRequest();
    
    expect(server.getStats().pendingRequests).toBe(3);
    
    // Server processes in REVERSE order (h3 first, then h2, then h1)
    await server.processQueuedRequest(h3.msgId!);
    await server.sendResponse(h3.msgId!);
    
    await server.processQueuedRequest(h2.msgId!);
    await server.sendResponse(h2.msgId!);
    
    await server.processQueuedRequest(h1.msgId!);
    await server.sendResponse(h1.msgId!);
    
    expect(server.getStats().pendingRequests).toBe(0);
    expect(server.getStats().bufferedResponses).toBe(0);
    
    // Client collects all responses - they should be matched correctly by msgId
    const responses = await client.collectResponses(2000);
    
    expect(responses.size).toBe(3);
    
    // Verify each response is matched to correct request
    const r1 = responses.get(h1.msgId!);
    const r2 = responses.get(h2.msgId!);
    const r3 = responses.get(h3.msgId!);
    
    expect(r1?.status).toBe(0);
    expect(new TextDecoder().decode(r1?.value!).replace(/\0/g, '')).toBe('first');
    
    expect(r2?.status).toBe(0);
    expect(new TextDecoder().decode(r2?.value!).replace(/\0/g, '')).toBe('second');
    
    expect(r3?.status).toBe(0);
    expect(new TextDecoder().decode(r3?.value!).replace(/\0/g, '')).toBe('third');
  });

  test('collectResponses returns immediately for already-buffered responses', async () => {
    // Fire a request
    const handle = await client.fire(1, new TextEncoder().encode('buffered'));
    
    // Server queues and processes synchronously before client collects
    await server.queueIncomingRequest();
    await server.processAndFlush();
    
    // Client collect should return immediately (response already buffered)
    const responses = await client.collectResponses(100);
    
    expect(responses.size).toBe(1);
    expect(responses.get(handle.msgId)?.status).toBe(0);
  });

  test('getResponse waits for delayed server processing', async () => {
    // Client fires request
    const handle = await client.fire(1, new TextEncoder().encode('delayed'));
    
    // Small delay before server processes
    await new Promise(r => setTimeout(r, 100));
    
    // Server queues and processes
    await server.queueIncomingRequest();
    await server.processAndFlush();
    
    // Client getResponse will receive the response (arrived after initial fire)
    const result = await client.getResponse(handle.msgId, 2000);
    
    expect(result.status).toBe(0);
    expect(new TextDecoder().decode(result.value!).replace(/\0/g, '')).toBe('delayed');
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
      await server.queueIncomingRequest();
    }
    
    // Burst process all at once
    const { processed, sent } = await server.processAndFlush();
    expect(processed).toBe(5);
    expect(sent).toBe(5);
    
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

  test('timeout when server does not respond', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('timeout-test'));
    
    // Server queues but NEVER processes
    await server.queueIncomingRequest();
    
    // Client tries to get response but should timeout
    await expect(client.getResponse(handle.msgId, 500))
      .rejects
      .toThrow(/timeout/i);
  });

});

describe('Non-blocking TCP with concurrent clients', () => {
  let server: RpcServer;
  let port: number;

  beforeEach(async () => {
    port = getPort();
    const transport = new TcpTransport('127.0.0.1', port, true);
    server = new RpcServer({ transport });
    server.register(1, 2, (args: Uint8Array) => args);
    await server.start(false); // Don't wait for client connection
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
    const h1 = await client1.fire(1, new TextEncoder().encode('client1-msg'));
    
    // Client2 fires second (interleaved in server's receive buffer)
    const h2 = await client2.fire(1, new TextEncoder().encode('client2-msg'));
    
    // Server queues both requests
    await server.queueIncomingRequest();
    await server.queueIncomingRequest();
    
    // Server processes both and flushes responses
    await server.processAndFlush();
    
    // Each client collects its own response
    const r1 = await client1.getResponse(h1.msgId, 2000);
    const r2 = await client2.getResponse(h2.msgId, 2000);
    
    expect(r1.status).toBe(0);
    expect(r2.status).toBe(0);
    
    // Responses are correctly matched to their respective clients
    expect(new TextDecoder().decode(r1.value!).replace(/\0/g, '')).toBe('client1-msg');
    expect(new TextDecoder().decode(r2.value!).replace(/\0/g, '')).toBe('client2-msg');
    
    // Cleanup
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
    
    // Server queues all 4 requests
    await server.queueIncomingRequest();
    await server.queueIncomingRequest();
    await server.queueIncomingRequest();
    await server.queueIncomingRequest();
    
    expect(server.getStats().pendingRequests).toBe(4);
    
    // Process in mixed order
    await server.processAndFlush();
    
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
    expect(new TextDecoder().decode(c1_r1?.value!).replace(/\0/g, '')).toBe('C1-req1');
    expect(c1_r2?.status).toBe(0);
    expect(new TextDecoder().decode(c1_r2?.value!).replace(/\0/g, '')).toBe('C1-req2');
    expect(c2_r1?.status).toBe(0);
    expect(new TextDecoder().decode(c2_r1?.value!).replace(/\0/g, '')).toBe('C2-req1');
    expect(c2_r2?.status).toBe(0);
    expect(new TextDecoder().decode(c2_r2?.value!).replace(/\0/g, '')).toBe('C2-req2');
    
    // Verify correct matching - client1 doesn't get client2's responses
    expect(c1_responses.get(c2_h1.msgId!)).toBeUndefined();
    expect(c1_responses.get(c2_h2.msgId!)).toBeUndefined();
    expect(c2_responses.get(c1_h1.msgId!)).toBeUndefined();
    expect(c2_responses.get(c1_h2.msgId!)).toBeUndefined();
    
    // Cleanup
    await client1.disconnect();
    await client2.disconnect();
    await server.stop();
  });
});

