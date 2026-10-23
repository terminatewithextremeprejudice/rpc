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
  let serverConnId: string | undefined;

  beforeEach(async () => {
    port = getPort();
    const transport = new TcpTransport('127.0.0.1', port, true);
    server = new RpcServer({ transport });
    
    server.register(1, 2, (args: Uint8Array) => args);
    server.register(2, 2, (args: Uint8Array) => args);
    
    await server.start(false);
    
    client = await createTcpClient(100003, 2, '127.0.0.1', port);
    serverConnId = client.getConnectionId();
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

  test('queued request is processed and response can be retrieved', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('queued-msg'));
    
    const queuedMsgId = await server.queueIncomingRequest(serverConnId!);
    expect(queuedMsgId).toEqual(handle.msgId);
    
    await server.processAndFlush();
    
    const result = await client.getResponse(handle.msgId, 2000);
    expect(result.status).toBe(0);
    expect(new TextDecoder().decode(result.value!).replace(/\0/g, '')).toBe('queued-msg');
  });

  test('multiple fire-and-forget requests processed out of order', async () => {
    const h1 = await client.fire(1, new TextEncoder().encode('first'));
    const h2 = await client.fire(2, new TextEncoder().encode('second'));
    const h3 = await client.fire(1, new TextEncoder().encode('third'));
    
    await server.queueIncomingRequest(serverConnId!);
    await server.queueIncomingRequest(serverConnId!);
    await server.queueIncomingRequest(serverConnId!);
    
    expect(server.getStats().pendingRequests).toBe(3);
    
    await server.processQueuedRequest(h3.msgId!);
    await server.processQueuedRequest(h2.msgId!);
    await server.processQueuedRequest(h1.msgId!);
    
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

  test('collectResponses returns immediately for buffered responses', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('buffered'));
    
    await server.queueIncomingRequest(serverConnId!);
    await server.processAndFlush();
    
    const responses = await client.collectResponses(100);
    expect(responses.size).toBe(1);
    expect(responses.get(handle.msgId)?.status).toBe(0);
  });

  test('timeout when server does not respond', async () => {
    const handle = await client.fire(1, new TextEncoder().encode('timeout-test'));
    await server.queueIncomingRequest(serverConnId!);
    
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
    
    const connId1 = client1.getConnectionId();
    const connId2 = client2.getConnectionId();

    const h1 = await client1.fire(1, new TextEncoder().encode('client1-msg'));
    const h2 = await client2.fire(1, new TextEncoder().encode('client2-msg'));
    
    await server.queueIncomingRequest(connId1);
    await server.queueIncomingRequest(connId2);
    
    await server.processAndFlush();
    
    const r1 = await client1.getResponse(h1.msgId!, 2000);
    const r2 = await client2.getResponse(h2.msgId!, 2000);
    
    expect(r1.status).toBe(0);
    expect(r2.status).toBe(0);
    
    expect(new TextDecoder().decode(r1.value!).replace(/\0/g, '')).toBe('client1-msg');
    expect(new TextDecoder().decode(r2.value!).replace(/\0/g, '')).toBe('client2-msg');
    
    await client1.disconnect();
    await client2.disconnect();
  });

  test('multi-client pipelined requests processed out of order', async () => {
    const client1 = await createTcpClient(100003, 2, '127.0.0.1', port);
    const client2 = await createTcpClient(100003, 2, '127.0.0.1', port);
    
    const connId1 = client1.getConnectionId();
    const connId2 = client2.getConnectionId();
    
    const c1_h1 = await client1.fire(1, new TextEncoder().encode('C1-req1'));
    const c1_h2 = await client1.fire(1, new TextEncoder().encode('C1-req2'));
    const c2_h1 = await client2.fire(1, new TextEncoder().encode('C2-req1'));
    const c2_h2 = await client2.fire(1, new TextEncoder().encode('C2-req2'));
    
    await server.queueIncomingRequest(connId1);
    await server.queueIncomingRequest(connId1);
    await server.queueIncomingRequest(connId2);
    await server.queueIncomingRequest(connId2);
    
    expect(server.getStats().pendingRequests).toBe(4);
    
    await server.processAndFlush();
    
    const c1_responses = await client1.collectResponses(2000);
    const c2_responses = await client2.collectResponses(2000);
    
    expect(c1_responses.size).toBe(2);
    expect(c2_responses.size).toBe(2);
    
    expect(new TextDecoder().decode(c1_responses.get(c1_h1.msgId!)!.value!).replace(/\0/g, '')).toBe('C1-req1');
    expect(new TextDecoder().decode(c1_responses.get(c1_h2.msgId!)!.value!).replace(/\0/g, '')).toBe('C1-req2');
    expect(new TextDecoder().decode(c2_responses.get(c2_h1.msgId!)!.value!).replace(/\0/g, '')).toBe('C2-req1');
    expect(new TextDecoder().decode(c2_responses.get(c2_h2.msgId!)!.value!).replace(/\0/g, '')).toBe('C2-req2');
    
    expect(c1_responses.get(c2_h1.msgId!)).toBeUndefined();
    expect(c1_responses.get(c2_h2.msgId!)).toBeUndefined();
    expect(c2_responses.get(c1_h1.msgId!)).toBeUndefined();
    expect(c2_responses.get(c1_h2.msgId!)).toBeUndefined();
    
    await client1.disconnect();
    await client2.disconnect();
  });
});
