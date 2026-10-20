#  RPC in Node.JS (very much still work in progress)

A simple Remote Procedure Call (RPC) implementation using Node.js, sockets and binary encoding - simple, direct and brutally basic.

## How Non-Blocking Works:

Client fires: Request A → Request B → Request C
              (msgId: a)   (msgId: b)   (msgId: c)

Server queues them in any order, processes independently:
  Process B first → buffer response B
  Process A second → buffer response A  
  Process C third → buffer response C

Server flushes responses: Response B → Response C → Response A

Client receives: All responses arrive with msgId tags
Client matches: Each response.msgId → original request handle

## How to use 

``` typescript
  console.log('=== Non-Blocking RPC Demo ===\n');

  // Setup server
  const serverTransport = new TcpTransport('127.0.0.1', 5001, true);
  const server = new RpcServer({ transport: serverTransport });
  
  // Register handlers with varying delays
  server.register(1, 2, async (args: any) => {
    await new Promise(r => setTimeout(r, 300)); // Fast
    console.log('[Handler 1] Processed');
    return args;
  });
  
  server.register(2, 2, async (args: any) => {
    await new Promise(r => setTimeout(r, 600)); // Slow
    console.log('[Handler 2] Processed');
    return args;
  });
  
  server.running = true;
  await serverTransport.connect();
  console.log('Server started and listening\n');

  // Client will connect and trigger server ready
  const clientTransport = new TcpTransport('127.0.0.1', 5001, false);
  const client = new RpcClient(100003, 2, { transport: clientTransport });
  await client.connect();
  console.log('Client connected\n');

  // === TEST: Process in reverse order ===
  console.log('Step 1: Client fires 2 requests in order');
  
  const h1 = await client.fire(1, new TextEncoder().encode('fast'));   // proc=1, fast
  await new Promise(r => setTimeout(r, 1100)); // Wait for server to receive first
  
  const h2 = await client.fire(2, new TextEncoder().encode('slow'));   // proc=2, slow
  await new Promise(r => setTimeout(r, 1100)); // Wait for server to receive second
  
  console.log(`  Request A: msgId=${h1.msgId} (proc=1, fast)`);
  console.log(`  Request B: msgId=${h2.msgId} (proc=2, slow)`);
  console.log(`  Expected: B should finish first due to shorter delay\n`);

  console.log('Step 2: Server receives and queues both requests');
  await server.queueIncomingRequest();
  await server.queueIncomingRequest();
  const stats = server.getStats();
  console.log(`  Server has ${stats.pendingRequests} pending requests\n`);

  console.log('Step 3: Server processes requests in REVERSE order (B then A)');
  // Process B (slow proc=2) first
  await server.processQueuedRequest(h2.msgId);
  await server.sendResponse(h2.msgId);
  console.log(`  Processed request ${h2.msgId} (proc=2, slow)`);
  
  // Process A (fast proc=1) second
  await server.processQueuedRequest(h1.msgId);
  await server.sendResponse(h1.msgId);
  console.log(`  Processed request ${h1.msgId} (proc=1, fast)\n`);

  const stats2 = server.getStats();
  console.log(`  Server now has ${stats2.pendingRequests} pending, ${stats2.bufferedResponses} buffered\n`);

  console.log('Step 4: Client collects responses');
  const responses = await client.collectResponses(2000);
  console.log(`  Client received ${responses.size} responses:`);
  
  for (const [msgId, result] of responses) {
    const decoded = new TextDecoder().decode(result.value || new Uint8Array(0));
    console.log(`    msgId=${msgId}: "${decoded}" ✓`);
  }
  
  // Verify correctness
  console.log('\nVerification:');
  const respForH1 = responses.get(h1.msgId);
  const respForH2 = responses.get(h2.msgId);
  
  if (respForH1 && respForH1.status === 0) {
    console.log(`  ✓ Response for '${h1.msgId}' arrived correctly`);
  }
  if (respForH2 && respForH2.status === 0) {
    console.log(`  ✓ Response for '${h2.msgId}' arrived correctly`);
  }

  console.log('\n=== SUCCESS ===');
  console.log('Key insight:');
  console.log('- Server processed request B BEFORE request A (reverse order)');
  console.log('- Both responses were sent back with their msgId tags');
  console.log('- Client received both and matched them correctly to original requests');
  console.log('- Non-blocking RPC with out-of-order processing works!');

  await client.disconnect();
  await server.stop();

```

## Why on earth would anyone use something like this when gRPC exists?

** Simplicity is sometimes a virtue worth paying for. **

- Lower overhead - no protobufs, multiplexing, streaming, auth and a million knobs. 
- Original RPC works over any transport layers (TCP, UDP, shared memory, Unix pipes etc.) unlike gRPC which is heavily paired with HTTP/2.
- Control & Customization - You can always modify a simple RPC library to your needs.
- Zero Dependencies - this library is literally just RPC runtime + XDR encoder. 
- No HTTP Assumptions - request/response framing, headers etc.


