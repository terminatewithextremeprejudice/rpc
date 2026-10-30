# RPC in Node.JS (very much still work in progress)

A simple Remote Procedure Call (RPC) implementation using Node.js, sockets and binary encoding - simple, direct and brutally basic.

[![CI](https://img.shields.io/github/actions/workflow/status/terminatewithextremeprejudice/rpc/ci.yml?label=CI)]()
[![Coverage](https://img.shields.io/badge/Coverage-71%25-green)]()


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

## API Reference

### Server
```typescript
const server = new RpcServer({ transport });

// Register a handler
server.register(procedure: number, version: number, handler: (args: Uint8Array) => unknown)

// Start server (false = don't auto-receive)
await server.start(waitForConnection: boolean)

// Receive next request from queue
const request = await server.receiveNext()

// Flush all buffered responses
await server.flushResponses()

// Stop server
await server.stop()

// Check if running
server.isRunning()

// Get stats
const stats = server.getStats()  // { pendingRequests, bufferedResponses }
```

### Client
```typescript
const client = new RpcClient(prognum: number, version: number, {
  transport,
  timeout?: number
});

// Or use convenience function
const client = await createTcpClient(prognum, vers, 'host', port)

// Connect
await client.connect()

// Fire-and-forget request (returns AsyncRpcHandle)
const handle = await client.fire(procedure: number, args: Uint8Array)

// Synchronous call (waits for reply)
const result = await client.call(procedure: number, args: Uint8Array)

// Get specific reply by msgId
const reply = await client.getReply(msgId: string)

// Collect all pending replies
const replies = await client.collectResponses(timeout: number)

// Disconnect
await client.disconnect()
```

## How to use 

``` typescript
  // Setup server
const serverTransport = new TcpTransport({
  host: '127.0.0.1',
  port: 5001,
  isServer: true
});
const server = new RpcServer({ transport: serverTransport });

// Register handlers
server.register(1, 2, (args: Uint8Array) => {
  console.log('[Handler 1]', new TextDecoder().decode(args));
  return args;
});

// Start server
await server.start(false);
console.log('Server started and listening\n');

// Setup client
const clientTransport = new TcpTransport({
  host: '127.0.0.1',
  port: 5001,
  isServer: false
});
const client = new RpcClient(100003, 2, { transport: clientTransport });
await client.connect();
console.log('Client connected\n');

// Fire requests (fire-and-forget)
const handle1 = await client.fire(1, new TextEncoder().encode('fast'));

// Server receives requests into queue
await server.receiveNext();

// Server processes all and flushes responses
await server.flushResponses();
console.log('Server flushed all responses\n');

// Client collects all pending responses
const responses = await client.collectResponses(2000);
console.log(`Client received ${responses.size} responses:`);

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


