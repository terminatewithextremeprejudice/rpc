#  RPC in Node.JS (very much still work in progress)

A simple Remote Procedure Call (RPC) implementation using Node.js, sockets and binary encoding - simple, direct and brutally basic.

## How to use 

```
// Simple RPC Client/Server Demo
// Shows basic request/response working via different transports

import { RpcClient, RpcServer, encodeCall, decode, RpcConstants } from './src';
import { TcpTransport, UnixSocketTransport } from './src/transport';

async function demoEchoServer(transport: any, name: string) {
  console.log(`\n=== ${name} Server Demo ===`);

  // Create server
  const server = new RpcServer({ transport });
  await server.start();
  console.log(`[Server] Started (${name})`);

  // Give server time to bind
  await new Promise(r => setTimeout(r, 100));

  // Create client with same transport
  const clientOptions: any = { transport: transport.constructor.name === 'TcpTransport' 
    ? new TcpTransport(transport.host, transport.port)
    : new UnixSocketTransport(transport.path) };
  const client = new RpcClient(100000, 1, clientOptions);
  await client.connect();
  console.log(`[Client] Connected`);

  // Make a call
  const result = await client.callWithString(1, `Hello from ${name}!`);
  console.log(`[Client] Received response, status: ${result.status}`);

  // Cleanup
  await client.disconnect();
  await server.stop();
  console.log(`[Server] Stopped`);
}

async function main() {
  try {
    // TCP Demo
    await demoEchoServer(new TcpTransport('127.0.0.1', 5050), 'TCP');
    
    // Unix Socket Demo
    const unixPath = '/tmp/demo-rpc-simple.sock';
    await demoEchoServer(new UnixSocketTransport(unixPath), 'Unix Socket');
    
    // Cleanup unix socket file
    const fs = await import('fs');
    try { fs.unlinkSync(unixPath); } catch (e) {}

    console.log('\n✅ All demos completed successfully!');
  } catch (error) {
    console.error('\n❌ Demo failed:', error);
    process.exit(1);
  }
}
```

## Why on earth would anyone use something like this when gRPC exists?

** Simplicity is sometimes a virtue worth paying for. **

- Lower overhead - no protobufs, multiplexing, streaming, auth and a million knobs. 
- Original RPC works over any transport layers (TCP, UDP, shared memory, Unix pipes etc.) unlike gRPC which is heavily paired with HTTP/2.
- Control & Customization - You can always modify a simple RPC library to your needs.
- Zero Dependencies - this library is literally just RPC runtime + XDR encoder. 
- No HTTP Assumptions - request/response framing, headers etc.


