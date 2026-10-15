#  RPC in Node.JS

A simple Remote Procedure Call (RPC) implementation using Node.js, sockets and binary encoding - simple, direct and brutally basic.

## Why on earth would anyone use something like this when gRPC exists?

** Simplicity is sometimes a virtue worth paying for. **

- Lower overhead - no protobufs, multiplexing, streaming, auth and a million knobs. 
- Original RPC works over any transport layers (TCP, UDP, shared memory, Unix pipes etc.) unlike gRPC which is heavily paired with HTTP/2.
- Control & Customization - You can always modify a simple RPC library to your needs.
- Zero Dependencies - this library is literally just RPC runtime + XDR encoder. 
- No HTTP Assumptions - request/response framing, headers etc.


