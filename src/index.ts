// Main exports for the RPC library
export { 
  encode, 
  decode, 
  encodeCall, 
  decodeCall,
  encodeString,
  decodeString,
  encodeOpaque,
  decodeOpaque,
  encodeUint8,
  decodeUint8,
  encodeUint16,
  decodeUint16,
  encodeUint32,
  decodeUint32,
  encodeUint64,
  decodeUint64,
  encodeHyper,
  decodeHyper,
  RpcConstants,
  type XDRHeader
} from './shared/xdr.ts';

export {
  Transport,
  TcpTransport,
  Connection,
  LengthPrefixedFramer,
  XdrFramer,
  type MessageFramer,
  type TransportEventMap,
  type TcpTransportOptions,
} from './transport.ts';

// Client and server are slated for a rewrite on top of the new transport
// API (they reference the old positional TcpTransport constructor and
// helper methods that no longer exist). Re-enable these exports once they
// compile against the new interface.
//
// export {
//   RpcClient,
//   createTcpClient
// } from './client.ts';
//
// export type {
//   RpcCallResult
// } from './client.ts';
//
// export {
//   RpcServer,
//   startRpcServer
// } from './server.ts';
//
// export type {
//   RpcHandler,
//   RpcServerOptions
// } from './server.ts';
