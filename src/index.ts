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
  UdpTransport,
  Connection,
  LengthPrefixedFramer,
  XdrFramer,
  type MessageFramer,
  type TransportEventMap,
  type TcpTransportOptions,
  type UdpTransportOptions,
} from './transport.ts';

// Client and server compile cleanly against the generic Transport API
// (previously disabled pending that rewrite — now done).
export {
  RpcClient,
  createTcpClient,
  createUdpClient,
  type RpcCallResult,
} from './client.ts';

export {
  RpcServer,
  startRpcServer,
  type RpcHandler,
  type RpcServerOptions,
} from './server.ts';
