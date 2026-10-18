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
} from './shared/xdr';

export { 
  Transport,
  TcpTransport,
  HttpTransport,
  UnixSocketTransport,
  UdpTransport,
  SharedMemoryTransport,
  TransportFactory
} from './transport';

export { 
  RpcClient,
  RpcCallResult,
  createTcpClient,
  createUnixClient,
  createHttpClient
} from './client';

export { 
  RpcServer,
  RpcHandler,
  RpcServerOptions,
  startRpcServer,
  startUnixRpcServer
} from './server';