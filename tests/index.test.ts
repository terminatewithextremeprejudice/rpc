// Public API surface: importing the barrel must expose every documented export.
import { describe, expect, test } from '@jest/globals';
import * as rpc from '../src/index';

const REQUIRED_EXPORTS = [
  // xdr
  'encode', 'decode', 'encodeCall', 'decodeCall',
  'encodeString', 'decodeString', 'encodeOpaque', 'decodeOpaque',
  'encodeUint8', 'decodeUint8', 'encodeUint16', 'decodeUint16',
  'encodeUint32', 'decodeUint32', 'encodeUint64', 'decodeUint64',
  'encodeHyper', 'decodeHyper', 'RpcConstants',
  // transport
  'TcpTransport', 'UdpTransport', 'Connection',
  'LengthPrefixedFramer', 'XdrFramer',
  // client / server
  'RpcClient', 'createTcpClient', 'createUdpClient',
  'RpcServer', 'startRpcServer',
];

describe('index barrel', () => {
  test('exports the full public API', () => {
    const missing = REQUIRED_EXPORTS.filter((name) => (rpc as any)[name] === undefined);
    expect(missing).toEqual([]);
  });

  test('exports usable constructs', () => {
    expect(typeof rpc.RpcClient).toBe('function');
    expect(typeof rpc.RpcServer).toBe('function');
    expect(typeof rpc.TcpTransport).toBe('function');
    expect(typeof rpc.UdpTransport).toBe('function');
    expect(typeof rpc.Connection).toBe('function');
    expect(typeof rpc.LengthPrefixedFramer).toBe('function');
    expect(typeof rpc.XdrFramer).toBe('function');
    expect(typeof rpc.createTcpClient).toBe('function');
    expect(typeof rpc.createUdpClient).toBe('function');
    expect(typeof rpc.startRpcServer).toBe('function');
    expect(rpc.RpcConstants.CallTypes.RPC_CALL).toBe(0);
    expect(rpc.RpcConstants.CallTypes.RPC_REPLY).toBe(2);
  });
});
