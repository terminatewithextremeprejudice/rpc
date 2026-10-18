// XDR Encoder/Decoder for RPC messages
// Implements XDR (External Data Representation) encoding for network byte order (big-endian)

export interface XDRHeader {
  version: number;
  type: number;
  headerLength: number;
  authType: number;
}

// XDR Constants
export const RpcConstants = {
  RPC_VERSION: 2,
  CallTypes: {
    RPC_CALL: 0,
    RPC_CALL_ACK: 1,
    RPC_REPLY: 2,
    RPC_MISMATCH: 3,
    RPC_SYSTEM_ERR: 4,
    RPC_ACCEPT_ERR: 5,
  },
  AuthFlavors: {
    AUTH_NULL: 0,
    UNIX_AUTH: 1,
    DES_AUTH: 2,
  },
  ReplyStates: {
    RPC_ACCEPTED: 0,
    RPC_GARBAGE: 1,
    RPC_VERSION: 2,
    PROG_MISMATCH: 3,
    PROC_MISMATCH: 4,
    SYSERR: 5,
  },
};

/**
 * XDR Integer Encoding (all big-endian)
 */
export function encodeUint8(value: number): Uint8Array {
  return new Uint8Array([value]);
}

export function encodeUint16(value: number): Uint8Array {
  const buf = new Uint8Array(4);
  // Store as big-endian with high byte first, zero-padded
  buf[0] = (value >> 8) & 0xFF;
  buf[1] = value & 0xFF;
  buf[2] = 0;
  buf[3] = 0;
  return buf;
}

export function encodeUint32(value: number): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer, 0, 4);
  view.setUint32(0, value, false);
  return buf;
}

export function encodeUint64(value: bigint): Uint8Array {
  const buf = new Uint8Array(8);
  const view = new DataView(buf.buffer, 0, 8);
  view.setBigUint64(0, value, false);
  return buf;
}

export function encodeHyper(value: BigInt64Array | Uint8Array | bigint | number): Uint8Array {
  if (typeof value === 'bigint') {
    return encodeUint64(value);
  }
  if (typeof value === 'number') {
    return encodeUint64(BigInt(value));
  }
  const buf = new Uint8Array(8);
  const view = new DataView(buf.buffer, 0, 8);
  if (value instanceof BigInt64Array) {
    view.setBigUint64(0, BigInt(value[0] ?? 0n), false);
  } else if (value instanceof Uint8Array) {
    buf.set(value.slice(0, 8));
  } else {
    view.setBigUint64(0, BigInt(value), false);
  }
  return buf;
}

/**
 * XDR String Encoding
 * Format: count (uint32) + null-terminated string padded to 4-byte boundary
 */
export function encodeString(str: string): Uint8Array {
  const encoder = new TextEncoder();
  const rawBytes = encoder.encode(str);
  const paddedLen = Math.ceil((rawBytes.length + 1) / 4) * 4;
  
  const buf = new Uint8Array(4 + paddedLen);
  const view = new DataView(buf.buffer, 0, 4);
  // Store the actual string length (not padded)
  view.setUint32(0, rawBytes.length, false);
  
  // Copy string bytes
  for (let i = 0; i < rawBytes.length; i++) {
    buf[4 + i] = rawBytes[i]!;
  }
  // Fill rest with null bytes (includes null terminator)
  for (let i = rawBytes.length + 4; i < buf.length; i++) {
    buf[i] = 0;
  }
  
  return buf;
}

/**
 * XDR Opaque (binary) Encoding
 * Format: count (uint32) + binary data padded to 4-byte boundary
 */
export function encodeOpaque(data: Uint8Array): Uint8Array {
  const paddedLen = Math.ceil(data.length / 4) * 4;
  const buf = new Uint8Array(4 + paddedLen);
  const view = new DataView(buf.buffer, 0, 4);
  view.setUint32(0, data.length, false);
  
  // Copy data into padded buffer
  for (let i = 0; i < data.length; i++) {
    buf[4 + i] = data[i] ?? 0;
  }
  // Fill rest with zeros
  for (let i = data.length + 4; i < buf.length; i++) {
    buf[i] = 0;
  }
  
  return buf;
}

/**
 * Decode XDR types
 */
export function decodeUint32(buffer: Uint8Array, offset: number = 0): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
  return view.getUint32(0, false);
}

export function decodeUint64(buffer: Uint8Array, offset: number = 0): bigint {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 8);
  return view.getBigUint64(0, false);
}

export function decodeUint8(buffer: Uint8Array, offset: number = 0): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
  return view.getUint32(0, false); // XDR pads to 32-bit
}

export function decodeUint16(buffer: Uint8Array, offset: number = 0): number {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 4);
  return view.getUint32(0, false); // XDR pads to 32-bit
}

export function decodeHyper(buffer: Uint8Array, offset: number = 0): bigint {
  const view = new DataView(buffer.buffer, buffer.byteOffset + offset, 8);
  return view.getBigUint64(0, false);
}

export function decodeString(buffer: Uint8Array, offset: number = 0): string {
  const count = decodeUint32(buffer, offset);
  const strBytes = buffer.subarray(offset + 4, offset + 4 + count);
  const decoder = new TextDecoder();
  return decoder.decode(strBytes);
}

export function decodeOpaque(buffer: Uint8Array, offset: number = 0): Uint8Array {
  const count = decodeUint32(buffer, offset);
  return buffer.subarray(offset + 4, offset + 4 + count);
}

/**
 * Encode XDR Header (4 x uint32 = 16 bytes)
 */
export function encode(header: XDRHeader): Uint8Array {
  return new Uint8Array([
    ...encodeUint32(header.version),
    ...encodeUint32(header.type),
    ...encodeUint32(header.headerLength),
    ...encodeUint32(header.authType)
  ]);
}

/**
 * Decode XDR Header from buffer
 */
export function decode(buffer: Uint8Array): XDRHeader {
  if (buffer.length < 16) {
    throw new Error(`XDR header buffer must be at least 16 bytes (got ${buffer.length})`);
  }
  const offset = 0;
  return {
    version: decodeUint32(buffer, offset),
    type: decodeUint32(buffer, offset + 4),
    headerLength: decodeUint32(buffer, offset + 8),
    authType: decodeUint32(buffer, offset + 12),
  };
}

/**
 * Encode RPC Call header
 * Format: prognum(4) + vers(4) + proc(4) + arglen(4) + opaque_header(4) = 20 bytes
 * The arglen contains the opaque header info for the arguments
 */
export function encodeCall(prognum: number, vers: number, proc: number, header: XDRHeader): Uint8Array {
  // For now, simplified: just encode the numeric fields
  return new Uint8Array([
    ...encodeUint32(prognum),
    ...encodeUint32(vers),
    ...encodeUint32(proc),
    ...encodeUint32(0), // Reserved
    ...encodeUint32(header.version),
    ...encodeUint32(header.type),
    ...encodeUint32(header.headerLength),
    ...encodeUint32(header.authType)
  ]);
}

/**
 * Decode RPC Call header
 */
export function decodeCall(buffer: Uint8Array, offset: number = 0): { procedure: number, version: number, argType: number, authType: number, header: XDRHeader } {
  const o = offset;
  return {
    procedure: decodeUint32(buffer, o),
    version: decodeUint32(buffer, o + 4),
    argType: decodeUint32(buffer, o + 8),
    authType: decodeUint32(buffer, o + 20),
    header: {
      version: decodeUint32(buffer, o + 16),
      type: decodeUint32(buffer, o + 20),
      headerLength: decodeUint32(buffer, o + 24),
      authType: decodeUint32(buffer, o + 28),
    }
  };
}

/**
 * Align value to 4-byte boundary (XDR alignment)
 */
export function align4(value: number): number {
  return Math.ceil(value / 4) * 4;
}

/**
 * Concatenate multiple Uint8Arrays
 */
export function concat(...arrays: Uint8Array[]): Uint8Array {
  const totalLength = arrays.reduce((sum, arr) => sum + arr.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const arr of arrays) {
    result.set(arr, offset);
    offset += arr.length;
  }
  return result;
}
