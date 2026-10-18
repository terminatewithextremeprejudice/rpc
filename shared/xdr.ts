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
    RPC_REPLY: 1,
  },
  AuthFlavors: {
    AUTH_NULL: 0,
    AUTH_SHORT: 1,
    AUTH_KRB: 2,
    AUTHKERB4: 3,
  },
  ReplyStates: {
    RPC_ACCEPTED: 0,
    RPC_REJECTED: 1,
    RPC_GARBAGE: 0,
    RPC_MISMATCH: 1,
    RPC_UNKNOWN: 2,
  },
};

/**
 * XDR Integer Encoding (all big-endian)
 */
export function encodeUint8(value: number): Uint8Array {
  const buf = new Uint8Array(4); // XDR pads to 32-bit boundaries
  const view = new DataView(buf.buffer, 0, 4);
  view.setUint32(0, value, false); // false = big-endian
  return buf;
}

export function encodeUint16(value: number): Uint8Array {
  const buf = new Uint8Array(4);
  const view = new DataView(buf.buffer, 0, 4);
  view.setUint32(0, value, false);
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

export function encodeHyper(value: BigInt64Array | Uint8Array | bigint): Uint8Array {
  if (typeof value === 'bigint') {
    return encodeUint64(value);
  }
  const buf = new Uint8Array(8);
  const view = new DataView(buf.buffer, 0, 8);
  if (value instanceof BigInt64Array) {
    view.setBigUint64(0, BigInt(value[0]), false);
  } else if (value instanceof Uint8Array) {
    buf.set(value.slice(0, 8));
  } else {
    view.setBigUint64(0, value as any, false);
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
  view.setUint32(0, rawBytes.length, false);
  
  // Copy string bytes + null terminator
  for (let i = 0; i < rawBytes.length; i++) {
    buf[4 + i] = rawBytes[i];
  }
  // Fill rest with null bytes
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
    buf[4 + i] = data[i];
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
export function decodeCall(buffer: Uint8Array, offset: number = 0): { prognum: number, vers: number, proc: number, header: XDRHeader } {
  const o = offset;
  return {
    prognum: decodeUint32(buffer, o),
    vers: decodeUint32(buffer, o + 4),
    proc: decodeUint32(buffer, o + 8),
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
