// XDR (External Data Representation) Encoder/Decoder for RPC
// Based on RFC 4506 / XDR: A Standard for Data Representation

export type XDRHeader = {
  version: number;     // uint32: RPC protocol version
  type: number;         // uint32: message type (0=callsite, 1=daemon, etc.)
  headerLength: number; // uint32: size of header in bytes
  authType: number;     // uint32: authentication flavor
};

/**
 * XDR primitives follow network byte order (big-endian)
 * All integers are 32-bit unless specified otherwise
 * Strings are null-terminated and padded to 4-byte boundaries
 */

class XDRStream {
  constructor(private buffer: Uint8Array, public offset: number = 0) {}

  align4(): void {
    const remainder = this.offset % 4;
    if (remainder !== 0) {
      // Fill padding bytes with zeros to reach 4-byte boundary
      this.buffer.fill(0, this.offset, this.offset + 4 - remainder);
      this.offset += 4 - remainder;
    }
  }

  uint8(value: number): void {
    // XDR: single byte, no alignment needed
    this.buffer[this.offset] = value & 0xFF;
    this.offset += 1;
  }

  readUint8(): number {
    const value = this.buffer[this.offset] || 0;
    this.offset += 1;
    return value;
  }

  uint16(value: number): void {
    // XDR: 2 bytes, must be 4-byte aligned
    this.align4();
    const view = new DataView(this.buffer.buffer, this.offset, 2);
    view.setUint16(0, value & 0xFFFF, false); // big-endian
    this.offset += 2;
    // Pad remaining 2 bytes to maintain 4-byte alignment
    this.buffer[this.offset] = 0;
    this.buffer[this.offset + 1] = 0;
    this.offset += 2;
  }

  readUint16(): number {
    this.align4();
    const view = new DataView(this.buffer.buffer, this.offset, 2);
    const value = view.getUint16(0, false);
    this.offset += 4; // Skip the padded region
    return value;
  }

  uint32(value: number): void {
    const view = new DataView(this.buffer.buffer, this.offset, 4);
    view.setUint32(0, value & 0xFFFFFFFF, false); // big-endian
    this.offset += 4;
  }

  readUint32(): number {
    const view = new DataView(this.buffer.buffer, this.offset, 4);
    this.offset += 4;
    return view.getUint32(0, false);
  }

  uint64(value: bigint): void {
    // XDR uint64 = two consecutive uint32 (high:low)
    const high = Number(value / 4294967296n); // 0x100000000
    const low = Number(value % 4294967296n);
    this.uint32(high);
    this.uint32(low);
  }

  readUint64(): bigint {
    const high = this.readUint32();
    const low = this.readUint32();
    return BigInt(high) * 4294967296n + BigInt(low);
  }

  hyper(value: number): void {
    // XDR hyper = two consecutive uint32 (high:low)
    const high = Math.floor(value / 0x100000000);
    const low = value % 0x100000000;
    this.uint32(high);
    this.uint32(low);
  }

  readHyper(): bigint {
    const high = this.readUint32();
    const low = this.readUint32();
    return BigInt(high) * 4294967296n + BigInt(low);
  }

  string(str: string): number {
    const encoded = new TextEncoder().encode(str);
    const paddedLength = Math.ceil((encoded.length + 1) / 4) * 4;

    // Write length (4 bytes)
    const lengthView = new DataView(this.buffer.buffer, this.offset, 4);
    lengthView.setUint32(0, paddedLength, false);
    this.offset += 4;

    // Write null-terminated string + padding
    const stringSlice = this.buffer.subarray(this.offset, this.offset + paddedLength);
    stringSlice.set(encoded);
    // Fill rest with zeros, already zeroed from new ArrayBuffer
    this.offset += paddedLength;

    return paddedLength + 4;
  }

  readString(): string {
    const length = this.readUint32();
    const bytes = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    const str = new TextDecoder().decode(bytes);
    return str.slice(0, str.indexOf('\0'));
  }

  bytes(data: Uint8Array): number {
    const paddedLength = data.length > 0 ? Math.ceil(data.length / 4) * 4 : 0;
    // Write ORIGINAL length (not padded) as per XDR spec
    const lengthView = new DataView(this.buffer.buffer, this.offset, 4);
    lengthView.setUint32(0, data.length, false);
    this.offset += 4;
    
    // Write padded bytes (skip if empty)
    if (paddedLength > 0) {
      const slice = this.buffer.subarray(this.offset, this.offset + paddedLength);
      slice.set(data);
      slice.fill(0, data.length);
      this.offset += paddedLength;
    }
    
    return paddedLength + 4;
  }

  readBytes(): Uint8Array {
    const originalLength = this.readUint32();
    
    if (originalLength === 0) {
      return new Uint8Array(0);
    }
    
    const paddedLength = Math.ceil(originalLength / 4) * 4;
    const result = this.buffer.subarray(this.offset, this.offset + originalLength);
    this.offset += paddedLength;
    return result;
  }
}

export function encode(header: XDRHeader): Uint8Array {
  // RPC XDR header: 4 x uint32 = 16 bytes minimum
  // We allocate a bit more for safety margin
  const buffer = new Uint8Array(64);
  const stream = new XDRStream(buffer);

  stream.uint32(header.version);
  stream.uint32(header.type);
  stream.uint32(header.headerLength);
  stream.uint32(header.authType);

  // Return only the used portion
  return stream.offset === 16
    ? buffer.subarray(0, 16)
    : buffer.subarray(0, stream.offset);
}

export function decode(data: Uint8Array): XDRHeader {
  if (data.length < 16) {
    throw new Error(`XDR header must be at least 16 bytes, got ${data.length}`);
  }

  const stream = new XDRStream(data);

  return {
    version: stream.readUint32(),
    type: stream.readUint32(),
    headerLength: stream.readUint32(),
    authType: stream.readUint32()
  };
}

// Convenience functions for full message handling
export function encodeCall(procedure: number, version: number, argType: number, args: XDRHeader): Uint8Array {
  const header: XDRHeader = {
    version,
    type: 0, // RPC_CALL
    headerLength: 20, // 4 uint32 for header + 4 for call
    authType: args.authType || 0
  };

  const buffer = new Uint8Array(64);
  const stream = new XDRStream(buffer);

  stream.uint32(header.version);
  stream.uint32(header.type);
  stream.uint32(header.headerLength);
  stream.uint32(procedure);
  stream.uint32(version);
  stream.uint32(argType);
  stream.uint32(args.authType || 0);

  return buffer.subarray(0, stream.offset);
}

export function decodeCall(data: Uint8Array): { procedure: number; version: number; argType: number; authType: number } {
  const stream = new XDRStream(data);
  
  // Skip the 3 header uint32s (version, type, headerLength)
  stream.readUint32(); // version
  stream.readUint32(); // type
  stream.readUint32(); // headerLength
  
  return {
    procedure: stream.readUint32(),
    version: stream.readUint32(),
    argType: stream.readUint32(),
    authType: stream.readUint32()
  };
}

// Well-known RPC constants
export const RpcConstants = {
  RPC_VERSION: 2,
  
  CallTypes: {
    RPC_CALL: 0,
    RPC_CALL_ACK: 1,
    RPC_REPLY: 2,
    RPC_MISMATCH: 3,
    RPC_SYSTEM_ERR: 4,
    RPC_ACCEPT_ERR: 5
  },
  
  AuthFlavors: {
    AUTH_NULL: 0,
    UNIX_AUTH: 1,
    DES_AUTH: 2
  },
  
  ReplyStates: {
    RPC_ACCEPTED: 0,
    RPC_GARBAGE: 1,
    RPC_VERSION: 2,
    PROG_MISMATCH: 3,
    PROC_MISMATCH: 4,
    SYSERR: 5
  }
} as const;

// Helper functions for string encoding/decoding
export function encodeString(str: string): Uint8Array {
  const buffer = new Uint8Array(256);
  const stream = new XDRStream(buffer);
  const totalSize = stream.string(str);
  return buffer.subarray(0, totalSize);
}

export function decodeString(data: Uint8Array): string {
  const stream = new XDRStream(data);
  return stream.readString();
}

// Helper functions for opaque bytes encoding/decoding  
export function encodeOpaque(data: Uint8Array): Uint8Array {
  // XDR opaque: length prefix (uint32) followed by bytes padded to 4-byte boundary
  // For 0 bytes: length=0, no padding needed, total=4
  // For 1-3 bytes: length=N, padded to 4, total=4+4=8
  // For 4 bytes: length=4, no extra padding, total=4+4=8
  
  const paddedLength = data.length > 0 ? Math.ceil(data.length / 4) * 4 : 0;
  const buffer = new Uint8Array(paddedLength + 4); // +4 for length prefix
  const stream = new XDRStream(buffer);
  stream.bytes(data);
  return buffer.subarray(0, stream.offset);
}

export function decodeOpaque(data: Uint8Array): Uint8Array {
  const stream = new XDRStream(data);
  return stream.readBytes();
}

// Helper functions for integer types
export function encodeUint8(value: number): Uint8Array {
  const buffer = new Uint8Array(1);
  const stream = new XDRStream(buffer);
  stream.uint8(value);
  return buffer.subarray(0, stream.offset);
}

export function decodeUint8(data: Uint8Array): number {
  const stream = new XDRStream(data);
  return stream.readUint8();
}

export function encodeUint16(value: number): Uint8Array {
  const buffer = new Uint8Array(8);
  const stream = new XDRStream(buffer);
  stream.uint16(value);
  return buffer.subarray(0, stream.offset);
}

export function decodeUint16(data: Uint8Array): number {
  const stream = new XDRStream(data);
  return stream.readUint16();
}

export function encodeUint32(value: number): Uint8Array {
  const buffer = new Uint8Array(4);
  const stream = new XDRStream(buffer);
  stream.uint32(value);
  return buffer.subarray(0, stream.offset);
}

export function decodeUint32(data: Uint8Array): number {
  const stream = new XDRStream(data);
  return stream.readUint32();
}

export function encodeUint64(value: bigint): Uint8Array {
  const buffer = new Uint8Array(16);
  const stream = new XDRStream(buffer);
  stream.uint64(value);
  return buffer.subarray(0, stream.offset);
}

export function decodeUint64(data: Uint8Array): bigint {
  const stream = new XDRStream(data);
  return stream.readUint64();
}

export function encodeHyper(value: number): Uint8Array {
  const buffer = new Uint8Array(16);
  const stream = new XDRStream(buffer);
  stream.hyper(value);
  return buffer.subarray(0, stream.offset);
}

export function decodeHyper(data: Uint8Array): bigint {
  const stream = new XDRStream(data);
  return stream.readHyper();
}