import { describe, expect, test } from '@jest/globals';
import { 
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
} from '../src/shared/xdr';

describe('XDR Encoder/Decoder', () => {
  describe('encode/decode header', () => {
    test('encodes basic XDR header correctly', () => {
      const header: XDRHeader = {
        version: 2,
        type: 0,
        headerLength: 16,
        authType: 0
      };

      const encoded = encode(header);

      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBe(16);
      
      // Verify big-endian encoding
      expect(encoded[0]).toBe(0); // version high bytes
      expect(encoded[1]).toBe(0);
      expect(encoded[2]).toBe(0);
      expect(encoded[3]).toBe(2); // version = 2

      expect(encoded[4]).toBe(0); // type high bytes
      expect(encoded[5]).toBe(0);
      expect(encoded[6]).toBe(0);
      expect(encoded[7]).toBe(0); // type = 0
    });

    test('decodes encoded header back to original', () => {
      const original: XDRHeader = {
        version: RpcConstants.RPC_VERSION,
        type: RpcConstants.CallTypes.RPC_CALL,
        headerLength: 16,
        authType: RpcConstants.AuthFlavors.AUTH_NULL
      };

      const encoded = encode(original);
      const decoded = decode(encoded);

      expect(decoded.version).toBe(original.version);
      expect(decoded.type).toBe(original.type);
      expect(decoded.headerLength).toBe(original.headerLength);
      expect(decoded.authType).toBe(original.authType);
    });

    test('handles large version numbers (big-endian)', () => {
      const header: XDRHeader = {
        version: 0x12345678,
        type: 0xABCDEF01,
        headerLength: 0xFEDCBA98,
        authType: 0x87654321
      };

      const encoded = encode(header);
      const decoded = decode(encoded);

      expect(decoded.version).toBe(header.version);
      expect(decoded.type).toBe(header.type);
      expect(decoded.headerLength).toBe(header.headerLength);
      expect(decoded.authType).toBe(header.authType);
    });

    test('round-trip preserves all header fields', () => {
      const testCases: XDRHeader[] = [
        { version: 0, type: 0, headerLength: 0, authType: 0 },
        { version: 1, type: 1, headerLength: 1, authType: 1 },
        { version: 2, type: 2, headerLength: 20, authType: 2 },
        { version: 65535, type: 42, headerLength: 100, authType: 255 }
      ];

      for (const header of testCases) {
        const encoded = encode(header);
        const decoded = decode(encoded);

        expect(decoded).toEqual(header);
      }
    });

    test('throws error when buffer is too small', () => {
      const smallBuffer = new Uint8Array(12); // Need at least 16

      expect(() => decode(smallBuffer)).toThrow(/must be at least 16 bytes/);
    });
  });

  describe('encodeCall/decodeCall', () => {
    test('encodes RPC call with procedure and version', () => {
      const args: XDRHeader = {
        version: 1,
        type: 0,
        headerLength: 0,
        authType: RpcConstants.AuthFlavors.UNIX_AUTH
      };

      const encoded = encodeCall(100000, 2, 100001, args);
      const decoded = decodeCall(encoded);

      expect(decoded.procedure).toBe(100000);
      expect(decoded.version).toBe(2);
      expect(decoded.argType).toBe(100001);
      expect(decoded.authType).toBe(RpcConstants.AuthFlavors.UNIX_AUTH);
    });

    test('handles program/procedure numbers for common RPC services', () => {
      const nfsProgram: XDRHeader = {
        version: 3,
        type: 0,
        headerLength: 0,
        authType: 0
      };

      const encoded = encodeCall(100003, 3, 1, nfsProgram); // NFS program number
      const decoded = decodeCall(encoded);

      expect(decoded.procedure).toBe(100003);
      expect(decoded.version).toBe(3);
    });
  });

  describe('RpcConstants', () => {
    test('exports correct RPC version', () => {
      expect(RpcConstants.RPC_VERSION).toBe(2);
    });

    test('exports call types', () => {
      expect(RpcConstants.CallTypes).toEqual({
        RPC_CALL: 0,
        RPC_CALL_ACK: 1,
        RPC_REPLY: 2,
        RPC_MISMATCH: 3,
        RPC_SYSTEM_ERR: 4,
        RPC_ACCEPT_ERR: 5
      });
    });

    test('exports auth flavors', () => {
      expect(RpcConstants.AuthFlavors).toEqual({
        AUTH_NULL: 0,
        UNIX_AUTH: 1,
        DES_AUTH: 2
      });
    });

    test('exports reply states', () => {
      expect(RpcConstants.ReplyStates).toEqual({
        RPC_ACCEPTED: 0,
        RPC_GARBAGE: 1,
        RPC_VERSION: 2,
        PROG_MISMATCH: 3,
        PROC_MISMATCH: 4,
        SYSERR: 5
      });
    });
  });

  describe('edge cases', () => {
    test('handles zero values correctly', () => {
      const header: XDRHeader = {
        version: 0,
        type: 0,
        headerLength: 0,
        authType: 0
      };

      const encoded = encode(header);
      expect(encoded.every(b => b === 0)).toBe(true);

      const decoded = decode(encoded);
      expect(decoded).toEqual(header);
    });

    test('handles maximum uint32 values', () => {
      const header: XDRHeader = {
        version: 0xFFFFFFFF,
        type: 0xFFFFFFFF,
        headerLength: 0xFFFFFFFF,
        authType: 0xFFFFFFFF
      };

      const encoded = encode(header);
      const decoded = decode(encoded);

      expect(decoded).toEqual(header);
    });
  });

  describe('string encoding/decoding', () => {
    test('encodes empty string correctly', () => {
      const encoded = encodeString('');
      
      // Length prefix (4 bytes) + null terminator (1 byte) + padding (3 bytes) = 8 bytes
      expect(encoded.length).toBe(8);
      expect(new DataView(encoded.buffer, 0, 4).getUint32(0, false)).toBe(4); // padded length
      
      const decoded = decodeString(encoded);
      expect(decoded).toBe('');
    });

    test('encodes simple ASCII string with correct padding', () => {
      const original = 'hello';
      const encoded = encodeString(original);
      const decoded = decodeString(encoded);

      // 5 chars + null = 6, padded to 8 bytes + 4 byte length = 12 bytes
      expect(encoded.length).toBe(12);
      expect(decoded).toBe(original);
    });

    test('encodes string exactly divisible by 4', () => {
      const original = 'abcd';
      const encoded = encodeString(original);
      const decoded = decodeString(encoded);

      // 4 chars + null = 5, padded to 8 bytes + 4 byte length = 12 bytes
      expect(encoded.length).toBe(12);
      expect(decoded).toBe(original);
    });

    test('encodes string longer than 4 bytes', () => {
      const original = 'abcdef';
      const encoded = encodeString(original);
      const decoded = decodeString(encoded);

      // 6 chars + null = 7, padded to 8 bytes + 4 byte length = 12 bytes
      expect(encoded.length).toBe(12);
      expect(decoded).toBe(original);
    });

    test('encodes string with special characters', () => {
      const original = 'Hello, World! @#$%^&*()';
      const encoded = encodeString(original);
      const decoded = decodeString(encoded);

      expect(decoded).toBe(original);
    });

    test('encodes Unicode string correctly', () => {
      const original = 'こんにちは世界 🌍';
      const encoded = encodeString(original);
      const decoded = decodeString(encoded);

      expect(decoded).toBe(original);
    });

    test('round-trip preserves all string content', () => {
      const testCases = [
        '',
        'a',
        'ab',
        'abc',
        'abcd',
        'abcde',
        'test',
        'test123',
        'Hello World!',
        '日本語テスト',
        'Mixed content 123 !@# αβγδ'
      ];

      for (const str of testCases) {
        const encoded = encodeString(str);
        const decoded = decodeString(encoded);

        expect(decoded).toBe(str);
      }
    });

    test('verifies null termination in encoded string', () => {
      const encoded = encodeString('hello');
      
      // Length prefix (4 bytes) + padded string (8 bytes) = 12 bytes total
      // String is at positions 4-11, null terminator should be at position 8 (6th byte of string region)
      expect(encoded.length).toBe(12);
      
      // Check that there's a null byte somewhere in the padded region
      const paddedLength = new DataView(encoded.buffer, 0, 4).getUint32(0, false);
      const stringStart = 4; // after length prefix
      
      // Find null terminator in string region
      let foundNull = false;
      for (let i = stringStart; i < stringStart + paddedLength; i++) {
        if (encoded[i] === 0) {
          foundNull = true;
          break;
        }
      }
      expect(foundNull).toBe(true);
    });
  });

  describe('opaque bytes encoding/decoding', () => {
    test('encodes empty bytes correctly', () => {
      const original = new Uint8Array(0);
      const encoded = encodeOpaque(original);
      const decoded = decodeOpaque(encoded);

      // 4 byte length prefix + 0 bytes padded to 0 = 4 bytes total
      expect(encoded.length).toBe(4);
      expect(decoded).toEqual(original);
    });

    test('encodes small byte array with padding', () => {
      const original = new Uint8Array([0x01, 0x02, 0x03]);
      const encoded = encodeOpaque(original);
      const decoded = decodeOpaque(encoded);

      // 3 bytes padded to 4 + 4 byte length = 8 bytes
      expect(encoded.length).toBe(8);
      expect(decoded).toEqual(original);
    });

    test('encodes byte array exactly divisible by 4', () => {
      const original = new Uint8Array([0x01, 0x02, 0x03, 0x04]);
      const encoded = encodeOpaque(original);
      const decoded = decodeOpaque(encoded);

      // 4 bytes no padding + 4 byte length = 8 bytes
      expect(encoded.length).toBe(8);
      expect(decoded).toEqual(original);
    });

    test('encodes larger byte array', () => {
      const original = new Uint8Array(100);
      for (let i = 0; i < 100; i++) {
        original[i] = i;
      }
      
      const encoded = encodeOpaque(original);
      const decoded = decodeOpaque(encoded);

      // 100 bytes padded to 100 + 4 byte length = 104 bytes
      expect(encoded.length).toBe(104);
      expect(decoded).toEqual(original);
    });

    test('encodes bytes with various values', () => {
      const original = new Uint8Array([0x00, 0x01, 0x7F, 0x80, 0xFF]);
      const encoded = encodeOpaque(original);
      const decoded = decodeOpaque(encoded);

      expect(decoded).toEqual(original);
    });
  });

  describe('Integer types (uint8, uint16, uint32, uint64, hyper)', () => {
    describe('uint8', () => {
      test('encodes and decodes byte values', () => {
        const testValues = [0, 1, 127, 128, 255];
        
        for (const value of testValues) {
          const encoded = encodeUint8(value);
          expect(encoded).toEqual(new Uint8Array([value]));
          
          const decoded = decodeUint8(encoded);
          expect(decoded).toBe(value);
        }
      });

      test('round-trip preserves value', () => {
        const original = 42;
        const encoded = encodeUint8(original);
        const decoded = decodeUint8(encoded);
        expect(decoded).toBe(original);
      });
    });

    describe('uint16', () => {
      test('encodes with 4-byte alignment', () => {
        const encoded = encodeUint16(0x1234);
        
        // uint16 is padded to 4 bytes in XDR
        expect(encoded.length).toBe(4);
        
        // First 2 bytes should be the value (big-endian)
        expect(encoded[0]).toBe(0x12);
        expect(encoded[1]).toBe(0x34);
        // Last 2 bytes are padding
        expect(encoded[2]).toBe(0);
        expect(encoded[3]).toBe(0);
      });

      test('round-trip preserves value', () => {
        const testValues = [0, 1, 0x1234, 0xFFFF];
        
        for (const value of testValues) {
          const encoded = encodeUint16(value);
          const decoded = decodeUint16(encoded);
          expect(decoded).toBe(value);
        }
      });
    });

    describe('uint32', () => {
      test('encodes full 32-bit values (big-endian)', () => {
        const encoded = encodeUint32(0x12345678);
        
        expect(encoded.length).toBe(4);
        expect(encoded[0]).toBe(0x12);
        expect(encoded[1]).toBe(0x34);
        expect(encoded[2]).toBe(0x56);
        expect(encoded[3]).toBe(0x78);
      });

      test('round-trip with various values', () => {
        const testValues = [0, 1, 0x12345678, 0xFFFFFFFF];
        
        for (const value of testValues) {
          const encoded = encodeUint32(value);
          const decoded = decodeUint32(encoded);
          expect(decoded).toBe(value);
        }
      });
    });

    describe('uint64', () => {
      test('encodes 64-bit bigint as two uint32s', () => {
        const original = 0x123456789ABCDEF0n;
        const encoded = encodeUint64(original);
        
        expect(encoded.length).toBe(8);
        
        // High 32 bits
        expect(encoded[0]).toBe(0x12);
        expect(encoded[1]).toBe(0x34);
        expect(encoded[2]).toBe(0x56);
        expect(encoded[3]).toBe(0x78);
        
        // Low 32 bits
        expect(encoded[4]).toBe(0x9A);
        expect(encoded[5]).toBe(0xBC);
        expect(encoded[6]).toBe(0xDE);
        expect(encoded[7]).toBe(0xF0);
      });

      test('round-trip preserves bigint', () => {
        const testValues: bigint[] = [
          0n,
          1n,
          0x12345678n,
          0x123456789ABCDEF0n,
          0xFFFFFFFFFFFFFFFFn
        ];
        
        for (const value of testValues) {
          const encoded = encodeUint64(value);
          const decoded = decodeUint64(encoded);
          expect(decoded).toBe(value);
        }
      });
    });

    describe('hyper', () => {
      test('encodes hyper as two uint32s', () => {
        const original = 0x123456789ABCDEF0;
        const encoded = encodeHyper(original);
        
        expect(encoded.length).toBe(8);
        
        const decoded = decodeHyper(encoded);
        // hyper uses Number so we can compare with .toNumber()
        expect(decoded - BigInt(original)).toBe(0n);
      });

      test('handles maximum uint64 value', () => {
        const original = 0xFFFFFFFFFFFFFFFF;
        const encoded = encodeHyper(original);
        const decoded = decodeHyper(encoded);
        
        expect(decoded).toBe(BigInt(original));
      });
    });
  });
});
