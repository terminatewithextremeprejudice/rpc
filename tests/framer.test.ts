import { LengthPrefixedFramer, XdrFramer } from '../src/transport';
import { encodeString, encodeOpaque, RpcConstants, messageSize } from '../src/shared/xdr';
import { buildCall, buildReply } from '../src/shared/message';

describe('LengthPrefixedFramer', () => {
  let framer: LengthPrefixedFramer;

  beforeEach(() => {
    framer = new LengthPrefixedFramer();
  });

  describe('messageSize', () => {
    it('returns -1 when buffer is empty', () => {
      const buffer = new Uint8Array(0);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('returns -1 when buffer is shorter than prefix', () => {
      const buffer = new Uint8Array(3); // prefixBytes defaults to 4
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('returns -1 when total buffer size is less than prefix + payload', () => {
      // Prefix indicates 10 bytes of payload, but only 5 available
      const buffer = new Uint8Array([0, 0, 0, 10, 1, 2, 3, 4, 5]);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('returns correct size for complete message', () => {
      // 4-byte prefix + 5-byte payload
      const payload = new Uint8Array([1, 2, 3, 4, 5]);
      const prefix = new Uint8Array([0, 0, 0, 5]); // size = 5
      const buffer = new Uint8Array([...prefix, ...payload]);
      const size = framer.messageSize(buffer);
      expect(size).toBe(9); // 4 prefix + 5 payload
    });

    it('handles prefixBytes = 2', () => {
      const framer2 = new LengthPrefixedFramer(2);
      const payload = new Uint8Array([1, 2, 3]);
      const prefix = new Uint8Array([0, 3]); // size = 3
      const buffer = new Uint8Array([...prefix, ...payload]);
      const size = framer2.messageSize(buffer);
      expect(size).toBe(5); // 2 prefix + 3 payload
    });

    it('handles prefixBytes = 8', () => {
      const framer8 = new LengthPrefixedFramer(8);
      const payload = new Uint8Array([1, 2]);
      const prefix = new Uint8Array([0, 0, 0, 0, 0, 0, 0, 2]); // size = 2
      const buffer = new Uint8Array([...prefix, ...payload]);
      const size = framer8.messageSize(buffer);
      expect(size).toBe(10); // 8 prefix + 2 payload
    });

    it('handles size 0 as invalid', () => {
      // A size of 0 means no valid message
      const buffer = new Uint8Array([0, 0, 0, 0, 1, 2, 3]);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('works with multiple consecutive messages in a larger buffer', () => {
      const frame1 = new Uint8Array([0, 0, 0, 3, 1, 2, 3]);
      const frame2 = new Uint8Array([0, 0, 0, 2, 4, 5]);
      const buffer = new Uint8Array([...frame1, ...frame2]);
      const size = framer.messageSize(buffer);
      expect(size).toBe(7); // First message size (4 prefix + 3 payload)
    });
  });

  describe('edge cases', () => {
    it('handles very large payload sizes', () => {
      const prefixBytes = 4;
      const payloadSize = 1024;
      const buffer = new Uint8Array(prefixBytes + payloadSize);
      // First 4 bytes are the size prefix in big-endian
      buffer[0] = (payloadSize >> 24) & 0xFF;
      buffer[1] = (payloadSize >> 16) & 0xFF;
      buffer[2] = (payloadSize >> 8) & 0xFF;
      buffer[3] = payloadSize & 0xFF;
      const size = framer.messageSize(buffer);
      expect(size).toBe(4 + payloadSize);
    });

    it('detects incomplete large payloads', () => {
      const prefixBytes = 4;
      const prefixSize = 1000;
      const actualPayload = 10;
      const buffer = new Uint8Array(prefixBytes + actualPayload);
      buffer[0] = (prefixSize >> 24) & 0xFF;
      buffer[1] = (prefixSize >> 16) & 0xFF;
      buffer[2] = (prefixSize >> 8) & 0xFF;
      buffer[3] = prefixSize & 0xFF;
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1); // Insufficient data for the declared size
    });

    it('handles buffer with only prefix but payload still coming', () => {
      const prefix = new Uint8Array([0, 0, 0, 100]);
      const buffer = new Uint8Array(prefix);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });
  });
});

describe('XdrFramer', () => {
  let framer: XdrFramer;

  beforeEach(() => {
    framer = new XdrFramer();
  });

  describe('messageSize', () => {
    it('wraps the xdr.messageSize function correctly', () => {
      const fullMessage = buildCall(1, 1, 1, 'test-message-id', new Uint8Array([1, 2, 3]));
      const size = framer.messageSize(fullMessage);
      expect(size).toBe(fullMessage.length);
      expect(size).toBeGreaterThanOrEqual(48);
    });

    it('returns -1 for buffer shorter than base header', () => {
      const buffer = new Uint8Array(15);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('returns -1 for incomplete message', () => {
      // Only the base header (16 bytes) - incomplete
      const buffer = new Uint8Array(16);
      buffer.fill(0);
      buffer[4] = 0; // type = RPC_CALL
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('handles invalid RPC type', () => {
      const buffer = new Uint8Array(16);
      buffer[4] = 42; // Invalid type
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('accepts RPC_CALL type messages', () => {
      const fullMessage = buildCall(1, 1, 1, 'test-id', new Uint8Array([1, 2, 3]));
      const size = framer.messageSize(fullMessage);
      expect(size).toBeGreaterThan(0);
      expect(size).toBe(fullMessage.length);
    });

    it('accepts RPC_REPLY type messages', () => {
      const fullMessage = buildReply(0, 'test-id', new Uint8Array([1, 2, 3]));
      const size = framer.messageSize(fullMessage);
      expect(size).toBeGreaterThan(0);
    });

    it('returns -1 for RPC_MISMATCH type', () => {
      const buffer = new Uint8Array(16);
      buffer[4] = RpcConstants.CallTypes.RPC_MISMATCH;
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('returns -1 for RPC_SYSTEM_ERR type', () => {
      const buffer = new Uint8Array(16);
      buffer[4] = RpcConstants.CallTypes.RPC_SYSTEM_ERR;
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('handles messages with various string and opaque field sizes', () => {
      // Test with different msgId lengths
      for (const msgIdLen of [0, 1, 3, 4, 5, 10, 100]) {
        const msgIdStr = 'a'.repeat(msgIdLen);
        const fullMessage = buildCall(1, 1, 1, msgIdStr, new Uint8Array(10));
        const size = framer.messageSize(fullMessage);
        expect(size).toBe(fullMessage.length);
      }
    });

    it('detects when msgId field is incomplete', () => {
      // Create a valid message then truncate after call header
      const fullMessage = buildCall(1, 1, 1, 'test-id', new Uint8Array(10));
      // Truncate to just the base+call header (48 bytes) - msgId is missing
      const buffer = fullMessage.subarray(0, 48);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });

    it('detects when message body is incomplete', () => {
      // Create valid message, truncate body
      const fullMessage = buildCall(1, 1, 1, 'test', new Uint8Array(100));
      // Take 48 bytes (headers) + msgId size but truncate body
      const bodyHeader = new Uint8Array([0, 0, 0, 100]);
      const buffer = new Uint8Array([...fullMessage.subarray(0, 48), ...encodeString('test'), ...bodyHeader.subarray(0, 2)]);
      const size = framer.messageSize(buffer);
      expect(size).toBe(-1);
    });
    it('handles empty msgId and empty body', () => {
      const fullMessage = buildCall(1, 1, 1, '', new Uint8Array(0));
      const size = framer.messageSize(fullMessage);
      expect(size).toBe(fullMessage.length);
    });
  });

  describe('integration with xdr.messageSize', () => {
    it('matches direct xdr.messageSize results', () => {
      const fullMessage = buildCall(1, 1, 1, 'integration-test', new Uint8Array([0xde, 0xad, 0xbe, 0xef]));
      const directSize = messageSize(fullMessage);
      const framerSize = framer.messageSize(fullMessage);
      expect(framerSize).toBe(directSize);
    });
  });
});
