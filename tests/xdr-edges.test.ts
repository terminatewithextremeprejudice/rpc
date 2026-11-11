// Edge-case coverage for the XDR primitives and wire-format helpers.
import { describe, expect, test } from '@jest/globals';
import {
  decodeHyper,
  decodeUint8,
  decodeUint16,
  decodeUint64,
  encodeHyper,
  encodeUint64,
  messageSize,
} from '../src/shared/xdr';
import { parseMessage } from '../src/shared/message';

describe('encodeHyper', () => {
  test('accepts a bigint', () => {
    const buf = encodeHyper(42n);
    expect(buf.length).toBe(8);
    expect(decodeUint64(buf)).toBe(42n);
  });

  test('accepts a number', () => {
    expect(decodeHyper(encodeHyper(7))).toBe(7n);
  });

  test('accepts a BigInt64Array', () => {
    expect(decodeHyper(encodeHyper(new BigInt64Array([9n])))).toBe(9n);
  });

  test('accepts a Uint8Array (copies the first 8 bytes)', () => {
    const buf = encodeHyper(new Uint8Array([1, 2, 3, 4]));
    expect(buf.length).toBe(8);
    expect(Array.from(buf.slice(0, 4))).toEqual([1, 2, 3, 4]);
    expect(Array.from(buf.slice(4))).toEqual([0, 0, 0, 0]);
  });

  test('falls back to BigInt coercion for other values', () => {
    const obj = { [Symbol.toPrimitive]: () => '5' } as any;
    const buf = encodeHyper(obj as any);
    expect(decodeHyper(buf)).toBe(5n);
  });
});

describe('decode guards', () => {
  test('decodeUint8 throws on a short buffer', () => {
    expect(() => decodeUint8(new Uint8Array(0), 0)).toThrow(/too small/i);
  });

  test('decodeUint16 throws on a short buffer', () => {
    expect(() => decodeUint16(new Uint8Array(2), 0)).toThrow(/too small/i);
  });
});

describe('messageSize', () => {
  test('rejects a header of an unknown message type', () => {
    // 16B base header with type = RPC_MISMATCH (5), 32B zeros behind it.
    const buf = new Uint8Array(48);
    const view = new DataView(buf.buffer);
    view.setUint32(0, 2, false); // version
    view.setUint32(4, 5, false); // type: not CALL / not REPLY
    expect(messageSize(buf)).toBe(-1);
  });

  test('rejects a buffer shorter than the base header', () => {
    expect(messageSize(new Uint8Array(8))).toBe(-1);
  });
});

describe('parseMessage', () => {
  test('throws on a message shorter than the base header', () => {
    expect(() => parseMessage(new Uint8Array(8))).toThrow(/too short/i);
  });
});
