// RPC message wire format shared by the client and the server.
//
//   16B   XDR base header   version | type | headerLength | authType
//   32B   call header       prognum | vers | proc | (header copy)
//   XDR   msgId             string — echoed in the reply, used for matching
//   XDR   body              opaque — args on calls, value on replies
//
// `type` uses RpcConstants.CallTypes (RPC_CALL = 0, RPC_REPLY = 2); on
// replies, `authType` carries the reply state (RPC_ACCEPTED = 0, ...).

import {
  concat,
  decode,
  decodeString,
  decodeOpaque,
  encode,
  encodeCall,
  encodeOpaque,
  encodeString,
  RpcConstants,
} from './xdr';

export interface ParsedMessage {
  version: number;
  type: number;
  status: number; // base header authType — reply state for RPC_REPLY
  prognum: number; // call header — meaningful for RPC_CALL
  vers: number;
  proc: number;
  msgId: string | undefined;
  body: Uint8Array;
}

function u32(data: Uint8Array, offset: number): number {
  return new DataView(data.buffer, data.byteOffset + offset, 4).getUint32(0, false
);
}

export function parseMessage(message: Uint8Array): ParsedMessage {
  if (message.length < 16) {
    throw new Error(`RPC message too short: ${message.length} bytes`);
  }
  const header = decode(message.subarray(0, 16));
  const hasCallHeader = message.length >= 48;

  let msgId: string | undefined;
  let offset = 48;
  if (hasCallHeader && message.length >= offset + 4) {
    const len = u32(message, offset);
    if (len > 0) {
      msgId = decodeString(message, offset);
      offset += 4 + len;
    }
  }

  let body: Uint8Array = new Uint8Array(0);
  if (offset + 4 <= message.length) {
    body = decodeOpaque(message, offset);
  }

  return {
    version: header.version,
    type: header.type,
    status: header.authType,
    prognum: hasCallHeader ? u32(message, 16) : 0,
    vers: hasCallHeader ? u32(message, 20) : 0,
    proc: hasCallHeader ? u32(message, 24) : 0,
    msgId,
    body,
  };
}

export function buildCall(
  prognum: number,
  vers: number,
  proc: number,
  msgId: string,
  args: Uint8Array = new Uint8Array(0),
): Uint8Array {
  return concat(
    encode({
      version: RpcConstants.RPC_VERSION,
      type: RpcConstants.CallTypes.RPC_CALL,
      headerLength: 16,
      authType: RpcConstants.AuthFlavors.AUTH_NULL,
    }),
    encodeCall(prognum, vers, proc, { version: 0, type: 0, headerLength: 0, authType
: 0 }),
    encodeString(msgId),
    encodeOpaque(args),
  );
}

export function buildReply(
  status: number,
  msgId: string,
  value: Uint8Array = new Uint8Array(0),
): Uint8Array {
  return concat(
    encode({
      version: RpcConstants.RPC_VERSION,
      type: RpcConstants.CallTypes.RPC_REPLY,
      headerLength: 16,
      authType: status,
    }),
    encodeCall(0, 0, 0, { version: 0, type: 0, headerLength: 0, authType: 0 }),
    encodeString(msgId),
    encodeOpaque(value),
  );
}
