// RPC Client Implementation
// Encodes RPC calls and sends them through the configured transport

import { encode, decode, encodeCall, encodeString, RpcConstants, type XDRHeader } from './shared/xdr';
import { Transport, TcpTransport } from './transport';

export interface RpcClientOptions {
  transport: TcpTransport;
  timeout?: number;
}

export interface RpcCallResult {
  status: number;
  value: Uint8Array | undefined;
  msgId: string;
}

export interface AsyncRpcHandle {
  _async: true;
  msgId: string;
  proc: number;
  connectionId: string;
}

export class RpcClient {
  private transport: TcpTransport;
  private prognum: number;
  private vers: number;
  private timeout: number;
  private connected: boolean = false;
  private connectionId: string | undefined;
  
  // Pending responses: msgId -> raw buffer
  private pendingResponses: Map<string, Buffer> = new Map();

  constructor(prognum: number, vers: number, options: RpcClientOptions) {
    this.prognum = prognum;
    this.vers = vers;
    this.transport = options.transport;
    this.timeout = options.timeout || 5000;
  }

  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      await this.transport.connect();
      this.connected = true;
      console.log(`[RPC Client] Connected`);
    } catch (error) {
      this.connected = false;
      throw new Error(`Connection failed: ${error}`);
    }
  }

  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    if (this.connectionId) {
      await this.transport.close(this.connectionId);
    }
    this.connected = false;
    this.pendingResponses.clear();
    console.log(`[RPC Client] Disconnected from server`);
  }

  /**
   * Get the connection ID for this client's transport
   */
  getConnectionId(): string {
    if (this.connectionId) {
      return this.connectionId;
    }
    // Client mode: transport assigns ID on connect
    // For now we use 'client' as a fallback - the transport will find the right connection
    return 'client';
  }

  async call(proc: number, args: Uint8Array = new Uint8Array(0), options?: {
    async?: boolean;
    msgId?: string;
    timeout?: number;
  }): Promise<RpcCallResult | AsyncRpcHandle> {
    if (!this.connected) {
      await this.connect();
    }

    if (!this.transport.isConnected(this.getConnectionId())) {
      throw new Error('Not connected to server');
    }

    const useAsync = options?.async === true;
    const callMsgId = options?.msgId || this.generateMsgId();
    const callTimeout = options?.timeout ?? this.timeout;

    const callHeader: XDRHeader = {
      version: RpcConstants.RPC_VERSION,
      type: RpcConstants.CallTypes.RPC_CALL,
      headerLength: 16,
      authType: RpcConstants.AuthFlavors.AUTH_NULL
    };

    const encodedCallHeader = encodeCall(
      this.prognum,
      this.vers,
      proc,
      callHeader
    );

    const encodedMsgId = encodeString(callMsgId as string);
    const encodedArgs = this.encodeOpaqueArgs(args);
    const fullCall = new Uint8Array(
      [...new Uint8Array(16), ...encodedCallHeader, ...encodedMsgId, ...encodedArgs]
    );

    console.log(
      `[RPC Client] Sending CALL: prog=${this.prognum}, vers=${this.vers}, proc=${proc}, msgId=${callMsgId}`
    );

    await this.transport.send(this.getConnectionId(), fullCall);

    if (useAsync) {
      return { _async: true, msgId: callMsgId as string, proc, connectionId: this.getConnectionId() };
    }

    const response = await this.receiveWithTimeout(this.getConnectionId(), 1024, callTimeout);
    return this.decodeResponse(response, callMsgId as string);
  }

  async fire(proc: number, args: Uint8Array = new Uint8Array(0), msgId?: string): Promise<AsyncRpcHandle> {
    return this.call(proc, args, { async: true, msgId }) as Promise<AsyncRpcHandle>;
  }

  async getResponse(msgId: string, timeout: number = 5000): Promise<RpcCallResult> {
    if (this.pendingResponses.has(msgId)) {
      const responseBuffer = this.pendingResponses.get(msgId)!;
      this.pendingResponses.delete(msgId);
      return this.decodeResponse(new Uint8Array(responseBuffer), msgId);
    }

    const connId = this.getConnectionId();
    const response = await this.receiveWithTimeout(connId, 2048, timeout);
    
    this.pendingResponses.set(msgId, Buffer.from(response));
    return this.decodeResponse(response, msgId);
  }

  async collectResponses(timeout: number = 1000): Promise<Map<string, RpcCallResult>> {
    const results = new Map<string, RpcCallResult>();
    const connId = this.getConnectionId();

    // Check cached responses
    for (const [msgId, responseBuffer] of this.pendingResponses) {
      results.set(msgId, this.decodeResponse(new Uint8Array(responseBuffer), msgId));
      this.pendingResponses.delete(msgId);
    }

    // Poll transport for new responses
    const deadline = Date.now() + timeout;
    let consecutiveDuplicates = 0;

    while (Date.now() < deadline && consecutiveDuplicates < 2) {
      try {
        const response = await this.transport.receive(connId, 2048);
        const result = this.decodeResponse(response);
        if (result.msgId) {
          if (results.has(result.msgId)) {
            consecutiveDuplicates++;
          } else {
            results.set(result.msgId, result);
            consecutiveDuplicates = 0;
          }
        }
      } catch (err: any) {
        if (err.message?.includes('timeout')) {
          break;
        }
      }
      await new Promise(r => setTimeout(r, 10));
    }

    return results;
  }

  private encodeOpaqueArgs(data: Uint8Array): Uint8Array {
    const paddedLength = data.length > 0 ? Math.ceil(data.length / 4) * 4 : 0;
    const buffer = new Uint8Array(paddedLength + 4);
    const view = new DataView(buffer.buffer, 0, 4);
    view.setUint32(0, data.length, false);
    if (paddedLength > 0) {
      buffer.subarray(4, 4 + paddedLength).set(data);
    }
    return buffer.subarray(0, paddedLength + 4);
  }

  private decodeResponse(response: Uint8Array, expectedMsgId?: string): RpcCallResult {
    if (response.length < 16) {
      throw new Error(`Response too short: ${response.length} bytes`);
    }

    const header = decode(response.subarray(0, 16));

    let offset = 48;
    let responseMsgId: string | undefined;
    if (response.length >= offset + 4) {
      const msgIdLen = new DataView(response.buffer, response.byteOffset + offset, 4).getUint32(0, false);
      if (msgIdLen > 0) {
        const msgIdPadded = Math.ceil(msgIdLen / 4) * 4;
        const msgIdTotal = 4 + msgIdPadded;
        responseMsgId = new TextDecoder().decode(response.subarray(offset + 4, offset + 4 + msgIdLen)).split('\0')[0];
        offset += msgIdTotal;
      }
    }

    let resultValue: Uint8Array | undefined;
    if (response.length >= offset + 4) {
      const resultLen = new DataView(response.buffer, response.byteOffset + offset, 4).getUint32(0, false);
      resultValue = response.subarray(offset + 4, offset + 4 + resultLen);
    }

    console.log(
      `[RPC Client] Received reply: type=${header.type}, status=${header.authType}${responseMsgId ? `, msgId=${responseMsgId}` : ''}`
    );

    return {
      status: header.type === RpcConstants.CallTypes.RPC_REPLY ? RpcConstants.ReplyStates.RPC_ACCEPTED : header.authType,
      value: resultValue,
      msgId: responseMsgId!
    };
  }

  private generateMsgId(): string {
    return crypto.randomUUID().split('-')[0]!;
  }

  private async receiveWithTimeout(connectionId: string, bufferSize: number, timeoutMs: number): Promise<Uint8Array> {
    return this.transport.receive(connectionId, bufferSize);
  }

  getTransport(): TcpTransport {
    return this.transport;
  }

  isConnected(): boolean {
    return this.connected && this.transport.isConnected(this.getConnectionId());
  }
}

export async function createTcpClient(
  prognum: number,
  vers: number,
  host: string = '127.0.0.1',
  port: number = 5000
): Promise<RpcClient> {
  const transport = new TcpTransport(host, port, false);
  const client = new RpcClient(prognum, vers, { transport });
  await client.connect();
  return client;
}
