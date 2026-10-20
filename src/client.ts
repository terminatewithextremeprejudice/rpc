// RPC Client Implementation
// Encodes RPC calls and sends them through the configured transport

import { encode, decode, encodeCall, encodeString, type XDRHeader, RpcConstants } from './shared/xdr.ts';
import { Transport, TcpTransport } from './transport.ts';

export interface RpcClientOptions {
  transport: Transport;
  timeout?: number; // milliseconds
}

export interface RpcCallResult {
  status: number; // RPC_ACCEPTED (0), RPC_GARBAGE (1), etc.
  value: any; // The return value from the procedure
  msgId?: string; // Message ID for async response matching
}

export interface AsyncRpcHandle {
  _async: true;
  msgId: string;
  proc: number;
}

export class RpcClient {
  private transport: Transport;
  private prognum: number; // Program number (e.g., 100003 for NFS)
  private vers: number; // Program version
  private timeout: number;
  private connected: boolean = false;
  
  // Async response cache: msgId -> Buffer
  private pendingResponses = new Map<string, Buffer>();
  private pendingLock = false;

  constructor(prognum: number, vers: number, options: RpcClientOptions) {
    this.prognum = prognum;
    this.vers = vers;
    this.transport = options.transport;
    this.timeout = options.timeout || 5000;
  }

  /**
   * Connect to the RPC server
   */
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      await this.transport.connect();
      this.connected = true;
      console.log(`[RPC Client] Connected to server`);
    } catch (error) {
      this.connected = false;
      throw new Error(`Connection failed: ${error}`);
    }
  }

  /**
   * Disconnect from the RPC server
   */
  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    await this.transport.close();
    this.connected = false;
    console.log(`[RPC Client] Disconnected from server`);
  }

  /**
   * Make an RPC call
   * @param proc - Procedure number
   * @param args - Arguments to pass (simplified - assumes single opaque argument for now)
   * @param options - Optional settings for the call
   * @returns The result from the procedure (sync) or an async handle (async mode)
   */
  async call(proc: number, args: Uint8Array = new Uint8Array(0), options?: {
    async?: boolean;      // Fire-and-forget mode
    msgId?: string;       // Custom message ID (if not provided, auto-generated)
    timeout?: number;     // Override default timeout
  }): Promise<RpcCallResult | AsyncRpcHandle> {
    if (!this.connected) {
      await this.connect();
    }

    if (!this.transport.isConnected()) {
      throw new Error('Not connected to server');
    }

    const useAsync = options?.async === true;
    const callMsgId = options?.msgId || this.generateMsgId();
    const callTimeout = options?.timeout ?? this.timeout;

    // Encode the RPC call message with arguments
    const callHeader: XDRHeader = {
      version: RpcConstants.RPC_VERSION,
      type: RpcConstants.CallTypes.RPC_CALL,
      headerLength: 16,
      authType: RpcConstants.AuthFlavors.AUTH_NULL
    };

    const encodedCallHeader = encodeCall(
      this.prognum, // prognum
      this.vers,    // vers
      proc,          // proc
      callHeader
    );

    // Append message ID to the call header (XDR-encoded string)
    const encodedMsgId = encodeString(callMsgId as string);

    // Append arguments as opaque data
    const encodedArgs = this.encodeOpaqueArgs(args);
    
    // Full call = header + callHeader + msgId + args
    const fullCall = new Uint8Array([...new Uint8Array(16), ...encodedCallHeader, ...encodedMsgId, ...encodedArgs]);
    
    const bufferSize = 16 + 20 + 4 + encodedMsgId.length + args.length + 20; // Estimate response size

    console.log(`[RPC Client] Sending CALL: prog=${this.prognum}, vers=${this.vers}, proc=${proc}, msgId=${callMsgId}, args=${args.length} bytes`);

    // Send the request
    await this.transport.send(fullCall);

    if (useAsync) {
      // Fire-and-forget: return handle immediately
      return { _async: true, msgId: callMsgId, proc } as AsyncRpcHandle;
    }

    // Wait for response with timeout
    const response = await this.receiveWithTimeout(bufferSize, callTimeout);

    // Decode and return
    return this.decodeResponse(response, callMsgId);
  }

  /**
   * Fire-and-forget RPC call (shorthand)
   * @param proc - Procedure number
   * @param args - Arguments
   * @param msgId - Optional custom message ID
   * @returns Async handle for later retrieval
   */
  async fire(proc: number, args: Uint8Array = new Uint8Array(0), msgId?: string): Promise<AsyncRpcHandle> {
    return this.call(proc, args, { async: true, msgId }) as Promise<AsyncRpcHandle>;
  }

  /**
   * Get result for a previously fired async call
   * @param msgId - The message ID from the async handle
   * @param timeout - How long to wait for the response (ms)
   * @returns The RPC call result
   */
  async getResponse(msgId: string, timeout: number = 5000): Promise<RpcCallResult> {
    // Check cache first (server might have already responded)
    if (this.pendingResponses.has(msgId)) {
      const responseBuffer = this.pendingResponses.get(msgId)!;
      this.pendingResponses.delete(msgId);
      return this.decodeResponse(new Uint8Array(responseBuffer), msgId);
    }

    // Wait for response with timeout
    const bufferSize = 2048; // Typical response size
    const response = await this.receiveWithTimeout(bufferSize, timeout);
    
    // Store in cache and return
    this.pendingResponses.set(msgId, Buffer.from(response));
    return this.decodeResponse(response, msgId);
  }

  /**
   * Collect all pending async responses
   * @param timeout - How long to wait for new responses (ms)
   * @returns Map of msgId -> RpcCallResult for all ready responses
   */
  async collectResponses(timeout: number = 1000): Promise<Map<string, RpcCallResult>> {
    const results = new Map<string, RpcCallResult>();

    // First, check in-memory cache
    for (const [msgId, responseBuffer] of this.pendingResponses) {
      results.set(msgId, this.decodeResponse(new Uint8Array(responseBuffer), msgId));
      this.pendingResponses.delete(msgId);
    }

    // Then poll the transport for any new responses
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      try {
        // Try to receive with short timeout
        const chunk = await new Promise<Buffer>((resolve) => {
          const transport = this.getTransport() as any;
          if (transport.socket && transport.socket.on) {
            const onData = (data: Buffer) => {
              transport.socket.removeListener('data', onData);
              resolve(data);
            };
            transport.socket.once('data', onData);
            setTimeout(() => resolve(Buffer.alloc(0)), 100);
          } else {
            setTimeout(() => resolve(Buffer.alloc(0)), 100);
          }
        });

        if (chunk.length > 0) {
          const response = new Uint8Array(chunk);
          const header = decode(response.subarray(0, 16));
          // Extract msgId from response if present
          if (response.length > 36) {
            // Response has msgId after call header (16 + 20 = 36)
            const msgIdBytes = response.subarray(36, Math.min(response.length, 40));
            const msgId = new TextDecoder().decode(msgIdBytes).trim();
            if (msgId && !results.has(msgId)) {
              this.pendingResponses.set(msgId, chunk);
              results.set(msgId, this.decodeResponse(response, msgId));
              this.pendingResponses.delete(msgId);
            }
          }
        }
      } catch (err) {
        // No data available, continue polling
      }
      if (Date.now() >= deadline) break;
    }

    return results;
  }

  /**
   * Encode opaque arguments for RPC call
   */
  private encodeOpaqueArgs(data: Uint8Array): Uint8Array {
    const paddedLength = data.length > 0 ? Math.ceil(data.length / 4) * 4 : 0;
    const buffer = new Uint8Array(paddedLength + 4);
    
    // Write length prefix
    const view = new DataView(buffer.buffer, 0, 4);
    view.setUint32(0, data.length, false);
    
    // Write padded data
    if (paddedLength > 0) {
      buffer.subarray(4, 4 + paddedLength).set(data);
      buffer.subarray(4 + data.length, 4 + paddedLength).fill(0);
    }
    
    return buffer.subarray(0, paddedLength + 4);
  }

  /**
   * Decode the RPC response
   */
  private decodeResponse(response: Uint8Array, expectedMsgId?: string): RpcCallResult {
    if (response.length < 16) {
      throw new Error(`Response too short: ${response.length} bytes`);
    }

    const header = decode(response.subarray(0, 16));

    // Extract msgId from response if present (after call header: 16 + 20 = 36)
    let responseMsgId: string | undefined;
    if (response.length > 36) {
      // Response has msgId after call header
      const msgIdLength = new DataView(response.buffer, response.byteOffset + 36, 4).getUint32(0, false);
      const msgIdBytes = response.subarray(40, 40 + msgIdLength);
      responseMsgId = new TextDecoder().decode(msgIdBytes).trim();
    }

    if (expectedMsgId && responseMsgId && responseMsgId !== expectedMsgId) {
      console.warn(`[RPC Client] Message ID mismatch: expected ${expectedMsgId}, got ${responseMsgId}`);
      // Cache this response anyway, it might be for a pending async call
      if (responseMsgId) {
        this.pendingResponses.set(responseMsgId, Buffer.from(response));
      }
    }

    console.log(`[RPC Client] Received reply: type=${header.type}, status=${header.authType}${responseMsgId ? `, msgId=${responseMsgId}` : ''}`);

    return {
      status: header.type === RpcConstants.CallTypes.RPC_REPLY 
        ? RpcConstants.ReplyStates.RPC_ACCEPTED 
        : header.authType,
      value: null, // For now, we don't decode the actual return value
      msgId: responseMsgId
    };
  }

  /**
   * Generate a unique message ID
   */
  private generateMsgId(): string|undefined {
    return crypto.randomUUID().split('-')[0]; // Short UUID (first 8 hex chars)
  }

  /**
   * Receive data with timeout
   */
  private async receiveWithTimeout(bufferSize: number, timeoutMs: number): Promise<Uint8Array> {
    const chunks: Uint8Array[] = [];
    let received = 0;

    return new Promise<Uint8Array>((resolve, reject) => {
      const transport = this.getTransport() as any;
      const socket = transport.socket;

      if (!socket) {
        reject(new Error('Transport not connected'));
        return;
      }

      const onData = (chunk: Buffer) => {
        chunks.push(Buffer.from(chunk));
        received += chunk.length;
        if (received >= bufferSize) {
          socket.removeListener('data', onData);
          socket.removeListener('error', onError);
          clearTimeout(timeoutHandle);
          resolve(new Uint8Array(Buffer.concat(chunks, bufferSize)));
        }
      };

      const onError = (err: Error) => {
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
        clearTimeout(timeoutHandle);
        reject(err);
      };

      socket.on('data', onData);
      socket.once('error', onError);

      const timeoutHandle = setTimeout(() => {
        socket.removeListener('data', onData);
        socket.removeListener('error', onError);
        if (chunks.length > 0) {
          resolve(new Uint8Array(Buffer.concat(chunks)));
        } else {
          reject(new Error(`Receive timeout after ${timeoutMs}ms`));
        }
      }, timeoutMs);
    });
  }

  /**
   * Call with string argument
   */
  async callWithString(proc: number, arg: string): Promise<RpcCallResult> {
    const encodedArg = encodeString(arg);
    return this.call(proc, encodedArg) as Promise<RpcCallResult>;
  }

  /**
   * Call with opaque bytes argument
   */
  async callWithBytes(proc: number, arg: Uint8Array): Promise<RpcCallResult> {
    return this.call(proc, arg) as Promise<RpcCallResult>;
  }

  /**
   * Check if the client is connected
   */
  isConnected(): boolean {
    return this.connected && this.transport.isConnected();
  }

  /**
   * Get the transport instance
   */
  getTransport(): Transport {
    return this.transport;
  }
}

/**
 * Convenience function to create a TCP-based RPC client
 */
export async function createTcpClient(
  prognum: number,
  vers: number,
  host: string = '127.0.0.1',
  port: number = 5000
): Promise<RpcClient> {
  const transport = new TcpTransport(host, port);
  const client = new RpcClient(prognum, vers, { transport });
  await client.connect();
  return client;
}
