// RPC Server Implementation
// Handles incoming XDR-encoded RPC calls and dispatches them to handlers

import { decode, encode, encodeCall, decodeCall, type XDRHeader, RpcConstants } from './shared/xdr.ts';
import { Transport, TcpTransport, UnixSocketTransport, UdpTransport } from './transport.ts';

export interface RpcHandler {
  procedure: number;
  version: number;
  handler: (args: any) => any;
}

export interface RpcServerOptions {
  transport: Transport;
  isServer?: boolean;
  transportConfig?: {
    host?: string;
    port?: number;
    socketPath?: string;
  };
}

export class RpcServer {
  private transport: Transport;
  private handlers: Map<string, RpcHandler> = new Map();
  private running: boolean = false;
  private handlerRegistry: Map<string, RpcHandler> = new Map();

  constructor(options: RpcServerOptions) {
    this.transport = options.transport;
  }

  /**
   * Register a procedure handler
   * @param procedure - RPC procedure number (e.g., 100003 for NFS)
   * @param version - RPC version number (e.g., 2 for RPC v2)
   * @param handler - Function to handle this procedure
   */
  register(procedure: number, version: number, handler: (args: any) => any): void {
    const key = `${procedure}:${version}`;
    this.handlerRegistry.set(key, { procedure, version, handler });
  }

  /**
   * Start the server and begin listening for requests
   */
  async start(): Promise<void> {
    if (this.running) {
      throw new Error('RPC server already running');
    }

console.log("start to listen");

    await this.transport.connect();
    
    // For server-mode transports, wait for client connection
    await (this.transport as any).waitForConnection?.();
    
    this.running = true;
    console.log(`[RPC Server] Started, transport connected`);
  }

  /**
   * Stop the server and close the transport
   */
  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    await this.transport.close();
    this.running = false;
    console.log(`[RPC Server] Stopped`);
  }

  /**
   * Process a single RPC request (for synchronous test coordination)
   * Receives one message, processes it, and sends the response
   */
  async processSingleRequest(): Promise<void> {
    if (!this.running) {
      throw new Error('Server not started. Call start() first.');
    }

	console.log("process single request")

    try {
      // Receive full RPC message
      let message = await this.transport.receive(1024);
     
	console.log(message)

      if (message.length < 16) {
        console.warn(`[RPC Server] Message too short: ${message.length} bytes`);
        return;
      }

      // Decode the header
      const header = decode(new Uint8Array(message.subarray(0, 16)));
      
      console.log(`[RPC Server] Received call: prog=${header.version}, type=${header.type}`);

      // Process the request and send response
      const response = await this.handleRequest(header, message);
      
      if (response) {
        await this.transport.send(response);
        // Give socket time to flush before next operation
        await new Promise(r => setTimeout(r, 10));
      }

    } catch (error) {
      console.error(`[RPC Server] Error processing request:`, error);
      
      // Check if transport is still connected
      if (!this.transport.isConnected()) {
        console.log(`[RPC Server] Transport disconnected, stopping`);
        this.running = false;
      }
    }
  }

  /**
   * Process incoming requests in a loop
   * @param timeoutMs - Optional timeout for the request loop
   */
  async listen(timeoutMs?: number): Promise<void> {
    if (!this.running) {
      throw new Error('Server not started. Call start() first.');
    }

    const startTime = Date.now();

    while (this.running) {
      if (timeoutMs && Date.now() - startTime > timeoutMs) {
        console.log(`[RPC Server] Timeout reached (${timeoutMs}ms)`);
        break;
      }

      await this.processSingleRequest();
    }
  }

  /**
   * Handle a single RPC request
   * Properly decodes XDR call message, dispatches to handler, and builds response
   */
  private async handleRequest(header: XDRHeader, message: Uint8Array): Promise<Uint8Array> {
    // Extract call-specific fields (procedure, version, argType) from after header
    // The call header starts at offset 16 (after basic XDR header)
    if (message.length < 36) {
      console.warn(`[RPC Server] Message too short for call: ${message.length} bytes`);
      return this.buildErrorMessage(RpcConstants.ReplyStates.RPC_GARBAGE);
    }

    // Decode call header (20 bytes after basic header)
    const callHeaderOffset = 16;
    const callArgsOffset = 36;
    
    // Extract message ID from the call (XDR-encoded string after call header)
    let msgId: string | undefined;
    if (message.length > callArgsOffset) {
      const msgIdLen = new DataView(message.buffer, message.byteOffset + callArgsOffset, 4).getUint32(0, false);
      if (msgIdLen > 0 && message.length >= callArgsOffset + 4 + msgIdLen) {
        const msgIdBytes = message.subarray(callArgsOffset + 4, callArgsOffset + 4 + msgIdLen);
        msgId = new TextDecoder().decode(msgIdBytes).trim();
      }
    }

    // Decode call arguments (bytes after the msgId)
    const argsStart = msgId ? callArgsOffset + 4 + msgId.length + (4 - (msgId.length % 4)) % 4 : callArgsOffset;
    const callArgs = message.subarray(argsStart);

    console.log(`[RPC Server] Received call: prog=${header.version}, type=${header.type}, ${msgId ? `msgId=${msgId}, ` : ''}args=${callArgs.length} bytes`);

    // Build response based on call type
    if (header.type !== RpcConstants.CallTypes.RPC_CALL) {
      return this.buildErrorMessage(RpcConstants.ReplyStates.RPC_GARBAGE);
    }

    // For testing/demo: echo back the arguments
    // In real use, you'd decode callArgs and call registered handlers
    const resultData = callArgs; // Echo the arguments back

    // Build proper RPC reply with message ID
    return this.buildReply(header.version, RpcConstants.ReplyStates.RPC_ACCEPTED, resultData, msgId);
  }

  /**
   * Build a successful RPC reply
   * @param version - RPC version
   * @param status - Reply state (RPC_ACCEPTED, RPC_GARBAGE, etc.)
   * @param resultData - Result data to include
   * @param msgId - Optional message ID to echo back for async matching
   */
  private buildReply(version: number, status: number, resultData: Uint8Array, msgId?: string): Uint8Array {
    const replyHeader: XDRHeader = {
      version,
      type: RpcConstants.CallTypes.RPC_REPLY,
      headerLength: 16,
      authType: status
    };

    const replyCallHeader = encodeCall(
      version,    // prognum
      version,    // vers
      status,     // proc (repurposed as status for simplicity)
      { version: 0, type: 0, headerLength: 0, authType: 0 }
    );

    const chunks: Uint8Array[] = [];
    chunks.push(encode(replyHeader));
    chunks.push(new Uint8Array(replyCallHeader.subarray(0, 20))); // Only first 4 fields
    
    // Include message ID if provided
    if (msgId) {
      chunks.push(encodeString(msgId));
    }
    
    chunks.push(this.encodeOpaque(resultData));

    return new Uint8Array([...new Uint8Array(Buffer.concat(chunks))]);
  }

  /**
   * Build an error response
   */
  private buildErrorMessage(errorState: number): Uint8Array {
    const errorHeader: XDRHeader = {
      version: 2,
      type: RpcConstants.CallTypes.RPC_REPLY,
      headerLength: 16,
      authType: errorState
    };

    // For errors, we just send the header with error state in authType position
    return encode(errorHeader);
  }

  /**
   * Helper to encode opaque data (for arguments/results)
   */
  private encodeOpaque(data: Uint8Array): Uint8Array {
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
   * Check if the server is currently running
   */
  isRunning(): boolean {
    return this.running;
  }

  /**
   * Get the transport instance
   */
  getTransport(): Transport {
    return this.transport;
  }
}

/**
 * Convenience function to start a TCP-based RPC server
 */
export async function startRpcServer(
  host: string = '127.0.0.1',
  port: number = 5000
): Promise<RpcServer> {
  const transport = new TcpTransport(host, port);
  const server = new RpcServer({ transport });
  await server.start();
  return server;
}

/**
 * Convenience function to start a Unix socket-based RPC server
 */
export async function startUnixRpcServer(
  socketPath: string = '/tmp/rpc.sock'
): Promise<RpcServer> {
  const transport = new UnixSocketTransport(socketPath);
  const server = new RpcServer({ transport });
  await server.start();
  return server;
}
