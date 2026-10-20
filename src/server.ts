// RPC Server Implementation
// Handles incoming XDR-encoded RPC calls and dispatches them to handlers

import { decode, encode, encodeCall, decodeCall, encodeString, type XDRHeader, RpcConstants } from './shared/xdr.ts';
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

export interface QueuedRequest {
  proc: number;
  args: Uint8Array;
  msgId: string | undefined;
}

export class RpcServer {
  private transport: Transport;
  private handlers: Map<string, RpcHandler> = new Map();
  private running: boolean = false;
  private handlerRegistry: Map<string, RpcHandler> = new Map();
  
  private pendingRequests = new Map<string, QueuedRequest>();
  
  // Buffered responses waiting to be sent
  private responseBuffer = new Map<string, Uint8Array>();

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
   * Receive and queue a single RPC request without processing
   * Use this for non-blocking mode - call queueAndProcess later to handle it
   * @returns The msgId of the queued request, or undefined if receive failed
   */
  async queueIncomingRequest(): Promise<string | undefined> {
    if (!this.running) {
      throw new Error('Server not started. Call start() first.');
    }

    try {
      // Receive full RPC message
      const message = await this.transport.receive(1024);
      
      if (message.length < 16) {
        console.warn(`[RPC Server] Message too short: ${message.length} bytes`);
        return undefined;
      }

      // Decode the header
      const header = decode(new Uint8Array(message.subarray(0, 16)));
      
      // Extract proc from call header at offset 16
      // Call header format from encodeCall: prognum(4) + vers(4) + proc(4) + reserved(4) + authHeader(16) = 32 bytes
      const callHeader = new DataView(message.buffer, message.byteOffset + 16, 32);
      const proc = callHeader.getUint32(8, false);  // proc is at offset 8 within call header
      
      // Extract msgId if present (after call header, at offset 16 + 32 = 48)
      let msgId: string | undefined;
      let msgIdLen: number = 0;
      const callHeaderOffset = 48; // After 16-byte header + 32-byte call header
      if (message.length > callHeaderOffset) {
        msgIdLen = new DataView(message.buffer, message.byteOffset + callHeaderOffset, 4).getUint32(0, false);
        if (msgIdLen > 0 && message.length >= callHeaderOffset + 4 + msgIdLen) {
          const msgIdBytes = message.subarray(callHeaderOffset + 4, callHeaderOffset + 4 + msgIdLen);
          msgId = new TextDecoder().decode(msgIdBytes).trim();
        }
      }
      
      // Extract arguments (after msgId)
      const argsStart = msgId ? callHeaderOffset + 4 + msgIdLen + ((4 - (msgIdLen % 4)) % 4) : callHeaderOffset;
      const args = message.subarray(argsStart);

      console.log(`[RPC Server] Queued incoming: proc=${proc}, ${msgId ? `msgId=${msgId}, ` : ''}args=${args.length} bytes`);

      // Queue the request
      const requestId = await this.queueRequest(proc, args, msgId);
      return requestId;

    } catch (error) {
      console.error(`[RPC Server] Error receiving request:`, error);
      if (!this.transport.isConnected()) {
        this.running = false;
      }
      return undefined;
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
   * Queue an RPC request for async processing
   * Returns immediately, handler can process later
   */
  async queueRequest(proc: number, args: Uint8Array, msgId?: string): Promise<string> {
    const requestId = msgId || crypto.randomUUID().split('-')[0];
    
    this.pendingRequests.set(requestId, { proc, args, msgId: requestId });
    console.log(`[RPC Server] Queued request: proc=${proc}, msgId=${requestId}, args=${args.length} bytes`);
    
    return requestId;
  }

  /**
   * Process a queued request and build response
   * @param msgId - The message ID of the request to process
   * @returns The response buffer, or undefined if no request with that msgId
   */
  async processQueuedRequest(msgId: string): Promise<Uint8Array | undefined> {
    const request = this.pendingRequests.get(msgId);
    if (!request) {
      console.warn(`[RPC Server] No pending request found for msgId=${msgId}`);
      return undefined;
    }
    
    this.pendingRequests.delete(msgId);
    console.log(`[RPC Server] Processing queued request: proc=${request.proc}, msgId=${msgId}`);
    
    // Build the response
    const response = await this.buildResponseForRequest(request.proc, request.args, msgId);
    
    // Buffer the response for later sending
    this.responseBuffer.set(msgId, response);
    
    return response;
  }

  /**
   * Send all buffered responses in order
   * @returns Number of responses sent
   */
  async flushResponses(): Promise<number> {
    if (this.responseBuffer.size === 0) {
      return 0;
    }
    
    const sentCount = this.responseBuffer.size;
    console.log(`[RPC Server] Flushing ${sentCount} buffered response(s)`);
    
    for (const [msgId, response] of this.responseBuffer) {
      try {
        await this.transport.send(response);
        console.log(`[RPC Server] Sent response for msgId=${msgId}`);
      } catch (error) {
        console.error(`[RPC Server] Failed to send response for msgId=${msgId}:`, error);
      }
    }
    
    this.responseBuffer.clear();
    return sentCount;
  }

  /**
   * Send a specific buffered response by msgId
   * @param msgId - The message ID of the response to send
   * @returns true if sent, false if not found
   */
  async sendResponse(msgId: string): Promise<boolean> {
    const response = this.responseBuffer.get(msgId);
    if (!response) {
      console.warn(`[RPC Server] No buffered response found for msgId=${msgId}`);
      return false;
    }
    
    this.responseBuffer.delete(msgId);
    await this.transport.send(response);
    console.log(`[RPC Server] Sent buffered response for msgId=${msgId}`);
    return true;
  }

  /**
   * Process all queued requests and send their buffered responses
   * @param batchSize - Max number of requests to process per call (0 = all)
   * @returns { processed: number, sent: number }
   */
  async processAndFlush(batchSize = 0): Promise<{ processed: number; sent: number }> {
    let processed = 0;
    const maxBatch = batchSize === 0 ? this.pendingRequests.size : batchSize;
    
    // Process queued requests
    for (let i = 0; i < maxBatch && this.pendingRequests.size > 0; i++) {
      const entry = this.pendingRequests.entries().next().value;
      if (entry) {
        const [msgId, { proc, args }] = entry;
        await this.processQueuedRequest(msgId);
        processed++;
      }
    }
    
    // Send buffered responses
    let sent = 0;
    for (const [msgId, response] of this.responseBuffer) {
      try {
        await this.transport.send(response);
        console.log(`[RPC Server] Sent response for msgId=${msgId}`);
        sent++;
      } catch (error) {
        console.error(`[RPC Server] Failed to send response for msgId=${msgId}:`, error);
      }
    }
    this.responseBuffer.clear();
    
    return { processed, sent };
  }

  /**
   * Get statistics about pending requests and buffered responses
   */
  getStats(): { pendingRequests: number; bufferedResponses: number } {
    return {
      pendingRequests: this.pendingRequests.size,
      bufferedResponses: this.responseBuffer.size
    };
  }

  /**
   * Build a response for a request
   * @param proc - Procedure number
   * @param args - Request arguments
   * @param msgId - Message ID to echo back
   */
  private async buildResponseForRequest(proc: number, args: Uint8Array, msgId: string | undefined): Promise<Uint8Array> {
    // Look up the handler for this procedure
    const key = `${proc}:2`; // Assuming version 2 for now
    const handler = this.handlerRegistry.get(key);
    
    if (!handler) {
      console.warn(`[RPC Server] No handler for proc=${proc}, returning RPC_PROCUNAV`);
      return this.buildReply(2, RpcConstants.ReplyStates.SYSERR, new Uint8Array(0), msgId!);
    }
    
    try {
      // For now, echo back the arguments (simple demo)
      // In real use: call handler.handler(decodedArgs)
      const resultData = args;
      return this.buildReply(2, RpcConstants.ReplyStates.RPC_ACCEPTED, resultData, msgId);
    } catch (error) {
      console.error(`[RPC Server] Handler error for proc=${proc}:`, error);
      return this.buildReply(2, RpcConstants.ReplyStates.SYSERR, new Uint8Array(0), msgId!);
    }
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
  const transport = new TcpTransport(host, port, true); // Server mode!
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
