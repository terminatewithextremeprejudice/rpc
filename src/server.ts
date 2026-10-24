// RPC Server Implementation
// Handles incoming XDR-encoded RPC calls with multi-client support

import { decode, encode, encodeCall, decodeCall, encodeString, decodeOpaque, type XDRHeader, RpcConstants } from './shared/xdr';
import { TcpTransport } from './transport';

export interface RpcHandler {
  procedure: number;
  version: number;
  handler: (args: any) => any;
}

export interface RpcServerOptions {
  transport: TcpTransport;
  connectionId?: string; // For client mode
  isServer?: boolean;
}

export interface QueuedRequest {
  connectionId: string;
  proc: number;
  args: Uint8Array;
  msgId: string;
}

export class RpcServer {
  private transport: TcpTransport;
  private running: boolean = false;
  private handlerRegistry: Map<string, RpcHandler> = new Map();
  
  // Pending requests indexed by msgId: { connectionId, proc, args, msgId }
  private pendingRequests: Map<string, QueuedRequest> = new Map();
  
  // Buffered responses ready to be sent: msgId -> { connectionId, data }
  private responseBuffer: Map<string, { connectionId: string; data: Uint8Array }> = new Map();

  constructor(options: RpcServerOptions) {
    this.transport = options.transport;
  }

  register(procedure: number, version: number, handler: (args: any) => any): void {
    const key = `${procedure}:${version}`;
    this.handlerRegistry.set(key, { procedure, version, handler });
  }

  async start(waitForConnection: boolean = true): Promise<void> {
    if (this.running) {
      throw new Error('RPC server already running');
    }

    console.log("Starting RPC server...");
    await this.transport.connect();

    if (waitForConnection) {
      const connId = await this.transport.acceptConnection();
      console.log(`[RPC Server] Connection accepted: ${connId}`);
    }
    
    this.running = true;
    console.log(`[RPC Server] Started and ready`);
  }

  async stop(): Promise<void> {
    if (!this.running) return;

    await this.transport.shutdown();
    this.running = false;
    this.pendingRequests.clear();
    this.responseBuffer.clear();
    console.log(`[RPC Server] Stopped`);
  }

  /**
   * Queue an incoming request from a specific connection
   * @param connectionId - The connection ID that sent this request
   * @returns The msgId of the queued request
   */
  async queueIncomingRequest(connectionId: string): Promise<string | undefined> {
    if (!this.running) {
      throw new Error('Server not started. Call start() first.');
    }

    try {
      const message = await this.transport.receive(connectionId, 1024);
      
      if (message.length < 16) {
        console.warn(`[RPC Server] Message too short: ${message.length} bytes`);
        return undefined;
      }

      const header = decode(new Uint8Array(message.subarray(0, 16)));
      const callHeader = new DataView(message.buffer, message.byteOffset + 16, 32);
      const proc = callHeader.getUint32(8, false);

      let msgId: string | undefined;
      let msgIdLen: number = 0;
      const callHeaderOffset = 48;
      if (message.length > callHeaderOffset + 4) {
        msgIdLen = new DataView(message.buffer, message.byteOffset + callHeaderOffset, 4).getUint32(0, false);
        if (msgIdLen > 0 && message.length >= callHeaderOffset + 4 + msgIdLen) {
          const msgIdBytes = message.subarray(callHeaderOffset + 4, callHeaderOffset + 4 + msgIdLen);
          msgId = new TextDecoder().decode(msgIdBytes).split('\0')[0];
        }
      }

      // Extract arguments
      const argsStart = msgIdLen > 0 ? callHeaderOffset + 4 + msgIdLen : callHeaderOffset;
      const rawArgs = message.subarray(argsStart);
      const args = decodeOpaque(rawArgs);

      const requestId = msgId || crypto.randomUUID().split('-')[0]!;
      
      this.pendingRequests.set(requestId, { connectionId, proc, args, msgId: requestId });
      console.log(`[RPC Server] Queued: conn=${connectionId}, proc=${proc}, msgId=${requestId}`);

      return requestId;

    } catch (error) {
      console.error(`[RPC Server] Error receiving request from ${connectionId}:`, error);
      if (!this.transport.isConnected(connectionId)) {
        this.running = false;
      }
      return undefined;
    }
  }

  /**
   * Receive the next ready request from any connection
   * @returns { connectionId, msgId, proc, args } for the first available request, or undefined if none ready
   */
  async receiveNext(): Promise<{ connectionId: string; msgId: string; proc: number; args: Uint8Array } | undefined> {
    if (!this.running) {
      throw new Error('Server not started. Call start() first.');
    }

    // Try to receive from each connection until we find one with a message
    for (const [connId] of this.transport.getConnections()) {
      try {
        // Check if this connection has a complete message (non-blocking peek)
        // For now, we try receive and catch if no message is available
        const message = await this.transport.receive(connId, 1024);
        
        // Parse the message
        if (message.length < 16) {
          continue;
        }

        const header = decode(new Uint8Array(message.subarray(0, 16)));
        const callHeader = new DataView(message.buffer, message.byteOffset + 16, 32);
        const proc = callHeader.getUint32(8, false);

        let msgId: string | undefined;
        let msgIdLen: number = 0;
        const callHeaderOffset = 48;
        if (message.length > callHeaderOffset + 4) {
          msgIdLen = new DataView(message.buffer, message.byteOffset + callHeaderOffset, 4).getUint32(0, false);
          if (msgIdLen > 0 && message.length >= callHeaderOffset + 4 + msgIdLen) {
            const msgIdBytes = message.subarray(callHeaderOffset + 4, callHeaderOffset + 4 + msgIdLen);
            msgId = new TextDecoder().decode(msgIdBytes).split('\0')[0];
          }
        }

        // Extract arguments
        const argsStart = msgIdLen > 0 ? callHeaderOffset + 4 + msgIdLen : callHeaderOffset;
        const args = decodeOpaque(message.subarray(argsStart));

        const requestId = msgId || crypto.randomUUID().split('-')[0]!;
        
        this.pendingRequests.set(requestId, { connectionId: connId, proc, args, msgId: requestId });
        console.log(`[RPC Server] Received: conn=${connId}, proc=${proc}, msgId=${requestId}`);

        return { connectionId: connId, msgId: requestId, proc, args };

      } catch (error: any) {
        // This connection didn't have a message ready, try the next one
        if (!error.message?.includes('timeout') && !error.message?.includes('complete message')) {
          console.error(`[RPC Server] Error receiving from ${connId}:`, error);
        }
        continue;
      }
    }

    // No messages ready from any connection
    console.log(`[RPC Server] No requests ready from any connection`);
    return undefined;
  }

  /**
   * Process a queued request and build response
   * @param msgId - The message ID of the request to process
   */
  async processQueuedRequest(msgId: string): Promise<Uint8Array | undefined> {
    const request = this.pendingRequests.get(msgId);
    if (!request) {
      console.warn(`[RPC Server] No pending request found for msgId=${msgId}`);
      return undefined;
    }
    
    this.pendingRequests.delete(msgId);
    console.log(`[RPC Server] Processing: conn=${request.connectionId}, proc=${request.proc}, msgId=${msgId}`);
    
    const response = await this.buildResponseForRequest(request.proc, request.args, msgId);
    
    // Buffer the response with the correct connection ID
    this.responseBuffer.set(msgId, { connectionId: request.connectionId, data: response });
    
    return response;
  }

  /**
   * Send all buffered responses to their respective connections
   * @returns Number of responses sent
   */
  async flushResponses(): Promise<number> {
    if (this.responseBuffer.size === 0) {
      return 0;
    }
    
    const sentCount = this.responseBuffer.size;
    console.log(`[RPC Server] Flushing ${sentCount} buffered response(s)`);
    
    let actualSent = 0;
    for (const [msgId, { connectionId, data }] of this.responseBuffer) {
      try {
        await this.transport.send(connectionId, data);
        console.log(`[RPC Server] Sent response for msgId=${msgId} to conn=${connectionId}`);
        actualSent++;
      } catch (error) {
        console.error(`[RPC Server] Failed to send response for msgId=${msgId} to ${connectionId}:`, error);
      }
    }
    
    this.responseBuffer.clear();
    return actualSent;
  }

  /**
   * Process queued requests and send responses together
   * @param connectionId - Only process requests from this connection (optional)
   * @param batchSize - Max number of requests to process per call (0 = all)
   * @returns { processed: number, sent: number }
   */
  async processAndFlush(connectionId?: string, batchSize = 0): Promise<{ processed: number; sent: number }> {
    let processed = 0;
    let sent = 0;
    const maxBatch = batchSize === 0 ? this.pendingRequests.size : batchSize;
    
    // Filter requests by connection if specified
    const requestsToProcess: string[] = [];
    for (const [msgId, req] of this.pendingRequests) {
      if (connectionId && req.connectionId !== connectionId) continue;
      if (requestsToProcess.length < maxBatch) {
        requestsToProcess.push(msgId);
      }
    }

    // Process all queued requests
    for (const msgId of requestsToProcess) {
      const request = this.pendingRequests.get(msgId);
      if (request) {
        this.pendingRequests.delete(msgId);
        const response = await this.buildResponseForRequest(request.proc, request.args, msgId);
        this.responseBuffer.set(msgId, { connectionId: request.connectionId, data: response });
        processed++;
      }
    }
    
    // Send buffered responses to their respective connections
    for (const [msgId, { connectionId: connId, data }] of this.responseBuffer) {
      try {
        await this.transport.send(connId, data);
        console.log(`[RPC Server] Sent response for msgId=${msgId} to conn=${connId}`);
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
  getStats(): { pendingRequests: number; bufferedResponses: number; activeConnections: number } {
    return {
      pendingRequests: this.pendingRequests.size,
      bufferedResponses: this.responseBuffer.size,
      activeConnections: this.transport.getConnectionCount()
    };
  }

  /**
   * Check if the server is currently running
   */
  isRunning(): boolean {
    return this.running;
  }

   /**
   * Check if a connection has a complete message ready (non-blocking)
   */
  hasMessageReady(connectionId: string): boolean {
    const connection = this.transport.getConnectionById(connectionId);
    if (!connection) return false;
    return connection.hasCompleteMessage();
  }

  /**
   * Get list of connection IDs that have messages ready
   */
  getReadyConnections(): string[] {
    const ready: string[] = [];
    for (const [connId, conn] of this.transport.getConnections()) {
      if (conn.hasCompleteMessage()) {
        ready.push(connId);
      }
    }
    return ready;
  }

  /**
   * Build a response for a request
   */
  private async buildResponseForRequest(proc: number, args: Uint8Array, msgId: string | undefined): Promise<Uint8Array> {
    const key = `${proc}:2`; // Assuming version 2 for now
    const handler = this.handlerRegistry.get(key);
    
    if (!handler) {
      console.warn(`[RPC Server] No handler for proc=${proc}`);
      return this.buildReply(2, RpcConstants.ReplyStates.SYSERR, new Uint8Array(0), msgId!);
    }
    
    try {
      const resultData = handler.handler(args);
      return this.buildReply(2, RpcConstants.ReplyStates.RPC_ACCEPTED, resultData, msgId);
    } catch (error) {
      console.error(`[RPC Server] Handler error for proc=${proc}:`, error);
      return this.buildReply(2, RpcConstants.ReplyStates.SYSERR, new Uint8Array(0), msgId!);
    }
  }

  /**
   * Build a successful RPC reply
   */
  private buildReply(version: number, status: number, resultData: Uint8Array, msgId?: string): Uint8Array {
    const replyHeader: XDRHeader = {
      version,
      type: RpcConstants.CallTypes.RPC_REPLY,
      headerLength: 16,
      authType: status
    };

    const replyCallHeader = encodeCall(
      version,
      version,
      status,
      { version: 0, type: 0, headerLength: 0, authType: 0 }
    );

    const chunks: Uint8Array[] = [];
    chunks.push(encode(replyHeader));
    chunks.push(new Uint8Array(replyCallHeader.subarray(0, 32)));
    
    if (msgId) {
      chunks.push(encodeString(msgId));
    }
    
    // Encode result as opaque
    const paddedLength = resultData.length > 0 ? Math.ceil(resultData.length / 4) * 4 : 0;
    const resultBuffer = new Uint8Array(paddedLength + 4);
    const view = new DataView(resultBuffer.buffer, 0, 4);
    view.setUint32(0, resultData.length, false);
    if (paddedLength > 0) {
      resultBuffer.subarray(4, 4 + paddedLength).set(resultData);
    }
    chunks.push(resultBuffer);

    return new Uint8Array([...new Uint8Array(Buffer.concat(chunks))]);
  }

  /**
   * Get the transport instance
   */
  getTransport(): TcpTransport {
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
  const transport = new TcpTransport(host, port, true);
  const server = new RpcServer({ transport });
  await server.start();
  return server;
}
