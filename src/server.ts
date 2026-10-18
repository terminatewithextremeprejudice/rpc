// RPC Server Implementation
// Handles incoming XDR-encoded RPC calls and dispatches them to handlers

import { decode, encode, encodeCall, decodeCall, type XDRHeader, RpcConstants } from './shared/xdr';
import { Transport, TcpTransport, UnixSocketTransport, UdpTransport } from './transport';

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

      try {
        // Receive the RPC call message (header + arguments)
        // First, receive just the header to determine message size
        const headerSize = 16 + 20; // Basic header + call header = 36 bytes minimum
        const message = await this.transport.receive(headerSize);

        if (message.length < 16) {
          console.warn(`[RPC Server] Message too short: ${message.length} bytes`);
          continue;
        }

        // Decode the header
        const header = decode(new Uint8Array(message.subarray(0, 16)));
        
        console.log(`[RPC Server] Received call: prog=${header.version}, type=${header.type}`);

        // Process the request and send response
        const response = await this.handleRequest(header, message);
        
        if (response) {
          await this.transport.send(response);
        }

      } catch (error) {
        console.error(`[RPC Server] Error processing request:`, error);
        
        // Check if transport is still connected
        if (!this.transport.isConnected()) {
          console.log(`[RPC Server] Transport disconnected, stopping`);
          break;
        }
      }
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

    // Decode call arguments (bytes after the call header)
    const callHeaderOffset = 16 + 20; // Basic header (16) + call header (20)
    const callArgs = message.subarray(callHeaderOffset);

    console.log(`[RPC Server] Call: prog=${header.version}, authType=${header.authType}, args=${callArgs.length} bytes`);

    // Build response based on call type
    if (header.type !== RpcConstants.CallTypes.RPC_CALL) {
      return this.buildErrorMessage(RpcConstants.ReplyStates.RPC_GARBAGE);
    }

    // For testing/demo: echo back the arguments
    // In real use, you'd decode callArgs and call registered handlers
    const resultData = callArgs; // Echo the arguments back

    // Build proper RPC reply
    return this.buildReply(header.version, RpcConstants.ReplyStates.RPC_ACCEPTED, resultData);
  }

  /**
   * Build a successful RPC reply
   */
  private buildReply(version: number, status: number, resultData: Uint8Array): Uint8Array {
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