// RPC Server Implementation
// Handles incoming XDR-encoded RPC calls and dispatches them to handlers

import { decode, encode, type XDRHeader, RpcConstants } from './shared/xdr';
import { Transport, TcpTransport, UnixSocketTransport } from './transport';

export interface RpcHandler {
  procedure: number;
  version: number;
  handler: (args: any) => any;
}

export interface RpcServerOptions {
  transport: Transport;
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
   */
  private async handleRequest(header: XDRHeader, message: Uint8Array): Promise<Uint8Array> {
    // For now, we'll do a simple echo-based test
    // In a real implementation, you'd decode the arguments and call the handler
    
    const response: XDRHeader = {
      version: header.version,
      type: RpcConstants.CallTypes.RPC_REPLY,
      headerLength: 16,
      authType: header.authType
    };

    const responseBytes: number[] = [
      // Response header
      ...new Uint8Array(encode(response)),
      // Reply header (simplified - just echo back the call structure)
      (header.version >> 24) & 0xFF,
      (header.version >> 16) & 0xFF,
      (header.version >> 8) & 0xFF,
      header.version & 0xFF,
      (header.type >> 24) & 0xFF,
      (header.type >> 16) & 0xFF,
      (header.type >> 8) & 0xFF,
      header.type & 0xFF,
      0, 0, 0, 1, // RPC_ACCEPTED
      0, 0, 0, 1  // dummy result
    ];

    return new Uint8Array(responseBytes);
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