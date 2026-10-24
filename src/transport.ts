// Transport abstraction layer for RPC
// Supports multiple concurrent connections with per-connection message tracking

import { decode, RpcConstants, messageSize } from './shared/xdr';

/**
 * Abstract base for all transport implementations.
 * Each transport handles its own connection(s) and message delivery.
 */
export abstract class Transport {
  /**
   * Initialize the transport connection(s)
   * Called before any send/receive operations
   */
  abstract connect(): Promise<void>;

  /**
   * Send data through the transport
   * @param connectionId - Identifier for the connection to use
   * @param data - Binary data to send
   */
  abstract send(connectionId: string, data: Uint8Array): Promise<void>;

  /**
   * Receive data from the transport
   * @param connectionId - Identifier for the connection to receive from
   * @param bufferSize - Expected or maximum buffer size
   * @returns Promise that resolves with received data
   */
  abstract receive(connectionId: string, bufferSize: number): Promise<Uint8Array>;

  /**
   * Close a specific connection
   * @param connectionId - Identifier for the connection to close
   */
  abstract close(connectionId: string): Promise<void>;

  /**
   * Check if a connection is active
   */
  abstract isConnected(connectionId: string): boolean;

  /**
   * Get the connection ID associated with a socket/connection object
   */
  abstract getConnectionId(socket: any): string | undefined;
}

/**
 * Represents a single connection within a multi-connection transport
 */
export class Connection {
  id: string;
  socket: any;
  receiveBuffer: Uint8Array = new Uint8Array(0);
  resolveReceive: ((data: Uint8Array) => void) | null = null;
  rejectReceive: ((error: Error) => void) | null = null;

  constructor(id: string, socket: any) {
    this.id = id;
    this.socket = socket;
  }

  /**
   * Check if a complete message is available in the buffer
   */
  hasCompleteMessage(): boolean {
    const size = messageSize(this.receiveBuffer);
    console.log("has complete message", size, this.receiveBuffer);
    return size > 0;
  }

  /**
   * Extract a complete message from the buffer
   * Returns the message if complete, or null if we need more data
   */
  extractMessage(): Uint8Array | null {
    const size = messageSize(this.receiveBuffer);
    if (size <= 0) return null;
    
    const message = this.receiveBuffer.subarray(0, size);
    this.receiveBuffer = this.receiveBuffer.subarray(size);
    return message;
  }

  /**
   * Append incoming data to the receive buffer
   */
  appendToBuffer(chunk: Uint8Array): void {
    const oldBuffer = this.receiveBuffer;
    const newBuffer = new Uint8Array(oldBuffer.length + chunk.length);
    newBuffer.set(oldBuffer, 0);
    newBuffer.set(chunk, oldBuffer.length);
    this.receiveBuffer = newBuffer;
  }
}

/**
 * TCP Transport Implementation
 * Supports multiple concurrent client connections via a server socket
 */
export class TcpTransport implements Transport {
  private server: any = null;
  private connections: Map<string, Connection> = new Map();
  private host: string;
  private port: number;
  private isServer: boolean;
  private clientConnectionId?: string; // For client mode (single connection)

  constructor(host: string = '127.0.0.1', port: number = 5000, isServer: boolean = false) {
    this.host = host;
    this.port = port;
    this.isServer = isServer;
  }

  async connect(): Promise<void> {
    const net = await import('net');

    if (this.isServer) {
      // Server mode: create server and listen for multiple connections
      this.server = net.createServer((sock: any) => {
        // Create a unique connection ID for this socket
        const connId = this.generateConnectionId(sock);
        const connection = new Connection(connId, sock);

        // Store the connection
        this.connections.set(connId, connection);
        
        console.log(`[TCP Transport] New connection: ${connId}`);

        // Set up data handler for this connection
        sock.on('data', (chunk: Buffer) => {
	  console.log("got data on", chunk);						
          connection.appendToBuffer(Uint8Array.from(chunk));
          connection.resolveReceive?.(connection.extractMessage()!);
        });

        sock.on('end', () => {
          console.log(`[TCP Transport] Connection closed: ${connId}`);
          this.connections.delete(connId);
          if (connection.resolveReceive) {
            connection.rejectReceive?.(new Error('Connection ended'));
          }
        });

        sock.on('error', (err: Error) => {
          console.error(`[TCP Transport] Socket error on ${connId}:`, err.message);
          this.connections.delete(connId);
          if (connection.resolveReceive) {
            connection.rejectReceive?.(new Error(err.message));
          }
        });
      });

      return new Promise<void>((resolve, reject) => {
        this.server.once('listening', () => {
          resolve();
        });
        this.server.once('error', (err: Error) => reject(err));
        this.server.listen(this.port, this.host);
      });
    } else {
      // Client mode: single connection
      this.clientConnectionId = crypto.randomUUID().split('-')[0]!;
      const socket = net.createConnection({ host: this.host, port: this.port });
      const connection = new Connection(this.clientConnectionId, socket);

      socket.on('data', (chunk: Buffer) => {
	console.log('data on socket');
        connection.appendToBuffer(Uint8Array.from(chunk));
        if (!connection.resolveReceive) return;
        const msg = connection.extractMessage();
        if (msg) connection.resolveReceive(msg);
      });

      socket.on('connect', () => {
        connection.resolveReceive = null; // Will be set when receive() is called
      });
      
      socket.on('error', (err: Error) => {
        if (connection.resolveReceive) {
          connection.rejectReceive?.(err);
          connection.rejectReceive?.(new Error(`Connection error: ${err.message}`));
        }
      });

      socket.once('connect', () => {
        this.connections.set(this.clientConnectionId!, connection);
      });
      
      return new Promise<void>((resolve, reject) => {
        socket.once('connect', () => resolve());
        socket.once('error', (err: Error) => reject(err));
      });
    }
  }

  private generateConnectionId(socket: any): string {
    return crypto.randomUUID().split('-')[0]!;
  }

  async send(connectionId: string, data: Uint8Array): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (!connection || !connection.socket) {
      throw new Error(`No active connection for ID: ${connectionId}`);
    }

    console.log("send", connectionId, data);
    return new Promise<void>((resolve, reject) => {
      connection.socket.write(data, (err?: Error) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async receive(connectionId: string, bufferSize: number): Promise<Uint8Array> {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      throw new Error(`No active connection for ID: ${connectionId}`);
    }

    console.log("received", connectionId);
    // Check if we already have a complete message
    if (connection.hasCompleteMessage()) {
      const msg = connection.extractMessage();
      if (msg) return msg;
    }

    // Wait for incoming data
    return new Promise<Uint8Array>((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        if (!connection.hasCompleteMessage()) {
          const buffered = this.connections.get(connectionId);
          reject(
            new Error(
              `Receive timeout: no complete message (${buffered?.receiveBuffer.length || 0} bytes buffered)`
            )
          );
        }
        if (connection.resolveReceive === resolve) {
          const msg = connection.extractMessage();
          if (msg) resolve(msg);
        }
      }, 5000);

      connection.resolveReceive = (msg) => {
        clearTimeout(timeoutHandle);
        connection.resolveReceive = null;
        resolve(msg);
      };

      connection.rejectReceive = (err) => {
        clearTimeout(timeoutHandle);
        connection.resolveReceive = null;
        reject(err);
      };

      // Check immediately if data already arrived
      if (connection.hasCompleteMessage()) {
        clearTimeout(timeoutHandle);
        connection.resolveReceive = null;
        const msg = connection.extractMessage();
        if (msg) resolve(msg);
      } else {
        // No data yet, the resolve will be called when socket data arrives
      }
    });
  }

  async close(connectionId: string): Promise<void> {
    const connection = this.connections.get(connectionId);
    if (connection) {
      if (connection.resolveReceive) {
        connection.rejectReceive?.(new Error('Connection closed'));
      }
      connection.socket.destroy();
      this.connections.delete(connectionId);
    }
  }

  isConnected(connectionId: string): boolean {
    const connection = this.connections.get(connectionId);
    return !!(connection && connection.socket && !connection.socket.destroyed);
  }

  /**
   * Client mode: simplified connect check
   */
  clientIsConnected(): boolean {
    if (this.isServer) {
      return this.connections.size > 0;
    }
    return this.clientConnectionId ? this.isConnected(this.clientConnectionId!) : false;
  }

  /**
   * Get list of connection IDs that have messages ready (non-blocking)
   */
  getReadyConnections(): string[] {
    const ready: string[] = [];
    for (const [connId, conn] of this.connections) {
      if (conn.hasCompleteMessage()) {
        ready.push(connId);
      }
    }
    return ready;
  }

  /** 
   * Get specific connection 
   */ 
  getConnectionById(connectionId: string): Connection | undefined {
    return this.connections.get(connectionId)
  }

  /**
   * Get info for specific connection (non-blocking)
   */
  getConnection(hasCompleteMessage: boolean): boolean {
    const conn = this.connections.get('');
    return conn?.hasCompleteMessage() || false;
  }

  /**
   * Client-only: send data (client mode only)
   */
  clientSend(data: Uint8Array): Promise<void> {
    if (this.isServer) {
      throw new Error('clientSend is server-only');
    }
    if (!this.clientConnectionId) {
      throw new Error('Not connected');
    }
    console.log("client send", data);
    return this.send(this.clientConnectionId, data);
  }

  /**
   * Client-only: receive data (client mode only)
   */
  clientReceive(bufferSize: number): Promise<Uint8Array> {
    if (this.isServer) {
      throw new Error('clientReceive is server-only');
    }
    if (!this.clientConnectionId) {
      throw new Error('Not connected');
    }
    return this.receive(this.clientConnectionId, bufferSize);
  }

  /**
   * Client-only: close connection (client mode only)
   */
  clientClose(): Promise<void> {
    if (this.isServer) {
      throw new Error('clientClose is server-only');
    }
    if (!this.clientConnectionId) {
      throw new Error('Not connected');
    }
    return this.close(this.clientConnectionId);
  }

  getConnectionId(socket: any): string | undefined {
    for (const [connId, conn] of this.connections) {
      if (conn.socket === socket) return connId;
    }
    
    if (!this.isServer && this.clientConnectionId) {
      const conn = this.connections.get(this.clientConnectionId);
      if (conn?.socket === socket) return this.clientConnectionId;
    }
    
    return undefined;
  }

  getConnectionsWithSocket(): MapIterator<[string, Connection]> {
    return this.connections.entries();
  }

  getConnections(): MapIterator<[string, Connection]> {
    return this.connections.entries();
  }

  getHost(): string {
    return this.host;
  }

  /**
   * Server-side: accept a new connection
   * Returns the connection ID for the newly connected client
   */
  async acceptConnection(): Promise<string> {
    if (!this.isServer) {
      throw new Error('acceptConnection() is only available in server mode');
    }

    return new Promise<string>((resolve, reject) => {
      // Check if there's already a connection waiting
      for (const [id, conn] of this.connections) {
        if (conn.socket && !this.connections.has(id)) {
          resolve(id);
          return;
        }
      }

      this.server.once('connection', (sock: any) => {
        const connId = this.generateConnectionId(sock);
        resolve(connId);
      });

      setTimeout(() => {
        reject(new Error('No client connected within timeout'));
      }, 10000);
    });
  }

  /**
   * Server shutdown: close all connections and the server
   */
  async shutdown(): Promise<void> {
    for (const connId of this.connections.keys()) {
      await this.close(connId);
    }
    
    if (this.server) {
      this.server.close();
      this.server = null;
    }
    this.connections.clear();
  }

  /**
   * Get count of active connections
   */
  getConnectionCount(): number {
    return this.connections.size;
  }
}

/**
 * Abstract base for other transport implementations (UDP, Unix socket, shared memory)
 */
export abstract class MultiplexedTransport implements Transport {
  abstract connect(): Promise<void>;
  abstract send(connectionId: string, data: Uint8Array): Promise<void>;
  abstract receive(connectionId: string, bufferSize: number): Promise<Uint8Array>;
  abstract close(connectionId: string): Promise<void>;
  abstract isConnected(connectionId: string): boolean;
  abstract getConnectionId(socket: any): string | undefined;
}

/**
 * Factory function to create TCP transport
 */
export function createTcpTransport(
  host: string = '127.0.0.1',
  port: number = 5000,
  isServer: boolean = false
): TcpTransport {
  return new TcpTransport(host, port, isServer);
}
