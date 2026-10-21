// Transport abstraction layer for RPC
// Defines the interface that all transport implementations must satisfy
// Includes message framing based on XDR header

import { decode, RpcConstants } from './shared/xdr';

export abstract class Transport {
  /**
   * Initialize the transport connection
   * Called before any send/receive operations
   */
  abstract connect(): Promise<void>;

  /**
   * Send data through the transport
   * @param data - Binary data to send
   * @returns Promise that resolves when data is sent
   */
  abstract send(data: Uint8Array): Promise<void>;

  /**
   * Receive data from the transport
   * @param bufferSize - Expected or maximum buffer size
   * @returns Promise that resolves with received data
   */
  abstract receive(bufferSize: number): Promise<Uint8Array>;

  /**
   * Close the transport connection
   * Called when the RPC session is complete
   */
  abstract close(): Promise<void>;

  /**
   * Check if the transport is connected
   */
  abstract isConnected(): boolean;
}

// TCP Transport (supports both server and client modes)
export class TcpTransport implements Transport {
  private server: any = null;
  private socket: any = null;
  private host: string;
  private port: number;
  private connected: boolean = false;
  private isServer: boolean;
  private receiveBuffer: Uint8Array = new Uint8Array(0); // Persistent buffer for pipelined messages
  private bufferListeners: Array<(data: Uint8Array) => void> = []; // Listeners for buffer-ready messages
  private isBuffering: boolean = false; // Track if we're actively waiting for a receive() call

  constructor(host: string = '127.0.0.1', port: number = 5000, isServer: boolean = false) {
    this.host = host;
    this.port = port;
    this.isServer = isServer;
  }

  async connect(): Promise<void> {
    const net = await import('net');
    
    if (this.isServer) {
      // Server mode: create server and listen
      this.server = net.createServer((sock: any) => {
        this.socket = sock;
        this.connected = true;
        
        // Set up persistent data handler that accumulates into receiveBuffer
        sock.on('data', (chunk: Buffer) => {
          this.appendToBuffer(chunk);
          this.checkAndDispatch();
        });
        
        sock.on('error', (err: Error) => {
          console.error(`[TCP Transport] Socket error:`, err);
          this.connected = false;
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
      // Client mode: connect to server
      this.socket = net.createConnection({ host: this.host, port: this.port });
      
      // Set up persistent data handler that accumulates into receiveBuffer
      this.socket.on('data', (chunk: Buffer) => {
        this.appendToBuffer(chunk);
        this.checkAndDispatch();
      });
      
      await new Promise<void>((resolve, reject) => {
        this.socket.once('connect', () => {
          this.connected = true;
          resolve();
        });
        this.socket.once('error', (err: Error) => reject(err));
      });
    }
  }

  /**
   * Append incoming data chunk to the persistent receive buffer
   */
  private appendToBuffer(chunk: Buffer): void {
    const oldBuffer = this.receiveBuffer;
    const newBuffer = new Uint8Array(oldBuffer.length + chunk.length);
    newBuffer.set(oldBuffer, 0);
    newBuffer.set(Buffer.from(chunk), oldBuffer.length);
    this.receiveBuffer = newBuffer;
  }

  /**
   * Check if we have a complete message in the buffer and notify waiters
   */
  private checkAndDispatch(): void {
    // Loop while we have complete messages in the buffer and listeners waiting
    while (this.bufferListeners.length > 0) {
      const message = this.tryParseCompleteMessage();
      if (message) {
        // Dispatch to the first waiting listener (FIFO order)
        const listener = this.bufferListeners.shift();
        if (listener) {
          listener(message);
        }
      } else {
        break; // No more complete messages to dispatch
      }
    }
  }

  /**
   * Try to parse a complete RPC message from the buffer
   * Returns the message if complete, or null if we need more data
   */
  private tryParseCompleteMessage(): Uint8Array | null {
    const data = this.receiveBuffer;
    const minHeaderBytes = 16; // Basic XDR header

    console.log(`[tryParseCompleteMessage] Called with ${data.length} bytes in buffer`);

    // Not enough for even the header
    if (data.length < minHeaderBytes) {
      console.log(`[tryParseCompleteMessage] Too few bytes for header, returning null`);
      return null;
    }

    try {
      // Parse the header
      const header = decode(new Uint8Array(data.subarray(0, 16)));
      
      // Check if we have at least header + call header (16 + 20 = 36 bytes total)
      if (data.length < 36) {
        return null;
      }

      // For RPC calls, we need to calculate full message size
      if (header.type === RpcConstants.CallTypes.RPC_CALL) {
        const msgIdOffset = 36;  // After 16-byte header + 20-byte call header
        console.log(`[tryParseCompleteMessage] RPC_CALL detected, data=${data.length}, msgIdOffset=${msgIdOffset}`);
        
        if (data.length <= msgIdOffset + 4) {
          console.log(`[tryParseCompleteMessage] Not enough for msgIdLen field, returning null`);
          return null;
        }

        const msgIdLen = new DataView(data.buffer, data.byteOffset + msgIdOffset, 4).getUint32(0, false);
        console.log(`[tryParseCompleteMessage] msgIdLen=${msgIdLen}`);
        
        const msgIdField = 4 + msgIdLen;
        const argsOffset = msgIdOffset + msgIdField;
        console.log(`[tryParseCompleteMessage] msgIdField=${msgIdField}, argsOffset=${argsOffset}, data.length=${data.length}`);

        if (data.length < argsOffset + 4) {
          console.log(`[tryParseCompleteMessage] Not enough for argsLen field, returning null`);
          return null;
        }

        const argsLen = new DataView(data.buffer, data.byteOffset + argsOffset, 4).getUint32(0, false);
        console.log(`[tryParseCompleteMessage] argsLen=${argsLen}`);
        
        const argsPadded = Math.ceil(argsLen / 4) * 4;
        const messageTotal = 16 + 20 + msgIdField + 4 + argsPadded;
        console.log(`[tryParseCompleteMessage] messageTotal=${messageTotal}, data.length=${data.length}`);

        if (data.length < messageTotal) {
          console.log(`[tryParseCompleteMessage] Incomplete message, need more data (${data.length} < ${messageTotal})`);
          return null; // Incomplete message, need more data
        }

        // Extract the complete message and update buffer
        const message = new Uint8Array(data.subarray(0, messageTotal));
        this.receiveBuffer = new Uint8Array(data.subarray(messageTotal));
        
        console.log(`[TCP Transport] Extracted complete message: ${messageTotal} bytes, ${this.receiveBuffer.length} remaining in buffer`);
        return message;
      }

      // For replies (RPC_REPLY), extract complete reply
      // Reply: header(16) + callHeader(16) + optional msgId + result
      if (header.type === RpcConstants.CallTypes.RPC_REPLY) {
        // Check if we have header + at least some call header
        if (data.length < 32) {
          return null;
        }
        // Reply callHeader is 20 bytes but we only need first 16 for status
        // Check if msgId is present (after call header offset = 32)
        let replySize = 32;
        if (data.length > 32 + 4) {
          const msgIdLen = new DataView(data.buffer, data.byteOffset + 32, 4).getUint32(0, false);
          if (msgIdLen > 0) {
            const msgIdPadded = 4 + msgIdLen + ((4 - (msgIdLen % 4)) % 4);
            replySize += msgIdPadded;
          }
        }
        // Add any remaining data as result
        if (data.length >= replySize) {
          const message = new Uint8Array(data.subarray(0, data.length));
          this.receiveBuffer = new Uint8Array(0);
          console.log(`[TCP Transport] Extracted reply: ${message.length} bytes`);
          return message;
        }
        return null;
      }

      // For other/unrecognized message types, require complete data
      // Return null if we can't determine message boundaries
      return null;

    } catch (e) {
      // If parsing fails, we might have malformed data or incomplete header
      console.warn(`[TCP Transport] Parse error, waiting for more data:`, e instanceof Error ? e.message : String(e));
      return null;
    }

    return null;
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.socket || !this.connected) {
      throw new Error('TCP transport not connected');
    }

    return new Promise<void>((resolve, reject) => {
      this.socket.write(data, (err?: Error) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async receive(bufferSize: number): Promise<Uint8Array> {
    if (!this.socket || !this.connected) {
      throw new Error('TCP transport not connected');
    }

    // Check if we already have a complete message in the buffer
    const cachedMessage = this.tryParseCompleteMessage();
    if (cachedMessage) {
      console.log(`[TCP Transport] Returning cached message from buffer: ${cachedMessage.length} bytes`);
      return cachedMessage;
    }

    // No complete message ready, register a listener and wait
    return new Promise<Uint8Array>((resolve, reject) => {
      const timeoutHandle = setTimeout(() => {
        this.bufferListeners = this.bufferListeners.filter(l => l !== onData);
        // Check once more if data arrived during cleanup
        const message = this.tryParseCompleteMessage();
        if (message) {
          resolve(message);
        } else {
          reject(new Error(`TCP receive timeout: no complete message available (${this.receiveBuffer.length} bytes buffered)`));
        }
      }, 500); // Reduced timeout for testing

      const onData = (message: Uint8Array) => {
        clearTimeout(timeoutHandle);
        this.bufferListeners = this.bufferListeners.filter(l => l !== onData);
        resolve(message);
      };

      this.bufferListeners.push(onData);
        
      // Check immediately in case data arrived just now
      this.checkAndDispatch();
    });
  }

  async close(): Promise<void> {
    // Clear any pending listeners
    this.bufferListeners = [];
    
    if (this.isServer && this.server) {
      this.server.close();
    }
    if (this.socket) {
      this.socket.destroy();
      this.connected = false;
    }
    // Clear the receive buffer
    this.receiveBuffer = new Uint8Array(0);
  }

  isConnected(): boolean {
    return this.connected && this.socket && !this.socket.destroyed;
  }
  
  // For server mode, signal that we have an incoming connection
  async waitForConnection(): Promise<void> {
    if (!this.isServer) return;
    return new Promise<void>((resolve, reject) => {
      if (this.connected && this.socket) {
        resolve();
      } else {
        this.server.once('connection', () => resolve());
        setTimeout(() => reject(new Error('Connection wait timeout')), 15000);
      }
    });
  }
}
