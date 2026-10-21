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
   * Does NOT extract messages from buffer - just dispatches to listeners
   */
  private checkAndDispatch(): void {
    // Loop while we have complete messages in the buffer and listeners waiting
    while (this.bufferListeners.length > 0) {
      const message = this.peekCompleteMessage();
      if (message) {
        // Dispatch to the first waiting listener (FIFO order)
        const listener = this.bufferListeners.shift();
        // Extract the message before dispatching
        this.extractFromBuffer(message.length);
        if (listener) {
          listener(message);
        }
      } else {
        break; // No more complete messages to dispatch
      }
    }
  }

  /**
   * Peek at the next complete message in the buffer without extracting it
   * Returns the message if complete, or null if we need more data
   * Does NOT modify this.receiveBuffer
   */
  private peekCompleteMessage(): Uint8Array | null {
    const data = this.receiveBuffer;
    const minHeaderBytes = 16; // Basic XDR header

    // Not enough for even the header
    if (data.length < minHeaderBytes) {
      return null;
    }

    try {
      // Parse the header
      const header = decode(new Uint8Array(data.subarray(0, 16)));

      // For RPC calls, we need to calculate full message size
      // Message layout: header(16) + callHeader(32) + msgId + args
      if (header.type === RpcConstants.CallTypes.RPC_CALL) {
        const msgIdOffset = 48; // After header(16) + callHeader(32)

        if (data.length <= msgIdOffset + 4) {
          return null;
        }

        const msgIdLen = new DataView(data.buffer, data.byteOffset + msgIdOffset, 4).getUint32(0, false);
        const msgIdEncodedLen = 4 + msgIdLen;
        const argsOffset = msgIdOffset + msgIdEncodedLen;

        if (data.length < argsOffset + 4) {
          return null;
        }

        const argsLen = new DataView(data.buffer, data.byteOffset + argsOffset, 4).getUint32(0, false);
        const argsPadded = Math.ceil(argsLen / 4) * 4;
        const messageTotal = 16 + 32 + msgIdEncodedLen + 4 + argsPadded;

        if (data.length < messageTotal) {
          return null;
        }

        // Return the complete message WITHOUT extracting
        return new Uint8Array(data.subarray(0, messageTotal));
      }

      // For replies (RPC_REPLY)
      // Format: header(16) + callHeader(32) + msgId(opaque) + result(opaque)
      if (header.type === RpcConstants.CallTypes.RPC_REPLY) {
        if (data.length < 48) {
          return null;
        }

        let offset = 48;
        
        // Check if msgId is present
        if (data.length < offset + 4) {
          return null;
        }
        
        const msgIdLen = new DataView(data.buffer, data.byteOffset + offset, 4).getUint32(0, false);
        if (msgIdLen > 0) {
          const msgIdPadded = Math.ceil(msgIdLen / 4) * 4;
          const msgIdTotal = 4 + msgIdPadded;
          offset += msgIdTotal;
        }

        // Now parse the result (opaque field)
        if (data.length < offset + 4) {
          return null;
        }

        const resultLen = new DataView(data.buffer, data.byteOffset + offset, 4).getUint32(0, false);
        const resultPadded = Math.ceil(resultLen / 4) * 4;
        const resultTotal = 4 + resultPadded;
        const fullMsgSize = offset + resultTotal;

        if (data.length < fullMsgSize) {
          return null;
        }

        return new Uint8Array(data.subarray(0, fullMsgSize));
      }

      return null;
    } catch (e) {
      return null;
    }
  }

  /**
   * Extract N bytes from the front of the receive buffer
   * @param count - Number of bytes to extract
   * @returns The extracted bytes, or null if not enough in buffer
   */
  private extractFromBuffer(count: number): Uint8Array | null {
    if (this.receiveBuffer.length < count) {
      return null;
    }
    const extracted = new Uint8Array(this.receiveBuffer.subarray(0, count));
    this.receiveBuffer = new Uint8Array(this.receiveBuffer.subarray(count));
    return extracted;
  }

  /**
   * Try to parse and extract a complete RPC message from the buffer
   * Returns the message if complete, or null if we need more data
   * Modifies this.receiveBuffer by removing the extracted message
   */
  private tryParseCompleteMessage(): Uint8Array | null {
    const msg = this.peekCompleteMessage();
    if (msg) {
      this.extractFromBuffer(msg.length);
      return msg;
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

    // Check if we already have a complete message in the buffer (without extracting)
    const cachedMessage = this.peekCompleteMessage();
    if (cachedMessage) {
      // Extract it from buffer before returning
      this.extractFromBuffer(cachedMessage.length);
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
