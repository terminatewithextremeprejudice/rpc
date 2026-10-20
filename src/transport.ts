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
      
      await new Promise<void>((resolve, reject) => {
        this.socket.once('connect', () => {
          this.connected = true;
          resolve();
        });
        this.socket.once('error', (err: Error) => reject(err));
      });
    }
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

    const chunks: Uint8Array[] = []
    const minHeaderBytes = 16 // Basic XDR header

    return new Promise<Uint8Array>((resolve, reject) => {
      const onData = (chunk: Buffer) => {
        chunks.push(Buffer.from(chunk))
      }

      const onError = (err: Error) => {
        this.socket.removeListener('data', onData)
        this.socket.removeListener('error', onError)
        clearTimeout(timeoutHandle)
        reject(err)
      }

      this.socket.on('data', onData)
      this.socket.once('error', onError)

      const timeoutHandle = setTimeout(() => {
        this.socket.removeListener('data', onData)
        this.socket.removeListener('error', onError)
        if (chunks.length > 0) {
          const data = Buffer.concat(chunks)
          if (data.length >= minHeaderBytes) {
            // Parse header to extract headerLength field
            try {
              const header = decode(new Uint8Array(data.subarray(0, 16)))
              // headerLength indicates call header size. For RPC_CALL/RPC_REPLY it's typically fixed.
              // We need to calculate total message size from call header
              // Call header structure: prognum(4) + vers(4) + proc(4) + reserved(4) + auth_header(16) = 32 bytes
              // Total = header(16) + call_header(32) + msgid(optional) + args
              
              // For simplicity, we'll extract one complete RPC message
              // Check if we have at least header + call header
              if (data.length >= 48) {  // 16 + 32
                // Extract call header fields to find auth_type which tells us if there's more data
                const callHeaderView = new DataView(data.buffer, data.byteOffset + 16)
                const authType = callHeaderView.getUint32(28, false) // auth type is at offset 28 within call header block
                
                // For now, extract based on message type:
                // If it's a call, look for msgId field at offset 48+
                if (header.type === RpcConstants.CallTypes.RPC_CALL) {
                  const msgIdOffset = 48
                  if (data.length > msgIdOffset + 4) {
                    const msgIdLen = new DataView(data.buffer, data.byteOffset + msgIdOffset, 4).getUint32(0, false)
                    // XDR string stores padded length (including null terminator) in length field
                    // msgId field total size = 4 (length field) + msgIdLen (padded content size)
                    const msgIdField = 4 + msgIdLen
                    console.log(`[TCP Transport] msgIdLen=${msgIdLen}, msgIdField=${msgIdField}, data.len=${data.length}`)
                    
                    const argsOffset = msgIdOffset + msgIdField
                    console.log(`[TCP Transport] argsOffset=${argsOffset}`)
                    
                    if (data.length >= argsOffset + 4) {
                      const argsLen = new DataView(data.buffer, data.byteOffset + argsOffset, 4).getUint32(0, false)
                      const argsPadded = Math.ceil(argsLen / 4) * 4
                      const messageTotal = 16 + 32 + msgIdField + 4 + argsPadded
                      console.log(`[TCP Transport] argsLen=${argsLen}, argsPadded=${argsPadded}, total=${messageTotal}, have=${data.length}`)
                      
                      if (data.length >= messageTotal) {
                        const message = new Uint8Array(data.subarray(0, messageTotal))
                        const remaining = data.subarray(messageTotal)
                        if (remaining.length > 0) {
                          console.log(`[TCP Transport] Split: extracted ${messageTotal}, remaining ${remaining.length}`)
                          this.socket.unshift(remaining)
                        }
                        resolve(message)
                        return
                      }
                    }
                    // Not complete, wait more
                  }
                }
              }
              
              // If complete, return the message; otherwise wait for more data
              if (data.length >= 48) {
                // Return what we have if it looks like a reasonable message
                resolve(new Uint8Array(data))
              } else {
                // Not enough data yet, continue waiting
                timeoutHandle.refresh()
              }
            } catch (e) {
              // If parsing fails but we have data, return it
              if (data.length >= 16) {
                resolve(new Uint8Array(data))
              }
            }
          } else {
            // Don't have full header yet, wait more
            timeoutHandle.refresh()
          }
        } else {
          reject(new Error('TCP receive timeout'))
        }
      }, 1000)
    })
  }

  async close(): Promise<void> {
    if (this.isServer && this.server) {
      this.server.close();
    }
    if (this.socket) {
      this.socket.destroy();
      this.connected = false;
    }
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

// HTTP Transport (simple implementation using fetch)
export class HttpTransport implements Transport {
  private url: string;
  private connected: boolean = false;

  constructor(url: string) {
    this.url = url;
  }

  async connect(): Promise<void> {
    // HTTP is stateless, so "connect" just validates the URL
    try {
      await fetch(`${this.url}/ping`, { method: 'GET' });
      this.connected = true;
    } catch (error) {
      throw new Error(`HTTP transport connection failed: ${error}`);
    }
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.connected) {
      throw new Error('HTTP transport not connected');
    }

    await fetch(this.url, {
      method: 'POST',
      body: data
    });
  }

  async receive(bufferSize: number): Promise<Uint8Array> {
    if (!this.connected) {
      throw new Error('HTTP transport not connected');
    }

    const response = await fetch(this.url, { method: 'POST' });
    const arrayBuffer = await response.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  }

  async close(): Promise<void> {
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected;
  }
}

// Unix Domain Socket Transport (supports both server and client modes)
export class UnixSocketTransport implements Transport {
  private server: any = null;
  private socket: any = null;
  private path: string;
  private connected: boolean = false;
  private isServer: boolean;

  constructor(path: string, isServer: boolean = false) {
    this.path = path;
    this.isServer = isServer;
  }

  async connect(): Promise<void> {
    const net = await import('net');
    
    if (this.isServer) {
      // Server mode: create server and listen
      this.server = net.createServer((sock: any) => {
        this.socket = sock;
        this.connected = true;
      });
      
      return new Promise<void>((resolve, reject) => {
        this.server.once('listening', () => {
          resolve();
        });
        this.server.once('error', (err: Error) => reject(err));
        this.server.listen(this.path);
      });
    } else {
      // Client mode: connect to server
      this.socket = net.createConnection({ path: this.path });
      
      await new Promise<void>((resolve, reject) => {
        this.socket.once('connect', () => {
          this.connected = true;
          resolve();
        });
        this.socket.once('error', (err: Error) => reject(err));
      });
    }
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.socket || !this.connected) {
      throw new Error('Unix socket transport not connected');
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
      throw new Error('Unix socket transport not connected');
    }

    const chunks: Uint8Array[] = [];
    let received = 0;

    return new Promise<Uint8Array>((resolve, reject) => {
      const onData = (chunk: Buffer) => {
        chunks.push(Buffer.from(chunk));
        received += chunk.length;
        if (received >= bufferSize) {
          this.socket.removeListener('data', onData);
          this.socket.removeListener('error', onError);
          resolve(new Uint8Array(Buffer.concat(chunks, bufferSize)));
        }
      };

      const onError = (err: Error) => {
        this.socket.removeListener('data', onData);
        this.socket.removeListener('error', onError);
        reject(err);
      };

      this.socket.on('data', onData);
      this.socket.once('error', onError);
    });
  }

  async close(): Promise<void> {
    if (this.isServer && this.server) {
      this.server.close();
    }
    if (this.socket) {
      this.socket.destroy();
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected && this.socket && !this.socket.destroyed;
  }
  
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

// Shared Memory Transport (basic implementation using file-based approach)
export class SharedMemoryTransport implements Transport {
  private fd: number = -1;
  private path: string;
  private connected: boolean = false;

  constructor(path: string) {
    this.path = path;
  }

  async connect(): Promise<void> {
    const fs = await import('fs');
    
    try {
      if (!fs.existsSync(this.path)) {
        fs.writeFileSync(this.path, Buffer.alloc(65536)); // 64KB shared memory
      }
      
      this.fd = fs.openSync(this.path, 'r+');
      this.connected = true;
    } catch (error) {
      throw new Error(`Shared memory connection failed: ${error}`);
    }
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.connected || this.fd === -1) {
      throw new Error('Shared memory transport not connected');
    }

    const fs = await import('fs');
    fs.writeSync(this.fd, data, 0, data.length, 0);
  }

  async receive(bufferSize: number): Promise<Uint8Array> {
    if (!this.connected || this.fd === -1) {
      throw new Error('Shared memory transport not connected');
    }

    const fs = await import('fs');
    const buffer = Buffer.alloc(bufferSize);
    fs.readSync(this.fd, buffer, 0, bufferSize, 0);
    
    return new Uint8Array(buffer);
  }

  async close(): Promise<void> {
    if (this.fd !== -1) {
      const fs = await import('fs');
      fs.closeSync(this.fd);
      this.fd = -1;
    }
    this.connected = false;
  }

  isConnected(): boolean {
    return this.connected && this.fd !== -1;
  }
}

// UDP Transport (supports both server and client modes)
export class UdpTransport implements Transport {
  public port: number;
  public host: string;
  private socket: any = null;
  private connectedState: boolean = false;
  private isServer: boolean;

  constructor(host: string = '127.0.0.1', port: number = 5000, isServer: boolean = false) {
    this.host = host;
    this.port = port;
    this.isServer = isServer;
  }

  async connect(): Promise<void> {
    const dgram = await import('dgram');
    this.socket = dgram.createSocket('udp4');
    this.connectedState = true;

    // Bind to port (both server and client need to bind for recv/send)
    return new Promise((resolve, reject) => {
      this.socket.once('ready', resolve);
      this.socket.once('error', reject);
      this.socket.bind(this.port, this.host);
    });
  }

  async send(data: Uint8Array): Promise<void> {
    if (!this.connectedState) {
      throw new Error('UDP transport not connected');
    }

    return new Promise((resolve, reject) => {
      const destPort = this.isServer ? this.port : this.port;
      this.socket.send(data, 0, data.length, destPort, this.host, (err?: Error) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async receive(bufferSize: number): Promise<Uint8Array> {
    if (!this.connectedState) {
      throw new Error('UDP transport not connected');
    }

    return new Promise<Uint8Array>((resolve, reject) => {
      const onData = (data: Buffer) => {
        this.socket.removeListener('data', onData);
        this.socket.removeListener('error', onError);
        resolve(new Uint8Array(data));
      };

      const onError = (err: Error) => {
        this.socket.removeListener('data', onData);
        this.socket.removeListener('error', onError);
        reject(err);
      };

      this.socket.once('data', onData);
      this.socket.once('error', onError);

      // Timeout for receive
      setTimeout(() => {
        this.socket.removeListener('data', onData);
        this.socket.removeListener('error', onError);
        reject(new Error('UDP receive timeout'));
      }, 3000);
    });
  }

  async close(): Promise<void> {
    if (this.socket) {
      this.socket.close();
      this.connectedState = false;
    }
  }

  isConnected(): boolean {
    return this.connectedState && this.socket && this.socket._handle;
  }
  
  async waitForConnection(): Promise<void> {
    if (!this.isServer) return;
    // For UDP, we need to receive at least one message to confirm client is connected
    return new Promise<void>((resolve, reject) => {
      const onData = (data: Buffer) => {
        this.socket.removeListener('data', onData);
        this.socket.removeListener('error', onError);
        resolve();
      };
      const onError = (err: Error) => {
        this.socket.removeListener('data', onData);
        this.socket.removeListener('error', onError);
        reject(err);
      };
      this.socket.once('data', onData);
      this.socket.once('error', onError);
      setTimeout(() => reject(new Error('Connection wait timeout')), 5000);
    });
  }
}

// Factory for creating transport instances
export class TransportFactory {
  static create(type: 'tcp', host: string, port: number): TcpTransport;
  static create(type: 'http', url: string): HttpTransport;
  static create(type: 'unix', path: string): UnixSocketTransport;
  static create(type: 'udp', host: string, port: number): UdpTransport;
  static create(type: 'memory', path: string): SharedMemoryTransport;
  static create(type: string, ...args: any[]): Transport {
    switch (type) {
      case 'tcp':
        return new TcpTransport(args[0], args[1]);
      case 'http':
        return new HttpTransport(args[0]);
      case 'unix':
        return new UnixSocketTransport(args[0]);
      case 'udp':
        return new UdpTransport(args[0], args[1]);
      case 'memory':
        return new SharedMemoryTransport(args[0]);
      default:
        throw new Error(`Unknown transport type: ${type}`);
    }
  }
}