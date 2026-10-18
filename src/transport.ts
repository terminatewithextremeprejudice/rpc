// Transport abstraction layer for RPC
// Defines the interface that all transport implementations must satisfy

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

// TCP Transport
export class TcpTransport implements Transport {
  private socket: any = null;
  private host: string;
  private port: number;
  private connected: boolean = false;

  constructor(host: string = '127.0.0.1', port: number = 5000) {
    this.host = host;
    this.port = port;
  }

  async connect(): Promise<void> {
    const net = await import('net');
    this.socket = net.createConnection({ host: this.host, port: this.port });
    
    await new Promise<void>((resolve, reject) => {
      this.socket.once('connect', () => {
        this.connected = true;
        resolve();
      });
      this.socket.once('error', (err: Error) => reject(err));
    });

    // Handle disconnection
    this.socket.once('close', () => {
      this.connected = false;
    });
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
    if (this.socket) {
      this.socket.destroy();
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected && this.socket && !this.socket.destroyed;
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

// Unix Domain Socket Transport
export class UnixSocketTransport implements Transport {
  private socket: any = null;
  private path: string;
  private connected: boolean = false;

  constructor(path: string) {
    this.path = path;
  }

  async connect(): Promise<void> {
    const net = await import('net');
    this.socket = net.createConnection({ path: this.path });
    
    await new Promise<void>((resolve, reject) => {
      this.socket.once('connect', () => {
        this.connected = true;
        resolve();
      });
      this.socket.once('error', (err: Error) => reject(err));
    });

    this.socket.once('close', () => {
      this.connected = false;
    });
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
    if (this.socket) {
      this.socket.destroy();
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected && this.socket && !this.socket.destroyed;
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

// UDP Transport
export class UdpTransport implements Transport {
  public port: number;
  public host: string;
  private socket: any = null;
  private connectedState: boolean = false;

  constructor(host: string = '127.0.0.1', port: number = 5000) {
    this.host = host;
    this.port = port;
  }

  async connect(): Promise<void> {
    const dgram = await import('dgram');
    this.socket = dgram.createSocket('udp4');
    this.connectedState = true;

    // Bind to port
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
      this.socket.send(data, 0, data.length, this.port, this.host, (err?: Error) => {
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
      }, 2000);
    });
  }

  async close(): Promise<void> {
    if (this.socket) {
      this.socket.close();
      this.connectedState = false;
    }
  }

  isConnected(): boolean {
    return this.connectedState && this.socket && !this.socket._handle;
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