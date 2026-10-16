// RPC Client Implementation
// Encodes RPC calls and sends them through the configured transport

import { encode, decode, encodeCall, encodeString, type XDRHeader, RpcConstants } from './shared/xdr';
import { Transport, TcpTransport, UnixSocketTransport, HttpTransport } from './transport';

export interface RpcClientOptions {
  transport: Transport;
  timeout?: number; // milliseconds
}

export interface RpcCallResult {
  status: number; // RPC_ACCEPTED (0), RPC_GARBAGE (1), etc.
  value: any; // The return value from the procedure
}

export class RpcClient {
  private transport: Transport;
  private prognum: number; // Program number (e.g., 100003 for NFS)
  private vers: number; // Program version
  private timeout: number;
  private connected: boolean = false;

  constructor(prognum: number, vers: number, options: RpcClientOptions) {
    this.prognum = prognum;
    this.vers = vers;
    this.transport = options.transport;
    this.timeout = options.timeout || 5000;
  }

  /**
   * Connect to the RPC server
   */
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }

    try {
      await this.transport.connect();
      this.connected = true;
      console.log(`[RPC Client] Connected to server`);
    } catch (error) {
      this.connected = false;
      throw new Error(`Connection failed: ${error}`);
    }
  }

  /**
   * Disconnect from the RPC server
   */
  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }

    await this.transport.close();
    this.connected = false;
    console.log(`[RPC Client] Disconnected from server`);
  }

  /**
   * Make an RPC call
   * @param proc - Procedure number
   * @param args - Arguments to pass (simplified - assumes single opaque argument for now)
   * @returns The result from the procedure
   */
  async call(proc: number, args: Uint8Array = new Uint8Array(0)): Promise<RpcCallResult> {
    if (!this.connected) {
      await this.connect();
    }

    if (!this.transport.isConnected()) {
      throw new Error('Not connected to server');
    }

    // Encode the RPC call message
    const callHeader: XDRHeader = {
      version: RpcConstants.RPC_VERSION,
      type: RpcConstants.CallTypes.RPC_CALL,
      headerLength: 16,
      authType: RpcConstants.AuthFlavors.AUTH_NULL
    };

    const encodedCall = encodeCall(
      this.prognum, // procedure
      this.vers,    // version
      proc,          // procedure number
      callHeader
    );

    console.log(`[RPC Client] Sending CALL: prog=${this.prognum}, vers=${this.vers}, proc=${proc}`);

    // Send the request
    await this.transport.send(encodedCall);

    // Receive the response
    // Response should be at least 16 bytes (header) + 12 bytes (reply)
    const response = await this.transport.receive(28);

    // Decode and return
    return this.decodeResponse(response);
  }

  /**
   * Decode the RPC response
   */
  private decodeResponse(response: Uint8Array): RpcCallResult {
    if (response.length < 16) {
      throw new Error(`Response too short: ${response.length} bytes`);
    }

    const header = decode(new Uint8Array(response.subarray(0, 16)));

    console.log(`[RPC Client] Received reply: type=${header.type}, status=${header.authType}`);

    return {
      status: header.type === RpcConstants.CallTypes.RPC_REPLY 
        ? RpcConstants.ReplyStates.RPC_ACCEPTED 
        : header.authType,
      value: null // For now, we don't decode the actual return value
    };
  }

  /**
   * Call with string argument
   */
  async callWithString(proc: number, arg: string): Promise<RpcCallResult> {
    const encodedArg = encodeString(arg);
    return this.call(proc, encodedArg);
  }

  /**
   * Call with opaque bytes argument
   */
  async callWithBytes(proc: number, arg: Uint8Array): Promise<RpcCallResult> {
    return this.call(proc, arg);
  }

  /**
   * Check if the client is connected
   */
  isConnected(): boolean {
    return this.connected && this.transport.isConnected();
  }

  /**
   * Get the transport instance
   */
  getTransport(): Transport {
    return this.transport;
  }
}

/**
 * Convenience function to create a TCP-based RPC client
 */
export async function createTcpClient(
  prognum: number,
  vers: number,
  host: string = '127.0.0.1',
  port: number = 5000
): Promise<RpcClient> {
  const transport = new TcpTransport(host, port);
  const client = new RpcClient(prognum, vers, { transport });
  await client.connect();
  return client;
}

/**
 * Convenience function to create a Unix socket RPC client
 */
export async function createUnixClient(
  prognum: number,
  vers: number,
  socketPath: string = '/tmp/rpc.sock'
): Promise<RpcClient> {
  const transport = new UnixSocketTransport(socketPath);
  const client = new RpcClient(prognum, vers, { transport });
  await client.connect();
  return client;
}

/**
 * Convenience function to create an HTTP-based RPC client
 */
export async function createHttpClient(
  prognum: number,
  vers: number,
  url: string
): Promise<RpcClient> {
  const transport = new HttpTransport(url);
  const client = new RpcClient(prognum, vers, { transport });
  await client.connect();
  return client;
}