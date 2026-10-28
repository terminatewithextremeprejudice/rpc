// RPC client implementation
// Encodes RPC calls and transmits them through the configured transport.
// Matching replies to calls is done via msgId, so out-of-order /
// interleaved replies (pipelined calls) get routed to the correct
// caller.

import { RpcConstants } from './shared/xdr';
import { buildCall, parseMessage } from './shared/message';
import { Transport, TcpTransport } from './transport';

export interface RpcClientOptions {
  transport: Transport;
  timeout?: number;
}

export interface RpcCallResult {
  status: number;
  value: Uint8Array | undefined;
  msgId: string;
}

export interface AsyncRpcHandle {
  _async: true;
  msgId: string;
  proc: number;
  connectionId: string;
}

export class RpcClient {
  private readonly prognum: number;
  private readonly vers: number;
  private readonly timeout: number;
  private readonly transport: Transport;
  private connectionId: string | undefined;
  private connected = false;

  // Replies that arrived out of order, waiting for their caller: msgId -> raw
  private readonly replyCache = new Map<string, Uint8Array>();

  constructor(prognum: number, vers: number, options: RpcClientOptions) {
    this.prognum = prognum;
    this.vers = vers;
    this.transport = options.transport;
    this.timeout = options.timeout ?? Transport.DEFAULT_TIMEOUT_MS;
  }

  async connect(): Promise<void> {
    if (this.connected) return;
    await this.transport.connect();
    this.connectionId = this.transport.connectionIds()[0];
    if (!this.connectionId) {
      throw new Error('Transport connected but exposed no connection');
    }
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (!this.connected && this.connectionId === undefined) return;
    await this.transport.closeAll();
    this.connected = false;
    this.connectionId = undefined;
    this.replyCache.clear();
  }

  isConnected(): boolean {
    return this.connected && this.connectionId !== undefined
      && this.transport.isConnected(this.connectionId);
  }

  getTransport(): Transport {
    return this.transport;
  }

  /**
   * Call a procedure.
   * - Synchronous (default): waits for the reply matching this call's msgId.
   * - `async: true`: returns an AsyncRpcHandle immediately (fire & forget);
   *   retrieve the reply later with getResponse() / collectResponses().
   */
  async call(
    proc: number,
    args: Uint8Array = new Uint8Array(0),
    options?: { async?: boolean; msgId?: string; timeout?: number },
  ): Promise<RpcCallResult | AsyncRpcHandle> {
    if (!this.isConnected()) await this.connect();
    const connectionId = this.requireConnection();
    const callMsgId = options?.msgId ?? this.generateMsgId();
    const timeout = options?.timeout ?? this.timeout;

    const message = buildCall(this.prognum, this.vers, proc, callMsgId, args);
    await this.transport.send(connectionId, message);

    if (options?.async) {
      return { _async: true, msgId: callMsgId, proc, connectionId };
    }
    const reply = await this.readReply(callMsgId, timeout);
    return this.decodeResult(reply, callMsgId);
  }

  async fire(proc: number, args: Uint8Array = new Uint8Array(0), msgId?: string):
 Promise<AsyncRpcHandle> {
    return this.call(proc, args, { async: true, msgId });
  }

  /**
   * Wait for the reply to an earlier fire() / async call.
   */
  async getResponse(msgId: string, timeout: number = 5000): Promise<RpcCallResult
> {
    if (!this.isConnected()) {
      throw new Error('Client is not connected. Call connect() first.');
    }
    const reply = await this.readReply(msgId, timeout);
    return this.decodeResult(reply, msgId);
  }

  /**
   * Drain every reply that arrives until `timeout` elapses.
   * Returns a map keyed by msgId.
   */
  async collectResponses(timeout = 1000): Promise<Map<string, RpcCallResult>> {
    if (!this.isConnected()) {
      throw new Error('Client is not connected. Call connect() first.');
    }
    const connectionId = this.requireConnection();
    const results = new Map<string, RpcCallResult>();

    for (const [msgId, replay] of this.replyCache) {
      results.set(msgId, this.decodeResult(replay, msgId));
    }
    this.replyCache.clear();

    const deadline = Date.now() + timeout;
    for (;;) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;
      let message: Uint8Array;
      try {
        message = await this.transport.receive(connectionId, remaining);
      } catch {
        break; // timeout, or the connection went away — drain ended
      }
      const msgId = parseMessage(message).msgId ?? this.generateMsgId();
      results.set(msgId, this.decodeResult(message, msgId));
    }

    return results;
  }

  // ----- internals -----

  private requireConnection(): string {
    if (this.connectionId === undefined) {
      throw new Error('Client is not connected. Call connect() first.');
    }
    return this.connectionId;
  }

  private async readReply(expectedMsgId: string, timeoutMs: number): Promise<Uint8Array> {
    const connectionId = this.requireConnection();
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const cached = this.replyCache.get(expectedMsgId);
      if (cached !== undefined) {
        this.replyCache.delete(expectedMsgId);
        return cached;
      }
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new Error(`Timed out after ${timeoutMs}ms waiting for reply to ${expectedMsgId}`);
      }
      const message = await this.transport.receive(connectionId, remaining);
      const msgId = parseMessage(message).msgId;
      if (msgId === expectedMsgId) return message;
      if (msgId !== undefined) this.replyCache.set(msgId, message); // out of order — stash it
    }
  }

  private decodeResult(reply: Uint8Array, expectedMsgId: string): RpcCallResult {

    const parsed = parseMessage(reply);
    const accepted = parsed.type === RpcConstants.CallTypes.RPC_REPLY;
    return {
      status: accepted ? parsed.status : RpcConstants.ReplyStates.RPC_GARBAGE,
      value: parsed.body,
      msgId: parsed.msgId ?? expectedMsgId,
    };
  }

  private generateMsgId(): string {
    return crypto.randomUUID().split('-')[0]!;
  }
}

/**
 * Convenience: connect a fully configured client over TCP.
 */
export async function createTcpClient(
  prognum: number,
  vers: number,
  host: string = '127.0.0.1',
  port: number = 5000,
): Promise<RpcClient> {
  const transport = new TcpTransport({ host, port, isServer: false });
  const client = new RpcClient(prognum, vers, { transport });
  await client.connect();
  return client;
}

