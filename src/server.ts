// RPC server implementation
// Accepts XDR-encoded RPC calls on the configured transport, dispatches them
// to registered handlers, and routes the reply back to the originating
// connection. Concurrent connections are first-class: every request tracks
// the connection it arrived on, and the reply is written back to exactly
// that connection.

import { RpcConstants } from './shared/xdr';
import { buildReply, parseMessage } from './shared/message';
import { Transport, TcpTransport } from './transport';

export interface RpcHandler {
  procedure: number;
  version: number;
  handler: (args: Uint8Array) => unknown;
}

export interface RpcServerOptions {
  transport: Transport;
}

export interface QueuedRequest {
  connectionId: string;
  proc: number;
  vers: number;
  args: Uint8Array;
  msgId: string;
}

export interface IncomingRequest {
  connectionId: string;
  msgId: string;
  proc: number;
  args: Uint8Array;
}

export class RpcServer {
  private readonly transport: Transport;
  private running = false;

  // Handler lookup key is "proc:vers"
  private readonly handlerRegistry = new Map<string, RpcHandler>();
  // Requests received but not yet processed (insertion order)
  private readonly pendingRequests = new Map<string, QueuedRequest>();
  // Replies built but not yet transmitted: msgId -> { connectionId, data }
  private readonly responseBuffer = new Map<string, { connectionId: string; data:
 Uint8Array }>();

  constructor(options: RpcServerOptions) {
    this.transport = options.transport;
  }

  register(procedure: number, version: number, handler: RpcHandler['handler']): void
 {
    this.handlerRegistry.set(`${procedure}:${version}`, { procedure, version, handler
 });
  }

  async start(waitForConnection: boolean = true): Promise<void> {
    if (this.running) throw new Error('RPC server already running');
    await this.transport.connect();
    if (waitForConnection) {
      await this.transport.waitForConnection();
    }
    this.running = true;
  }

  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    await this.transport.closeAll();
    this.pendingRequests.clear();
    this.responseBuffer.clear();
  }

  isRunning(): boolean {
    return this.running;
  }

  getTransport(): Transport {
    return this.transport;
  }

  /**
   * Wait for the next incoming request from any connection, queue it,
   * and return its metadata. Resolves to `undefined` on timeout.
   */
  async receiveNext(timeoutMs: number = Transport.DEFAULT_TIMEOUT_MS): Promise<IncomingRequest | undefined> {
    if (!this.running) throw new Error('Server not started. Call start() first.');

    const next = await this.transport.receiveAny(timeoutMs);
    if (!next) return undefined;
    return this.queueIncomingMessage(next.connectionId, next.message);
  }

  /**
   * Wait for the next incoming request on a specific connection, queue it,
   * and return its msgId.
   */
  async queueIncomingRequest(connectionId: string, timeoutMs: number = Transport.DEFAULT_TIMEOUT_MS): Promise<string | undefined> {
    if (!this.running) throw new Error('Server not started. Call start() first.');

    const message = await this.transport.receive(connectionId, timeoutMs);
    return this.queueIncomingMessage(connectionId, message)?.msgId;
  }

  /**
   * Build the reply for a queued request and buffer it for transmission.
   */
  async processQueuedRequest(msgId: string): Promise<Uint8Array | undefined> {
    const request = this.pendingRequests.get(msgId);
    if (!request) {
      return undefined;
    }
    this.pendingRequests.delete(msgId);
    const response = await this.buildResponseForRequest(request.proc, request.vers
, request.args, request.msgId);
    this.responseBuffer.set(request.msgId, { connectionId: request.connectionId, data
: response });
    return response;
  }

  /**
   * Process every queued request and transmit every buffered reply
   * (on the connection it belongs to). Returns the number of replies sent.
   */
  async flushResponses(): Promise<number> {
    return (await this.processAndFlush()).sent;
  }

  /**
   * Process queued requests (optionally filtered by connection, capped by
   * batchSize) and flush the resulting replies.
   */
  async processAndFlush(connectionId?: string, batchSize = 0): Promise<{ processed
: number; sent: number }> {
    let processed = 0;
    const maxBatch = batchSize === 0 ? this.pendingRequests.size : batchSize;

    for (const [msgId, request] of this.pendingRequests) {
      if (processed >= maxBatch) break;
      if (connectionId !== undefined && request.connectionId !== connectionId) continue
;
      this.pendingRequests.delete(msgId);
      const response = await this.buildResponseForRequest(request.proc, request.vers
, request.args, request.msgId);
      this.responseBuffer.set(request.msgId, { connectionId: request.connectionId
, data: response });
      processed++;
    }

    let sent = 0;
    for (const [msgId, buffered] of this.responseBuffer) {
      try {
        await this.transport.send(buffered.connectionId, buffered.data);
        sent++;
      } catch {
        // The connection disappeared before the reply could be sent — drop it.
      }
    }
    this.responseBuffer.clear();
    return { processed, sent };
  }

  getStats(): { pendingRequests: number; bufferedResponses: number; activeConnections
: number } {
    return {
      pendingRequests: this.pendingRequests.size,
      bufferedResponses: this.responseBuffer.size,
      activeConnections: this.transport.connectionCount(),
    };
  }

  hasMessageReady(connectionId: string): boolean {
    return this.transport.hasCompleteMessage(connectionId);
  }

  getReadyConnections(): string[] {
    return this.transport.readyConnectionIds();
  }

  // ----- internals -----

  private queueIncomingMessage(
    connectionId: string,
    message: Uint8Array,
  ): IncomingRequest | undefined {
    const parsed = parseMessage(message);
    const msgId = parsed.msgId ?? this.generateMsgId();
    this.pendingRequests.set(msgId, {
      connectionId,
      proc: parsed.proc,
      vers: parsed.vers,
      args: parsed.body,
      msgId,
    });
    return { connectionId, msgId, proc: parsed.proc, args: parsed.body };
  }

  private async buildResponseForRequest(
    proc: number,
    vers: number,
    args: Uint8Array,
    msgId: string,
  ): Promise<Uint8Array> {
    const key = `${proc}:${vers}`;
    const handler = this.handlerRegistry.get(key) ?? this.handlerRegistry.get(`${
proc}:*`);
    if (!handler) {
      return buildReply(RpcConstants.ReplyStates.PROC_MISMATCH, msgId);
    }
    try {
      const result = handler.handler(args);
      return buildReply(RpcConstants.ReplyStates.RPC_ACCEPTED, msgId, toOpaquePayload
(result));
    } catch {
      return buildReply(RpcConstants.ReplyStates.SYSERR, msgId);
    }
  }

  private generateMsgId(): string {
    return crypto.randomUUID().split('-')[0]!;
  }
}

function toOpaquePayload(value: unknown): Uint8Array {
  if (value === undefined || value === null) return new Uint8Array(0);
  if (value instanceof Uint8Array) return value;
  if (typeof value === 'string') return new TextEncoder().encode(value);
  if (typeof value === 'number') {
    const buf = new Uint8Array(4);
    new DataView(buf.buffer).setUint32(0, Math.trunc(value), false);
    return buf;
  }
  if (typeof value === 'bigint') {
    const buf = new Uint8Array(8);
    new DataView(buf.buffer).setBigUint64(0, value, false);
    return buf;
  }
  return new TextEncoder().encode(String(value));
}

/**
 * Convenience: start a TCP-based RPC server.
 */
export async function startRpcServer(
  host: string = '127.0.0.1',
  port: number = 5000,
): Promise<RpcServer> {
  const transport = new TcpTransport({ host, port, isServer: true });
  const server = new RpcServer({ transport });
  await server.start();
  return server;
}

