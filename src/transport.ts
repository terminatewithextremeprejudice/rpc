// Transport abstraction layer for RPC
import { decode, RpcConstants, messageSize } from './shared/xdr';

// Generic transport layer for RPC.
//
// A Transport is a message-oriented channel: it owns one or more
// connections, accumulates bytes per connection, splits them into
// complete messages using a pluggable MessageFramer, and exposes
// both a pull API (receive / receiveAny / takeMessage) and an event
// API (message / connection / close / error).

/**
 * Splits a byte stream into whole messages.
 *
 * Implementations must be stateless across calls and must work on
 * any prefix of a connection's byte buffer.
 */
export interface MessageFramer {
  /**
   * Return the total size (in bytes) of the first complete message
   * starting at the head of `buffer`, or -1 if `buffer` does not yet
   * contain a complete message.
   */
  messageSize(buffer: Uint8Array): number;
}

export class LengthPrefixedFramer implements MessageFramer {
  constructor(private readonly prefixBytes: number = 4) {}

  messageSize(buffer: Uint8Array): number {
    if (buffer.length < this.prefixBytes) return -1;
    let size = 0;
    for (let i = 0; i < this.prefixBytes; i++) {
      size = size * 256 + (buffer[i] ?? 0);
    }
    if (size === 0 || buffer.length < this.prefixBytes + size) return -1;
    return this.prefixBytes + size;
  }
}

export class Connection {
  readonly id: string;
  readonly channel: unknown;
  private readonly framer: MessageFramer;
  private buffer: Uint8Array = new Uint8Array(0);
  private completed: Uint8Array[] = [];
  private closed = false;

  constructor(id: string, channel: unknown, framer: MessageFramer) {
    this.id = id;
    this.channel = channel;
    this.framer = framer;
  }

  get isClosed(): boolean { return this.closed; }

  bufferedBytes(): number { return this.buffer.length; }
  queuedMessages(): number { return this.completed.length; }

   // Append incoming bytes; returns how many complete messages became
   // available as a result.
  append(chunk: Uint8Array): number {
    if (this.closed) throw new Error(`Connection ${this.id} is closed`);
    const merged = new Uint8Array(this.buffer.length + chunk.length);
    merged.set(this.buffer, 0);
    merged.set(chunk, this.buffer.length);
    this.buffer = merged;
    return this.drain();
  }

  hasCompleteMessage(): boolean {
    if (this.completed.length > 0) return true;
    return this.drain() > 0;
  }

  // Remove and return the next complete message (or undefined).
  takeMessage(): Uint8Array | undefined {
    if (this.completed.length === 0) this.drain();
    return this.completed.shift();
  }

  markClosed(): void { this.closed = true; }

  private drain(): number {
    let drained = 0;
    for (;;) {
      const size = this.framer.messageSize(this.buffer);
      if (size <= 0) break;
      const message = this.buffer.subarray(0, size);
      this.buffer = this.buffer.subarray(size);
      this.completed.push(message);
      drained++;
    }
    return drained;
  }
}

type Listener = (...args: any[]) => void;
type Subscription = {
    unsubscribe: () => void
}

class SimpleEmitter {
  private readonly listeners = new Map<string, Listener[]>();

  on(event: string, listener: Listener): Subscription {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, []);
    }
    const list = this.listeners.get(event) as Listener[];
    list.push(listener);
    return { unsubscribe: () => list.splice(list.indexOf(listener), 1) };
  }
  once(event: string, listener: Listener): Subscription {
    const wrapper: Listener = (...args) => {
      this.off(event, wrapper);
      listener(...args);
    };
    return this.on(event, wrapper);
  }
  off(event: string, listener: Listener): void {
    const subs = this.listeners.get(event);
    if (!subs) return;
    const idx = subs.indexOf(listener);
    if (idx !== -1) {
      subs.splice(idx, 1);
      if (subs.length === 0) this.listeners.delete(event);
    }
  }
  emit(event: string, ...args: any[]): void {
    const subs = this.listeners.get(event);
    if (!subs) return;
    // Iterate over a copy so that off/once modifications during iteration are safe

    for (const listener of [...subs]) {
      listener(...args);
    }
  }
  clear(): void { this.listeners.clear(); }
}

export interface TransportEventMap {
  // A connection has at least one complete message ready.
  message: (connectionId: string) => void;
  // A new connection has been established. 
  connection: (connectionId: string) => void;
  // A connection has been torn down.
  close: (connectionId: string, reason?: string) => void;
  // A transport-level error on a connection.
  error: (connectionId: string, error: Error) => void;
}

export abstract class Transport {
  static readonly DEFAULT_TIMEOUT_MS = 5000;

  protected readonly connections = new Map<string, Connection>();
  protected readonly events = new SimpleEmitter();
  private idCounter = 0;

  constructor(protected readonly framer: MessageFramer) {}

  // ----- events -----
  on<K extends keyof TransportEventMap>(event: K, listener: TransportEventMap[K]): void { this.events.on(event, listener); }
  off<K extends keyof TransportEventMap>(event: K, listener: TransportEventMap[K]): void { this.events.off(event, listener); }
  once<K extends keyof TransportEventMap>(event: K, listener: TransportEventMap[K]): void { this.events.once(event, listener); }

  protected emitMessage(connectionId: string): void { this.events.emit('message', connectionId); }
  protected emitConnection(connectionId: string): void { this.events.emit('connection', connectionId); }
  protected emitClose(connectionId: string, reason?: string): void { this.events.emit('close', connectionId, reason); }
  protected emitError(connectionId: string, error: Error): void { this.events.emit('error', connectionId, error); }

  // ----- connections -----
  connection(connectionId: string): Connection | undefined { return this.connections
.get(connectionId); }
  connectionIds(): string[] { return [...this.connections.keys()]; }
  connectionCount(): number { return this.connections.size; }
  isConnected(connectionId: string): boolean {
    const conn = this.connections.get(connectionId);
    return conn !== undefined && !conn.isClosed;
  }
  readyConnectionIds(): string[] {
    const ready: string[] = [];
    for (const [id, conn] of this.connections) {
      if (conn.hasCompleteMessage()) ready.push(id);
    }
    return ready;
  }
  // ----- registration (used by concrete transports) -----
  registerConnection(channel: unknown): Connection {
    const id = this.generateConnectionId();
    const conn = new Connection(id, channel, this.framer);
    this.connections.set(id, conn);
    this.emitConnection(id);
    return conn;
  }

  unregisterConnection(connectionId: string, reason?: string): void {
    const conn = this.connections.get(connectionId);
    if (!conn) return;
    this.connections.delete(connectionId);
    conn.markClosed();
    this.emitClose(connectionId, reason);
  }

  protected generateConnectionId(): string {
    this.idCounter += 1;
    const random = Math.random().toString(36).slice(2, 10);
    return `conn${this.idCounter.toString(36)}${random}`;
  }

  hasCompleteMessage(connectionId: string): boolean {
    return this.connections.get(connectionId)?.hasCompleteMessage() ?? false;
  }

  takeMessage(connectionId: string): Uint8Array | undefined {
    return this.connections.get(connectionId)?.takeMessage();
  }

  // Wait for the next complete message on `connectionId`.
  // Resolves immediately if a message is already queued.
  async receive(connectionId: string, timeoutMs: number = Transport.DEFAULT_TIMEOUT_MS): Promise<Uint8Array> {
    const conn = this.connections.get(connectionId);
    if (!conn) throw new Error(`No active connection: ${connectionId}`);
    const result = await this.nextMessage([connectionId], timeoutMs);
    if (!result) {
      const still = this.connections.get(connectionId);
      throw new Error(
        still
          ? `Receive timeout: no complete message on ${connectionId} (${still.bufferedBytes()} bytes buffered, ${still.queuedMessages()} queued)`
          : `Connection ${connectionId} closed before a complete message arrived`,

      );
    }
    return result.message;
  }

   // Wait for the next complete message on any connection.
   // Resolves with `undefined` after `timeoutMs` if none arrive.
  receiveAny(timeoutMs: number = Transport.DEFAULT_TIMEOUT_MS): Promise<{ connectionId: string; message: Uint8Array } | undefined> {
    return this.nextMessage(null, timeoutMs);
  }

   // Resolve `true` when any listed connection (or any connection if
   // `connectionIds` is omitted) has a message ready, `false` on timeout.
  waitForData(timeoutMs: number, connectionIds?: string[]): Promise<boolean> {
    const list = connectionIds ?? null;
    const matches = (id: string) => list === null || list.includes(id);
    if (this.connectionIds().some((id) => matches(id) && this.hasCompleteMessage(id
))) return Promise.resolve(true);
    return new Promise((resolve) => {
      let settled = false;
      const finish = (value: boolean) => {
        if (settled) return;
        settled = true;
        this.events.off('message', onMessage);
        this.events.off('close', onClose);
        clearTimeout(timer);
        resolve(value);
      };
      const timer = setTimeout(() => finish(false), timeoutMs);
      const onMessage = (connectionId: string) => {
        if (matches(connectionId)) finish(true);
      };
      const onClose = (connectionId: string) => {
        if (matches(connectionId)) finish(false);
      };
      this.events.on('message', onMessage);
      this.events.on('close', onClose);
    });
  }

  // Wait for the next new connection to be established (server mode). 
  waitForConnection(timeoutMs: number = 10000): Promise<string> {
    const existing = this.connectionIds();
    if (existing.length > 0) return Promise.resolve(existing[0] as string);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.events.off('connection', onConnection);
        reject(new Error('Timed out waiting for a new connection'));
      }, timeoutMs);
      const onConnection = (connectionId: string) => {
        clearTimeout(timer);
        this.events.off('connection', onConnection);
        resolve(connectionId);
      };
      this.events.on('connection', onConnection);
    });
  }

  // ----- channel operations (implemented by concrete transports) -----
  abstract connect(): Promise<void>;
  abstract send(connectionId: string, data: Uint8Array): Promise<void>;
  abstract close(connectionId: string): Promise<void>;
  abstract closeAll(): Promise<void>;

  // ----- internals -----
  private nextMessage(connectionIds: string[] | null, timeoutMs: number): Promise<{ connectionId: string; message: Uint8Array } | undefined> {
    const matches = (id: string) => connectionIds === null || connectionIds.includes(id);
    const grab = (): { connectionId: string; message: Uint8Array } | undefined => {
      for (const [id, conn] of this.connections) {
        if (!matches(id)) continue;
        const message = conn.takeMessage();
        if (message !== undefined) return { connectionId: id, message };
      }
      return undefined;
    };

    const immediate = grab();
    if (immediate) return Promise.resolve(immediate);

    return new Promise((resolve) => {
      let settled = false;
      const finish = (value?: { connectionId: string; message: Uint8Array }) => {

        if (settled) return;
        settled = true;
        this.events.off('message', onMessage);
        this.events.off('close', onClose);
        clearTimeout(timer);
        resolve(value);
      };
      const timer = setTimeout(() => finish(undefined), timeoutMs);
      const onMessage = () => {
        const got = grab();
        if (got) finish(got);
      };
      const onClose = (connectionId: string) => {
        if (matches(connectionId)) finish(undefined);
      };
      this.events.on('message', onMessage);
      this.events.on('close', onClose);
    });
  }
}

export interface TcpTransportOptions {
  host?: string;
  port?: number;
  isServer?: boolean;
  framer?: MessageFramer; // framing to split the byte stream into messages
}

export class TcpTransport extends Transport {
  private server: { close(cb?: () => void): void } | null = null;
  private readonly host: string;
  private readonly port: number;
  private readonly isServer: boolean;
  private active = false;

  constructor(options: TcpTransportOptions = {}) {
    super(options.framer ?? new LengthPrefixedFramer());
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 5000;
    this.isServer = options.isServer ?? false;
  }

  get hostname(): string { return this.host; }
  get portNumber(): number { return this.port; }

  override async connect(): Promise<void> {}
  override async close(connectionId: string): Promise<void> {}
  override async send(connectionId: string, data: Uint8Array): Promise<void> {}
  override async closeAll(): Promise<void> {}
  
  private wireSocket(socket: import('net').Socket): string {}
}
