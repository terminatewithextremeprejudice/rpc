// Transport abstraction layer for RPC
import dgram from 'dgram';
import net from 'net';
import { messageSize } from './shared/xdr';

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

/**
 * Framer for the XDR RPC wire format: message boundaries are derived from
 * the XDR fields themselves (see messageSize in shared/xdr), no external
 * length prefix needed.
 */
export class XdrFramer implements MessageFramer {
  messageSize(buffer: Uint8Array): number {
    return messageSize(buffer);
  }
}

export interface TcpTransportOptions {
  host?: string;
  port?: number;
  isServer?: boolean;
  framer?: MessageFramer; // framing to split the byte stream into messages
}

/**
 * TCP transport for the generic Transport layer.
 *
 * - Server mode (`isServer: true`): `connect()` starts listening on
 *   host:port, and every accepted socket becomes its own Connection,
 *   so many concurrent clients can be handled at once.
 * - Client mode (`isServer: false`): `connect()` opens a single socket
 *   to host:port and registers it as a Connection.
 *
 * Byte buffering, message framing, event bus, and receive queue all
 * live on the base Transport/Connection classes — this class merely
 * wires `net` sockets into them.
 */
export class TcpTransport extends Transport {
  private server: net.Server | null = null;
  private clientSocket: net.Socket | null = null;
  private readonly host: string;
  private readonly port: number;
  private readonly isServer: boolean;
  private active = false;

  constructor(options: TcpTransportOptions = {}) {
    // The XDR wire format is self-describing (message size derived from
    // header fields), so that's this library's default framing.
    // Pass `framer: new LengthPrefixedFramer()` for an explicit
    // length-prefixed protocol instead.
    super(options.framer ?? new XdrFramer());
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 5000;
    this.isServer = options.isServer ?? false;
  }

  get hostname(): string { return this.host; }
  get portNumber(): number { return this.port; }
  get actualPort(): number {
    const addr = this.server?.address();
    return addr !== null && typeof addr === 'object' ? addr.port : this.port;
  }

  // ----- channel operations (abstract Transport API) -----
  override connect(): Promise<void> {
    if (this.active) return Promise.resolve();
    const started = this.isServer ? this.startListening() : this.connectClientSocket
();
    return started.then(() => { this.active = true; });
  }

  override send(connectionId: string, data: Uint8Array): Promise<void> {
    const conn = this.connection(connectionId);
    if (!conn || conn.isClosed) {
      return Promise.reject(new Error(`Cannot send: no active connection ${connectionId}`));
    }
    const socket = conn.channel as net.Socket;
    if (!socket || socket.destroyed) {
      return Promise.reject(new Error(`Cannot send: socket for ${connectionId} has been closed`));
    }
    // Resolve when the bytes have actually been flushed, i.e. naturally
    // applies backpressure to a stalled peer.
    return new Promise<void>((resolve, reject) => {
      socket.write(data, (err) => (err ? reject(err) : resolve()));
    });
  }

  override close(connectionId: string): Promise<void> {
    const conn = this.connection(connectionId);
    if (!conn) return Promise.resolve(); // idempotent
    const socket = conn.channel as net.Socket | null;
    if (!socket || socket.destroyed) return Promise.resolve();
    // Graceful shutdown: let the peer close, but don't wait forever.
    return new Promise<void>((resolve) => {
      const bail = setTimeout(() => socket.destroy(), 1000);
      bail.unref?.();
      socket.once('close', () => {
        clearTimeout(bail);
        resolve();
      });
      socket.end();
    });
  }

  override closeAll(): Promise<void> {
    const closes = this.connectionIds().map((id) => {
      // Never fail because one connection is already gone.
      return this.close(id).catch(() => undefined);
    });
    return Promise.all([...closes, this.stopListening()]).then(() => {
      this.active = false;
    });
  }

  // ----- internals -----
  private startListening(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const server = net.createServer((socket: net.Socket) => {
        this.wireSocket(socket);
      });
      this.server = server;
      let settled = false;
      server.on('error', (err: Error) => {
        if (!settled) {
          settled = true;
          this.server = null;
          reject(err);
        } else {
          // Transport-level error after bind: report on the event bus
          // (connectionId '' = not tied to a specific connection).
          this.emitError('', err);
        }
      });
      server.once('listening', () => {
        settled = true;
        resolve();
      });
      server.listen(this.port, this.host);
    });
  }

  private stopListening(): Promise<void> {
    const server = this.server;
    this.server = null;
    if (!server || !server.listening) return Promise.resolve();
    return new Promise<void>((resolve) => {
      server.once('close', () => resolve());
      server.close();
    });
  }

  private connectClientSocket(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const socket = net.connect({ host: this.host, port: this.port });
      let settled = false;

      const settle = () => {
        socket.removeListener('connect', onConnect);
        socket.removeListener('error', onError);
        socket.removeListener('close', onClose);
      };
      const onConnect = () => {
        if (settled) return;
        settled = true;
        settle();
        this.clientSocket = socket;
        this.wireSocket(socket);
        resolve();
      };
      const onError = (err: Error) => {
        if (!settled) { settled = true; settle(); socket.destroy(); reject(err); }

      };
      const onClose = () => {
        if (!settled) { settled = true; settle(); socket.destroy(); reject(new Error(`Connection to ${this.host}:${this.port} dropped before it was established`)); }

      };
      socket.once('connect', onConnect);
      socket.once('error', onError);
      socket.once('close', onClose);
    });
  }

  // Register a live socket as a Connection and route its raw socket events
  // into the Transport's event bus. Returns the connection ID.
  private wireSocket(socket: net.Socket): string {
    const conn = this.registerConnection(socket);
    const id = conn.id;

    socket.on('data', (chunk: Buffer) => {
      if (conn.isClosed) return;
      const drained = conn.append(chunk);
      if (drained > 0) this.emitMessage(id);
    });

    socket.on('error', (err: Error) => {
      this.emitError(id, err);
    });

    socket.once('close', () => {
      if (this.clientSocket === socket) this.clientSocket = null;
      this.unregisterConnection(id, 'connection closed');
    });

    return id;
  }
}

export interface UdpTransportOptions {
  host?: string;
  port?: number;
  isServer?: boolean;
  framer?: MessageFramer;
}

/**
 * UDP transport for the generic Transport layer.
 *
 * - Server mode (`isServer: true`): `connect()` binds host:port and every
 *   peer — identified by the datagram's source address + port (its rinfo) —
 *   becomes its own Connection, mirroring TCP's one-connection-per-socket
 *   model. The first datagram from a peer registers it; explicit
 *   close(connectionId) blacklists the peer so its later datagrams are
 *   dropped.
 * - Client mode (`isServer: false`): `connect()` binds an ephemeral local
 *   socket and registers a single default Connection addressed at host:port;
 *   every datagram received from the peer flows into that one connection.
 *
 * Each RPC message travels in one datagram in this library, so the framer
 * normally sees exactly one complete message per append. The generic
 * buffer + drain pipeline still reassembles a message that a peer
 * fragments or coalesces across datagrams.
 *
 * One semantic difference from TCP: UDP has no connection-teardown signal
 * (no FIN), so close events here are app-driven (close / closeAll) rather
 * than peer-driven like TCP socket hangs.
 */
export class UdpTransport extends Transport {
  private socket: dgram.Socket | null = null;
  private readonly host: string;
  private readonly port: number;
  private readonly isServer: boolean;
  private active = false;

  // Server mode: rinfo key -> connectionId, plus each connection's reply
  // address. closedPeers blocks re-registration of explicitly closed peers.
  private readonly peerByRinfo = new Map<string, string>();
  private readonly rinfoByPeer = new Map<string, dgram.RemoteInfo>();
  private readonly closedPeers = new Set<string>();

  private closePromise: Promise<void> | null = null;

  constructor(options: UdpTransportOptions = {}) {
    // XDR messages are self-delimiting, so they frame datagrams exactly —
    // same default as the TCP transport.
    super(options.framer ?? new XdrFramer());
    this.host = options.host ?? '127.0.0.1';
    this.port = options.port ?? 5000;
    this.isServer = options.isServer ?? false;
  }

  get hostname(): string { return this.host; }
  get portNumber(): number { return this.port; }
  get actualPort(): number {
    const socket = this.socket;
    if (!socket) return this.port;
    try { return socket.address().port; } catch { return this.port; }
  }

  private get family(): 'udp4' | 'udp6' {
    return this.host.includes(':') ? 'udp6' : 'udp4';
  }

  // ----- channel operations (abstract Transport API) -----
  override connect(): Promise<void> {
    if (this.active) return Promise.resolve();
    const settled = new Promise<void>((resolve, reject) => {
      const socket = dgram.createSocket(this.family);
      let done = false;
      socket.on('error', (err: Error) => {
        if (!done) {
          done = true;
          this.socket = null;
          try { socket.close(); } catch { /* already closing */ }
          reject(err);
        } else {
          // Transport-level error after bind.
          const ids = this.connectionIds();
          this.emitError(this.isServer ? '' : (ids[0] ?? ''), err);
        }
      });
      socket.once('close', () => {
        // Socket gone: tear down every connection wired to it.
        for (const id of [...this.connectionIds()]) {
          this.unregisterConnection(id, 'connection closed');
        }
      });
      this.socket = socket;
      socket.bind(this.isServer ? this.port : 0, this.isServer ? this.host : undefined, () => {
        if (this.isServer) {
          this.wireServerMessages(socket);
        } else {
          // Register the default connection before wiring so a datagram
          // can never race ahead of it.
          this.wireClientMessages(socket, this.registerConnection(socket).id);
        }
        if (!done) { done = true; resolve(); }
      });
    });
    return settled.then(() => { this.active = true; });
  }

  override send(connectionId: string, data: Uint8Array): Promise<void> {
    const conn = this.connection(connectionId);
    if (!conn || conn.isClosed) {
      return Promise.reject(new Error(`Cannot send: no active connection ${connectionId}`));
    }
    const socket = this.socket;
    if (!socket) return Promise.reject(new Error('Cannot send: socket is closed'));
    let address: string;
    let port: number;
    if (this.isServer) {
      const rinfo = this.rinfoByPeer.get(connectionId);
      if (!rinfo) return Promise.reject(new Error(`Cannot send: unknown peer for ${connectionId}`));
      address = rinfo.address;
      port = rinfo.port;
    } else {
      address = this.host;
      port = this.port;
    }
    // Resolving means the OS accepted the datagram — UDP has no delivery
    // ack, so that is the strongest flush signal available.
    return new Promise<void>((resolve, reject) => {
      let done = false;
      const finish = (err?: Error) => {
        if (done) return;
        done = true;
        socket.removeListener('error', onError);
        if (err) reject(err); else resolve();
      };
      // A failing socket reports the error on the socket, not (always) in
      // the callback — listen for both so this never hangs.
      const onError = (err: Error) => finish(err);
      socket.once('error', onError);
      try {
        socket.send(data, port, address, (err) => finish(err ?? undefined));
      } catch (err) {
        finish(err as Error);
      }
    });
  }

  override close(connectionId: string): Promise<void> {
    const conn = this.connection(connectionId);
    if (!conn || conn.isClosed) return Promise.resolve(); // idempotent
    if (this.isServer) {
      this.forgetPeer(connectionId);
      this.unregisterConnection(connectionId, 'connection closed');
      return Promise.resolve();
    }
    // Client mode: the one connection is the socket itself — closing it
    // closes the connection.
    return this.closeSocket();
  }

  override closeAll(): Promise<void> {
    return this.closeSocket().then(() => { this.active = false; });
  }

  // ----- internals -----
  private wireServerMessages(socket: dgram.Socket): void {
    socket.on('message', (msg: Buffer, rinfo: dgram.RemoteInfo) => {
      const key = this.rinfoKey(rinfo);
      if (this.closedPeers.has(key)) return; // peer was explicitly closed
      let connectionId = this.peerByRinfo.get(key);
      if (connectionId === undefined) {
        const conn = this.registerConnection(socket);
        connectionId = conn.id;
        this.peerByRinfo.set(key, connectionId);
        this.rinfoByPeer.set(connectionId, rinfo);
      }
      const conn = this.connection(connectionId);
      if (!conn || conn.isClosed) return;
      try {
        // First datagram from a peer: append also registers it (done above);
        // every later datagram reuses the same connection's buffer.
        const drained = conn.append(msg);
        if (drained > 0) this.emitMessage(connectionId);
      } catch {
        // Connection closed between lookup and append — drop the datagram.
      }
    });
  }

  private wireClientMessages(socket: dgram.Socket, connectionId: string): void {
    socket.on('message', (msg: Buffer) => {
      const conn = this.connection(connectionId);
      if (!conn || conn.isClosed) return;
      const drained = conn.append(msg);
      if (drained > 0) this.emitMessage(connectionId);
    });
  }

  private forgetPeer(connectionId: string): void {
    const rinfo = this.rinfoByPeer.get(connectionId);
    if (!rinfo) return;
    const key = this.rinfoKey(rinfo);
    this.closedPeers.add(key);
    this.peerByRinfo.delete(key);
    this.rinfoByPeer.delete(connectionId);
  }

  private rinfoKey(rinfo: dgram.RemoteInfo): string {
    return `${rinfo.address}:${rinfo.port}`;
  }

  private closeSocket(): Promise<void> {
    if (this.closePromise) return this.closePromise;
    const socket = this.socket;
    this.socket = null;
    this.active = false;
    if (!socket) return Promise.resolve();
    this.closePromise = new Promise<void>((resolve) => {
      let done = false;
      const finish = () => { if (!done) { done = true; resolve(); } };
      socket.once('close', finish);
      const safety = setTimeout(finish, 1000);
      safety.unref?.();
      try {
        socket.close();
      } catch {
        finish();
      }
    });
    return this.closePromise;
  }
}
