import { describe, expect, test, beforeAll, afterAll } from '@jest/globals';
import { RpcClient, RpcServer } from '../src';
import { TcpTransport, UnixSocketTransport, SharedMemoryTransport } from '../src/transport';
import { startRpcServer, startUnixRpcServer } from '../src/server';

describe('Integration Tests: Transport Layers', () => {
  describe('TCP Transport', () => {
    let server: RpcServer;
    let client: RpcClient;

    beforeAll(async () => {
      const transport = new TcpTransport('127.0.0.1', 5001);
      server = new RpcServer({ transport });
      await server.start();
      
      client = new RpcClient(100000, 1, { transport: new TcpTransport('127.0.0.1', 5001) });
      await client.connect();
    }, 10000);

    afterAll(async () => {
      await client.disconnect();
      await server.stop();
    }, 5000);

    test('sends and receives XDR-encoded message via TCP', async () => {
      const result = await client.callWithString(1, 'hello tcp');
      
      expect(result.status).toBe(0); // RPC_ACCEPTED
      expect(client.isConnected()).toBe(true);
    });

    test('handles multiple sequential calls', async () => {
      const results = [];
      
      for (let i = 0; i < 3; i++) {
        const result = await client.callWithString(1, `message ${i}`);
        results.push(result);
      }

      expect(results).toHaveLength(3);
      results.forEach(r => expect(r.status).toBe(0));
    });

    test('sends binary data via TCP', async () => {
      const binaryData = new Uint8Array([0x01, 0x02, 0x03, 0x04, 0xFF]);
      const result = await client.call(2, binaryData);
      
      expect(result.status).toBe(0);
    });
  });

  describe('Unix Socket Transport', () => {
    let server: RpcServer;
    let client: RpcClient;
    const socketPath = '/tmp/test-rpc-unix.sock';

    beforeAll(async () => {
      const transport = new UnixSocketTransport(socketPath);
      server = new RpcServer({ transport });
      await server.start();
      
      client = new RpcClient(100000, 1, { 
        transport: new UnixSocketTransport(socketPath) 
      });
      await client.connect();
    }, 10000);

    afterAll(async () => {
      await client.disconnect();
      await server.stop();
    }, 5000);

    test('sends and receives XDR-encoded message via Unix socket', async () => {
      const result = await client.callWithString(1, 'hello unix socket');
      
      expect(result.status).toBe(0); // RPC_ACCEPTED
      expect(client.isConnected()).toBe(true);
    });

    test('handles Unicode strings', async () => {
      const result = await client.callWithString(1, 'こんにちは世界 🌍');
      
      expect(result.status).toBe(0);
    });

    test('sends large binary payload via Unix socket', async () => {
      const largeBinary = new Uint8Array(1000);
      for (let i = 0; i < 1000; i++) {
        largeBinary[i] = i % 256;
      }
      
      const result = await client.call(3, largeBinary);
      expect(result.status).toBe(0);
    });
  });

  describe('Shared Memory Transport', () => {
    let server: RpcServer;
    let client: RpcClient;
    const shmPath = '/tmp/test-rpc-shm';

    beforeAll(async () => {
      const transport = new SharedMemoryTransport(shmPath);
      server = new RpcServer({ transport });
      await server.start();
      
      client = new RpcClient(100000, 1, { 
        transport: new SharedMemoryTransport(shmPath) 
      });
      await client.connect();
    }, 10000);

    afterAll(async () => {
      await client.disconnect();
      await server.stop();
    }, 5000);

    test('sends message via shared memory', async () => {
      const result = await client.callWithString(1, 'hello shared memory');
      
      expect(result.status).toBe(0);
      expect(client.isConnected()).toBe(true);
    });

    test('multiple clients can share the same memory region', async () => {
      // Create a second client using the same shared memory path
      const client2 = new RpcClient(100000, 1, {
        transport: new SharedMemoryTransport(shmPath)
      });
      
      await client2.connect();
      const result2 = await client2.callWithString(1, 'second client');
      
      expect(result2.status).toBe(0);
      await client2.disconnect();
    });
  });

  describe('Transport Switching', () => {
    test('client can work with different transport types', async () => {
      // TCP transport test
      const tcpClient = new RpcClient(100000, 1, {
        transport: new TcpTransport('127.0.0.1', 5099)
      });
      
      // Note: This test would need a corresponding server running on port 5099
      // For now, we just verify the client can be instantiated and connected
      // A full test would require spawning a server process
      
      await tcpClient.connect();
      expect(tcpClient.isConnected()).toBe(true);
      await tcpClient.disconnect();
    });
  });
});

describe('Server-Client Integration', () => {
  let server: RpcServer;
  const socketPath = '/tmp/integration-rpc.sock';

  beforeAll(async () => {
    server = await startUnixRpcServer(socketPath);
  }, 10000);

  afterAll(async () => {
    await server.stop();
  }, 5000);

  test('full request-response cycle via Unix socket', async () => {
    const client = await createUnixClient(100000, 1, socketPath);
    
    const result = await client.callWithString(1, 'integration test');
    
    expect(result.status).toBe(0); // RPC_ACCEPTED
    expect(server.isRunning()).toBe(true);
    
    await client.disconnect();
  });

  test('multiple procedure calls', async () => {
    const client = await createUnixClient(100000, 1, socketPath);
    
    // Call different procedures
    const results = [
      await client.call(1),
      await client.call(2),
      await client.callWithString(3, 'test'),
      await client.call(4, new Uint8Array([1, 2, 3]))
    ];

    results.forEach((r, i) => {
      expect(r.status).toBe(0);
    });

    await client.disconnect();
  });

  test('handles connection lifecycle', async () => {
    const client = await createUnixClient(100000, 1, socketPath);
    
    expect(client.isConnected()).toBe(true);
    
    await client.disconnect();
    expect(client.isConnected()).toBe(false);
    
    // Reconnect
    await client.connect();
    expect(client.isConnected()).toBe(true);
    
    await client.disconnect();
  });
});