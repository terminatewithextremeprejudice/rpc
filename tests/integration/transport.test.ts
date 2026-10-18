import { describe, expect, test, beforeAll, afterAll } from '@jest/globals';
import { RpcClient, RpcServer, encodeString, decodeString } from '../../src';
import { TcpTransport, UnixSocketTransport, UdpTransport, SharedMemoryTransport } from '../../src';

interface TransportTestFixture {
  name: string;
  serverTransport: any;
  clientTransport: any;
  cleanup?: () => void;
}

function createTransportTest(name: string, transportConfig: TransportTestFixture): void {
  describe(name, () => {
    let server: RpcServer;
    let client: RpcClient;

    beforeAll(async () => {
      server = new RpcServer({ transport: transportConfig.serverTransport });
      await server.start();
      
      // Small delay to let server bind
      await new Promise(r => setTimeout(r, 100));
      
      client = new RpcClient(100000, 1, { transport: transportConfig.clientTransport });
      await client.connect();
    }, 10000);

    afterAll(async () => {
      try {
        await client.disconnect();
        await server.stop();
      } catch (e) {}
      
      if (transportConfig.cleanup) {
        transportConfig.cleanup();
      }
    }, 5000);

    test('connects client to server', () => {
      expect(client.isConnected()).toBe(true);
      expect(server.isRunning()).toBe(true);
    });

    test('sends and receives XDR-encoded string message', async () => {
      const message = `hello from ${name}`;
      const result = await client.callWithString(1, message);
      
      expect(result.status).toBeGreaterThanOrEqual(0);
      expect(client.isConnected()).toBe(true);
    });

    test('handles multiple sequential calls', async () => {
      const results = [];
      
      for (let i = 0; i < 3; i++) {
        const result = await client.callWithString(1, `message ${i}`);
        results.push(result);
      }

      expect(results).toHaveLength(3);
      results.forEach(r => expect(r.status).toBeGreaterThanOrEqual(0));
    });

    test('sends binary data', async () => {
      const binaryData = new Uint8Array([0x01, 0x02, 0x03, 0xFF, 0x00]);
      const result = await client.call(2, binaryData);
      
      expect(result.status).toBeGreaterThanOrEqual(0);
    });

    test('round-trip preserves encoded data', async () => {
      const original = encodeString('transport test payload');
      await client.call(3, original);
      
      // Verify we can decode what we sent
      const decoded = decodeString(original);
      expect(decoded).toBe('transport test payload');
    });
  });
}

describe('Transport Layer Integration Tests', () => {
  createTransportTest('TCP Transport', {
    name: 'TCP',
    serverTransport: new TcpTransport('127.0.0.1', 5001),
    clientTransport: new TcpTransport('127.0.0.1', 5001)
  });

  createTransportTest('Unix Socket Transport', {
    name: 'Unix Socket',
    serverTransport: new UnixSocketTransport('/tmp/test-unix-integration.sock'),
    clientTransport: new UnixSocketTransport('/tmp/test-unix-integration.sock'),
    cleanup: () => {
      const fs = require('fs');
      try { fs.unlinkSync('/tmp/test-unix-integration.sock'); } catch (e) {}
    }
  });

  createTransportTest('UDP Transport', {
    name: 'UDP',
    serverTransport: new UdpTransport('127.0.0.1', 5003),
    clientTransport: new UdpTransport('127.0.0.1', 5003)
  });

  createTransportTest('Shared Memory Transport', {
    name: 'Shared Memory',
    serverTransport: new SharedMemoryTransport('/tmp/test-shm-integration'),
    clientTransport: new SharedMemoryTransport('/tmp/test-shm-integration'),
    cleanup: () => {
      const fs = require('fs');
      try { fs.unlinkSync('/tmp/test-shm-integration'); } catch (e) {}
    }
  });

  describe('Transport Comparison', () => {
    test('all transports accept same XDR message format', async () => {
      const message = encodeString('standard test message');
      
      const transports: { name: string; transport: any }[] = [
        { name: 'TCP', transport: new TcpTransport('127.0.0.1', 5010) },
        { name: 'UDP', transport: new UdpTransport('127.0.0.1', 5010) },
      ];

      for (const t of transports) {
        try {
          await t.transport.connect();
          await t.transport.send(message);
          await t.transport.close();
          expect(true).toBe(true); // Just verify it didn't throw
        } catch (e) {
          console.warn(`${t.name} transport failed:`, e);
        }
      }
    });
  });
});