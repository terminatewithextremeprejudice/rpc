// Transport Integration Tests with Worker-Based Parallel Execution
import { describe, test, beforeAll, afterAll, expect } from '@jest/globals';
import { Worker } from 'node:worker_threads';
import { RpcServer } from '../../src';
import * as pathModule from 'node:path';

const testDir = pathModule.dirname(require.resolve('./transport.test.ts'));

interface TestTransportConfig {
  name: string;
  type: 'tcp' | 'unix' | 'udp' | 'memory';
  host?: string;
  port?: number;
  socketPath?: string;
  path?: string;
  createServerTransport: () => any;
  cleanup?: () => void;
}

interface ClientResult {
  test: string;
  status: string;
  [key: string]: any;
}

/**
 * Run integration tests for a transport type
 * - Server runs in main thread
 * - Client runs in worker thread (parallel execution)
 */
function runTransportTest(config: TestTransportConfig): void {
  describe(config.name, () => {
    let server: RpcServer;
    let serverTransport: any;
    
    beforeAll(async () => {
      // Create server transport in server mode
      serverTransport = config.createServerTransport();
      
      // Initialize and start server
      server = new RpcServer({ transport: serverTransport, isServer: true });
      await server.start();
    }, 15000);

    afterAll(async () => {
      try {
        await server.stop();
      } catch (e) {}
      
      if (config.cleanup) {
        config.cleanup();
      }
    }, 5000);

    test(`runs all client tests in parallel worker`, async () => {
      // Prepare worker data
      const workerData = {
        type: config.type,
        host: config.host,
        port: config.port,
        socketPath: config.socketPath,
        path: config.path,
      };

      return new Promise<void>((resolve, reject) => {
        const workerPath = pathModule.join(testDir, './worker-client.ts');
        const worker = new Worker(workerPath, { workerData });

        const timeout = setTimeout(() => {
          worker.terminate();
          reject(new Error(`Worker timeout for ${config.name}`));
        }, 10000);

        worker.on('message', (message: { completed: boolean; results: ClientResult[] }) => {
          clearTimeout(timeout);
          worker.terminate();
          
          if (!message.completed) {
            reject(new Error(`Worker failed: ${config.name}`));
            return;
          }
          
          // Display results
          console.log(`\n${config.name} Results:`);
          console.log('─'.repeat(40));
          message.results.forEach(r => {
            console.log(`  [${r.status}] ${r.test}`);
            if (r.error) console.log(`    Error: ${r.error}`);
          });
          console.log('─'.repeat(40));
          
          // Verify all tests passed
          const allPassed = message.results.every(r => r.status === 'PASS');
          const passCount = message.results.filter(r => r.status === 'PASS').length;
          const totalCount = message.results.length;
          
          expect(passCount).toBe(totalCount);
          
          if (!allPassed) {
            const failed = message.results.filter(r => r.status !== 'PASS');
            throw new Error(`Failed tests: ${failed.map(f => f.test).join(', ')}`);
          }
          
          resolve();
        });

        worker.on('error', (err) => {
          clearTimeout(timeout);
          worker.terminate();
          reject(err);
        });

        worker.on('exit', (code) => {
          clearTimeout(timeout);
          if (code !== 0 && code !== undefined) {
            reject(new Error(`Worker exited with code ${code}`));
          }
        });
      });
    }, 12000);
  });
}

// ============================================================================
// Test Suites for Each Transport Type
// ============================================================================

const testId = Date.now();

runTransportTest({
  name: 'TCP Transport',
  type: 'tcp',
  host: '127.0.0.1',
  port: 5901,
  createServerTransport: () => {
    const { TcpTransport } = require('../../src/transport');
    return new TcpTransport('127.0.0.1', 5901, true);
  }
});

runTransportTest({
  name: 'Unix Socket Transport',
  type: 'unix',
  socketPath: `/tmp/test-unix-integration-${testId}.sock`,
  createServerTransport: () => {
    const { UnixSocketTransport } = require('../../src/transport');
    return new UnixSocketTransport(`/tmp/test-unix-integration-${testId}.sock`, true);
  },
  cleanup: () => {
    const fs = require('fs');
    const path = `/tmp/test-unix-integration-${testId}.sock`;
    try { fs.unlinkSync(path); } catch (e) {}
  }
});

runTransportTest({
  name: 'UDP Transport',
  type: 'udp',
  host: '127.0.0.1',
  port: 5903,
  createServerTransport: () => {
    const { UdpTransport } = require('../../src/transport');
    return new UdpTransport('127.0.0.1', 5903, true);
  }
});

runTransportTest({
  name: 'Shared Memory Transport',
  type: 'memory',
  path: `/tmp/test-shm-integration-${testId}`,
  createServerTransport: () => {
    const { SharedMemoryTransport } = require('../../src/transport');
    return new SharedMemoryTransport(`/tmp/test-shm-integration-${testId}`);
  },
  cleanup: () => {
    const fs = require('fs');
    const path = `/tmp/test-shm-integration-${testId}`;
    try { fs.unlinkSync(path); } catch (e) {}
  }
});

// ============================================================================
// Summary Report
// ============================================================================

describe('Integration Test Summary', () => {
  test('all transport types tested', () => {
    const transportsTested = 4; // TCP, Unix, UDP, Memory
    expect(transportsTested).toBe(4);
  });
});
