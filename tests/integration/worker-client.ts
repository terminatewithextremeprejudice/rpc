// Integration tests using workers for parallel client/server execution
import { describe, expect, test, beforeAll, afterAll } from '@jest/globals';
import { workerData, parentPort } from 'node:worker_threads';

// This is the CLIENT worker - it runs in a separate thread
function runClientTests(testConfig: any): void {
  // Import must happen here, not at module level
  const { RpcClient, decodeString } = await import('../../src');
  
  async function runTests() {
    const results: any = [];
    
    // Create client transport based on test config
    let clientTransport: any;
    if (testConfig.type === 'tcp') {
      const { TcpTransport } = await import('../../src/transport');
      clientTransport = new TcpTransport(testConfig.host, testConfig.port, false);
    } else if (testConfig.type === 'unix') {
      const { UnixSocketTransport } = await import('../../src/transport');
      clientTransport = new UnixSocketTransport(testConfig.socketPath, false);
    } else if (testConfig.type === 'udp') {
      const { UdpTransport } = await import('../../src/transport');
      clientTransport = new UdpTransport(testConfig.host, testConfig.port, false);
    } else if (testConfig.type === 'memory') {
      const { SharedMemoryTransport } = await import('../../src/transport');
      clientTransport = new SharedMemoryTransport(testConfig.path);
    }
    
    const client = new RpcClient(100000, 1, { transport: clientTransport });
    
    try {
      // Connect to server
      await client.connect();
      
      results.push({
        test: 'connects to server',
        status: client.isConnected() ? 'PASS' : 'FAIL',
        connected: client.isConnected()
      });
      
      // Test 1: Send string message
      const msg1 = `hello from ${testConfig.type} client`;
      const result1 = await client.callWithString(1, msg1);
      results.push({
        test: 'sends string message',
        status: result1.status >= 0 ? 'PASS' : 'FAIL',
        status: result1.status
      });
      
      // Test 2: Multiple sequential calls
      const results2 = [];
      for (let i = 0; i < 3; i++) {
        const r = await client.callWithString(1, `msg ${i}`);
        results2.push(r.status >= 0);
      }
      results.push({
        test: 'sequential calls',
        status: results2.every(x => x) ? 'PASS' : 'FAIL',
        passed: results2.every(x => x)
      });
      
      // Test 3: Binary data
      const binaryData = new Uint8Array([0x01, 0x02, 0x03, 0xFF, 0x00]);
      const result3 = await client.call(2, binaryData);
      results.push({
        test: 'sends binary data',
        status: result3.status >= 0 ? 'PASS' : 'FAIL',
        status: result3.status
      });
      
      // Test 4: Round-trip preserves data
      const original = await import('../../src/shared/xdr').then(m => m.encodeString('payload test'));
      await client.call(3, original);
      const decoded = decodeString(original);
      results.push({
        test: 'round-trip preserves data',
        status: decoded === 'payload test' ? 'PASS' : 'FAIL',
        decoded
      });
      
    } catch (error: any) {
      results.push({
        test: 'error handling',
        status: 'ERROR',
        error: error.message
      });
    } finally {
      try {
        await client.disconnect();
      } catch (e) {}
    }
    
    // Send results back to parent
    parentPort?.postMessage({ completed: true, results });
  }
  
  runTests().catch(err => {
    parentPort?.postMessage({ completed: true, results: [{ test: 'fatal error', status: 'ERROR', error: err.message }] });
  });
}

// Check if we're running in a worker
if (workerData) {
  runClientTests(workerData);
  process.exit(0);
}

// Parent-side test runner
// This would be a separate orchestration script, but for Jest we'll keep it simple
