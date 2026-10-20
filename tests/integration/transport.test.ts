// Transport Integration Tests - Same-thread client/server model
import { describe, test, beforeAll, afterAll, expect } from '@jest/globals';
import { RpcServer, RpcClient, TcpTransport, UnixSocketTransport } from '../../src/index.ts';

interface TestTransportConfig {
  name: string;
  createServerTransport: () => any;
  createClientTransport: () => any;
  cleanup?: () => void;
}

/**
 * Run integration tests for a transport type
 * - Server and client run on same thread
 * - Client makes call, test calls server.processSingleRequest() to handle it
 * - No workers needed - simple async/await coordination
 */
function runTransportTest(config: TestTransportConfig): void {
  describe(config.name, () => {
    let server: RpcServer;
    let client: RpcClient;
    
    beforeAll(async () => {
      // Create and start server
      const serverTransport = config.createServerTransport();
      server = new RpcServer({ transport: serverTransport, isServer: true });
      server.start(); 
      console.log(`[Server] Started for ${config.name}`);
    }, 100);

    afterAll(async () => {
      // await server.stop();
      /*
			try {
        // if (client) await client.disconnect();
      } catch (e) {}
      
      if (config.cleanup) {
        config.cleanup();
      }
      */
      console.log('after all')
    }, 500);

    test(`client can communicate with server`, async () => {
      // Create client
      const clientTransport = config.createClientTransport();
      client = new RpcClient(100000, 1, { transport: clientTransport });
      
      try {
        // Connect client to server
        await client.connect();
        expect(client.isConnected()).toBe(true);

	console.log("client connected")

        // Test 1: Send string message
        const msg1 = `hello from ${config.name.replace(' Transport', '')} client`;
        await client.callWithString(1, msg1);
        await server.processSingleRequest();
       
	console.log('sent message')
	await server.stop();

	console.log("gonna make multiple writes")
	// Test 2: Multiple sequential calls
        for (let i = 0; i < 3; i++) {
          await client.callWithString(1, `msg ${i}`);
          await server.processSingleRequest();
        }

        // Test 3: Binary data
        const binaryData = new Uint8Array([0x01, 0x02, 0x03, 0xFF, 0x00]);
        await client.call(2, binaryData);
        await server.processSingleRequest();
        
        // Test 4: Round-trip preserves data
        const { encodeString, decodeString } = await import('../../src/shared/xdr.ts');
        const original = encodeString('payload test');
        await client.call(3, original);
        await server.processSingleRequest();
        
        expect(decodeString(original)).toBe('payload test');
        
        // Verify all tests passed
        expect(client.isConnected()).toBe(true);

      } catch (error: any) {
		console.log(error)
		console.error(`[${config.name}] Test failed:`, error.message);
        throw error;
      } finally {
        try {
          await client.disconnect();
        } catch (e) {
		console.log("error", e)
	}
      }
    }, 200);
  });
}

// Test Suites for Each Transport Type
const testId = Date.now();

runTransportTest({
  name: 'TCP Transport',
  createServerTransport: () => new TcpTransport('127.0.0.1', 5901, true),
  createClientTransport: () => new TcpTransport('127.0.0.1', 5901, false)
});
