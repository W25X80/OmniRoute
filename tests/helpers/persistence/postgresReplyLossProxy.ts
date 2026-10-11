import { createConnection, createServer, type Socket } from "node:net";
import { createSecureContext, TLSSocket } from "node:tls";

export interface PostgresReplyLossProxyOptions {
  upstreamPort: number;
  certificate: string;
  key: string;
  dropCommit?: number;
}

/**
 * Test-only TLS termination for PostgreSQL's SSLRequest protocol. The upstream is the
 * isolated runner's plaintext loopback endpoint. Drop a COMMIT CommandComplete only
 * after receiving it from that endpoint, leaving durable state for a direct oracle.
 */
export async function createPostgresReplyLossProxy(options: PostgresReplyLossProxyOptions) {
  const dropCommit = options.dropCommit ?? 2;
  if (
    !Number.isInteger(options.upstreamPort) ||
    options.upstreamPort < 1 ||
    options.upstreamPort > 65_535 ||
    !Number.isInteger(dropCommit) ||
    dropCommit < 1
  ) {
    throw new Error("Invalid PostgreSQL reply-loss proxy options");
  }
  const secureContext = createSecureContext({ cert: options.certificate, key: options.key });
  const sockets = new Set<Socket>();
  const commitTag = Buffer.from("COMMIT\0");
  let commits = 0;
  let dropped = false;

  const server = createServer((client) => {
    let secureClient: TLSSocket | undefined;
    let upstream: Socket | undefined;
    let disconnected = false;
    let request = Buffer.alloc(0);
    let frames = Buffer.alloc(0);
    let backpressured = false;

    function disconnect() {
      if (disconnected) return;
      disconnected = true;
      client.destroy();
      secureClient?.destroy();
      upstream?.destroy();
    }

    function own(socket: Socket) {
      sockets.add(socket);
      socket.on("error", disconnect);
      socket.once("close", () => {
        sockets.delete(socket);
        disconnect();
      });
    }

    function forwardFrames() {
      if (!secureClient || !upstream || disconnected || backpressured) return;
      while (frames.length >= 5) {
        const payloadLength = frames.readInt32BE(1);
        if (payloadLength < 4) {
          disconnect();
          return;
        }
        const frameLength = payloadLength + 1;
        if (frames.length < frameLength) return;
        const frame = frames.subarray(0, frameLength);
        frames = frames.subarray(frameLength);
        if (frame[0] === 67 && frame.subarray(5).equals(commitTag)) {
          commits++;
          if (!dropped && commits === dropCommit) {
            dropped = true;
            disconnect();
            return;
          }
        }
        if (!secureClient.write(frame)) {
          backpressured = true;
          upstream.pause();
          secureClient.once("drain", () => {
            backpressured = false;
            forwardFrames();
            if (!backpressured && !disconnected) upstream?.resume();
          });
          return;
        }
      }
    }

    function receiveSSLRequest(chunk: Buffer) {
      request = Buffer.concat([request, chunk]);
      if (request.length < 8) return;
      // SSLRequest is exactly eight bytes: length and the PostgreSQL request code.
      if (request.readInt32BE(0) !== 8 || request.readInt32BE(4) !== 80877103) {
        disconnect();
        return;
      }
      client.pause();
      client.removeListener("data", receiveSSLRequest);
      if (request.length > 8) client.unshift(request.subarray(8));
      request = Buffer.alloc(0);
      // Queue the unencrypted acknowledgement before wrapping this same owned socket.
      client.write("S");
      secureClient = new TLSSocket(client, { isServer: true, secureContext });
      own(secureClient);
      upstream = createConnection({ host: "127.0.0.1", port: options.upstreamPort });
      own(upstream);
      // TLS decryption yields the StartupMessage; the direct upstream sees no SSLRequest.
      secureClient.pipe(upstream);
      upstream.on("data", (data: Buffer) => {
        frames = Buffer.concat([frames, data]);
        forwardFrames();
      });
      secureClient.resume();
    }

    own(client);
    client.on("data", receiveSSLRequest);
  });

  await new Promise<void>((resolve, reject) => {
    const failed = (error: Error) => reject(error);
    server.once("error", failed);
    server.listen(0, "127.0.0.1", () => {
      server.removeListener("error", failed);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("PostgreSQL proxy has no port");
  let closing: Promise<void> | undefined;
  return {
    port: address.port,
    dropped: () => dropped,
    close(): Promise<void> {
      closing ??= new Promise<void>((resolve, reject) => {
        for (const socket of sockets) socket.destroy();
        server.close((error) => (error ? reject(error) : resolve()));
      });
      return closing;
    },
  };
}
