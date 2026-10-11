import { createHash, randomUUID } from "node:crypto";
import { Agent } from "node:https";
import type {
  DeleteObjectCommand,
  DeleteObjectCommandOutput,
  GetObjectCommand,
  GetObjectCommandOutput,
  PutObjectCommand,
  PutObjectCommandOutput,
} from "@aws-sdk/client-s3";
import {
  ArtifactStorageError,
  type NativeArtifactStore,
  type StoredArtifact,
} from "@/domain/persistence/artifactStore";
import {
  parseS3ArtifactConfiguration,
  type S3ArtifactConfiguration,
} from "./s3ArtifactConfiguration";

export const MAX_ARTIFACT_BYTES = 8 * 1024 * 1024;
export const ARTIFACT_OPERATION_LIMIT = 8;
export const ARTIFACT_DEADLINE_MS = 5000;

const REFERENCE =
  /^s3\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_([0-9a-f]{64})\.json$/;

type ArtifactCommand = PutObjectCommand | GetObjectCommand | DeleteObjectCommand;
type ArtifactOutput = PutObjectCommandOutput | GetObjectCommandOutput | DeleteObjectCommandOutput;

/** Test seam is an SDK client transport, never a production-configurable endpoint. */
export interface S3ArtifactTransport {
  send(command: ArtifactCommand, options: { abortSignal: AbortSignal }): Promise<ArtifactOutput>;
  destroy(): void;
}

export interface S3ArtifactDependencies {
  loadClient?: (configuration: S3ArtifactConfiguration) => Promise<S3ArtifactTransport>;
  /** Tests may shorten, but never extend, the production deadline. */
  deadlineMs?: number;
}

async function loadDefaultClient(
  configuration: S3ArtifactConfiguration
): Promise<S3ArtifactTransport> {
  const { S3Client } = await import("@aws-sdk/client-s3");
  const agent = new Agent({
    keepAlive: true,
    maxSockets: 8,
    maxTotalSockets: 8,
    maxFreeSockets: 8,
  });
  const client = new S3Client({
    region: configuration.region,
    maxAttempts: 3,
    retryMode: "standard",
    ignoreConfiguredEndpointUrls: true,
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
    requestHandler: {
      httpsAgent: agent,
      connectionTimeout: 1000,
      requestTimeout: 4000,
      socketTimeout: 4000,
      throwOnRequestTimeout: true,
    },
  });
  return {
    send(command, options) {
      return client.send(command, options) as Promise<ArtifactOutput>;
    },
    destroy() {
      client.destroy();
      agent.destroy();
    },
  };
}

function isMissingObject(error: unknown): boolean {
  const candidate = error as { name?: unknown; $metadata?: { httpStatusCode?: unknown } } | null;
  return candidate?.name === "NoSuchKey" && candidate.$metadata?.httpStatusCode === 404;
}

function assertAcknowledged(output: ArtifactOutput): void {
  const status = output.$metadata.httpStatusCode;
  if (!Number.isInteger(status) || status! < 200 || status! >= 300) {
    throw new ArtifactStorageError("unavailable");
  }
}

function assertActive(signal: AbortSignal): void {
  if (signal.aborted) throw new ArtifactStorageError("deadline");
}

function parseReference(reference: string): string {
  const match = typeof reference === "string" ? REFERENCE.exec(reference) : null;
  if (!match) throw new ArtifactStorageError("invalid_reference");
  return match[1];
}

function digest(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

type NodeBody = AsyncIterable<unknown> & { destroy(): unknown };
type WebBody = { getReader(): ReadableStreamDefaultReader<Uint8Array>; cancel(): Promise<unknown> };

function nodeBody(body: unknown): body is NodeBody {
  return Boolean(
    body &&
    typeof body === "object" &&
    typeof (body as NodeBody)[Symbol.asyncIterator] === "function" &&
    typeof (body as NodeBody).destroy === "function"
  );
}

function webBody(body: unknown): body is WebBody {
  return Boolean(
    body &&
    typeof body === "object" &&
    typeof (body as WebBody).getReader === "function" &&
    typeof (body as WebBody).cancel === "function"
  );
}

async function discardBody(body: unknown): Promise<void> {
  if (nodeBody(body)) {
    body.destroy();
    return;
  }
  if (webBody(body)) await body.cancel();
}

async function collectBytes(
  body: unknown,
  expectedLength: number,
  signal: AbortSignal
): Promise<Uint8Array> {
  const chunks: Uint8Array[] = [];
  let length = 0;
  const append = (chunk: unknown) => {
    assertActive(signal);
    if (!(chunk instanceof Uint8Array)) throw new ArtifactStorageError("integrity");
    length += chunk.byteLength;
    if (length > MAX_ARTIFACT_BYTES || length > expectedLength)
      throw new ArtifactStorageError("integrity");
    chunks.push(Buffer.from(chunk));
  };
  if (body instanceof Uint8Array) append(body);
  else if (nodeBody(body)) {
    const abort = () => {
      body.destroy();
    };
    signal.addEventListener("abort", abort, { once: true });
    try {
      assertActive(signal);
      for await (const chunk of body) append(chunk);
    } finally {
      signal.removeEventListener("abort", abort);
    }
  } else if (webBody(body)) {
    const reader = body.getReader();
    const abort = () => {
      void reader.cancel().catch(() => {});
    };
    signal.addEventListener("abort", abort, { once: true });
    try {
      assertActive(signal);
      while (true) {
        const next = await reader.read();
        assertActive(signal);
        if (next.done) break;
        append(next.value);
      }
    } finally {
      signal.removeEventListener("abort", abort);
      reader.releaseLock();
    }
  } else throw new ArtifactStorageError("integrity");
  assertActive(signal);
  if (length !== expectedLength) throw new ArtifactStorageError("integrity");
  return Buffer.concat(chunks, length);
}

export function createS3ArtifactStore(
  input: S3ArtifactConfiguration,
  dependencies: S3ArtifactDependencies = {}
): NativeArtifactStore {
  const configuration = parseS3ArtifactConfiguration(input);
  const deadline = dependencies.deadlineMs ?? ARTIFACT_DEADLINE_MS;
  if (!Number.isInteger(deadline) || deadline < 1 || deadline > ARTIFACT_DEADLINE_MS) {
    throw new ArtifactStorageError("configuration");
  }
  let clientPromise: Promise<S3ArtifactTransport> | undefined;
  const active = new Set<Promise<unknown>>();
  let closing = false;
  let closePromise: Promise<void> | undefined;

  const client = () =>
    (clientPromise ??= (dependencies.loadClient ?? loadDefaultClient)(configuration));

  function operation<T>(
    body: (signal: AbortSignal) => Promise<T>,
    deadlineError = new ArtifactStorageError("deadline")
  ): Promise<T> {
    if (closing) return Promise.reject(new ArtifactStorageError("closed"));
    if (active.size >= ARTIFACT_OPERATION_LIMIT)
      return Promise.reject(new ArtifactStorageError("overloaded"));
    const controller = new AbortController();
    let rejectDeadline!: (error: ArtifactStorageError) => void;
    const expired = new Promise<never>((_, reject) => {
      rejectDeadline = reject;
    });
    const timer = setTimeout(() => {
      controller.abort();
      rejectDeadline(deadlineError);
    }, deadline);
    const work = body(controller.signal);
    active.add(work);
    void work
      .catch(() => {})
      .finally(() => {
        clearTimeout(timer);
        active.delete(work);
      });
    return Promise.race([work, expired]);
  }

  return {
    put(bytes: Uint8Array): Promise<StoredArtifact> {
      if (!(bytes instanceof Uint8Array))
        return Promise.reject(new ArtifactStorageError("integrity"));
      if (bytes.byteLength > MAX_ARTIFACT_BYTES)
        return Promise.reject(new ArtifactStorageError("too_large"));
      const referenceId = randomUUID();
      let reference: string | undefined;
      const deadlineError = new ArtifactStorageError("put_unknown");
      return operation(async (signal) => {
        const immutableBytes = Buffer.from(bytes);
        const checksum = digest(immutableBytes);
        reference = `s3/${referenceId}_${checksum}.json`;
        try {
          const sdk = await import("@aws-sdk/client-s3");
          const transport = await client();
          assertActive(signal);
          const result = await transport.send(
            new sdk.PutObjectCommand({
              Bucket: configuration.bucket,
              Key: configuration.prefix + reference,
              ExpectedBucketOwner: configuration.expectedBucketOwner,
              Body: immutableBytes,
              ContentLength: immutableBytes.byteLength,
              ContentType: "application/json",
              ChecksumSHA256: Buffer.from(checksum, "hex").toString("base64"),
              IfNoneMatch: "*",
              ServerSideEncryption: configuration.kmsKeyId ? "aws:kms" : "AES256",
              ...(configuration.kmsKeyId
                ? { SSEKMSKeyId: configuration.kmsKeyId, BucketKeyEnabled: true }
                : {}),
            }),
            { abortSignal: signal }
          );
          assertActive(signal);
          assertAcknowledged(result);
          return { reference, sizeBytes: immutableBytes.byteLength, checksum };
        } catch {
          // Even a retry's conditional rejection may follow an earlier acknowledged-lost write.
          throw new ArtifactStorageError("put_unknown", reference);
        }
      }, deadlineError).catch((error) => {
        if (error === deadlineError) throw new ArtifactStorageError("put_unknown", reference);
        throw error;
      });
    },
    get(reference: string): Promise<Uint8Array | null> {
      let checksum: string;
      try {
        checksum = parseReference(reference);
      } catch (error) {
        return Promise.reject(error);
      }
      return operation(async (signal) => {
        let body: unknown;
        let complete = false;
        try {
          const sdk = await import("@aws-sdk/client-s3");
          const transport = await client();
          assertActive(signal);
          const response = (await transport.send(
            new sdk.GetObjectCommand({
              Bucket: configuration.bucket,
              Key: configuration.prefix + reference,
              ExpectedBucketOwner: configuration.expectedBucketOwner,
              ChecksumMode: "ENABLED",
            }),
            { abortSignal: signal }
          )) as GetObjectCommandOutput;
          body = response.Body;
          assertActive(signal);
          assertAcknowledged(response);
          if (
            !Number.isSafeInteger(response.ContentLength) ||
            response.ContentLength! < 0 ||
            response.ContentLength! > MAX_ARTIFACT_BYTES
          )
            throw new ArtifactStorageError("integrity");
          const bytes = await collectBytes(body, response.ContentLength!, signal);
          if (
            digest(bytes) !== checksum ||
            (response.ChecksumSHA256 !== undefined &&
              response.ChecksumSHA256 !== Buffer.from(checksum, "hex").toString("base64"))
          ) {
            throw new ArtifactStorageError("integrity");
          }
          complete = true;
          return bytes;
        } catch (error) {
          if (error instanceof ArtifactStorageError) throw error;
          if (signal.aborted) throw new ArtifactStorageError("deadline");
          if (isMissingObject(error)) return null;
          throw new ArtifactStorageError("unavailable");
        } finally {
          if (!complete && body) {
            try {
              await discardBody(body);
            } catch {
              /* Original storage error remains authoritative. */
            }
          }
        }
      });
    },
    delete(reference: string): Promise<boolean> {
      try {
        parseReference(reference);
      } catch (error) {
        return Promise.reject(error);
      }
      return operation(async (signal) => {
        try {
          const sdk = await import("@aws-sdk/client-s3");
          const transport = await client();
          assertActive(signal);
          const result = await transport.send(
            new sdk.DeleteObjectCommand({
              Bucket: configuration.bucket,
              Key: configuration.prefix + reference,
              ExpectedBucketOwner: configuration.expectedBucketOwner,
            }),
            { abortSignal: signal }
          );
          assertActive(signal);
          assertAcknowledged(result);
          return true;
        } catch (error) {
          if (error instanceof ArtifactStorageError) throw error;
          if (signal.aborted) throw new ArtifactStorageError("deadline");
          if (isMissingObject(error)) return false;
          throw new ArtifactStorageError("unavailable");
        }
      });
    },
    close(): Promise<void> {
      if (closePromise) return closePromise;
      closing = true;
      closePromise = (async () => {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const drained = Promise.allSettled([...active]);
        try {
          await Promise.race([
            drained,
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () => reject(new ArtifactStorageError("deadline")),
                deadline + 100
              );
            }),
          ]);
          if (clientPromise) (await clientPromise).destroy();
        } catch (error) {
          // Successful close is forbidden while an opaque transport still ignores cancellation.
          if (clientPromise)
            void clientPromise.then((transport) => transport.destroy()).catch(() => {});
          throw error instanceof ArtifactStorageError
            ? error
            : new ArtifactStorageError("unavailable");
        } finally {
          if (timer) clearTimeout(timer);
        }
      })();
      return closePromise;
    },
  };
}
