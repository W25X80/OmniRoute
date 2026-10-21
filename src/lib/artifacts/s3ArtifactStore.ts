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
  type ArtifactPutOptions,
  type PreparedArtifact,
  type ArtifactStorageDiagnostic,
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
  /** Content-free shared HTTP pool occupancy, never its host/path keys. */
  snapshot?(): Readonly<{ activeSockets: number; queuedRequests: number }>;
}

export interface S3ArtifactDependencies {
  loadClient?: (configuration: S3ArtifactConfiguration) => Promise<S3ArtifactTransport>;
  /** Tests may shorten, but never extend, the production deadline. */
  deadlineMs?: number;
}

async function loadDefaultClient(
  configuration: S3ArtifactConfiguration
): Promise<S3ArtifactTransport> {
  const { S3Client, PutObjectCommand } = await import("@aws-sdk/client-s3");
  const agent = new Agent({
    keepAlive: true,
    maxSockets: 8,
    maxTotalSockets: 8,
    maxFreeSockets: 8,
  });
  const options = {
    region: configuration.region,
    followRegionRedirects: false,
    retryMode: "standard" as const,
    ignoreConfiguredEndpointUrls: true,
    requestChecksumCalculation: "WHEN_REQUIRED" as const,
    responseChecksumValidation: "WHEN_REQUIRED" as const,
    requestHandler: {
      httpsAgent: agent,
      connectionTimeout: 1000,
      requestTimeout: 4000,
      socketTimeout: 4000,
      throwOnRequestTimeout: true,
    },
  };
  // One writer attempt is required for positive-GET reconciliation to be terminal.
  // Reads/deletes retain their existing retry policy and share the bounded socket pool.
  const writer = new S3Client({ ...options, maxAttempts: 1 });
  const reader = new S3Client({ ...options, maxAttempts: 3 });
  return {
    send(command, requestOptions) {
      const client = command instanceof PutObjectCommand ? writer : reader;
      return client.send(command, requestOptions) as Promise<ArtifactOutput>;
    },
    snapshot() {
      const count = (groups: Readonly<Record<string, readonly unknown[] | undefined>>) =>
        Object.values(groups).reduce((total, items) => total + (items?.length ?? 0), 0);
      return {
        activeSockets: count(agent.sockets),
        queuedRequests: count(agent.requests),
      };
    },
    destroy() {
      writer.destroy();
      reader.destroy();
      agent.destroy();
    },
  };
}

export function artifactStorageDiagnostic(error: unknown) {
  const candidate = error as {
    name?: unknown;
    code?: unknown;
    message?: unknown;
    $metadata?: { httpStatusCode?: unknown; requestId?: unknown };
  } | null;
  const safe = (value: unknown, maximum: number) =>
    typeof value === "string" && value.length <= maximum && /^[A-Za-z0-9_+\/=.-]+$/.test(value)
      ? value
      : undefined;
  const status = candidate?.$metadata?.httpStatusCode;
  // Smithy uses the same error name for distinct timers. Retain only a fixed enum,
  // never the source message (which may contain payloads, URLs or credentials).
  const message = candidate?.message;
  const timeoutSource: ArtifactStorageDiagnostic["timeoutSource"] =
    candidate?.name !== "TimeoutError"
      ? undefined
      : typeof message === "string" &&
          message.startsWith(
            "@smithy/node-http-handler - the request socket did not establish a connection"
          )
        ? "connect"
        : typeof message === "string" &&
            message.startsWith("@smithy/node-http-handler - [ERROR] a request has exceeded")
          ? "request"
          : typeof message === "string" &&
              message.startsWith("@smithy/node-http-handler - the request socket timed out after")
            ? "socket"
            : "transport";
  return Object.freeze({
    name: safe(candidate?.name, 80),
    status:
      Number.isInteger(status) && Number(status) >= 100 && Number(status) <= 599
        ? Number(status)
        : undefined,
    requestId: safe(candidate?.$metadata?.requestId, 200),
    ...(safe(candidate?.code, 80) ? { code: safe(candidate?.code, 80) } : {}),
    ...(timeoutSource ? { timeoutSource } : {}),
  });
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
  const storageScope = createHash("sha256")
    .update(
      JSON.stringify({
        bucket: configuration.bucket,
        region: configuration.region,
        prefix: configuration.prefix,
        expectedBucketOwner: configuration.expectedBucketOwner,
      })
    )
    .digest("hex");
  const pendingPuts = new Map<string, number>();
  const deadline = dependencies.deadlineMs ?? ARTIFACT_DEADLINE_MS;
  if (!Number.isInteger(deadline) || deadline < 1 || deadline > ARTIFACT_DEADLINE_MS) {
    throw new ArtifactStorageError("configuration");
  }
  let clientPromise: Promise<S3ArtifactTransport> | undefined;
  const active = new Set<Promise<unknown>>();
  let closing = false;
  let closePromise: Promise<void> | undefined;

  const client = () => {
    if (!clientPromise) {
      const preparation = (dependencies.loadClient ?? loadDefaultClient)(configuration);
      clientPromise = preparation;
      void preparation.catch(() => {
        if (clientPromise === preparation) clientPromise = undefined;
      });
    }
    return clientPromise;
  };

  function prepare(bytes: Uint8Array): PreparedArtifact {
    if (!(bytes instanceof Uint8Array)) throw new ArtifactStorageError("integrity");
    if (bytes.byteLength > MAX_ARTIFACT_BYTES) throw new ArtifactStorageError("too_large");
    const checksum = digest(bytes);
    return Object.freeze({
      reference: `s3/${randomUUID()}_${checksum}.json`,
      sizeBytes: bytes.byteLength,
      checksum,
      storageScope,
      singleAttempt: true as const,
    });
  }

  type OperationContext = {
    signal: AbortSignal;
    check(): void;
    phase(value: NonNullable<ArtifactStorageDiagnostic["phase"]>): void;
    beginTransport(): void;
    transport(value: S3ArtifactTransport): void;
    diagnostic(error?: unknown): Readonly<ArtifactStorageDiagnostic>;
  };

  function operation<T>(
    name: NonNullable<ArtifactStorageDiagnostic["operation"]>,
    body: (context: OperationContext) => Promise<T>,
    options: {
      sizeBytes?: number;
      deadlineError?(diagnostic: Readonly<ArtifactStorageDiagnostic>): ArtifactStorageError;
    } = {}
  ): Promise<T> {
    if (closing) return Promise.reject(new ArtifactStorageError("closed"));
    if (active.size >= ARTIFACT_OPERATION_LIMIT)
      return Promise.reject(new ArtifactStorageError("overloaded"));
    const controller = new AbortController();
    const startedAt = performance.now();
    let phaseAt = startedAt;
    let phase: NonNullable<ArtifactStorageDiagnostic["phase"]> = "prepare";
    let expiresAt = startedAt + deadline;
    let transportAt: number | undefined;
    let transport: S3ArtifactTransport | undefined;
    let timer: ReturnType<typeof setTimeout>;
    let deadlineFailure: ArtifactStorageError | undefined;
    let rejectDeadline!: (error: ArtifactStorageError) => void;
    const expired = new Promise<never>((_, reject) => {
      rejectDeadline = reject;
    });
    const diagnostic = (error?: unknown): Readonly<ArtifactStorageDiagnostic> => {
      const now = performance.now();
      const milliseconds = (value: number) => Math.max(0, Math.round(value));
      let pool: ReturnType<NonNullable<S3ArtifactTransport["snapshot"]>> | undefined;
      try {
        pool = transport?.snapshot?.();
      } catch {
        // Diagnostics must not change a storage outcome.
      }
      const count = (value: unknown) =>
        Number.isSafeInteger(value) && Number(value) >= 0 ? Number(value) : undefined;
      return Object.freeze({
        ...(error instanceof ArtifactStorageError
          ? error.diagnostic
          : artifactStorageDiagnostic(error)),
        operation: name,
        phase,
        elapsedMs: milliseconds(now - startedAt),
        phaseElapsedMs: milliseconds(now - phaseAt),
        preparationMs: milliseconds((transportAt ?? now) - startedAt),
        transportMs: transportAt === undefined ? 0 : milliseconds(now - transportAt),
        deadlineOverrunMs: milliseconds(now - expiresAt),
        ...(options.sizeBytes === undefined ? {} : { sizeBytes: options.sizeBytes }),
        activeOperations: Math.max(1, active.size),
        ...(count(pool?.activeSockets) === undefined
          ? {}
          : { activeSockets: count(pool?.activeSockets) }),
        ...(count(pool?.queuedRequests) === undefined
          ? {}
          : { queuedRequests: count(pool?.queuedRequests) }),
      });
    };
    const expire = () => {
      if (!deadlineFailure) {
        const detail = Object.freeze({ ...diagnostic(), timeoutSource: "operation" as const });
        deadlineFailure =
          options.deadlineError?.(detail) ??
          new ArtifactStorageError("deadline", undefined, undefined, detail);
        controller.abort();
        rejectDeadline(deadlineFailure);
      }
      return deadlineFailure;
    };
    const check = () => {
      // The timer task can be delayed by CPU work; elapsed time remains authoritative.
      if (controller.signal.aborted || performance.now() >= expiresAt) throw expire();
    };
    const arm = () => {
      timer = setTimeout(expire, Math.max(1, Math.ceil(expiresAt - performance.now())));
    };
    const context: OperationContext = {
      signal: controller.signal,
      check,
      phase(value) {
        check();
        phase = value;
        phaseAt = performance.now();
        if (value === "transport") transportAt ??= phaseAt;
      },
      beginTransport() {
        check();
        if (transportAt !== undefined) throw new ArtifactStorageError("configuration");
        transportAt = performance.now();
        phase = "transport";
        phaseAt = transportAt;
        // Durable ownership/client preparation cannot borrow the transport's budget.
        expiresAt = transportAt + deadline;
        clearTimeout(timer);
        arm();
      },
      transport(value) {
        transport = value;
      },
      diagnostic,
    };
    // Callbacks can re-enter admission or close before the async body first yields.
    // Reserve ownership first, without deferring the caller-byte snapshot in body.
    let release!: () => void;
    const lease = new Promise<void>((resolve) => {
      release = resolve;
    });
    active.add(lease);
    arm();
    let work: Promise<T>;
    try {
      work = body(context);
    } catch (error) {
      work = Promise.reject(error);
    }
    void work
      .catch(() => {})
      .finally(() => {
        clearTimeout(timer);
        active.delete(lease);
        release();
      });
    return Promise.race([work, expired]);
  }

  return {
    storageScope,
    prepare,
    warmup: () =>
      operation("warmup", async (context) => {
        try {
          context.phase("client");
          context.transport(await client());
          context.check();
        } catch (error) {
          if (error instanceof ArtifactStorageError) throw error;
          throw new ArtifactStorageError(
            "unavailable",
            undefined,
            false,
            context.diagnostic(error)
          );
        }
      }),
    isPutActive(reference: string): boolean {
      return (pendingPuts.get(reference) ?? 0) > 0;
    },
    put(bytes: Uint8Array, options: ArtifactPutOptions = {}): Promise<StoredArtifact> {
      if (!(bytes instanceof Uint8Array))
        return Promise.reject(new ArtifactStorageError("integrity"));
      if (bytes.byteLength > MAX_ARTIFACT_BYTES)
        return Promise.reject(new ArtifactStorageError("too_large"));
      const allocated = options.prepared ?? prepare(bytes);
      if (options.prepared && !options.onPrepared)
        return Promise.reject(new ArtifactStorageError("configuration"));
      let reference: string | undefined;
      let putStarted = false;
      return operation(
        "put",
        async (context) => {
          const { signal } = context;
          const immutableBytes = Buffer.from(bytes);
          const checksum = digest(immutableBytes);
          reference = allocated.reference;
          if (
            parseReference(reference) !== checksum ||
            allocated.checksum !== checksum ||
            allocated.sizeBytes !== immutableBytes.byteLength ||
            allocated.storageScope !== storageScope ||
            allocated.singleAttempt !== true
          )
            throw new ArtifactStorageError("integrity");
          pendingPuts.set(reference, (pendingPuts.get(reference) ?? 0) + 1);
          try {
            context.phase("claim");
            // Caller SQL/checkpoint errors remain authoritative, not PUT errors.
            await options.onPrepared?.(
              Object.freeze({
                reference,
                sizeBytes: immutableBytes.byteLength,
                checksum,
                storageScope,
                singleAttempt: true as const,
              })
            );
            context.check();
            try {
              context.phase("client");
              const sdk = await import("@aws-sdk/client-s3");
              const transport = await client();
              context.transport(transport);
              context.beginTransport();
              putStarted = true;
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
              context.check();
              assertAcknowledged(result);
              return { reference, sizeBytes: immutableBytes.byteLength, checksum };
            } catch (error) {
              if (
                error instanceof ArtifactStorageError &&
                error.diagnostic?.timeoutSource === "operation"
              )
                throw error;
              throw new ArtifactStorageError(
                putStarted ? "put_unknown" : "unavailable",
                reference,
                putStarted,
                context.diagnostic(error)
              );
            }
          } finally {
            // A public deadline must not pretend the opaque transport has settled.
            const remaining = (pendingPuts.get(reference) ?? 1) - 1;
            if (remaining === 0) pendingPuts.delete(reference);
            else pendingPuts.set(reference, remaining);
          }
        },
        {
          sizeBytes: bytes.byteLength,
          deadlineError: (diagnostic) =>
            new ArtifactStorageError(
              putStarted ? "put_unknown" : "deadline",
              reference,
              putStarted,
              diagnostic
            ),
        }
      );
    },
    get(reference: string): Promise<Uint8Array | null> {
      let checksum: string;
      try {
        checksum = parseReference(reference);
      } catch (error) {
        return Promise.reject(error);
      }
      return operation("get", async (context) => {
        const { signal } = context;
        let body: unknown;
        let complete = false;
        try {
          context.phase("client");
          const sdk = await import("@aws-sdk/client-s3");
          const transport = await client();
          context.transport(transport);
          context.phase("transport");
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
          context.check();
          assertAcknowledged(response);
          if (
            !Number.isSafeInteger(response.ContentLength) ||
            response.ContentLength! < 0 ||
            response.ContentLength! > MAX_ARTIFACT_BYTES
          )
            throw new ArtifactStorageError("integrity");
          context.phase("body");
          const bytes = await collectBytes(body, response.ContentLength!, signal);
          context.check();
          if (
            digest(bytes) !== checksum ||
            (response.ChecksumSHA256 !== undefined &&
              response.ChecksumSHA256 !== Buffer.from(checksum, "hex").toString("base64"))
          ) {
            throw new ArtifactStorageError("integrity");
          }
          context.check();
          complete = true;
          return bytes;
        } catch (error) {
          if (error instanceof ArtifactStorageError) throw error;
          if (signal.aborted) throw new ArtifactStorageError("deadline");
          if (isMissingObject(error)) return null;
          throw new ArtifactStorageError(
            "unavailable",
            undefined,
            undefined,
            context.diagnostic(error)
          );
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
      return operation("delete", async (context) => {
        const { signal } = context;
        try {
          context.phase("client");
          const sdk = await import("@aws-sdk/client-s3");
          const transport = await client();
          context.transport(transport);
          context.phase("transport");
          const result = await transport.send(
            new sdk.DeleteObjectCommand({
              Bucket: configuration.bucket,
              Key: configuration.prefix + reference,
              ExpectedBucketOwner: configuration.expectedBucketOwner,
            }),
            { abortSignal: signal }
          );
          context.check();
          assertAcknowledged(result);
          return true;
        } catch (error) {
          if (error instanceof ArtifactStorageError) throw error;
          if (signal.aborted) throw new ArtifactStorageError("deadline");
          if (isMissingObject(error)) return false;
          throw new ArtifactStorageError(
            "unavailable",
            undefined,
            undefined,
            context.diagnostic(error)
          );
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
                2 * deadline + 100
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
