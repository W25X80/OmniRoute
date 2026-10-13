import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import {
  ArtifactStorageError,
  type PreparedArtifact,
} from "../../src/domain/persistence/artifactStore.ts";
import {
  createS3ArtifactStore,
  type S3ArtifactTransport,
} from "../../src/lib/artifacts/s3ArtifactStore.ts";
import type { CallLogArtifact } from "../../src/lib/usage/callLogArtifacts.ts";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";

function artifact(id: string): CallLogArtifact {
  return {
    schemaVersion: 5,
    summary: {
      id,
      timestamp: "2026-10-11T12:00:00.000Z",
      method: "POST",
      path: "/v1/messages",
      status: 200,
      model: "test-model",
      requestedModel: null,
      provider: "test-provider",
      account: "test-account",
      connectionId: null,
      duration: 10,
      tokens: {
        in: 1,
        out: 2,
        cacheRead: null,
        cacheWrite: null,
        reasoning: null,
        compressed: null,
      },
      requestType: "chat",
      sourceFormat: "claude",
      targetFormat: "openai",
      apiKeyId: null,
      apiKeyName: null,
      comboName: null,
      comboStepId: null,
      comboExecutionKey: null,
    },
    requestBody: { text: "hello" },
    responseBody: { text: "answer" },
    error: null,
  };
}

test("PostgreSQL call-log artifacts use worker serialization and native object storage", async (t) => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-native-artifacts-"));
  const dataDirectory = path.join(directory, "data-must-not-exist");
  const savedEnvironment = Object.fromEntries(
    [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "CHAT_DEBUG_FILE",
      "APP_LOG_LEVEL",
      "CALL_LOG_PIPELINE_MAX_SIZE_KB",
    ].map((name) => [name, process.env[name]])
  );
  const savedContext = globalThis.__omniroutePersistenceContext;
  const savedRuntime = globalThis.__omniroutePersistenceRuntime;
  const objects = new Map<string, { bytes: Buffer; checksum: string }>();
  let putCount = 0;
  let getUnavailable = false;
  let deleteUnavailable = false;
  let tamperedGet = false;
  let rejectPut = false;
  let holdPut: Promise<void> | undefined;
  const transport: S3ArtifactTransport = {
    async send(command) {
      if (command instanceof PutObjectCommand) {
        putCount++;
        if (rejectPut) throw new Error("untrusted SDK diagnostic");
        if (holdPut) await holdPut;
        assert(command.input.Body instanceof Uint8Array);
        const bytes = Buffer.from(command.input.Body);
        objects.set(command.input.Key!, {
          bytes,
          checksum: command.input.ChecksumSHA256!,
        });
        return { $metadata: { httpStatusCode: 200 } };
      }
      if (command instanceof GetObjectCommand) {
        if (getUnavailable) throw new Error("untrusted SDK diagnostic");
        const stored = objects.get(command.input.Key!);
        if (!stored)
          throw Object.assign(new Error("not found"), {
            name: "NoSuchKey",
            $metadata: { httpStatusCode: 404 },
          });
        const bytes = tamperedGet ? Buffer.from("tampered") : stored.bytes;
        return {
          Body: bytes,
          ContentLength: bytes.length,
          ChecksumSHA256: stored.checksum,
          $metadata: { httpStatusCode: 200 },
        };
      }
      assert(command instanceof DeleteObjectCommand);
      if (deleteUnavailable) throw new Error("untrusted SDK diagnostic");
      objects.delete(command.input.Key!);
      return { $metadata: { httpStatusCode: 204 } };
    },
    destroy() {},
  };
  const store = createS3ArtifactStore(
    {
      bucket: "omniroute-artifact-test",
      region: "eu-central-1",
      prefix: "gateway/call-logs/",
      expectedBucketOwner: "123456789012",
    },
    { loadClient: async () => transport }
  );
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.DATA_DIR = dataDirectory;
  process.env.CHAT_DEBUG_FILE = "false";
  process.env.APP_LOG_LEVEL = "info";
  process.env.CALL_LOG_PIPELINE_MAX_SIZE_KB = "512";
  // This fixture qualifies the object-store/worker seam only; it does not attest SQL startup.
  const identity = Object.freeze({});
  const binding = {
    databaseIdentity: identity,
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  const context = await import("../../src/lib/db/controlPlaneContext.ts");
  const uninstall = context.installApplicationPersistence(binding);
  globalThis.__omniroutePersistenceRuntime = {
    backend: "postgres",
    phase: "ready",
    artifacts: store,
    applicationIdentity: identity,
    requests: new Set(),
    attempts: new Set(),
    attemptAdmissions: new Set(),
    attemptCancellation: new AbortController(),
    background: new Set(),
    tails: new Set(),
    owners: new AsyncLocalStorage(),
  };
  const writer = await import("../../src/lib/usage/callLogArtifactWriter.ts");
  const owner = await import("../../src/lib/usage/callLogArtifacts.ts");
  // Controlled callback receipt only: this bridge does not attest durable SQL preparation.
  const preparedArtifacts = new Map<string, PreparedArtifact>();
  const preparation = {
    async onPrepared(prepared: PreparedArtifact) {
      assert.equal(prepared.storageScope, store.storageScope);
      assert.match(prepared.storageScope, /^[0-9a-f]{64}$/);
      assert.equal(prepared.singleAttempt, true);
      assert.equal(Object.isFrozen(prepared), true);
      assert.equal(prepared.reference.endsWith(`_${prepared.checksum}.json`), true);
      assert.equal(preparedArtifacts.has(prepared.reference), false);
      preparedArtifacts.set(prepared.reference, prepared);
    },
  };
  const writePreparedArtifact = async (
    input: Parameters<typeof writer.writeCallArtifactAsync>[0]
  ) => {
    const result = await writer.writeCallArtifactAsync(input, preparation);
    assert.ok(result);
    const prepared = preparedArtifacts.get(result.relPath);
    assert.ok(prepared);
    assert.equal(prepared.sizeBytes, result.sizeBytes);
    assert.equal(prepared.checksum, result.sha256);
    return result;
  };
  try {
    await t.test("missing preparation is refused before serialization or transport", async () => {
      await assert.rejects(
        writer.writeCallArtifactAsync(artifact("missing-preparation")),
        /preparation is required/
      );
      await assert.rejects(
        owner.writeCallArtifactToNativeStore(Buffer.from("{}")),
        /preparation is required/
      );
      assert.equal(putCount, 0);
      assert.equal(preparedArtifacts.size, 0);
      assert.equal(fs.existsSync(dataDirectory), false);
    });
    await t.test(
      "serializer failure is explicit and never writes a fallback artifact",
      async () => {
        const crashFile = path.join(directory, "serializer-crash.mjs");
        fs.writeFileSync(crashFile, 'throw new Error("private-worker-detail-marker");\n');
        writer.__setCallLogWorkerOverrideForTests({
          workerFile: crashFile,
          execArgv: [],
        });
        const warnings: unknown[][] = [];
        const originalWarn = console.warn;
        console.warn = (...values: unknown[]) => warnings.push(values);
        try {
          await assert.rejects(
            writePreparedArtifact(artifact("worker-failure")),
            writer.CallLogArtifactSerializationError
          );
        } finally {
          console.warn = originalWarn;
        }
        assert.equal(putCount, 0);
        assert.equal(fs.existsSync(dataDirectory), false);
        assert(warnings.length > 0);
        assert(!JSON.stringify(warnings).includes("private-worker-detail-marker"));
        assert(!JSON.stringify(warnings).includes(crashFile));
        writer.__setCallLogWorkerOverrideForTests(
          process.env.OMNIROUTE_TEST_ARTIFACT_WORKER
            ? {
                workerFile: process.env.OMNIROUTE_TEST_ARTIFACT_WORKER,
                execArgv: [],
              }
            : null
        );
      }
    );

    await t.test(
      "small payload bytes, reference and SHA-256 are acknowledged unchanged",
      async () => {
        const input = artifact("native-round-trip");
        const result = await writePreparedArtifact(input);
        assert(result);
        assert.match(result.relPath, /^s3\/[0-9a-f-]+_[0-9a-f]{64}\.json$/);
        const bytes = objects.get(`gateway/call-logs/${result.relPath}`)!.bytes;
        assert.equal(bytes.toString("utf8"), JSON.stringify(input));
        assert.equal(result.sizeBytes, bytes.length);
        assert.equal(result.sha256, createHash("sha256").update(bytes).digest("hex"));
        assert.deepEqual(await owner.readCallArtifactAsync(result.relPath), {
          artifact: input,
          state: "ready",
        });
        assert.equal(fs.existsSync(dataDirectory), false);
      }
    );

    await t.test(
      "worker preserves truncation order, diagnostic error and per-job debug mode",
      async () => {
        const input = artifact("native-truncated");
        input.requestBody = { content: "x".repeat(800_000) };
        input.error = "upstream timed out";
        input.pipeline = {
          providerResponse: { content: "valuable provider output" },
          clientResponse: { content: "answer" },
        };
        const result = await writePreparedArtifact(input);
        assert(result);
        const stored = await owner.readCallArtifactAsync(result.relPath);
        assert.equal(stored.state, "ready");
        assert.equal(
          stored.artifact?.requestBody,
          "[omitted: call log artifact size limit exceeded]"
        );
        assert.deepEqual(stored.artifact?.responseBody, input.responseBody);
        assert.deepEqual(stored.artifact?.pipeline, input.pipeline);
        assert.equal(stored.artifact?.error, input.error);
        process.env.CHAT_DEBUG_FILE = "true";
        try {
          const debug = artifact("native-debug");
          const debugResult = await writePreparedArtifact(debug);
          assert(debugResult);
          assert.equal(
            objects.get(`gateway/call-logs/${debugResult.relPath}`)!.bytes.toString(),
            JSON.stringify(debug, null, 2)
          );
        } finally {
          process.env.CHAT_DEBUG_FILE = "false";
        }
      }
    );

    await t.test("missing, invalid JSON, integrity and availability remain distinct", async () => {
      const missing = `s3/${crypto.randomUUID()}_${"0".repeat(64)}.json`;
      assert.deepEqual(await owner.readCallArtifactAsync(missing), {
        artifact: null,
        state: "missing",
      });
      const invalid = await store.put(Buffer.from("{"));
      assert.deepEqual(await owner.readCallArtifactAsync(invalid.reference), {
        artifact: null,
        state: "corrupt",
      });
      tamperedGet = true;
      assert.deepEqual(await owner.readCallArtifactAsync(invalid.reference), {
        artifact: null,
        state: "corrupt",
      });
      tamperedGet = false;
      getUnavailable = true;
      await assert.rejects(
        owner.readCallArtifactAsync(invalid.reference),
        (error: unknown) => error instanceof ArtifactStorageError && error.kind === "unavailable"
      );
      getUnavailable = false;
      deleteUnavailable = true;
      await assert.rejects(
        owner.deleteCallArtifactAsync(invalid.reference),
        (error: unknown) => error instanceof ArtifactStorageError && error.kind === "unavailable"
      );
      deleteUnavailable = false;
      assert.equal(await owner.deleteCallArtifactAsync(invalid.reference), true);
      assert.equal(await owner.deleteCallArtifactAsync(null), false);
    });

    await t.test(
      "unknown upload outcome is forwarded to the persistence reconciliation owner",
      async () => {
        rejectPut = true;
        try {
          await assert.rejects(
            writePreparedArtifact(artifact("unknown-put")),
            (error: unknown) =>
              error instanceof ArtifactStorageError &&
              error.kind === "put_unknown" &&
              Boolean(error.reference)
          );
        } finally {
          rejectPut = false;
        }
      }
    );

    await t.test(
      "native serializer admission is bounded and cannot fall back to disk",
      async () => {
        const results = await Promise.allSettled(
          Array.from({ length: 150 }, (_, index) =>
            writePreparedArtifact(artifact(`bounded-${index}`))
          )
        );
        const rejected = results.filter((result) => result.status === "rejected");
        assert(rejected.length >= 22);
        for (const result of rejected) {
          assert(
            result.status === "rejected" &&
              (result.reason instanceof writer.CallLogArtifactSerializationError ||
                result.reason instanceof ArtifactStorageError)
          );
        }
        assert.equal(fs.existsSync(dataDirectory), false);
        assert.throws(() => owner.writeCallArtifact(artifact("sync-fallback")));
        assert.throws(() => owner.readCallArtifact("a.json"));
        assert.throws(() => owner.deleteCallArtifact("a.json"));
      }
    );

    await t.test(
      "shutdown rejects an incomplete drain, then closes admission after completion",
      async () => {
        let entered!: () => void;
        const started = new Promise<void>((resolve) => {
          entered = resolve;
        });
        let release!: () => void;
        holdPut = new Promise<void>((resolve) => {
          release = resolve;
        });
        const originalSend = transport.send;
        transport.send = async (command, options) => {
          if (command instanceof PutObjectCommand) entered();
          return originalSend(command, options);
        };
        const pending = writePreparedArtifact(artifact("drain"));
        await Promise.race([
          started,
          pending.then(() => {
            throw new Error("Upload did not reach the drain fixture");
          }),
        ]);
        const keepAlive = setTimeout(release, 1000);
        await assert.rejects(writer.closeCallLogArtifactWriter(10), /still draining/);
        clearTimeout(keepAlive);
        release();
        assert(await pending);
        holdPut = undefined;
        await writer.closeCallLogArtifactWriter();
        await assert.rejects(
          writePreparedArtifact(artifact("after-close")),
          writer.CallLogArtifactSerializationError
        );
        assert.equal(fs.existsSync(dataDirectory), false);
      }
    );
  } finally {
    writer.__setCallLogWorkerOverrideForTests(null);
    await writer.closeCallLogArtifactWriter(1000).catch(() => {});
    await store.close();
    uninstall();
    globalThis.__omniroutePersistenceContext = savedContext;
    globalThis.__omniroutePersistenceRuntime = savedRuntime;
    for (const [name, value] of Object.entries(savedEnvironment)) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
