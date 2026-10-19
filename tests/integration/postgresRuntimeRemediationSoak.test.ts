import {
  insertConversationFixture,
  deleteConversationFixture,
} from "../helpers/persistence/conversationHistoryFixture.ts";
import assert from "node:assert/strict";
import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { createHash, randomUUID, randomFillSync } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { setImmediate as turn, setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import {
  ARTIFACT_OPERATION_LIMIT,
  createS3ArtifactStore as nativeStore,
} from "../../src/lib/artifacts/s3ArtifactStore.ts";
import {
  isArtifactStorageErrorKind,
  type NativeArtifactStore,
} from "../../src/domain/persistence/artifactStore.ts";
import type { PostgresControlPlaneOptions } from "../../src/lib/db/postgres/controlPlane.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { acquirePostgresLock } from "../../src/lib/db/postgres/locks.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  assertDisposableDiskBudget,
  createDisposableDiskBudget,
  readDisposablePostgresDisk,
} from "../helpers/persistence/disposablePostgresStorage.ts";

function bounded(name: string, fallback: number, minimum: number, maximum: number) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum)
    throw new Error(`Invalid bounded soak setting: ${name}`);
  return value;
}
const enabled = process.env.RUN_PG_RUNTIME_SOAK === "1";
const durationMs = enabled ? bounded("PG_RUNTIME_SOAK_SECONDS", 60, 60, 3600) * 1000 : 60000;
const historyRows = enabled ? bounded("PG_RUNTIME_SOAK_HISTORY_ROWS", 50000, 1000, 4800000) : 1000;
const FILE_BYTES = 64 * 1024 * 1024;
// Ten initial file offers, then one file lane in every six timed offers. The
// fastest scheduled interval is 500ms; the assertion below also bounds runaway timers.
const maximumFileOffers = 10 + Math.ceil((Math.ceil(durationMs / 500) + 1) / 6);
const maximumLogicalFileBytes =
  maximumFileOffers * FILE_BYTES +
  (process.env.RUN_PG_IMPORT_MAX_BLOB === "1" ? 512 * 1024 * 1024 : 0) +
  FILE_BYTES; // Conservative preparation margin; preparation itself persists no file.
const lanes = ["history", "reads", "files", "receipt", "publication", "cancelled"] as const;
type Lane = (typeof lanes)[number];
function barrier() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}
const configuration = {
  bucket: "runtime-soak-fixture",
  region: "eu-central-1",
  prefix: "soak/",
  expectedBucketOwner: "123456789012",
};
const objects = new Map<string, Buffer>();
const putCounts = new Map<string, number>();
const effects = new Map<
  string,
  {
    mode: "ack" | "lost" | "late" | "private";
    gate?: ReturnType<typeof barrier>;
    pending?: Promise<void>;
    revoke?: () => Promise<void>;
  }
>();
const diagnostics: unknown[] = [];
const slowCommits: Record<string, unknown>[] = [];
const cleanupDiagnostics = new AsyncLocalStorage<Record<string, unknown>[]>();
const accountingLockStatement = createHash("sha256")
  .update("SELECT pg_advisory_xact_lock(18765,$1)")
  .digest("hex");
const artifactErrors: { operation: string; name?: string; kind?: string; sqlState?: string }[] = [];
let capacityDiagnostics = 0;
let expectedStorageWarnings = 0;
const logger = {
  warn(fields: Record<string, unknown>, message?: string) {
    const cleanup = cleanupDiagnostics.getStore();
    // Successful SQL I/O can exceed one second for the 512 MiB fixture. Keep
    // its new latency telemetry separate from failures and callback stalls;
    // the durability, overlap, admission, RSS and event-loop gates still apply.
    if (
      message === "Slow PostgreSQL operation committed" &&
      fields.outcome === "committed" &&
      fields.phase === "commit" &&
      fields.sqlState === undefined &&
      typeof fields.callbackGapMs === "number" &&
      fields.callbackGapMs < 1000 &&
      typeof fields.durationMs === "number" &&
      Number.isFinite(fields.durationMs) &&
      fields.durationMs >= 1000
    ) {
      if (slowCommits.length < 64) slowCommits.push(fields);
      return;
    }
    if (!cleanup && fields.outcome === "overloaded" && !fields.sqlState) {
      capacityDiagnostics++;
      return;
    }
    if (cleanup || diagnostics.length < 64) {
      const record = {
        operation: fields.operation,
        phase: fields.phase,
        outcome: fields.outcome,
        sqlState: fields.sqlState,
        statementId: fields.statementId,
        lockOwner: fields.lockOwner,
        lockMode: fields.lockMode,
        queryCount: fields.queryCount,
        queryMs: fields.queryMs,
        callbackGapMs: fields.callbackGapMs,
        durationMs: fields.durationMs,
      };
      if (cleanup) {
        if (cleanup.length < 64) cleanup.push(record);
      } else diagnostics.push(record);
    }
  },
};
export async function readPostgresEnvironment() {
  const { readPostgresEnvironment: readEnvironment } =
    await import("../../src/lib/db/postgres/environment.ts");
  const options = await testPostgresOptions();
  return readEnvironment({
    OMNIROUTE_POSTGRES_HOST: options.host,
    OMNIROUTE_POSTGRES_PORT: String(options.port),
    OMNIROUTE_POSTGRES_DATABASE: options.database,
    OMNIROUTE_POSTGRES_USER: options.user,
    OMNIROUTE_POSTGRES_PASSWORD: options.password,
    OMNIROUTE_POSTGRES_TLS: "verify-full",
    OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
  });
}
export function readS3ArtifactConfiguration() {
  return configuration;
}
export async function initializePostgresControlPlane(options: PostgresControlPlaneOptions) {
  const { initializePostgresControlPlane: initialize } =
    await import("../../src/lib/db/postgres/controlPlane.ts");
  return initialize({
    ...options,
    logger,
    runtime: { async applyRuntimeSettings() {}, async scheduleCatalogRevalidationAfterInit() {} },
  });
}
/** Actual S3 store and checksums, deterministic transport only; never resolves AWS credentials. */
export function createS3ArtifactStore(): NativeArtifactStore {
  const store = nativeStore(configuration, {
    loadClient: async () => ({
      async send(command) {
        const key = command.input.Key!;
        if (command instanceof PutObjectCommand) {
          assert.ok(command.input.Body instanceof Uint8Array);
          const bytes = Buffer.from(command.input.Body);
          const reference = key.slice(configuration.prefix.length);
          const count = (putCounts.get(reference) ?? 0) + 1;
          putCounts.set(reference, count);
          assert.equal(count, 1, "one PUT per immutable object key");
          const effect = effects.get(reference);
          if (effect?.mode === "private") await effect.revoke!();
          if (effect?.mode === "late")
            effect.pending = effect.gate!.promise.then(() => {
              objects.set(key, bytes);
            });
          else objects.set(key, bytes);
          if (effect?.mode === "lost" || effect?.mode === "late") {
            expectedStorageWarnings++;
            throw Object.assign(new Error("controlled acknowledgement loss"), {
              name: "TimeoutError",
            });
          }
          return { $metadata: { httpStatusCode: 200 } };
        }
        if (command instanceof GetObjectCommand) {
          const bytes = objects.get(key);
          if (!bytes)
            throw Object.assign(new Error("controlled absence"), {
              name: "NoSuchKey",
              $metadata: { httpStatusCode: 404 },
            });
          return {
            Body: bytes,
            ContentLength: bytes.byteLength,
            ChecksumSHA256: createHash("sha256").update(bytes).digest("base64"),
            $metadata: { httpStatusCode: 200 },
          };
        }
        assert.ok(command instanceof DeleteObjectCommand);
        objects.delete(key);
        return { $metadata: { httpStatusCode: 204 } };
      },
      destroy() {},
    }),
  });
  async function observe<T>(operation: string, run: () => Promise<T>): Promise<T> {
    try {
      return await run();
    } catch (error) {
      const controlled =
        isArtifactStorageErrorKind(error, "put_unknown") &&
        error.diagnostic?.name === "TimeoutError";
      if (!controlled && artifactErrors.length < 64) {
        const value = error as { name?: string; kind?: string; sqlState?: string };
        artifactErrors.push({
          operation,
          name: value?.name,
          kind: value?.kind,
          sqlState: value?.sqlState,
        });
      }
      throw error;
    }
  }
  return {
    ...store,
    warmup: () =>
      observe("warmup", async () => {
        await store.warmup?.();
      }),
    put: (bytes, options) => observe("put", () => store.put(bytes, options)),
    get: (reference) => observe("get", () => store.get(reference)),
    delete: (reference) => observe("delete", () => store.delete(reference)),
  };
}

/** Component qualification: persistent logical clients are deliberately not HTTP requests. */
test(
  "bounded mixed native runtime qualification with exact durability and explicit drain",
  {
    skip: !enabled || !postgresTestsEnabled,
    timeout: durationMs + 20 * 60 * 1000,
  },
  async (t) => {
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const hooks = registerHooks({
      resolve(specifier, context, next) {
        if (
          context.parentURL?.split("?")[0] === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: import.meta.url, shortCircuit: true };
        return next(specifier, context);
      },
    });
    const directory = await mkdtemp(join(tmpdir(), "runtime-mixed-soak-"));
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "NODE_ENV",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
    ];
    const previous = new Map(names.map((name) => [name, process.env[name]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const runtime = (await import(
      runtimeUrl
    )) as typeof import("../../src/lib/db/persistenceRuntime.ts");
    const { getSelectedPostgresDatabase, getSelectedPersistenceIdentity } =
      await import("../../src/lib/db/controlPlaneContext.ts");
    const { createPostgresDatabase } = await import("../../src/lib/db/postgres/connect.ts");
    const { isPostgresErrorKind } = await import("../../src/lib/db/postgres/database.ts");
    const { createPostgresAgenticConversationsRepository } =
      await import("../../src/lib/db/postgres/agenticConversationsRepository.ts");
    const { createPostgresCompressionRepository } =
      await import("../../src/lib/db/postgres/compressionRepository.ts");
    const { listSkillRowsAsync, insertSkillRowAsync } =
      await import("../../src/lib/db/skillStorage.ts");
    const files = await import("../../src/lib/db/files.ts");
    const { prepareFileContent } = await import("../../src/lib/db/postgres/serviceFiles.ts");
    const { acquireNativeFileConsumer, isNativeFileCapacityError } =
      await import("../../src/lib/db/nativeFileWork.ts");
    const { PostgresCallLogRepository } =
      await import("../../src/lib/db/postgres/callLogRepository.ts");
    const { PostgresDomainStateRepository } =
      await import("../../src/lib/db/postgres/domainStateRepository.ts");
    const recovery = await import("../../src/lib/usage/nativeArtifactRecovery.ts");
    const { deleteNativeCallArtifacts } =
      await import("../../src/lib/usage/nativeArtifactPublication.ts");
    const { createPostgresCleanupRepository } =
      await import("../../src/lib/db/postgres/cleanupRepository.ts");
    const prefix = "soak-" + randomUUID();
    const counters = Object.fromEntries(
      lanes.map((lane) => [
        lane,
        {
          offered: 0,
          admitted: 0,
          completed: 0,
          productRejected: 0,
          plannedThrottled: 0,
          failed: 0,
        },
      ])
    ) as Record<
      Lane,
      {
        offered: number;
        admitted: number;
        completed: number;
        productRejected: number;
        plannedThrottled: number;
        failed: number;
      }
    >;
    const receipts = new Set<string>();
    const spend = new Map<string, { apiKeyId: string; cost: number; timestamp: number }>();
    const publicationModes = { ack: 0, lost: 0, late: 0, private: 0 };
    const orphanFiles = new Set<string>();
    const publicationRefs = new Map<string, string>();
    const deferredCleanupRefs = new Set<string>();
    let controlledCleanupDeferrals = 0,
      controlledHoldWarnings = 0,
      measuredCleanupDeferrals = 0,
      cleanupLockTimeouts = 0,
      drainedCleanupDeferrals = 0;
    const appendIds = new Set<string>();
    const active = new Map<number, Promise<void>>();
    const parents: Promise<void>[] = [];
    const clients: AsyncResource[] = [];
    const parentRelease = barrier();
    const stopRelease = barrier();
    let finalWork: Promise<void> | undefined;
    let storageScope = "";
    let maximumPhase: Record<string, unknown> = {
      requested: process.env.RUN_PG_IMPORT_MAX_BLOB === "1",
      status: "not_requested",
    };
    let measuredElapsedMs = 0;
    const initialRelease = barrier(),
      initialEntered = barrier();
    const fatal: unknown[] = [];
    const latencies: number[] = [];
    let observer: PostgresDatabase | undefined;
    let monitor: Promise<void> | undefined;
    let running = false,
      actualOverlap = 0,
      currentAttempts = 0,
      fileActive = 0,
      publicationActive = 0,
      peakPublications = 0;
    let maxRss = 0,
      maxOwners = 0,
      sqlHeartbeats = 0,
      fileSqlOverlap = 0,
      ticks = 0;
    let retentionDeleted = 0,
      seenStorageWarnings = 0;
    let nativeIdentity: object | undefined;
    const originalWarn = console.warn;
    const loop = monitorEventLoopDelay({ resolution: 10 });
    const fail = (error: unknown) => {
      if (fatal.length < 64) fatal.push(error);
      running = false;
    };
    const allowedCapacity = (error: unknown) =>
      isPostgresErrorKind(error, "overloaded") ||
      runtime.isPersistenceStreamCapacityError(error) ||
      isNativeFileCapacityError(error);
    const snapshot = () => ({
      event: "runtime-mixed-soak",
      counters,
      actualOverlap,
      fileActive,
      publicationActive,
      peakPublications,
      artifactCleanup: {
        controlledDeferrals: controlledCleanupDeferrals,
        controlledHoldWarnings,
        measuredDeferrals: measuredCleanupDeferrals,
        lockTimeouts: cleanupLockTimeouts,
        pending: deferredCleanupRefs.size,
        drained: drainedCleanupDeferrals,
      },
      active: active.size,
      sqlHeartbeats,
      fileSqlOverlap,
      maxRss,
      maxOwners,
      retainedSeedRows: historyRows,
      retentionDeleted,
      capacityDiagnostics,
      diagnostics: diagnostics.length,
      slowCommits,
      fatal: fatal.length,
      eventLoopP99Ms: loop.percentile(99) / 1e6,
      eventLoopMaxMs: loop.max / 1e6,
    });
    let diskBudget: ReturnType<typeof createDisposableDiskBudget> | undefined;
    let diskSamples = 0,
      minimumFreeBytes = Number.MAX_SAFE_INTEGER,
      peakChunkBytes = 0,
      baselineChunkBytes = 0,
      provedFileDeletions = 0;
    let latestStorage: Record<string, unknown> | undefined;
    let storageIdentity: string | undefined;
    async function sampleStorage(label: string, measureDisk = false, remainingLogicalBytes = 0) {
      assert.ok(observer);
      const row = (
        await observer.transaction((tx) =>
          tx.query<Record<string, string>>(
            `SELECT c.oid::text AS relation_id, pg_relation_filenode(c.oid)::text AS filenode,
              c.reltoastrelid::text AS toast_id, pg_relation_filenode(c.reltoastrelid)::text AS toast_filenode,
              pg_total_relation_size(c.oid)::text AS chunk_bytes,
              pg_relation_size(c.oid)::text AS heap_bytes, pg_indexes_size(c.oid)::text AS index_bytes,
              pg_total_relation_size(c.reltoastrelid)::text AS toast_bytes,
              pg_relation_size(c.reltoastrelid)::text AS toast_heap_bytes,
              pg_indexes_size(c.reltoastrelid)::text AS toast_index_bytes,
              s.n_dead_tup::text AS dead_estimate, s.autovacuum_count::text AS autovacuum_count,
              ts.n_dead_tup::text AS toast_dead_estimate, ts.autovacuum_count::text AS toast_autovacuum_count,
              (SELECT COUNT(*) FROM omniroute.files)::text AS live_files,
              (SELECT COUNT(*) FROM omniroute.files_content_state)::text AS live_states,
              (SELECT COUNT(*) FROM omniroute.files_content_chunks)::text AS live_chunks,
              (SELECT COALESCE(SUM(size_bytes),0) FROM omniroute.files_content_state)::text AS logical_bytes,
              (SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database()
                AND state='idle in transaction' AND clock_timestamp()-state_change>interval '1 second')::text AS stale_idle
            FROM pg_class c JOIN pg_stat_all_tables s ON s.relid=c.oid
            JOIN pg_stat_all_tables ts ON ts.relid=c.reltoastrelid
            WHERE c.oid='omniroute.files_content_chunks'::regclass`
          )
        )
      ).rows[0];
      assert.ok(row);
      const bytes = Number(row.chunk_bytes);
      assert.ok(Number.isSafeInteger(bytes) && bytes >= 0);
      peakChunkBytes = Math.max(peakChunkBytes, bytes);
      const disk = measureDisk ? await readDisposablePostgresDisk() : undefined;
      if (disk) {
        diskSamples++;
        minimumFreeBytes = Math.min(minimumFreeBytes, disk.freeBytes);
      }
      latestStorage = {
        event: "runtime-soak-storage",
        label,
        ...row,
        disk,
        completedFiles: counters.files.completed,
        completedFileBytes: counters.files.completed * FILE_BYTES,
        activeFiles: fileActive,
        orphanFiles: orphanFiles.size,
      };
      // Keep the actual operands before any assertion can abort or cleanup can mask them.
      if (measureDisk) t.diagnostic(JSON.stringify(latestStorage));
      try {
        const identity = JSON.stringify([
          row.relation_id,
          row.filenode,
          row.toast_id,
          row.toast_filenode,
        ]);
        storageIdentity ??= identity;
        assert.equal(identity, storageIdentity);
        assert.equal(row.stale_idle, "0", "no unexplained prolonged idle transaction");
        assert.ok(
          Number(row.logical_bytes) <= 6 * FILE_BYTES,
          "bounded in-flight and orphan file content"
        );
        if (diskBudget) {
          assert.ok(
            bytes - baselineChunkBytes <= diskBudget.growthBudgetBytes,
            "finite fixture chunk growth"
          );
          if (disk)
            assertDisposableDiskBudget(
              diskBudget,
              disk,
              Math.max(0, bytes - baselineChunkBytes),
              remainingLogicalBytes
            );
        }
      } catch (error) {
        if (!measureDisk) t.diagnostic(JSON.stringify(latestStorage));
        throw error;
      }
      return row;
    }
    let payload: Buffer | undefined;
    const keys = Array.from({ length: 10 }, (_, i) => `${prefix}-key-${i}`);
    const privateKeys = keys.map((key) => key + "-private");
    const conversations = keys.map((key) => key + "-conversation");
    const retainedConversation = prefix + "-retained-conversation";
    try {
      const serverProfile = await readPostgresEnvironment();
      assert.equal(serverProfile.maxConnections, 16);
      assert.equal(serverProfile.maxQueuedOperations, 256);
      assert.equal(serverProfile.admissionWaitTimeoutMs, 3000);
      await runtime.ensurePersistenceReady();
      observer = await createPostgresDatabase(await testPostgresOptions(), logger);
      const config = (
        await observer.transaction((tx) =>
          tx.query(
            `SELECT current_setting('server_version_num') AS version,current_setting('statement_timeout') AS statement,
       current_setting('lock_timeout') AS lock,current_setting('transaction_timeout') AS transaction,
       current_setting('idle_in_transaction_session_timeout') AS idle,
       (SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()) AS tls`
          )
        )
      ).rows[0];
      assert.deepEqual(config, {
        version: "180006",
        statement: "5s",
        lock: "1s",
        transaction: "15s",
        idle: "5s",
        tls: true,
      });
      diskBudget = createDisposableDiskBudget(await readDisposablePostgresDisk());
      assertDisposableDiskBudget(diskBudget, diskBudget, 0, maximumLogicalFileBytes);
      const initialStorage = await sampleStorage("before-seed", true, maximumLogicalFileBytes);
      baselineChunkBytes = Number(initialStorage.chunk_bytes);
      await runtime.runPersistenceRequest(async () => {
        nativeIdentity = getSelectedPersistenceIdentity();
        storageScope = runtime.getPersistenceArtifactStore().storageScope!;
        const database = getSelectedPostgresDatabase()!;
        for (const key of [...keys, ...privateKeys])
          await database.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.api_keys(id,name,key,no_log,created_at) VALUES($1,$1,$2,0,$3)",
              [key, randomUUID(), new Date().toISOString()]
            )
          );
        for (let i = 0; i < 10; i++) {
          const repo = createPostgresAgenticConversationsRepository(database);
          await repo.createAgenticConversation({
            id: conversations[i],
            apiKeyId: keys[i],
            fingerprintHash: conversations[i],
          });
          await repo.insertConversationTurnNodes(
            conversations[i],
            prefix,
            Array.from({ length: 1024 }, (_, n) => ({
              id: `${conversations[i]}-${n}`,
              parentId: n ? `${conversations[i]}-${n - 1}` : null,
              role: n % 2 ? "assistant" : "user",
              contentHash: createHash("sha256").update(String(n)).digest("hex"),
            }))
          );
          await insertSkillRowAsync({
            id: keys[i] + "-skill",
            api_key_id: keys[i],
            name: keys[i],
            version: "1",
            description: null,
            schema: "{}",
            handler: "test-only",
            enabled: 1,
            mode: "local",
            source_provider: null,
            tags: "[]",
            install_count: 0,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
          });
        }
      });
      await runtime.runPersistenceRequest(() =>
        createPostgresAgenticConversationsRepository(
          getSelectedPostgresDatabase()!
        ).createAgenticConversation({
          id: retainedConversation,
          apiKeyId: null,
          fingerprintHash: retainedConversation,
        })
      );
      // Offline fixture preparation has its own labeled client and budget; it
      // closes before any measured live traffic and cannot alter server defaults.
      const seedStarted = performance.now();
      const seeder = await createPostgresDatabase(
        await testPostgresOptions({
          maxConnections: 1,
          maxQueuedOperations: 0,
          statementTimeoutMs: 30000,
          queryTimeoutMs: 40000,
          transactionTimeoutMs: 90000,
          idleInTransactionTimeoutMs: 30000,
        }),
        logger
      );
      try {
        // Committed bounded setup statements; the timed phase never widens live transaction guards.
        for (let offset = 0; offset < historyRows; offset += 10000)
          await seeder.transaction((tx) =>
            insertConversationFixture(
              tx,
              `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,parent_id,role,content_hash,first_seen_at,last_seen_at)
       SELECT $1||n,$2,NULL,'user',repeat('a',64),$3,$3 FROM generate_series($4::int,$5::int) n`,
              [
                prefix + "-retained-",
                retainedConversation,
                new Date().toISOString(),
                offset,
                Math.min(historyRows, offset + 10000) - 1,
              ]
            )
          );
      } finally {
        await seeder.close();
      }
      t.diagnostic(
        JSON.stringify({
          event: "runtime-soak-fixture-seeded",
          rows: historyRows,
          elapsedMs: performance.now() - seedStarted,
          offlineBudget: true,
          seedClientClosed: true,
        })
      );
      if (process.env.RUN_PG_IMPORT_MAX_BLOB === "1") {
        const maximum = 512 * 1024 * 1024;
        let fileId: string | undefined,
          expectedHash = "",
          peakRss = process.memoryUsage().rss;
        let readCount = 0,
          responseCount = 0;
        const readsReady = barrier(),
          responsesReady = barrier(),
          allowResponses = barrier(),
          allowDrain = barrier();
        const downloads: Promise<void>[] = [];
        try {
          // Fixture-only bounded seed: no extra 512-MiB input remains reachable
          // while the real serving readers and Response bodies own their bytes.
          fileId = prefix + "-maximum";
          await (async () => {
            const chunk = Buffer.alloc(8 * 1024 * 1024);
            randomFillSync(chunk);
            const fullHash = createHash("sha256");
            for (let index = 0; index < 64; index++) {
              fullHash.update(chunk);
              await turn();
            }
            expectedHash = fullHash.digest("hex");
            const chunkHash = createHash("sha256").update(chunk).digest("hex");
            await observer!.transaction(async (tx) => {
              await tx.query(
                "INSERT INTO omniroute.files(id,bytes,created_at,filename,purpose,content,deleted_at) VALUES($1,$2,$3,'maximum-soak.bin','fine-tune',NULL,$3)",
                [fileId, maximum, Math.floor(Date.now() / 1000)]
              );
              await tx.query(
                "INSERT INTO omniroute.files_content_state(file_id,size_bytes,chunk_count,checksum) VALUES($1,$2,64,$3)",
                [fileId, maximum, expectedHash]
              );
            });
            for (let index = 0; index < 64; index++)
              await observer!.transaction((tx) =>
                tx.query(
                  "INSERT INTO omniroute.files_content_chunks(file_id,chunk_index,content,checksum) VALUES($1,$2,$3,$4)",
                  [fileId, index, chunk, chunkHash]
                )
              );
            await observer!.transaction((tx) =>
              tx.query("UPDATE omniroute.files SET deleted_at=NULL WHERE id=$1", [fileId])
            );
          })();
          const baselineRss = process.memoryUsage().rss;
          for (let index = 0; index < 2; index++)
            downloads.push(
              runtime.runPersistenceRequest(async () => {
                const consumer = acquireNativeFileConsumer();
                try {
                  let content = await files.getFileContentForConsumerAsync(fileId!, consumer);
                  assert.equal(content?.byteLength, maximum);
                  peakRss = Math.max(
                    peakRss,
                    process.memoryUsage().rss,
                    process.resourceUsage().maxRSS * 1024
                  );
                  if (++readCount === 2) readsReady.release();
                  await allowResponses.promise;
                  const response = new Response(content as unknown as BodyInit);
                  content = null;
                  peakRss = Math.max(
                    peakRss,
                    process.memoryUsage().rss,
                    process.resourceUsage().maxRSS * 1024
                  );
                  if (++responseCount === 2) responsesReady.release();
                  await allowDrain.promise;
                  const reader = response.body!.getReader(),
                    hash = createHash("sha256");
                  let bytes = 0;
                  try {
                    while (true) {
                      const value = await reader.read();
                      if (value.done) break;
                      bytes += value.value.byteLength;
                      hash.update(value.value);
                      await turn();
                    }
                  } finally {
                    reader.releaseLock();
                  }
                  assert.equal(bytes, maximum);
                  assert.equal(hash.digest("hex"), expectedHash);
                  peakRss = Math.max(
                    peakRss,
                    process.memoryUsage().rss,
                    process.resourceUsage().maxRSS * 1024
                  );
                } finally {
                  consumer.release();
                }
              })
            );
          const both = Promise.all(downloads);
          void both.catch(() => {
            allowResponses.release();
            allowDrain.release();
          });
          await Promise.race([readsReady.promise, both]);
          assert.equal(readCount, 2);
          assert.equal(
            globalThis.__omnirouteNativeFileWork?.budgets.get(nativeIdentity!)?.bytes,
            4 * 1024 ** 3
          );
          allowResponses.release();
          await Promise.race([responsesReady.promise, both]);
          assert.equal(responseCount, 2);
          peakRss = Math.max(
            peakRss,
            process.memoryUsage().rss,
            process.resourceUsage().maxRSS * 1024
          );
          t.diagnostic(
            JSON.stringify({
              event: "two-maximum-downloads-held",
              readCount,
              responseCount,
              baselineRss,
              retainedFixturePayloadBytes: 0,
              peakRss,
              creditBytes: 4 * 1024 ** 3,
              bytesPerDownload: maximum,
            })
          );
          allowDrain.release();
          await both;
          maximumPhase = {
            requested: true,
            status: "passed",
            readCount,
            responseCount,
            baselineRss,
            peakRss,
            retainedFixturePayloadBytes: 0,
            bytesPerDownload: maximum,
          };
          assert.ok(
            peakRss < 4 * 1024 ** 3,
            "two admitted maximum Response bodies must fit the independently enforced 4 GiB cgroup"
          );
        } finally {
          allowResponses.release();
          allowDrain.release();
          await Promise.allSettled(downloads);
          if (fileId)
            await runtime.runPersistenceRequest(async () => {
              await files.deleteFileAsync(fileId!);
              await getSelectedPostgresDatabase()!.transaction((tx) =>
                tx.query("DELETE FROM omniroute.files WHERE id=$1", [fileId])
              );
            });
        }
      }
      payload = Buffer.alloc(FILE_BYTES);
      randomFillSync(payload);
      const expectedFileHash = createHash("sha256").update(payload).digest("hex");
      // Direct preparation owner: observe real SQL completion and macrotasks while CPU work yields.
      let preparing = true,
        preparationQueries = 0,
        preparationTicks = 0;
      const queryDispatched = barrier();
      let queryCompletedAt = 0;
      const sqlProgress = observer!.transaction(async (tx) => {
        const query = tx.query("SELECT 1");
        queryDispatched.release();
        await query;
        queryCompletedAt = performance.now();
        if (preparing) preparationQueries++;
      });
      // Acquisition and BEGIN must finish before the preparation observation window.
      await Promise.race([queryDispatched.promise, sqlProgress]);
      const preparationStarted = performance.now();
      const preparationHeartbeat = (async () => {
        while (preparing) {
          await turn();
          if (preparing) preparationTicks++;
        }
      })();
      let preparationFinished = 0;
      try {
        await prepareFileContent(payload);
      } finally {
        preparationFinished = performance.now();
        preparing = false;
        await Promise.all([sqlProgress, preparationHeartbeat]);
      }
      t.diagnostic(
        JSON.stringify({
          event: "file-preparation-sql-overlap",
          preparationMs: preparationFinished - preparationStarted,
          queryCompletionMs: queryCompletedAt - preparationStarted,
          preparationQueries,
          preparationTicks,
        })
      );
      assert.ok(preparationQueries > 0, "independent SQL completes while file preparation yields");
      assert.ok(
        preparationTicks >= FILE_BYTES / (8 * 1024 * 1024),
        "preparation yields between bounded chunks"
      );

      async function receipt(database: PostgresDatabase, id: string, value: string) {
        await database.transaction((tx) =>
          tx.query(
            "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3) ON CONFLICT(namespace,key) DO NOTHING",
            [prefix, id, value]
          )
        );
        receipts.add(id);
      }
      async function publication(
        database: PostgresDatabase,
        id: string,
        client: number,
        sequence: number,
        holdCleanup = false
      ) {
        const mode = (["ack", "lost", "late", "private"] as const)[Math.floor(sequence / 6) % 4];
        const key = mode === "private" ? privateKeys[client] : keys[client];
        const intent = recovery.prepareNativeArtifactRecovery(
          Buffer.from(JSON.stringify({ id, content: "x".repeat(8192) }))
        );
        const effect = {
          mode,
          gate: mode === "late" ? barrier() : undefined,
          pending: undefined as Promise<void> | undefined,
          revoke:
            mode === "private"
              ? () =>
                  database.transaction(async (tx) => {
                    await tx.query("UPDATE omniroute.api_keys SET no_log=1 WHERE id=$1", [key]);
                  })
              : undefined,
        };
        const inserted = await new PostgresCallLogRepository(database).insert(
          {
            id,
            timestamp: new Date().toISOString(),
            status: 200,
            apiKeyId: key,
            detailState: "missing",
            detailReason: "publication_pending",
            hasRequestBody: 1,
            hasResponseBody: 0,
            hasPipelineDetails: 0,
            videoContentRemoved: 0,
          },
          intent
        );
        assert.equal(inserted.inserted, true);
        assert.equal(inserted.intentAccepted, true);
        effects.set(intent.artifact.reference, effect);
        publicationRefs.set(id, intent.artifact.reference);
        try {
          const first = await recovery.recoverNativeArtifact(intent.artifact.reference);
          if (mode === "lost" || mode === "late") {
            assert.equal(first, false);
            if (mode === "late") {
              assert.equal(await recovery.recoverNativeArtifact(intent.artifact.reference), false);
              effect.gate!.release();
              await effect.pending;
            }
            assert.equal(await recovery.recoverNativeArtifact(intent.artifact.reference), true);
          } else assert.equal(first, mode === "ack");
          const row = (
            await database.transaction((tx) =>
              tx.query<{ detail_state: string; artifact_relpath: string | null }>(
                "SELECT detail_state,artifact_relpath FROM omniroute.call_logs WHERE id=$1",
                [id]
              )
            )
          ).rows[0];
          assert.equal(row.detail_state, mode === "private" ? "none" : "ready");
          if (mode === "private") assert.equal(row.artifact_relpath, null);
          assert.equal(putCounts.get(intent.artifact.reference), 1);
          await receipt(database, id, "publication");
          publicationModes[mode]++;
        } finally {
          effect.gate?.release();
          await effect.pending;
          await database.transaction(async (tx) => {
            await acquirePostgresLock(tx, "accounting");
            await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [id]);
            if (mode === "private")
              await tx.query("UPDATE omniroute.api_keys SET no_log=0 WHERE id=$1", [key]);
          });
          const held = barrier(),
            release = barrier();
          const heldDiagnostics: Record<string, unknown>[] = [];
          let holder: Promise<unknown> | undefined;
          try {
            if (holdCleanup) {
              // The metadata deletion commits before this controlled optional-cleanup conflict.
              holder = cleanupDiagnostics.run(heldDiagnostics, () =>
                observer!.transaction(
                  async (tx) => {
                    await acquirePostgresLock(tx, "accounting");
                    held.release();
                    await release.promise;
                  },
                  { operation: "soak.controlled_cleanup_hold" }
                )
              );
              void holder.catch(() => {});
              await Promise.race([held.promise, holder]);
            }
            const observed: Record<string, unknown>[] = [];
            const deleted = await cleanupDiagnostics.run(observed, () =>
              deleteNativeCallArtifacts([intent.artifact.reference])
            );
            if (deleted.pendingArtifactDeletions === 0) {
              assert.equal(holdCleanup, false, "the controlled owner must exclude cleanup");
              assert.equal(deleted.errors, 0);
              assert.equal(observed.length, 0);
              effects.delete(intent.artifact.reference);
              publicationRefs.delete(id);
            } else {
              assert.equal(deleted.pendingArtifactDeletions, 1);
              assert.equal(deleted.errors, 1);
              assert.ok(observed.length >= 1 && observed.length <= 2);
              for (const event of observed) {
                assert.equal(event.outcome, "query_failed");
                assert.equal(event.sqlState, "55P03");
                assert.equal(event.phase, "query");
                assert.equal(event.queryCount, 1);
                assert.equal(event.lockOwner, "accounting");
                assert.equal(event.lockMode, "exclusive");
                assert.equal(event.statementId, accountingLockStatement);
              }
              const proof = await database.transaction((tx) =>
                tx.query(
                  `SELECT publication_state,staged_payload IS NULL AND staged_bytes=0 AS unstaged,
                    NOT EXISTS(SELECT 1 FROM omniroute.call_logs WHERE artifact_relpath=$1
                      UNION ALL SELECT 1 FROM omniroute.call_logs_v1_legacy WHERE artifact_relpath=$1) AS unowned
                   FROM omniroute.artifact_deletion_queue WHERE relpath=$1 AND store_scope=$2`,
                  [intent.artifact.reference, storageScope]
                )
              );
              assert.deepEqual(proof.rows, [
                { publication_state: "delete_pending", unstaged: true, unowned: true },
              ]);
              deferredCleanupRefs.add(intent.artifact.reference);
              cleanupLockTimeouts += observed.length;
              if (holdCleanup) controlledCleanupDeferrals++;
              else measuredCleanupDeferrals++;
            }
          } finally {
            release.release();
            await holder;
            if (holdCleanup) {
              assert.equal(heldDiagnostics.length, 1);
              const event = heldDiagnostics[0];
              assert.equal(event.operation, "soak.controlled_cleanup_hold");
              assert.equal(event.outcome, "committed");
              assert.equal(event.phase, "commit");
              assert.equal(event.sqlState, undefined);
              assert.equal(event.queryCount, 1);
              assert.equal(event.lockOwner, "accounting");
              assert.equal(event.lockMode, "exclusive");
              assert.equal(event.statementId, accountingLockStatement);
              assert.ok(Number(event.callbackGapMs) >= 1000);
              controlledHoldWarnings++;
            }
          }
        }
      }
      async function perform(lane: Lane, client: number, sequence: number) {
        const database = getSelectedPostgresDatabase()!;
        const id = `${prefix}-${lane}-${client}-${sequence}`;
        if (lane === "history") {
          const repo = createPostgresAgenticConversationsRepository(database);
          const nodes = Array.from({ length: 64 }, (_, n) => ({
            id: id + "-" + n,
            parentId: `${conversations[client]}-1023`,
            role: "assistant",
            contentHash: String(n),
          }));
          await repo.insertConversationTurnNodes(conversations[client], id, nodes);
          for (const node of nodes) appendIds.add(node.id);
          await repo.insertConversationTurnNodes(conversations[client], id, nodes);
          const count = (
            await database.transaction((tx) =>
              tx.query<{ n: string }>(
                "SELECT COUNT(*) AS n FROM omniroute.conversation_turn_nodes WHERE id=ANY($1::text[])",
                [nodes.map((node) => node.id)]
              )
            )
          ).rows[0].n;
          assert.equal(count, "64");
          const index = await repo.getConversationTurnIndex(conversations[client]);
          assert.ok(index.nodeIds.size >= 1088);
          assert.ok(nodes.every((node) => index.nodeIds.has(node.id)));
          await database.transaction((tx) =>
            deleteConversationFixture(
              tx,
              "DELETE FROM omniroute.conversation_turn_nodes WHERE id=ANY($1::text[])",
              [nodes.map((node) => node.id)]
            )
          );
          for (const node of nodes) appendIds.delete(node.id);
        } else if (lane === "reads") {
          await createPostgresCompressionRepository(database).readConfigSnapshot();
          assert.equal((await listSkillRowsAsync([keys[client]])).length, 1);
        } else if (lane === "files") {
          if (fileActive >= 2 || orphanFiles.size >= 4) {
            counters.files.plannedThrottled++;
            return false;
          }
          const consumer = acquireNativeFileConsumer();
          fileActive++;
          try {
            consumer.beforeBuffer(FILE_BYTES);
            const file = await files.createFileAsync({
              bytes: FILE_BYTES,
              filename: "mixed.bin",
              purpose: "fine-tune",
              content: payload!,
              apiKeyId: keys[client],
            });
            orphanFiles.add(file.id);
            const content = await files.getFileContentForConsumerAsync(file.id, consumer);
            assert.ok(content);
            assert.equal(content.byteLength, FILE_BYTES);
            assert.equal(createHash("sha256").update(content).digest("hex"), expectedFileHash);
            await files.deleteFileAsync(file.id);
            const deleted = await database.transaction((tx) =>
              tx.query(
                `SELECT f.deleted_at IS NOT NULL AS deleted, f.content IS NULL AS cleared,
                  NOT EXISTS (SELECT 1 FROM omniroute.files_content_state WHERE file_id=$1) AS state_absent,
                  NOT EXISTS (SELECT 1 FROM omniroute.files_content_chunks WHERE file_id=$1) AS chunks_absent
                FROM omniroute.files f WHERE f.id=$1`,
                [file.id]
              )
            );
            assert.deepEqual(
              deleted.rows,
              [{ deleted: true, cleared: true, state_absent: true, chunks_absent: true }],
              "product deletion must clear content before fixture hard cleanup"
            );
            provedFileDeletions++;
            await database.transaction((tx) =>
              tx.query("DELETE FROM omniroute.files WHERE id=$1", [file.id])
            );
            orphanFiles.delete(file.id);
          } finally {
            consumer.release();
            fileActive--;
          }
        } else if (lane === "receipt") {
          const receiptId = randomUUID(),
            event = { apiKeyId: keys[client], cost: 0.125, timestamp: Date.now() };
          const repository = new PostgresDomainStateRepository(database);
          await repository.batchSaveCostEntries([{ ...event, receiptId }]);
          spend.set(receiptId, event);
          assert.equal(
            await repository.loadCostTotal(keys[client], 0),
            [...spend.values()].filter((row) => row.apiKeyId === keys[client]).length * 0.125
          );
        } else if (lane === "publication") {
          if (publicationActive + deferredCleanupRefs.size >= ARTIFACT_OPERATION_LIMIT) {
            counters.publication.plannedThrottled++;
            return false;
          }
          publicationActive++;
          peakPublications = Math.max(peakPublications, publicationActive);
          try {
            await publication(database, id, client, sequence);
          } finally {
            publicationActive--;
          }
        } else if (lane === "cancelled") {
          const cancelled = new AbortController();
          let dispatched = false;
          const preparation = (async () => {
            await turn();
            await delay(5);
            if (!cancelled.signal.aborted) dispatched = true;
          })();
          cancelled.abort();
          // The deadline-facing winner has returned; the actual attempt owner joins loser cleanup.
          await preparation;
          assert.equal(dispatched, false);
          await receipt(database, id + "-cleanup", "cancelled-cleanup");
        }
        await receipt(database, id, lane);
        return true;
      }
      await runtime.runPersistenceRequest(async () => {
        await publication(getSelectedPostgresDatabase()!, prefix + "-cleanup-proof", 0, 0, true);
      });
      assert.equal(controlledCleanupDeferrals, 1);
      assert.equal(deferredCleanupRefs.size, 1);
      publicationModes.ack--;
      for (let client = 0; client < 10; client++) {
        const ready = barrier();
        const parent = runtime.runPersistenceRequest(async () => {
          clients[client] = new AsyncResource("mixed-soak-client");
          ready.release();
          await parentRelease.promise;
        });
        parents.push(parent);
        void parent.catch(fail);
        await Promise.race([ready.promise, parent]);
      }
      let sequence = 0;
      const offer = (slot: number, initial = false) => {
        const lane = lanes[slot % 6],
          client = Math.floor(slot / 6),
          row = counters[lane];
        row.offered++;
        assert.ok(
          counters.files.offered <= maximumFileOffers,
          "finite scheduled file-offer envelope"
        );
        if (active.has(slot)) {
          row.plannedThrottled++;
          return;
        }
        const ticket = sequence++,
          begin = performance.now();
        const work = clients[client]
          .runInAsyncScope(() =>
            runtime.runPersistenceAttempt(
              async () => {
                row.admitted++;
                currentAttempts++;
                actualOverlap = Math.max(actualOverlap, currentAttempts);
                if (initial && currentAttempts === 60) initialEntered.release();
                try {
                  if (initial) await initialRelease.promise;
                  if (await perform(lane, client, ticket)) row.completed++;
                } finally {
                  currentAttempts--;
                }
              },
              { independent: true }
            )
          )
          .catch((error: unknown) => {
            if (allowedCapacity(error)) row.productRejected++;
            else {
              row.failed++;
              fail(error);
            }
          })
          .finally(() => {
            active.delete(slot);
            latencies.push(performance.now() - begin);
          });
        active.set(slot, work);
      };
      console.warn = (...values: unknown[]) => {
        const detail = values[1] as { kind?: string; name?: string } | undefined;
        if (
          values[0] === "[callLogs] Artifact publication deferred" &&
          detail?.kind === "put_unknown" &&
          detail.name === "TimeoutError"
        )
          seenStorageWarnings++;
        else {
          diagnostics.push({ warning: String(values[0]).slice(0, 160) });
          originalWarn(...values);
          fail(new Error("Unexpected internal warning during mixed soak"));
        }
      };
      // Seed/index/WAL usage is included in this fresh remaining-headroom check.
      await sampleStorage("before-measured-load", true, maximumLogicalFileBytes);
      running = true;
      loop.enable();
      const began = performance.now();
      t.diagnostic(
        JSON.stringify({
          event: "runtime-mixed-soak-start",
          durationMs,
          historyRows,
          serverProfile: {
            connections: 16,
            queue: 256,
            admissionMs: 3000,
            statementMs: 5000,
            transactionMs: 15000,
          },
          requiresExternalCgroup: { cpus: 2, memoryBytes: 4 * 1024 ** 3 },
        })
      );
      monitor = (async () => {
        let nextReport = began + 60000;
        while (running) {
          await delay(1000);
          ticks++;
          const status = runtime.getPersistenceRuntimeStatus();
          maxOwners = Math.max(maxOwners, status.attemptOperations);
          maxRss = Math.max(
            maxRss,
            process.memoryUsage().rss,
            process.resourceUsage().maxRSS * 1024
          );
          assert.ok(status.attemptOperations <= 60 && status.activeAttempts <= 60);
          assert.ok(objects.size <= 20 && effects.size <= 20);
          const reportDue = performance.now() >= nextReport;
          await sampleStorage("measured-load", reportDue);
          sqlHeartbeats++;
          if (fileActive) fileSqlOverlap++;
          if (reportDue) {
            t.diagnostic(JSON.stringify({ ...snapshot(), elapsedMs: performance.now() - began }));
            nextReport += 60000;
          }
        }
      })().catch(fail);
      for (let slot = 0; slot < 60; slot++) offer(slot, true);
      await Promise.race([
        initialEntered.promise,
        delay(15000).then(() => {
          throw new Error("60 actual overlapping native attempts were not admitted");
        }),
      ]);
      assert.equal(runtime.getPersistenceRuntimeStatus().attemptOperations, 60);
      initialRelease.release();
      let slot = 0,
        nextCleanup = began + Math.min(10000, durationMs / 4);
      while (running && performance.now() - began < durationMs) {
        const fraction = (performance.now() - began) / durationMs;
        const rate = fraction < 0.2 ? 35 : fraction < 0.8 ? 70 : 120;
        await delay(Math.min(60000 / rate, Math.max(0, durationMs - (performance.now() - began))));
        if (!running || performance.now() - began >= durationMs) break;
        offer(slot++ % 60);
        if (performance.now() >= nextCleanup) {
          // Maintenance owns a separate observed scope, not an untracked async timer.
          await runtime.runPersistenceRequest(async () => {
            const database = getSelectedPostgresDatabase()!;
            const cutoff = new Date(Date.now() - 30 * 86400000).toISOString();
            await database.transaction((tx) =>
              insertConversationFixture(
                tx,
                `INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,role,content_hash,first_seen_at,last_seen_at)
             SELECT $1||n,$2,'user','expired',$3,$3 FROM generate_series(1,250) n`,
                [
                  prefix + "-expired-" + sequence + "-",
                  conversations[0],
                  new Date(Date.now() - 31 * 86400000).toISOString(),
                ]
              )
            );
            retentionDeleted += await createPostgresCleanupRepository(database).deleteBefore(
              { table: "conversation_turn_nodes", column: "last_seen_at", cutoff: "iso" },
              cutoff
            );
          });
          nextCleanup += 60000;
        }
      }
      measuredElapsedMs = performance.now() - began;
      running = false;
      await monitor;
      await Promise.all(active.values());
      loop.disable();
      if (fatal.length) {
        try {
          await sampleStorage("failure-before-cleanup", true);
        } catch (error) {
          t.diagnostic(
            JSON.stringify({
              event: "runtime-soak-storage-failure-sample",
              name: (error as Error).name,
              latestStorage,
            })
          );
        }
        t.diagnostic(
          JSON.stringify({
            ...snapshot(),
            event: "runtime-mixed-soak-failed",
            artifactErrors,
            diagnostics,
            failures: fatal.map((error) => {
              const value = error as { name?: string; kind?: string; sqlState?: string };
              return { name: value?.name, kind: value?.kind, sqlState: value?.sqlState };
            }),
          })
        );
        throw new AggregateError(fatal, "Unexplained mixed-soak failures");
      }
      assert.ok(
        performance.now() - began >= durationMs,
        "the requested measured duration must actually elapse"
      );
      assert.equal(
        fatal.length,
        0,
        JSON.stringify(
          fatal.map((error) => {
            const value = error as { name?: string; kind?: string; sqlState?: string };
            return { name: value?.name, kind: value?.kind, sqlState: value?.sqlState };
          })
        )
      );
      assert.equal(diagnostics.length, 0, JSON.stringify(diagnostics));
      assert.equal(seenStorageWarnings, expectedStorageWarnings);
      for (const lane of lanes)
        assert.ok(
          counters[lane].completed >=
            Math.max(
              lane === "files" ? 2 : 3,
              Math.floor(durationMs / 60000) * (lane === "files" ? 2 : 5)
            ),
          `${lane} requires successful real work`
        );
      for (const mode of Object.keys(publicationModes) as Array<keyof typeof publicationModes>)
        assert.ok(
          publicationModes[mode] >= 1,
          `publication fault mode ${mode} requires actual completion`
        );
      assert.equal(actualOverlap, 60);
      assert.ok(sqlHeartbeats >= Math.floor(durationMs / 2000));
      assert.ok(retentionDeleted >= 250);
      assert.ok(preparationQueries > 0);
      assert.ok(loop.percentile(99) / 1e6 < 50, "normal mixed event-loop p99 budget");
      assert.ok(loop.max / 1e6 < 250, "normal mixed event-loop maximum budget");
      assert.ok(
        maxRss < 4 * 1024 ** 3,
        "application RSS must fit the separately enforced 4 GiB cgroup"
      );
      for (const row of Object.values(counters))
        assert.equal(
          row.offered,
          row.completed + row.productRejected + row.plannedThrottled + row.failed
        );
      // Cleanup known temporary identities; never replay partially acknowledged work.
      await runtime.runPersistenceRequest(async () => {
        const database = getSelectedPostgresDatabase()!;
        for (const id of orphanFiles) {
          await files.deleteFileAsync(id);
          await database.transaction((tx) =>
            tx.query("DELETE FROM omniroute.files WHERE id=$1", [id])
          );
        }
        orphanFiles.clear();
        if (appendIds.size)
          await database.transaction((tx) =>
            deleteConversationFixture(
              tx,
              "DELETE FROM omniroute.conversation_turn_nodes WHERE id=ANY($1::text[])",
              [[...appendIds]]
            )
          );
        appendIds.clear();
        for (const [id, reference] of publicationRefs) {
          const effect = effects.get(reference);
          effect?.gate?.release();
          await effect?.pending;
          await database.transaction(async (tx) => {
            await acquirePostgresLock(tx, "accounting");
            await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [id]);
          });
          assert.equal((await deleteNativeCallArtifacts([reference])).pendingArtifactDeletions, 0);
          if (deferredCleanupRefs.delete(reference)) drainedCleanupDeferrals++;
          effects.delete(reference);
        }
        publicationRefs.clear();
      });
      assert.equal(orphanFiles.size, 0);
      assert.equal(appendIds.size, 0);
      assert.equal(deferredCleanupRefs.size, 0);
      assert.equal(drainedCleanupDeferrals, controlledCleanupDeferrals + measuredCleanupDeferrals);
      const drainedStorage = await sampleStorage("after-drain", true);
      for (const field of ["live_files", "live_states", "live_chunks", "logical_bytes"])
        assert.equal(drainedStorage[field], "0", "exact drained file absence");
      assert.ok(provedFileDeletions >= counters.files.completed);
      assert.ok(diskSamples >= 3);
      const durable = await observer.transaction(async (tx) => ({
        spend: (
          await tx.query<{ receipt_id: string; cost: number; history_cost: number }>(
            "SELECT r.receipt_id,r.cost,h.cost AS history_cost FROM omniroute.domain_cost_receipts r JOIN omniroute.domain_cost_history h ON h.id=r.history_id WHERE r.api_key_id=ANY($1::text[])",
            [keys]
          )
        ).rows,
        receipts: (
          await tx.query<{ key: string }>(
            "SELECT key FROM omniroute.key_value WHERE namespace=$1",
            [prefix]
          )
        ).rows,
        histories: (
          await tx.query<{ n: string }>(
            "SELECT COUNT(*) AS n FROM omniroute.conversation_turn_nodes WHERE conversation_id=ANY($1::text[])",
            [[...conversations, retainedConversation]]
          )
        ).rows[0].n,
        staged: (
          await tx.query<{ n: string }>(
            "SELECT COUNT(*) AS n FROM omniroute.artifact_deletion_queue WHERE store_scope=$1 AND publication_state<>'attached'",
            [storageScope]
          )
        ).rows[0].n,
      }));
      assert.deepEqual(new Set(durable.spend.map((row) => row.receipt_id)), new Set(spend.keys()));
      assert.ok(durable.spend.every((row) => row.cost === 0.125 && row.history_cost === 0.125));
      assert.deepEqual(new Set(durable.receipts.map((row) => row.key)), receipts);
      assert.equal(Number(durable.histories), historyRows + 10240);
      assert.equal(durable.staged, "0");
      assert.equal(objects.size, 0);
      // A real accepted owner survives admission close and commits its final cleanup.
      const stopEntered = barrier();
      finalWork = clients[0].runInAsyncScope(() =>
        runtime.runPersistenceAttempt(
          async () => {
            stopEntered.release();
            await stopRelease.promise;
            await receipt(getSelectedPostgresDatabase()!, prefix + "-drain", "drained");
          },
          { independent: true }
        )
      );
      await stopEntered.promise;
      await recovery.stopNativeArtifactRecovery();
      runtime.beginPersistenceAdmissionClose();
      runtime.cancelPersistenceAttempts();
      await assert.rejects(
        runtime.runPersistenceRequest(async () => {}),
        /draining|unavailable/
      );
      parentRelease.release();
      await Promise.all(parents);
      let drained = false;
      const draining = runtime.drainPersistenceAcceptedWork().then(() => {
        drained = true;
      });
      await turn();
      assert.equal(drained, false);
      stopRelease.release();
      await finalWork;
      await draining;
      await recovery.stopNativeArtifactRecovery();
      const budget = globalThis.__omnirouteNativeFileWork?.budgets.get(nativeIdentity!);
      assert.deepEqual(budget, { count: 0, bytes: 0 });
      const status = runtime.getPersistenceRuntimeStatus();
      assert.deepEqual(
        [
          status.activeRequests,
          status.activeAttempts,
          status.attemptOperations,
          status.backgroundOperations,
          status.streamTails,
        ],
        [0, 0, 0, 0, 0]
      );
      await runtime.closePersistenceRuntime();
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closed");
      assert.equal(diagnostics.length, 0, "no unexplained diagnostics during final drain");
      assert.equal(fatal.length, 0, "no late asynchronous failure during final drain");
      assert.equal(
        (
          await observer.transaction((tx) =>
            tx.query("SELECT key FROM omniroute.key_value WHERE namespace=$1 AND key=$2", [
              prefix,
              prefix + "-drain",
            ])
          )
        ).rowCount,
        1
      );
      t.diagnostic(
        JSON.stringify({
          ...snapshot(),
          elapsedMs: performance.now() - began,
          preparationQueries,
          preparationTicks,
          ticks,
          spendReceipts: spend.size,
          publicationModes,
          p99OperationMs: latencies.sort((a, b) => a - b)[Math.floor(latencies.length * 0.99)],
          limits: "domain fixture; no HTTP/provider/RDS/image qualification",
        })
      );
      const totals = Object.values(counters).reduce(
        (total, row) => ({
          offered: total.offered + row.offered,
          admitted: total.admitted + row.admitted,
          completed: total.completed + row.completed,
          rejected: total.rejected + row.productRejected,
          plannedThrottled: total.plannedThrottled + row.plannedThrottled,
          failed: total.failed + row.failed,
        }),
        { offered: 0, admitted: 0, completed: 0, rejected: 0, plannedThrottled: 0, failed: 0 }
      );
      console.log(
        "OMNIROUTE_RUNTIME_SOAK_RESULT " +
          JSON.stringify({
            schemaVersion: 1,
            status: "passed",
            durationRequestedMs: durationMs,
            measuredElapsedMs,
            historyRows,
            ...totals,
            maximumPhase,
            counters,
            actualOverlap,
            retentionDeleted,
            spendReceipts: spend.size,
            publicationModes,
            artifactCleanup: {
              version: 1,
              controlledDeferrals: controlledCleanupDeferrals,
              controlledHoldWarnings,
              measuredDeferrals: measuredCleanupDeferrals,
              lockTimeouts: cleanupLockTimeouts,
              pending: deferredCleanupRefs.size,
              drained: drainedCleanupDeferrals,
            },
            diagnostics: diagnostics.length,
            slowCommits,
            fatal: fatal.length,
            peakRss: maxRss,
            eventLoopP99Ms: loop.percentile(99) / 1e6,
            eventLoopMaxMs: loop.max / 1e6,
            scope: "native-domain-component",
            fixtureStorage: {
              version: 2,
              diskBudget,
              diskSamples,
              minimumFreeBytes,
              peakChunkBytes,
              maximumFileOffers,
              maximumLogicalFileBytes,
              provedFileDeletions,
              finalLogicalRows: 0,
              capacityClaim: "sampled disposable-volume containment; not RDS steady-state capacity",
            },
            externalCgroupRequired: { cpus: 2, memoryBytes: 4 * 1024 ** 3 },
          })
      );
    } finally {
      running = false;
      initialRelease.release();
      parentRelease.release();
      stopRelease.release();
      for (const effect of effects.values()) effect.gate?.release();
      await Promise.allSettled([
        ...active.values(),
        ...parents,
        ...(monitor ? [monitor] : []),
        ...(finalWork ? [finalWork] : []),
      ]);
      loop.disable();
      console.warn = originalWarn;
      await recovery.stopNativeArtifactRecovery();
      await runtime.closePersistenceRuntime();
      await observer?.close();
      for (const resource of clients) resource.emitDestroy();
      payload = undefined;
      objects.clear();
      effects.clear();
      putCounts.clear();
      hooks.deregister();
      delete globalThis.__omniroutePersistenceRuntime;
      for (const [name, value] of previous) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      await rm(directory, { recursive: true, force: true });
    }
  }
);
