import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import { withPostgresControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native tool-loop failure holds its scope until usage and attempt writes commit",
  {
    skip: !postgresTestsEnabled,
  },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "pg-finalization-logging-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
    };
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    const usageStarted = deferred(),
      usageGate = deferred();
    const attemptStarted = deferred(),
      attemptGate = deferred();
    const model = `logging-${randomUUID()}`;
    let settled = false,
      finalized = false,
      attemptRequested = false;
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-finalization-storage-secret";
      process.env.JWT_SECRET = "isolated-finalization-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-finalization-api-secret";
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 4 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      const execution = plane.run(async (binding) => {
        assert.ok(binding.database);
        const intercepted = new Proxy(binding.database, {
          get(target, property, receiver) {
            if (property !== "transaction") return Reflect.get(target, property, receiver);
            return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
              target.transaction((tx) =>
                body({
                  query: async <Row = Record<string, unknown>>(sql: string, values?: unknown[]) => {
                    if (sql.includes("INSERT INTO omniroute.usage_history")) {
                      usageStarted.resolve();
                      await usageGate.promise;
                    }
                    if (sql.includes("INSERT INTO omniroute.call_logs")) {
                      attemptStarted.resolve();
                      await attemptGate.promise;
                    }
                    return tx.query<Row>(sql, values);
                  },
                })
              );
          },
        });
        return withPostgresControlPlane({ ...binding, database: intercepted }, () =>
          withPostgresCatalogSnapshot(intercepted, async () => {
            const { finalizeToolLoopError } =
              await import("../../open-sse/handlers/chatCore/nonStreamingFinalization.ts");
            const { persistAttemptLogs } =
              await import("../../open-sse/handlers/chatCore/attemptLogging.ts");
            const { saveRequestUsage } = await import("../../src/lib/usage/usageHistory.ts");
            return finalizeToolLoopError({
              loop: {
                kind: "error",
                errorResult: {
                  success: false,
                  status: 429,
                  error: "rate limited",
                  errorCode: "rate_limited",
                  response: new Response(null, { status: 429 }),
                },
                cumulativeUsage: null,
                totalCostUsd: 0,
                receipts: [],
                followUps: 0,
                termination: "provider_error",
              },
              model,
              provider: "openai",
              persistFailureUsage: (status, errorCode) =>
                saveRequestUsage({
                  provider: "openai",
                  model,
                  tokens: { input: 0, output: 0 },
                  status: String(status),
                  success: false,
                  errorCode,
                  latencyMs: 0,
                  timeToFirstTokenMs: 0,
                  timestamp: new Date().toISOString(),
                }),
              persistAttemptLogs: (args) => {
                attemptRequested = true;
                return persistAttemptLogs(args, {
                  traceId: model,
                  provider: "openai",
                  model,
                  skillRequestId: model,
                  connectionId: null,
                  detailedLoggingEnabled: false,
                  reqLogger: null,
                  pendingRequestId: model,
                  clientRawRequest: null,
                  requestedModel: model,
                  credentials: null,
                  startTime: Date.now(),
                  body: null,
                  sourceFormat: "openai",
                  targetFormat: "openai",
                  comboName: null,
                  comboStepId: null,
                  comboExecutionKey: null,
                  tokensCompressed: 0,
                  apiKeyInfo: null,
                  noLogEnabled: true,
                  correlationId: model,
                });
              },
              trackPendingRequest: () => {
                finalized = true;
              },
            });
          })
        );
      });
      void execution.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        }
      );
      try {
        await Promise.race([
          usageStarted.promise,
          execution.then(() => {
            assert.fail("request scope ended before usage persistence reached its barrier");
          }),
        ]);
        assert.equal(settled, false);
        assert.equal(attemptRequested, false);
        usageGate.resolve();
        await Promise.race([
          attemptStarted.promise,
          execution.then(() => {
            assert.fail("request scope ended before attempt persistence reached its barrier");
          }),
        ]);
        assert.equal(settled, false);
        assert.equal(finalized, false);
        assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      } finally {
        usageGate.resolve();
        attemptGate.resolve();
      }
      const result = await execution;
      assert.equal(result.status, 429);
      assert.equal(finalized, true);
      await plane.run(async (binding) => {
        assert.ok(binding.database);
        const counts = await binding.database.transaction(async (tx) => {
          const usage = await tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.usage_history WHERE model=$1",
            [model]
          );
          const logs = await tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.call_logs WHERE correlation_id=$1",
            [model]
          );
          return [usage.rows[0].count, logs.rows[0].count];
        });
        assert.deepEqual(counts, ["1", "1"]);
      });
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      usageGate.resolve();
      attemptGate.resolve();
      await plane?.close();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
