import assert from "node:assert/strict";
import { AsyncResource } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { nativeRuntimeFixture } from "../helpers/persistence/runtimeNativeIntegrationFixture.ts";
import type { PersistenceTailPermit } from "../../src/lib/db/persistenceRuntime.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native shutdown persists final tail spend and private log metadata before closing",
  {
    skip: !postgresTestsEnabled,
    timeout: 30000,
  },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "omniroute-shutdown-accounting-"));
    const names = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "NODE_ENV",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "OMNIROUTE_SPEND_FLUSH_INTERVAL_MS",
      "OMNIROUTE_SPEND_MAX_BUFFER_SIZE",
    ];
    const previous = Object.fromEntries(names.map((key) => [key, process.env[key]]));
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const shutdownUrl = new URL("../../src/lib/startup/postgresShutdown.ts", import.meta.url).href;
    const nativeFixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const producersUrl = new URL(
      "../helpers/persistence/shutdownOrderingFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
        if (
          context.parentURL?.split("?")[0] === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: nativeFixtureUrl, shortCircuit: true };
        if (
          context.parentURL?.split("?")[0] === shutdownUrl &&
          ![
            "@/lib/db/persistenceRuntime",
            "./postgresBootstrap",
            "@/lib/spend/batchWriter",
            "@/lib/usage/callLogs",
          ].includes(specifier)
        )
          return { url: producersUrl, shortCircuit: true };
        return nextResolve(specifier, context);
      },
    });
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.DATA_DIR = directory;
    process.env.NODE_ENV = "test";
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.OMNIROUTE_SPEND_FLUSH_INTERVAL_MS = "3600000";
    process.env.OMNIROUTE_SPEND_MAX_BUFFER_SIZE = "1000";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    const id = randomUUID(),
      callId = randomUUID();
    const ownerSymbol = Symbol.for("omniroute.postgresStartupOwner");
    const ownerGlobals = globalThis as typeof globalThis & { [ownerSymbol]?: unknown };
    const external = new AsyncResource("native-shutdown-late-accounting");
    const entered = barrier(),
      complete = barrier();
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let permit: PersistenceTailPermit | undefined;
    let tail: Promise<void> | undefined;
    let closing: Promise<void> | undefined;
    try {
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      assert.equal(ownerGlobals[ownerSymbol], undefined);
      nativeRuntimeFixture.artifactsClosed = 0;
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      const cost = await import("../../src/domain/costRules.ts");
      const logs = await import("../../src/lib/usage/callLogs.ts");
      const { getSelectedPostgresDatabase } =
        await import("../../src/lib/db/controlPlaneContext.ts");
      const { spendBatchWriter } = await import("../../src/lib/spend/batchWriter.ts");
      await runtime.runPersistenceRequest(async () => {
        await getSelectedPostgresDatabase()!.transaction((tx) =>
          tx.query(
            "INSERT INTO omniroute.api_keys(id,name,key,created_at,no_log) VALUES($1,'shutdown accounting',$2,$3,1)",
            [id, randomUUID(), new Date().toISOString()]
          )
        );
        permit = await runtime!.acquirePersistenceTail();
      });
      tail = external.runInAsyncScope(() =>
        permit!.run(async () => {
          entered.resolve();
          await complete.promise;
          await cost.recordCostAsync(id, 2.5, {
            provider: "shutdown-provider",
            model: "shutdown-model",
            tokens: {},
            requestId: callId,
          });
          assert.equal(spendBatchWriter.getPendingCostTotal(id), 2.5);
          await logs.saveCallLog({
            id: callId,
            apiKeyId: id,
            apiKeyName: "shutdown accounting",
            noLog: true,
            model: "shutdown-model",
            provider: "shutdown-provider",
            status: 200,
            tokens: { input_tokens: 7, output_tokens: 3 },
            correlationId: callId,
            requestBody: { messages: [{ role: "user", content: "must not be stored" }] },
            responseBody: { choices: [{ message: { content: "private response" } }] },
          });
        })
      );
      void tail.catch(() => {});
      await entered.promise;
      const shutdown = await import("../../src/lib/startup/postgresShutdown.ts");
      let closed = false;
      closing = shutdown.shutdownPostgresApplication().then(() => {
        closed = true;
      });
      void closing.catch(() => {});
      assert.equal(runtime.getPersistenceRuntimeStatus().phase, "closing");
      await assert.rejects(
        runtime.runPersistenceRequest(async () => {}),
        /unavailable|draining/
      );
      await new Promise<void>((done) => setImmediate(done));
      assert.equal(closed, false);
      assert.equal(nativeRuntimeFixture.plane?.state, "ready");
      assert.equal(nativeRuntimeFixture.artifactsClosed, 0);
      complete.resolve();
      await tail;
      await permit!.release();
      await closing;
      assert.equal(nativeRuntimeFixture.plane?.state, "closed");
      assert.equal(nativeRuntimeFixture.artifactsClosed, 1);
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      const verification = await createTestPostgresDatabase();
      try {
        const result = await verification.transaction(async (tx) => {
          const spend = await tx.query<{ total: number; count: string }>(
            "SELECT COALESCE(SUM(cost),0) AS total,COUNT(*)::text AS count FROM omniroute.domain_cost_history WHERE api_key_id=$1",
            [id]
          );
          const receipt = await tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.domain_cost_receipts WHERE api_key_id=$1",
            [id]
          );
          const ledger = await tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.request_cost_ledger WHERE api_key_id=$1 AND request_id=$2",
            [id, callId]
          );
          const log = await tx.query<{
            detail_state: string;
            artifact_relpath: string | null;
            has_request_body: number;
            has_response_body: number;
            tokens_in: number;
            tokens_out: number;
          }>(
            "SELECT detail_state,artifact_relpath,has_request_body,has_response_body,tokens_in,tokens_out FROM omniroute.call_logs WHERE id=$1 AND api_key_id=$2",
            [callId, id]
          );
          return {
            spend: spend.rows[0],
            receipt: receipt.rows[0],
            ledger: ledger.rows[0],
            log: log.rows,
          };
        });
        assert.equal(Number(result.spend.total), 2.5);
        assert.equal(result.spend.count, "1");
        assert.equal(result.receipt.count, "1");
        assert.equal(result.ledger.count, "1");
        assert.equal(result.log.length, 1);
        assert.equal(result.log[0].detail_state, "none");
        assert.equal(result.log[0].artifact_relpath, null);
        assert.equal(result.log[0].has_request_body, 0);
        assert.equal(result.log[0].has_response_body, 0);
        assert.equal(Number(result.log[0].tokens_in), 7);
        assert.equal(Number(result.log[0].tokens_out), 3);
      } finally {
        await verification.close();
      }
    } finally {
      complete.resolve();
      await tail?.catch(() => {});
      await permit?.release().catch(() => {});
      await closing?.catch(() => {});
      await runtime?.closePersistenceRuntime().catch(() => {});
      hooks.deregister();
      external.emitDestroy();
      try {
        const cleanup = await createTestPostgresDatabase();
        try {
          await cleanup.transaction(async (tx) => {
            await tx.query("DELETE FROM omniroute.call_logs WHERE id=$1", [callId]);
            await tx.query("DELETE FROM omniroute.request_cost_ledger WHERE api_key_id=$1", [id]);
            await tx.query("DELETE FROM omniroute.domain_cost_receipts WHERE api_key_id=$1", [id]);
            await tx.query("DELETE FROM omniroute.domain_cost_history WHERE api_key_id=$1", [id]);
            await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [id]);
          });
        } finally {
          await cleanup.close();
        }
      } finally {
        delete globalThis.__omniroutePersistenceRuntime;
        delete ownerGlobals[ownerSymbol];
        for (const [name, value] of Object.entries(previous)) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
        rmSync(directory, { recursive: true, force: true });
      }
    }
  }
);
