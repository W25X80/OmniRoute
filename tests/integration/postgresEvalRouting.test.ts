import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native eval-driven routing ranks persisted history without SQLite",
  {
    skip: !postgresTestsEnabled,
  },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "pg-eval-routing-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
    };
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-eval-routing-storage-secret";
      process.env.JWT_SECRET = "isolated-eval-routing-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-eval-routing-api-secret";
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 4 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await plane.run(async () => {
        const { saveEvalRunAsync } = await import("../../src/lib/db/evals.ts");
        const { orderTargetsByEvalScoresAsync, resetEvalRoutingCache } =
          await import("../../open-sse/services/evalRouting.ts");
        const suite = randomUUID(),
          weakModel = `weak-${randomUUID()}`,
          strongModel = `strong-${randomUUID()}`;
        for (const [id, passed] of [
          [weakModel, 0],
          [strongModel, 10],
        ] as const) {
          await saveEvalRunAsync({
            suiteId: suite,
            suiteName: "Native routing test",
            target: { type: "model", id, label: id },
            summary: { total: 10, passed, failed: 10 - passed, passRate: passed * 10 },
            avgLatencyMs: 100,
            results: [],
          });
        }
        const targets = [
          { modelStr: `openai/${weakModel}` },
          { modelStr: `openai/${strongModel}` },
        ];
        const enabled = { enabled: true, suiteIds: [suite], qualityWeight: 1, latencyWeight: 0 };
        resetEvalRoutingCache();
        assert.strictEqual(
          await orderTargetsByEvalScoresAsync(targets, { enabled: false }),
          targets
        );
        assert.deepEqual(await orderTargetsByEvalScoresAsync(targets, enabled), [
          targets[1],
          targets[0],
        ]);
        assert.deepEqual(await orderTargetsByEvalScoresAsync(targets, enabled), [
          targets[1],
          targets[0],
        ]);
        assert.equal(existsSync(join(directory, "storage.sqlite")), false);
        resetEvalRoutingCache();
      });
    } finally {
      await plane?.close();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
