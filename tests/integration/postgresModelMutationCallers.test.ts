import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native public model mutations commit before acknowledgement and never open SQLite",
  { skip: !postgresTestsEnabled, timeout: 60_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "model-mutation-callers-postgres-"));
    const environmentNames = [
      "DATA_DIR",
      "OMNIROUTE_DATABASE_BACKEND",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "STORAGE_ENCRYPTION_KEY",
    ];
    const previous = new Map(environmentNames.map((name) => [name, process.env[name]]));
    process.env.DATA_DIR = directory;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.JWT_SECRET = "test-only-model-mutation-jwt-secret";
    process.env.API_KEY_SECRET = "test-only-model-mutation-api-key-secret";
    process.env.STORAGE_ENCRYPTION_KEY = "test-only-model-mutation-encryption-key";
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { withPostgresCatalogSnapshot } =
      await import("../../src/lib/db/postgres/catalogSnapshot.ts");
    const { runModelMutationCallerBehavior } =
      await import("../helpers/persistence/modelMutationCallerBehavior.ts");
    const database = await createTestPostgresDatabase();
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    let originalRows: Array<{ namespace: string; key: string; value: string }> | undefined;
    const prefix = `l65-${randomUUID()}`;
    const fixtureRowPredicate = `(namespace IN ('customModels','modelCompatOverrides') AND key IN ('openai','codex','deepseek-web')) OR (namespace='syncedAvailableModels' AND (key LIKE 'openai:%' OR key LIKE 'codex:%'))`;
    try {
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      originalRows = await database.transaction(async (tx) => {
        await tx.query("SELECT pg_advisory_xact_lock(18765,18)");
        await tx.query("SELECT pg_advisory_xact_lock(18765,20)");
        await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
        const { rows } = await tx.query<{ namespace: string; key: string; value: string }>(
          `SELECT namespace,key,value FROM omniroute.key_value WHERE ${fixtureRowPredicate}`
        );
        await tx.query(`DELETE FROM omniroute.key_value WHERE ${fixtureRowPredicate}`);
        await tx.query(
          "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
        );
        return rows;
      });
      await runModelMutationCallerBehavior(t, {
        prefix,
        selected: (body) =>
          plane!.run((binding) => withPostgresCatalogSnapshot(binding.database!, body)),
        async readDurable(namespace, key) {
          // Independent pool read observes committed rows, not the caller's prepared snapshot.
          const { rows } = await database.transaction((tx) =>
            tx.query<{ value: string }>(
              "SELECT value FROM omniroute.key_value WHERE namespace=$1 AND key=$2",
              [namespace, key]
            )
          );
          return rows[0]?.value;
        },
      });
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      assert.equal(existsSync(join(directory, "storage.sqlite-wal")), false);
      assert.equal(existsSync(join(directory, "storage.sqlite-shm")), false);
    } finally {
      try {
        if (originalRows) {
          const rowsToRestore = originalRows;
          await database.transaction(async (tx) => {
            await tx.query("SELECT pg_advisory_xact_lock(18765,4)");
            await tx.query("SELECT pg_advisory_xact_lock(18765,18)");
            await tx.query("SELECT pg_advisory_xact_lock(18765,20)");
            await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
            await tx.query(`DELETE FROM omniroute.key_value WHERE ${fixtureRowPredicate}`);
            for (const row of rowsToRestore) {
              await tx.query(
                "INSERT INTO omniroute.key_value(namespace,key,value) VALUES($1,$2,$3)",
                [row.namespace, row.key, row.value]
              );
            }
            await tx.query(
              "DELETE FROM omniroute.provider_connections WHERE name=ANY($1::text[])",
              [[`${prefix}-codex-account`, `${prefix}-web-account`]]
            );
            await tx.query(
              "UPDATE omniroute.api_key_policy_epoch SET generation=generation+1 WHERE singleton"
            );
          });
        }
      } finally {
        await plane?.close();
        await database.close();
        rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
        for (const [name, value] of previous) {
          if (value === undefined) delete process.env[name];
          else process.env[name] = value;
        }
      }
    }
  }
);
