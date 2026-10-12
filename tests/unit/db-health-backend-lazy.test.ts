import assert from "node:assert/strict";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import {
  PersistenceCapabilityError,
  persistenceCapabilityResponse,
} from "../../src/lib/db/persistenceCapabilities.ts";
import { runManagedDatabaseHealthCheck } from "../../src/lib/db/healthCheck.ts";

function selected<T>(database: PostgresDatabase, body: () => Promise<T>): Promise<T> {
  const binding = {
    database,
    databaseIdentity: {},
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  return withPostgresControlPlane(binding, body);
}

test("selected native health repair keeps the capability response before any SQL", async () => {
  let transactions = 0;
  const database = {
    async transaction() {
      transactions++;
      throw new Error("Native repair must reject before transaction admission");
    },
  } as unknown as PostgresDatabase;
  await assert.rejects(
    selected(database, () => runManagedDatabaseHealthCheck({ autoRepair: true })),
    (error: unknown) => {
      assert.ok(error instanceof PersistenceCapabilityError);
      assert.equal(error.status, 501);
      assert.deepEqual(persistenceCapabilityResponse(error), {
        error: {
          message:
            "PersistenceCapabilityError: The PostgreSQL backend does not support sqlite-integrity-repair",
          code: "persistence_capability_unavailable",
          backend: "postgresql",
          capability: "sqlite-integrity-repair",
        },
      });
      return true;
    }
  );
  assert.equal(transactions, 0);
});

test("selected native health diagnosis uses the native transaction and preserves its projection", async () => {
  const queries: string[] = [];
  const transaction = {
    async query(text: string) {
      queries.push(text);
      if (text === "SELECT 1") return { rows: [], rowCount: 0, command: "SELECT" };
      assert.match(text, /FROM omniroute\.quota_snapshots/);
      return {
        rows: [{ table_name: "quota_snapshots", count: "2" }],
        rowCount: 1,
        command: "SELECT",
      };
    },
  } as unknown as PostgresTransaction;
  const database = {
    transaction: <T>(body: (value: PostgresTransaction) => Promise<T>) => body(transaction),
  } as unknown as PostgresDatabase;
  const result = await selected(database, () =>
    runManagedDatabaseHealthCheck({ autoRepair: false, skipIntegrityCheck: true })
  );
  assert.equal(queries.length, 2);
  assert.deepEqual(
    { ...result, checkedAt: "observed" },
    {
      isHealthy: false,
      issues: [
        {
          type: "broken_reference",
          table: "quota_snapshots",
          description: "Rows reference a missing connection or API key",
          count: 2,
        },
      ],
      repairedCount: 0,
      backupCreated: false,
      autoRepair: false,
      checkedAt: "observed",
      driver: { name: "postgresql", degraded: false },
      integrityScope: "connectivity-and-application-references",
    }
  );
  assert.ok(!Number.isNaN(Date.parse(result.checkedAt)));
});
