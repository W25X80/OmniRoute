import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import type { DiscoveryResult } from "../../src/lib/db/discovery.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { POSTGRES_CATALOG_SCHEMA_STATEMENTS } from "../../src/lib/db/postgres/catalogSchema.ts";
import { POSTGRES_SERVICE_SCHEMA_STATEMENTS } from "../../src/lib/db/postgres/serviceSchema.ts";
import * as discovery from "../../src/lib/db/postgres/serviceOperations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
const options = { skip: !postgresTestsEnabled };
test.before(async () => {
  if (!postgresTestsEnabled) return;
  database = await createTestPostgresDatabase();
  await migratePostgresSchema(database);
  await database.transaction(async (transaction) => {
    for (const statement of POSTGRES_CATALOG_SCHEMA_STATEMENTS) await transaction.query(statement);
    for (const statement of POSTGRES_SERVICE_SCHEMA_STATEMENTS) await transaction.query(statement);
  });
});
test.after(async () => {
  if (database) await database.close();
});

function result(feasibility?: number): DiscoveryResult {
  return {
    providerId: `discovery-test-${randomUUID()}`,
    method: "public_api",
    authType: "none",
    endpoint: "https://discovery.example/v1",
    modelsJson: '[ "model-a", "model-b" ]',
    feasibility,
  };
}

test(
  "discovery preserves physical models bytes through its public modelsJson API",
  options,
  async () => {
    const input = result(5);
    const id = await database.transaction((transaction) =>
      discovery.insertDiscoveryResult(transaction, input)
    );
    try {
      const stored = await database.transaction((transaction) =>
        discovery.getDiscoveryResultById(transaction, id)
      );
      assert.equal(stored?.modelsJson, input.modelsJson);
      assert.equal(stored?.feasibility, 5);
      const listed = await database.transaction((transaction) =>
        discovery.listDiscoveryResults(transaction)
      );
      assert.equal(listed.find((row) => row.id === id)?.modelsJson, input.modelsJson);
      const updated = await database.transaction((transaction) =>
        discovery.updateDiscoveryStatus(transaction, id, "verified", "verified fixture")
      );
      assert.equal(updated, true);
      const verified = await database.transaction((transaction) =>
        discovery.getDiscoveryResultById(transaction, id)
      );
      assert.equal(verified?.notes, "verified fixture");
      assert.ok(verified?.verifiedAt);
    } finally {
      await database.transaction((transaction) => discovery.deleteDiscoveryResult(transaction, id));
    }
  }
);

test(
  "discovery honors source feasibility bounds without accepting the invalid zero default",
  options,
  async () => {
    const id = await database.transaction((transaction) =>
      discovery.insertDiscoveryResult(transaction, result(1))
    );
    await database.transaction((transaction) => discovery.deleteDiscoveryResult(transaction, id));
    for (const feasibility of [undefined, 0, 6]) {
      await assert.rejects(
        database.transaction((transaction) =>
          discovery.insertDiscoveryResult(transaction, result(feasibility))
        ),
        { sqlState: "23514" }
      );
    }
  }
);
