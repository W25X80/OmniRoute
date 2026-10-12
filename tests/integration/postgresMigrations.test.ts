import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  migratePostgresSchema,
  PostgresMigrationError,
} from "../../src/lib/db/postgres/migrations.ts";

test(
  "PostgreSQL migration history is atomic, serialized, immutable and version checked",
  { skip: !postgresTestsEnabled },
  async () => {
    // This case proves first-install serialization, not the inference lock budget.
    // Give the contender a finite startup budget while another connection creates all tables.
    const startupBudget = {
      lockTimeoutMs: 10_000,
      statementTimeoutMs: 12_000,
      queryTimeoutMs: 13_000,
      transactionTimeoutMs: 30_000,
    };
    const first = await createTestPostgresDatabase(startupBudget);
    const second = await createTestPostgresDatabase(startupBudget);
    try {
      await first.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await first.transaction((tx) => tx.query("CREATE SCHEMA omniroute"));
      await first.transaction((tx) =>
        tx.query("CREATE TABLE omniroute.model_combo_mappings(block integer)")
      );
      await assert.rejects(migratePostgresSchema(first));
      const { rows: rejected } = await first.transaction((tx) =>
        tx.query(
          "SELECT to_regclass('omniroute.schema_migrations') AS history, to_regclass('omniroute.combos') AS combos"
        )
      );
      assert.deepEqual(rejected, [{ history: null, combos: null }]);
      await first.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      await Promise.all([migratePostgresSchema(first), migratePostgresSchema(second)]);
      await migratePostgresSchema(first);
      const { rows } = await first.transaction((tx) =>
        tx.query(
          "SELECT version,name,length(checksum) AS digest_length FROM omniroute.schema_migrations ORDER BY version"
        )
      );
      assert.deepEqual(rows, [
        { version: 1, name: "routing_repositories", digest_length: 64 },
        { version: 2, name: "control_plane", digest_length: 64 },
        { version: 3, name: "server_persistence", digest_length: 64 },
        { version: 4, name: "file_content_chunks", digest_length: 64 },
      ]);
      await first.transaction((tx) =>
        tx.query("UPDATE omniroute.schema_migrations SET checksum = 'modified'")
      );
      await assert.rejects(
        migratePostgresSchema(first),
        (error: PostgresMigrationError) => error.reason === "history_mismatch"
      );
      await first.transaction((tx) => tx.query("DELETE FROM omniroute.schema_migrations"));
      await first.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.schema_migrations VALUES (999,'future','digest',CURRENT_TIMESTAMP)"
        )
      );
      await assert.rejects(migratePostgresSchema(first), PostgresMigrationError);
      await first.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      await migratePostgresSchema(first);
      await first.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.schema_migrations VALUES (999,'future','digest',CURRENT_TIMESTAMP)"
        )
      );
      await assert.rejects(
        migratePostgresSchema(first),
        (error: PostgresMigrationError) => error.reason === "newer_schema"
      );
    } finally {
      await first.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await first.close();
      await second.close();
    }
  }
);
