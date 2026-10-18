import assert from "node:assert/strict";
import test from "node:test";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
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
      const cli = await promisify(execFile)(
        process.execPath,
        ["--import", "tsx/esm", "scripts/ops/postgres-migrate-schema.ts", "--offline"],
        {
          cwd: fileURLToPath(new URL("../../", import.meta.url)),
          env: {
            ...process.env,
            OMNIROUTE_POSTGRES_HOST: "localhost",
            OMNIROUTE_POSTGRES_PORT: process.env.OMNIROUTE_TEST_POSTGRES_PORT,
            OMNIROUTE_POSTGRES_DATABASE: "omniroute_test",
            OMNIROUTE_POSTGRES_USER: "omniroute_test",
            OMNIROUTE_POSTGRES_PASSWORD: "omniroute_test",
            OMNIROUTE_POSTGRES_TLS: "verify-full",
            OMNIROUTE_POSTGRES_CA_FILE: process.env.OMNIROUTE_TEST_POSTGRES_CA,
          },
          timeout: 30_000,
        }
      );
      assert.match(cli.stdout, /schema and startup seed are current/);
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
        { version: 5, name: "artifact_publication_ownership", digest_length: 64 },
        { version: 6, name: "fractional_compression_durations", digest_length: 64 },
        { version: 7, name: "indexed_conversation_retention", digest_length: 64 },
        { version: 8, name: "durable_artifact_recovery", digest_length: 64 },
        { version: 9, name: "conversation_concurrency", digest_length: 64 },
        { version: 10, name: "indexed_artifact_recovery_retirement", digest_length: 64 },
      ]);
      // Commit the genuine migration prefix, stopping only after successful version-4 SQL.
      await first.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      const stopAtVersion4 = new Error("Test migration prefix complete");
      let prefixReached = false;
      const version4Database = {
        transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
          return first.transaction(async (tx) => {
            try {
              return await body({
                async query<Row>(sql: string, values?: unknown[]) {
                  const result = await tx.query<Row>(sql, values);
                  if (
                    sql ===
                      "INSERT INTO omniroute.schema_migrations(version,name,checksum) VALUES($1,$2,$3)" &&
                    values?.[0] === 4
                  ) {
                    prefixReached = true;
                    throw stopAtVersion4;
                  }
                  return result;
                },
              });
            } catch (error) {
              if (error !== stopAtVersion4) throw error;
              return undefined as T;
            }
          });
        },
      } as PostgresDatabase;
      await migratePostgresSchema(version4Database);
      assert.equal(prefixReached, true);
      const history = async () =>
        (
          await first.transaction((tx) =>
            tx.query(
              "SELECT version,name,checksum,applied_at::text AS applied_at FROM omniroute.schema_migrations ORDER BY version"
            )
          )
        ).rows;
      const version4History = await history();
      assert.deepEqual(
        version4History.map((row) => row.version),
        [1, 2, 3, 4]
      );
      const absent = await first.transaction((tx) =>
        tx.query(
          "SELECT column_name FROM information_schema.columns WHERE table_schema='omniroute' AND table_name='artifact_deletion_queue' AND column_name='publication_state'"
        )
      );
      assert.deepEqual(absent.rows, []);
      const legacyReference = "legacy/migration-v4.json";
      await first.transaction((tx) =>
        tx.query(
          "INSERT INTO omniroute.artifact_deletion_queue(relpath,created_at) VALUES($1,'2026-01-01T00:00:00Z')",
          [legacyReference]
        )
      );
      const oldQueue = async () =>
        (
          await first.transaction((tx) =>
            tx.query(
              "SELECT relpath,created_at::text AS created_at FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
              [legacyReference]
            )
          )
        ).rows;
      const version4Queue = await oldQueue();
      await Promise.all([migratePostgresSchema(first), migratePostgresSchema(second)]);
      const upgradedHistory = await history();
      assert.deepEqual(upgradedHistory.slice(0, 4), version4History);
      assert.deepEqual(
        upgradedHistory.map((row) => row.version),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      );
      assert.equal(upgradedHistory[4].name, "artifact_publication_ownership");
      assert.deepEqual(await oldQueue(), version4Queue);
      const migratedQueue = await first.transaction((tx) =>
        tx.query(
          "SELECT publication_state,upload_state,publication_owner,store_scope,expected_size_bytes,expected_sha256 FROM omniroute.artifact_deletion_queue WHERE relpath=$1",
          [legacyReference]
        )
      );
      assert.deepEqual(migratedQueue.rows, [
        {
          publication_state: "delete_pending",
          upload_state: "acknowledged",
          publication_owner: null,
          store_scope: null,
          expected_size_bytes: null,
          expected_sha256: null,
        },
      ]);
      await migratePostgresSchema(first);
      assert.deepEqual(
        await history(),
        upgradedHistory,
        "repeat upgrade preserves checksum and applied_at for every version"
      );
      assert.deepEqual(await oldQueue(), version4Queue);
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
