import assert from "node:assert/strict";
import test from "node:test";
import {
  captureSqliteApplicationSnapshot,
  type ApplicationSnapshotTable,
  type ApplicationStorageSnapshot,
} from "../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  capturePostgresApplicationSnapshot,
  importApplicationSnapshot,
  verifyApplicationImport,
} from "../../src/lib/db/postgres/applicationImport.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { withSqliteVectorSource } from "../helpers/persistence/sqliteVecSourceFixture.ts";

interface SourceLineage {
  sourceMetadata: ApplicationSnapshotTable[];
  sourceDefinitions: ApplicationStorageSnapshot["definitions"];
  excluded: ApplicationStorageSnapshot["excluded"];
}

function historicalReceipt(metadata: ApplicationSnapshotTable[]): SourceLineage {
  const control = metadata.find((table) => table.name === "application_import_receipts")!;
  const record = JSON.parse(String(control.rows[0].values[0])) as {
    payload: SourceLineage;
  };
  return record.payload;
}

test(
  "real vector capture imports and restores logical data with original bytes, definitions and version lineage",
  { skip: !postgresTestsEnabled },
  async () => {
    const prior = process.env.STORAGE_ENCRYPTION_KEY;
    let database: Awaited<ReturnType<typeof createTestPostgresDatabase>> | undefined;
    try {
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-vector-import-integration-key";
      const snapshot = withSqliteVectorSource(
        {
          largeFiles: true,
          mutate(writer) {
            writer.exec(
              "UPDATE vec_memories_info SET value='v0.1.8' WHERE key='CREATE_VERSION'; UPDATE vec_memories_info SET value=8 WHERE key='CREATE_VERSION_PATCH'"
            );
          },
        },
        captureSqliteApplicationSnapshot
      );
      database = await createTestPostgresDatabase({
        transactionTimeoutMs: 300000,
      });
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      await migratePostgresSchema(database);
      const receipt = await importApplicationSnapshot(database, snapshot);
      await verifyApplicationImport(database, snapshot);
      const expected = [
        {
          memory_id: "first",
          signature: "fixture-signature",
          embedding: [1, 2],
        },
        {
          memory_id: "second",
          signature: "fixture-signature",
          embedding: [-0.5, 4.25],
        },
      ];
      const readVectors = () =>
        database!.transaction(
          async (tx) =>
            (
              await tx.query<(typeof expected)[number]>(
                "SELECT memory_id,signature,embedding FROM omniroute.memory_vectors ORDER BY memory_id"
              )
            ).rows
        );
      assert.deepEqual(await readVectors(), expected);
      const payload = await database.transaction(
        async (tx) =>
          (
            await tx.query<{ payload: SourceLineage }>(
              "SELECT payload FROM omniroute.application_import_receipts WHERE source_digest=$1",
              [receipt.sourceDigest]
            )
          ).rows[0].payload
      );
      assert.deepEqual(payload.sourceMetadata, snapshot.metadataTables);
      assert.deepEqual(payload.sourceDefinitions, snapshot.definitions);
      assert.deepEqual(payload.excluded, snapshot.excluded);
      const exported = await capturePostgresApplicationSnapshot(database);
      const original = historicalReceipt(exported.metadataTables);
      assert.deepEqual(original.sourceMetadata, snapshot.metadataTables);
      assert.deepEqual(original.sourceDefinitions, snapshot.definitions);
      await database.transaction((tx) => tx.query("DROP SCHEMA omniroute CASCADE"));
      await migratePostgresSchema(database);
      const restored = await importApplicationSnapshot(database, exported);
      await verifyApplicationImport(database, exported);
      assert.deepEqual(await readVectors(), expected);
      const restoredPayload = await database.transaction(
        async (tx) =>
          (
            await tx.query<{ payload: SourceLineage }>(
              "SELECT payload FROM omniroute.application_import_receipts WHERE source_digest=$1",
              [restored.sourceDigest]
            )
          ).rows[0].payload
      );
      const retained = historicalReceipt(restoredPayload.sourceMetadata);
      assert.deepEqual(retained.sourceMetadata, snapshot.metadataTables);
      assert.deepEqual(retained.sourceDefinitions, snapshot.definitions);
      assert.equal(
        retained.sourceMetadata.find((table) => table.name === "vec_memories_info")?.rows[0]
          .values[1],
        "v0.1.8"
      );
    } finally {
      try {
        await database?.close();
      } finally {
        if (prior === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
        else process.env.STORAGE_ENCRYPTION_KEY = prior;
      }
    }
  }
);
