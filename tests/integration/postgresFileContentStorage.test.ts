import assert from "node:assert/strict";
import { createHash, randomBytes, randomFillSync } from "node:crypto";
import { performance } from "node:perf_hooks";
import test from "node:test";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { isPostgresErrorKind } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { NATIVE_BLOB_LIMITS } from "../../src/lib/db/postgres/nativeBlobCarrier.ts";
import {
  openStoredFileContent,
  replaceStoredFileContent,
  getStoredFileContentBounds,
} from "../../src/lib/db/postgres/serviceFileContent.ts";
import * as files from "../../src/lib/db/postgres/serviceFiles.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
const options = { skip: !postgresTestsEnabled };
const sha = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
test.before(async () => {
  if (!postgresTestsEnabled) return;
  database = await createTestPostgresDatabase();
  await migratePostgresSchema(database);
});
test.after(async () => {
  await database?.close();
});
async function remove(id: string) {
  await database.transaction((tx) => tx.query("DELETE FROM omniroute.files WHERE id=$1", [id]));
}

test(
  "native mapped content preserves incompressible bytes and source metadata",
  options,
  async () => {
    const input = randomBytes(NATIVE_BLOB_LIMITS.chunkBytes + 113);
    const file = await files.createFile(database, {
      bytes: 41,
      filename: "content.bin",
      purpose: "batch",
      content: input,
    });
    try {
      assert.equal(file.bytes, 41);
      assert.deepEqual(await files.getFileContent(database, file.id), input);
      const shape = await database.transaction(async (tx) => ({
        parent: (
          await tx.query<{ bytes: string; inline_length: number | null }>(
            "SELECT bytes,octet_length(content) AS inline_length FROM omniroute.files WHERE id=$1",
            [file.id]
          )
        ).rows[0],
        state: (
          await tx.query<{ size_bytes: string; chunk_count: number; checksum: string }>(
            "SELECT size_bytes,chunk_count,checksum FROM omniroute.files_content_state WHERE file_id=$1",
            [file.id]
          )
        ).rows[0],
        chunks: (
          await tx.query<{ chunk_index: number; size: number }>(
            "SELECT chunk_index,octet_length(content) AS size FROM omniroute.files_content_chunks WHERE file_id=$1 ORDER BY chunk_index",
            [file.id]
          )
        ).rows,
      }));
      assert.equal(Number(shape.parent.bytes), 41);
      assert.equal(shape.parent.inline_length, null);
      assert.equal(Number(shape.state.size_bytes), input.length);
      assert.equal(shape.state.chunk_count, 2);
      assert.equal(shape.state.checksum, sha(input));
      assert.deepEqual(shape.chunks, [
        { chunk_index: 0, size: NATIVE_BLOB_LIMITS.chunkBytes },
        { chunk_index: 1, size: 113 },
      ]);
    } finally {
      await remove(file.id);
    }
  }
);
test(
  "native NULL and empty markers remain distinct; owner delete clears only authorized content",
  options,
  async () => {
    for (const content of [null, Buffer.alloc(0)]) {
      const file = await files.createFile(database, {
        bytes: 0,
        filename: "empty",
        purpose: "batch",
        content,
        apiKeyId: "owner",
      });
      try {
        assert.deepEqual(await files.getFileContent(database, file.id), content);
        assert.equal(
          await database.transaction((tx) => files.deleteFileOwnedBy(tx, file.id, "foreign")),
          false
        );
        assert.deepEqual(await files.getFileContent(database, file.id), content);
        assert.equal(
          await database.transaction((tx) => files.deleteFileOwnedBy(tx, file.id, "owner")),
          true
        );
        assert.equal(await files.getFileContent(database, file.id), null);
        assert.equal(
          (
            await database.transaction((tx) =>
              tx.query<{ count: string }>(
                "SELECT COUNT(*)::text AS count FROM omniroute.files_content_state WHERE file_id=$1",
                [file.id]
              )
            )
          ).rows[0].count,
          "0"
        );
      } finally {
        await remove(file.id);
      }
    }
  }
);
test(
  "deferred marker integrity rejects orphan chunk COMMIT and rolls back parent metadata",
  options,
  async () => {
    const id = "orphan-content-fixture";
    await assert.rejects(
      database.transaction(async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.files(id,bytes,created_at,filename,purpose,content) VALUES($1,0,0,'orphan','batch',NULL)",
          [id]
        );
        await tx.query(
          "INSERT INTO omniroute.files_content_chunks(file_id,chunk_index,content,checksum) VALUES($1,0,$2,$3)",
          [id, Buffer.from([1]), sha(Buffer.from([1]))]
        );
      }),
      (error) => isPostgresErrorKind(error, "transaction_failed") && error.sqlState === "23503"
    );
    assert.equal(
      (
        await database.transaction((tx) =>
          tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.files WHERE id=$1",
            [id]
          )
        )
      ).rows[0].count,
      "0"
    );
    assert.equal(
      (
        await database.transaction((tx) =>
          tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.files_content_chunks WHERE file_id=$1",
            [id]
          )
        )
      ).rows[0].count,
      "0"
    );
  }
);
test("legacy deleted inline bytes remain portable and replacement is atomic", options, async () => {
  const id = "legacy-inline-content-fixture",
    input = randomBytes(113);
  await database.transaction((tx) =>
    tx.query(
      "INSERT INTO omniroute.files(id,bytes,created_at,filename,purpose,content,deleted_at) VALUES($1,41,0,'legacy','batch',$2,123)",
      [id, input]
    )
  );
  try {
    assert.equal(await files.getFileContent(database, id), null);
    await database.transaction(async (tx) => {
      const stored = await openStoredFileContent(tx, id);
      assert.ok(stored);
      assert.equal(stored.sizeBytes, input.length);
      assert.equal(stored.checksum, null);
      assert.deepEqual(await stored.readChunk(0), input);
      await replaceStoredFileContent(tx, id, input);
    });
    const row = (
      await database.transaction((tx) =>
        tx.query<{ bytes: string; deleted_at: string; inline_length: number | null }>(
          "SELECT bytes,deleted_at,octet_length(content) AS inline_length FROM omniroute.files WHERE id=$1",
          [id]
        )
      )
    ).rows[0];
    assert.equal(Number(row.bytes), 41);
    assert.equal(Number(row.deleted_at), 123);
    assert.equal(row.inline_length, null);
    await database.transaction(async (tx) => {
      const stored = await openStoredFileContent(tx, id);
      assert.ok(stored);
      assert.equal(stored.checksum, sha(input));
      assert.deepEqual(await stored.readChunk(0), input);
    });
  } finally {
    await remove(id);
  }
});
test(
  "stored chunk corruption fails closed and expiry clears content with source count",
  options,
  async () => {
    const input = Buffer.from([0, 255, 19]),
      file = await files.createFile(database, {
        bytes: 3,
        filename: "corrupt",
        purpose: "batch_output",
        content: input,
        expiresAt: 10,
      });
    try {
      await database.transaction((tx) =>
        tx.query("UPDATE omniroute.files_content_chunks SET checksum=$1 WHERE file_id=$2", [
          "0".repeat(64),
          file.id,
        ])
      );
      await assert.rejects(files.getFileContent(database, file.id), /incomplete or invalid/);
      assert.equal(await database.transaction((tx) => files.pruneExpiredFiles(tx, 20)), 1);
      assert.equal(await files.getFileContent(database, file.id), null);
      assert.equal(
        (
          await database.transaction((tx) =>
            tx.query<{ count: string }>(
              "SELECT COUNT(*)::text AS count FROM omniroute.files_content_chunks WHERE file_id=$1",
              [file.id]
            )
          )
        ).rows[0].count,
        "0"
      );
      await database.transaction((tx) => getStoredFileContentBounds(tx));
    } finally {
      await remove(file.id);
    }
  }
);
test(
  "partial bounded content replacement failure rolls back all prior chunks",
  options,
  async () => {
    const input = Buffer.from([1, 2, 3]),
      file = await files.createFile(database, {
        bytes: 3,
        filename: "rollback",
        purpose: "batch",
        content: input,
      });
    try {
      const failure = new Error("controlled partial write failure");
      await assert.rejects(
        database.transaction(async (tx) => {
          const observed: PostgresTransaction = {
            async query<Row>(text: string, values?: unknown[]) {
              const result = await tx.query<Row>(text, values);
              if (text.includes("INSERT INTO omniroute.files_content_chunks")) throw failure;
              return result;
            },
          };
          await replaceStoredFileContent(
            observed,
            file.id,
            Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 17, 8)
          );
        }),
        (error) => error === failure
      );
      assert.deepEqual(await files.getFileContent(database, file.id), input);
    } finally {
      await remove(file.id);
    }
  }
);
test("a complete mapped reader holds the parent lock until settlement", options, async (t) => {
  const input = Buffer.from([0, 255, 19, 73]);
  const file = await files.createFile(database, {
    bytes: input.length,
    filename: "locked.bin",
    purpose: "batch",
    content: input,
    apiKeyId: "lock-owner",
  });
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let locked!: () => void;
  let failed!: (reason: unknown) => void;
  const hasLock = new Promise<void>((resolve, reject) => {
    locked = resolve;
    failed = reject;
  });
  let reader: Promise<Buffer | null> | undefined;
  t.after(() => release());
  try {
    reader = database.transaction(async (tx) => {
      const stored = await openStoredFileContent(tx, file.id);
      assert.ok(stored);
      locked();
      await gate;
      return stored.readChunk(0);
    });
    void reader.catch(failed);
    await hasLock;
    await assert.rejects(
      database.transaction((tx) => files.deleteFileOwnedBy(tx, file.id, "lock-owner")),
      (error) => isPostgresErrorKind(error, "query_failed") && error.sqlState === "55P03"
    );
    const preserved = await database.transaction((tx) => files.getFile(tx, file.id));
    assert.ok(preserved);
    assert.equal(preserved.deletedAt, null);
    assert.deepEqual(await files.getFileContent(database, file.id), input);
    release();
    assert.deepEqual(await reader, input);
    assert.equal(
      await database.transaction((tx) => files.deleteFileOwnedBy(tx, file.id, "lock-owner")),
      true
    );
    assert.equal(await files.getFileContent(database, file.id), null);
    assert.equal(
      (
        await database.transaction((tx) =>
          tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.files_content_chunks WHERE file_id = $1",
            [file.id]
          )
        )
      ).rows[0].count,
      "0"
    );
  } finally {
    release();
    await Promise.allSettled(reader ? [reader] : []);
    await remove(file.id);
  }
});

test(
  "actual 512 MiB native create and read preserve exact bytes with an explicit 60-second transaction budget",
  {
    skip:
      !postgresTestsEnabled || process.env.RUN_PG_IMPORT_MAX_BLOB !== "1"
        ? "requires serial isolated TLS PostgreSQL and maximum-file memory qualification"
        : false,
  },
  async (t) => {
    // Maximum size and default deadline qualification are separate low-level witnesses.
    let maximumDatabase: PostgresDatabase | undefined;
    let id: string | undefined;
    let failure: unknown;
    let failed = false;
    try {
      maximumDatabase = await createTestPostgresDatabase({ transactionTimeoutMs: 60_000 });
      const backing = Buffer.allocUnsafe(NATIVE_BLOB_LIMITS.fileBytes + 2);
      backing[0] = 83;
      backing[backing.length - 1] = 25;
      const input = backing.subarray(1, backing.length - 1);
      randomFillSync(input);
      assert.equal(input.byteLength, NATIVE_BLOB_LIMITS.fileBytes);
      assert.equal(input.byteOffset, backing.byteOffset + 1);
      const expected = sha(input);
      const writeStarted = performance.now();
      const file = await files.createFile(maximumDatabase, {
        bytes: input.length,
        filename: "max-explicit-budget.bin",
        purpose: "batch",
        content: input,
      });
      id = file.id;
      t.diagnostic(
        `60-second maximum prepare/write completion: ${(performance.now() - writeStarted).toFixed(1)} ms`
      );
      const readStarted = performance.now();
      const content = await files.getFileContent(maximumDatabase, file.id);
      t.diagnostic(
        `60-second maximum capture/verified-read completion: ${(performance.now() - readStarted).toFixed(1)} ms`
      );
      assert.ok(Buffer.isBuffer(content));
      assert.equal(content.length, NATIVE_BLOB_LIMITS.fileBytes);
      assert.equal(sha(content), expected);
    } catch (error) {
      failure = error;
      failed = true;
    } finally {
      if (id && maximumDatabase) {
        try {
          await maximumDatabase.transaction((tx) =>
            tx.query("DELETE FROM omniroute.files WHERE id=$1", [id])
          );
        } catch (error) {
          if (failed) {
            t.diagnostic(
              `cleanup after primary max failure: ${isPostgresErrorKind(error, "query_failed") ? (error.sqlState ?? "query_failed") : "failed"}`
            );
          } else {
            failure = error;
            failed = true;
          }
        }
      }
      try {
        await maximumDatabase?.close();
      } catch (error) {
        if (failed) {
          t.diagnostic("maximum database close failed after primary failure");
        } else {
          failure = error;
          failed = true;
        }
      }
    }
    if (failed) throw failure;
  }
);
