import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { setImmediate as nextTurn } from "node:timers/promises";
import test from "node:test";
import { createFile, getFileContent } from "../../../src/lib/db/postgres/serviceFiles.ts";
import { completeBatchWithFiles } from "../../../src/lib/db/postgres/serviceBatches.ts";
import { NATIVE_BLOB_LIMITS } from "../../../src/lib/db/postgres/nativeBlobCarrier.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

function databaseFixture(
  handler: (sql: string, values: unknown[]) => unknown[],
  beforeCommit?: () => void
) {
  let active = false,
    committed = false;
  const calls: Array<{ sql: string; values: unknown[] }> = [];
  const database = {
    async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>) {
      assert.equal(active, false);
      active = true;
      try {
        const value = await body({
          async query<Row>(sql: string, values: unknown[] = []) {
            calls.push({ sql, values });
            return { rows: handler(sql, values) as Row[], rowCount: 1, command: "SELECT" };
          },
        });
        beforeCommit?.();
        committed = true;
        return value;
      } finally {
        active = false;
      }
    },
  } as unknown as PostgresDatabase;
  return { database, calls, active: () => active, committed: () => committed };
}
const sha = (input: Buffer) => createHash("sha256").update(input).digest("hex");

test("serving preparation yields before SQL and inserts privately captured bytes without a global fence", async () => {
  const input = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 1, 7);
  const expected = sha(input);
  let heartbeat = 0,
    running = true;
  const turn = (async () => {
    while (running) {
      await nextTurn();
      heartbeat++;
    }
  })();
  const chunks: Buffer[] = [];
  let whole: unknown;
  const f = databaseFixture((sql, values) => {
    assert.ok(heartbeat >= 2, "two prepared chunks must each permit a macrotask before SQL");
    assert.doesNotMatch(sql, /pg_advisory/);
    input.fill(9); // The original caller buffer cannot alter the already-prepared publication.
    if (sql.includes("SELECT id FROM omniroute.files")) return [{ id: values[0] }];
    if (sql.includes("INSERT INTO omniroute.files_content_chunks")) {
      const bytes = values[2] as Buffer;
      assert.equal(sha(bytes), values[3]);
      chunks.push(bytes);
    }
    if (sql.includes("INSERT INTO omniroute.files_content_state")) whole = values[3];
    return [];
  });
  try {
    await createFile(f.database, {
      bytes: input.length,
      filename: "bound",
      purpose: "batch",
      content: input,
    });
  } finally {
    running = false;
    await turn;
  }
  assert.equal(sha(Buffer.concat(chunks)), expected);
  assert.equal(whole, expected);
  assert.equal(f.committed(), true);
});

test("serving snapshot validation occurs after COMMIT and a later delete cannot change captured bytes", async () => {
  const original = Buffer.from([0, 255, 1]);
  let durable = original,
    sawCommit = false;
  const f = databaseFixture(
    (sql) => {
      if (sql.includes("FOR SHARE OF f"))
        return [
          {
            inline_length: durable.length,
            stored_file_id: null,
            size_bytes: null,
            chunk_count: null,
            checksum: null,
          },
        ];
      if (sql.includes("SELECT EXISTS")) return [{ present: false }];
      return [{ chunk_index: 0, content: durable.toString("base64"), checksum: null }];
    },
    () => {
      durable = Buffer.alloc(0);
      sawCommit = true;
    }
  );
  const pending = getFileContent(f.database, "owned", (size) => assert.equal(size, 3));
  const content = await pending;
  assert.equal(sawCommit, true);
  assert.equal(f.active(), false);
  assert.deepEqual(content, original);
  // A malformed transport value is not inspected until SQL has released its lease.
  const corrupt = databaseFixture((sql) => {
    if (sql.includes("FOR SHARE OF f"))
      return [
        {
          inline_length: 3,
          stored_file_id: null,
          size_bytes: null,
          chunk_count: null,
          checksum: null,
        },
      ];
    if (sql.includes("SELECT EXISTS")) return [{ present: false }];
    return [{ chunk_index: 0, content: "not-valid-base64", checksum: null }];
  });
  await assert.rejects(getFileContent(corrupt.database, "corrupt"), /incomplete or invalid/);
  assert.equal(corrupt.committed(), true);
});

test("verified serving projection yields a real macrotask between chunks after SQL release", async () => {
  const original = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 1, 3);
  const encoded = [
    original.subarray(0, NATIVE_BLOB_LIMITS.chunkBytes).toString("base64"),
    original.subarray(NATIVE_BLOB_LIMITS.chunkBytes).toString("base64"),
  ];
  let ticks = 0,
    running = true;
  const pulse = (async () => {
    while (running) {
      await nextTurn();
      ticks++;
    }
  })();
  const f = databaseFixture(
    (sql, values) => {
      if (sql.includes("FOR SHARE OF f"))
        return [
          {
            inline_length: original.length,
            stored_file_id: null,
            size_bytes: null,
            chunk_count: null,
            checksum: null,
          },
        ];
      if (sql.includes("SELECT EXISTS")) return [{ present: false }];
      return [{ chunk_index: values[3], content: encoded[Number(values[3])], checksum: null }];
    },
    () => assert.equal(ticks, 0, "the fixture's SQL resolves only through microtasks")
  );
  try {
    assert.deepEqual(await getFileContent(f.database, "owned"), original);
    assert.ok(ticks >= 2, "both projection chunks must yield to other event-loop owners");
    assert.equal(f.active(), false);
  } finally {
    running = false;
    await pulse;
  }
});

test("a late batch cancellation removes unpublished files and preserves prior referenced content", async () => {
  const files = new Set<string>(["previous-shared-file"]);
  let fence = false,
    status = "finalizing",
    inserted = 0;
  const f = databaseFixture((sql, values) => {
    if (sql.includes("pg_advisory")) {
      fence = true;
      return [];
    }
    if (/INSERT INTO omniroute.files\s/.test(sql)) {
      files.add(String(values[0]));
      return [];
    }
    if (sql.includes("SELECT id FROM omniroute.files")) return [{ id: values[0] }];
    if (sql.includes("INSERT INTO omniroute.files_content_chunks")) {
      assert.equal(fence, false, "content transfer must precede the unrelated publication fence");
      inserted++;
      status = "cancelling";
    }
    if (sql.includes("SELECT * FROM omniroute.batches"))
      return [{ id: "batch", status, input_file_id: "previous-shared-file", created_at: "1" }];
    if (sql.includes("DELETE FROM omniroute.files WHERE id=ANY"))
      for (const id of values[0] as string[]) files.delete(id);
    if (sql.includes("UPDATE omniroute.batches")) status = String(values[0]);
    return [];
  });
  const result = await completeBatchWithFiles(
    f.database,
    "batch",
    { requestCountsTotal: 1, requestCountsCompleted: 1, requestCountsFailed: 0, completedAt: 10 },
    { filename: "new-output", content: Buffer.from("private"), expiresAt: null },
    null
  );
  assert.equal(inserted, 1);
  assert.equal(result.batch?.status, "cancelled");
  assert.deepEqual([...files], ["previous-shared-file"]);
  assert.equal(f.committed(), true);
});
