import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../../src/lib/db/adapters/betterSqliteAdapter.ts";
import {
  captureSqliteApplicationSnapshot,
  checkedApplicationSnapshot,
  ApplicationImportError,
} from "../../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  captureNativeBlob,
  checkedNativeBlobReference,
  verifyNativeBlob,
  assembleNativeBlob,
  NATIVE_BLOB_LIMITS,
  NativeBlobCarrierError,
  type NativeBlobReference,
} from "../../../src/lib/db/postgres/nativeBlobCarrier.ts";
const sha = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const kind = (value: string) => (error: unknown) =>
  error instanceof NativeBlobCarrierError && error.kind === value;

function carrier(bytes: Uint8Array) {
  const chunks = new Map<number, Uint8Array>();
  const reference = captureNativeBlob(
    bytes.byteLength,
    (offset, length) => bytes.subarray(offset, offset + length),
    {
      writeChunk(_id, index, chunk) {
        chunks.set(index, Buffer.from(chunk));
      },
    }
  );
  const source = {
    async readChunk(_id: string, index: number) {
      return chunks.get(index) ?? null;
    },
  };
  return { reference, chunks, source };
}

test("8 MiB chunks preserve all bytes and reject missing, corrupt, reordered and wrong-size carriers", async () => {
  const bytes = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 17, 0xa5);
  bytes.fill(0x7c, NATIVE_BLOB_LIMITS.chunkBytes);
  const { reference, chunks, source } = carrier(bytes);
  assert.deepEqual(
    reference.chunks.map((value) => value.sizeBytes),
    [NATIVE_BLOB_LIMITS.chunkBytes, 17]
  );
  assert.equal(reference.checksum, sha(bytes));
  await verifyNativeBlob(reference, source);
  const result = await assembleNativeBlob(
    reference,
    source,
    NATIVE_BLOB_LIMITS.importMemoryBudgetBytes
  );
  assert.deepEqual(result, bytes);
  await assert.rejects(
    assembleNativeBlob(reference, source, NATIVE_BLOB_LIMITS.importMemoryBudgetBytes - 1),
    kind("limits")
  );
  const second = chunks.get(1)!;
  chunks.delete(1);
  await assert.rejects(verifyNativeBlob(reference, source), kind("verification"));
  chunks.set(1, Buffer.alloc(17, 0));
  await assert.rejects(verifyNativeBlob(reference, source), kind("verification"));
  chunks.set(1, second);
  await assert.rejects(
    verifyNativeBlob({ ...reference, checksum: "0".repeat(64) }, source),
    kind("verification")
  );
  assert.throws(
    () => checkedNativeBlobReference({ ...reference, sizeBytes: reference.sizeBytes - 1 }),
    kind("verification")
  );
  assert.throws(
    () => checkedNativeBlobReference({ ...reference, chunks: [...reference.chunks].reverse() }),
    kind("verification")
  );
});

test("maximum 512 MiB boundary is streamed as 64 chunks without a 512 MiB allocation", () => {
  const chunk = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes, 0x39),
    oracle = createHash("sha256");
  let reads = 0,
    writes = 0;
  const reference = captureNativeBlob(
    NATIVE_BLOB_LIMITS.fileBytes,
    (offset, length) => {
      assert.equal(offset, reads * NATIVE_BLOB_LIMITS.chunkBytes);
      assert.equal(length, NATIVE_BLOB_LIMITS.chunkBytes);
      reads++;
      return chunk;
    },
    {
      writeChunk(_id, index, bytes) {
        assert.equal(index, writes++);
        assert.equal(bytes.byteLength, chunk.byteLength);
      },
    }
  );
  for (let index = 0; index < 64; index++) oracle.update(chunk);
  assert.equal(reference.sizeBytes, 536870912);
  assert.equal(reference.chunks.length, 64);
  assert.equal(reference.checksum, oracle.digest("hex"));
  assert.equal(reads, 64);
  assert.equal(writes, 64);
  assert.throws(
    () =>
      captureNativeBlob(
        NATIVE_BLOB_LIMITS.fileBytes + 1,
        () => {
          throw new Error("oversized read must not occur");
        },
        {
          writeChunk() {
            throw new Error("oversized write must not occur");
          },
        }
      ),
    kind("limits")
  );
});

test("SQLite capture uses one read snapshot despite another WAL writer changing the BLOB", async () => {
  const folder = mkdtempSync(path.join(os.tmpdir(), "omniroute-carrier-")),
    filename = path.join(folder, "source.sqlite");
  const writer = new Database(filename);
  writer.pragma("journal_mode=WAL");
  writer.exec(
    "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,content BLOB)"
  );
  const original = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 17, 0x51);
  original[original.length - 1] = 0xff;
  writer.prepare("INSERT INTO files VALUES(?,?,?)").run("file-original", original.length, original);
  const reader = createBetterSqliteAdapter(new Database(filename, { readonly: true }));
  reader.pragma("query_only=ON");
  const chunks = new Map<number, Uint8Array>();
  try {
    assert.throws(
      () => captureSqliteApplicationSnapshot(reader),
      (error) => error instanceof ApplicationImportError && error.kind === "limits"
    );
    const snapshot = captureSqliteApplicationSnapshot(reader, {
      blobSink: {
        writeChunk(_id, index, bytes) {
          assert.equal(reader.inTransaction, true);
          chunks.set(index, Buffer.from(bytes));
          if (index === 0)
            writer
              .prepare("UPDATE files SET content=? WHERE id=?")
              .run(Buffer.alloc(original.length, 0x2d), "file-original");
        },
      },
    });
    const reference = snapshot.tables.find((value) => value.name === "files")!.rows[0]
      .values[2] as NativeBlobReference;
    assert.equal(reference.kind, "blob-reference");
    assert.equal(reference.checksum, sha(original));
    assert.ok(JSON.stringify(snapshot).length < 16384);
    checkedApplicationSnapshot(snapshot);
    await verifyNativeBlob(reference, {
      async readChunk(_id, index) {
        return chunks.get(index) ?? null;
      },
    });
    assert.equal(reader.inTransaction, false);
    assert.notEqual(
      sha((writer.prepare("SELECT content FROM files").get() as { content: Uint8Array }).content),
      reference.checksum
    );
    const bad = structuredClone(snapshot),
      file = bad.tables.find((value) => value.name === "files")!;
    file.name = "provider_connections";
    assert.throws(
      () => checkedApplicationSnapshot(bad),
      (error) => error instanceof ApplicationImportError && error.kind === "schema"
    );
  } finally {
    reader.close();
    writer.close();
    rmSync(folder, { recursive: true, force: true });
  }
});

test(
  "actual SQLite 512 MiB file boundary has a complete 64-chunk carrier",
  {
    skip:
      process.env.RUN_PG_IMPORT_MAX_BLOB !== "1"
        ? "requires explicit offline disk/memory budget"
        : false,
  },
  () => {
    const folder = mkdtempSync(path.join(os.tmpdir(), "omniroute-max-carrier-")),
      filename = path.join(folder, "source.sqlite");
    const writer = new Database(filename);
    writer.exec(
      "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,bytes INTEGER,content BLOB)"
    );
    writer
      .prepare("INSERT INTO files VALUES('max',?,zeroblob(?))")
      .run(NATIVE_BLOB_LIMITS.fileBytes, NATIVE_BLOB_LIMITS.fileBytes);
    writer.close();
    const reader = createBetterSqliteAdapter(new Database(filename, { readonly: true }));
    reader.pragma("query_only=ON");
    let chunks = 0,
      bytes = 0;
    const oracle = createHash("sha256"),
      zero = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes);
    try {
      const snapshot = captureSqliteApplicationSnapshot(reader, {
        blobSink: {
          writeChunk(_id, index, chunk) {
            assert.equal(reader.inTransaction, true);
            assert.equal(index, chunks++);
            assert.equal(chunk.byteLength, NATIVE_BLOB_LIMITS.chunkBytes);
            bytes += chunk.byteLength;
            assert.equal(sha(chunk), sha(zero));
          },
        },
      });
      for (let index = 0; index < 64; index++) oracle.update(zero);
      const reference = snapshot.tables.find((value) => value.name === "files")!.rows[0]
        .values[2] as NativeBlobReference;
      assert.equal(chunks, 64);
      assert.equal(bytes, NATIVE_BLOB_LIMITS.fileBytes);
      assert.equal(reference.checksum, oracle.digest("hex"));
      assert.equal(reference.sizeBytes, NATIVE_BLOB_LIMITS.fileBytes);
      assert.ok(JSON.stringify(snapshot).length < 16384);
    } finally {
      reader.close();
      rmSync(folder, { recursive: true, force: true });
    }
  }
);
