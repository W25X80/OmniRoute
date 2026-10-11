import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import test from "node:test";
import {
  ApplicationImportError,
  applicationSnapshotDigest,
  captureSqliteApplicationSnapshot,
  openOfflineSqliteSource,
} from "../../../src/lib/db/postgres/applicationSnapshot.ts";
import { NATIVE_BLOB_LIMITS } from "../../../src/lib/db/postgres/nativeBlobCarrier.ts";
import { runtimeRequire } from "../../../src/lib/db/adapters/runtimeRequire.ts";
import type { SqliteAdapter } from "../../../src/lib/db/adapters/types.ts";

function scratch() {
  const directory = mkdtempSync(path.join(os.tmpdir(), "omniroute-offline-source-"));
  return { directory, filename: path.join(directory, "source.sqlite") };
}
function fixture(filename: string, vectors = false) {
  const writer = new DatabaseSync(filename, { allowExtension: true });
  try {
    writer.exec(
      "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,content BLOB)"
    );
    writer.prepare("INSERT INTO key_value VALUES('settings','quoted','opaque value')").run();
    writer.prepare("INSERT INTO files VALUES('small',?)").run(Buffer.from([0, 1, 255]));
    if (vectors) {
      (runtimeRequire("sqlite-vec") as { load(database: unknown): void }).load(writer);
      writer.exec(
        "CREATE TABLE memories(id TEXT PRIMARY KEY); CREATE TABLE memory_vec_meta(id INTEGER PRIMARY KEY,embedding_signature TEXT,active_dim INTEGER); CREATE VIRTUAL TABLE vec_memories USING vec0(embedding float[2]); INSERT INTO memories(rowid,id) VALUES(1,'memory'); INSERT INTO memory_vec_meta VALUES(1,'fixture-signature',2)"
      );
      writer
        .prepare("INSERT INTO vec_memories(rowid,embedding) VALUES(1,?)")
        .run(new Float32Array([1, 2]));
    }
  } finally {
    writer.close();
  }
}
const schemaFailure = (error: unknown) =>
  error instanceof ApplicationImportError && error.kind === "schema";

test("explicit native offline source matches normal small-file capture without hooks or writes", () => {
  const { directory, filename } = scratch();
  let normal: SqliteAdapter | undefined, native: SqliteAdapter | undefined;
  try {
    fixture(filename);
    const before = createHash("sha256").update(readFileSync(filename)).digest("hex");
    const events = ["beforeExit", "SIGINT", "SIGTERM"] as const;
    const listeners = events.map((event) => process.listenerCount(event));
    const emit = process.emit;
    normal = openOfflineSqliteSource(filename);
    native = openOfflineSqliteSource(filename, { largeFiles: true });
    assert.equal(native.driver, "node:sqlite");
    assert.equal(native.pragma("query_only", { simple: true }), 1);
    assert.equal(native.pragma("busy_timeout", { simple: true }), 3000);
    assert.equal(
      applicationSnapshotDigest(captureSqliteApplicationSnapshot(native)),
      applicationSnapshotDigest(captureSqliteApplicationSnapshot(normal))
    );
    native.pragma("query_only=OFF");
    assert.throws(() =>
      native!.prepare("INSERT INTO key_value VALUES('settings','forbidden','no')").run()
    );
    native.pragma("query_only=ON");
    const raw = native.raw as { loadExtension(filename: string): void };
    assert.throws(
      () => raw.loadExtension("untrusted-extension"),
      /extension loading is not allowed/i
    );
    native.close();
    assert.equal(native.open, false);
    normal.close();
    assert.deepEqual(
      events.map((event) => process.listenerCount(event)),
      listeners
    );
    assert.equal(process.emit, emit);
    assert.equal(createHash("sha256").update(readFileSync(filename)).digest("hex"), before);
  } finally {
    native?.close();
    normal?.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("native offline source refuses existing raw and adapter transactions before capture", () => {
  const { directory, filename } = scratch();
  let source: SqliteAdapter | undefined;
  try {
    fixture(filename);
    source = openOfflineSqliteSource(filename, { largeFiles: true });
    source.exec("BEGIN");
    assert.equal(source.inTransaction, true);
    assert.throws(
      () => captureSqliteApplicationSnapshot(source!),
      (error: unknown) =>
        error instanceof ApplicationImportError && error.kind === "source_not_readonly"
    );
    source.exec("ROLLBACK");
    assert.equal(source.inTransaction, false);
    source.transaction(() =>
      assert.throws(
        () => captureSqliteApplicationSnapshot(source!),
        (error: unknown) =>
          error instanceof ApplicationImportError && error.kind === "source_not_readonly"
      )
    )();
    assert.equal(source.inTransaction, false);
  } finally {
    source?.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("native offline source requires a regular existing file and reports fixed safe errors", () => {
  const { directory, filename } = scratch();
  const missing = path.join(directory, "missing.sqlite");
  try {
    assert.throws(() => openOfflineSqliteSource(missing, { largeFiles: true }), schemaFailure);
    assert.equal(existsSync(missing), false);
    assert.throws(() => openOfflineSqliteSource(directory, { largeFiles: true }), schemaFailure);
    writeFileSync(filename, "not a SQLite database");
    assert.throws(
      () => openOfflineSqliteSource(filename, { largeFiles: true }),
      (error: unknown) =>
        schemaFailure(error) && !String((error as Error).message).includes(directory)
    );
    // A failed setup leaves no retained adapter; the source remains owned by the caller.
    assert.equal(readFileSync(filename, "utf8"), "not a SQLite database");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("native offline source loads known sqlite-vec then seals further extension loading", () => {
  const { directory, filename } = scratch();
  let normal: SqliteAdapter | undefined, native: SqliteAdapter | undefined;
  try {
    fixture(filename, true);
    normal = openOfflineSqliteSource(filename);
    const query = "SELECT embedding FROM vec_memories WHERE rowid=1";
    const normalEmbedding = Buffer.from(
      (normal.prepare(query).get() as { embedding: Uint8Array }).embedding
    );
    const normalVersion = normal.prepare("SELECT vec_version() AS version").get();
    // The loadable extension shares its SQLite API binding; compare drivers sequentially.
    normal.close();
    normal = undefined;
    native = openOfflineSqliteSource(filename, { largeFiles: true });
    const nativeEmbedding = (native.prepare(query).get() as { embedding: Uint8Array }).embedding;
    assert.deepEqual(Buffer.from(nativeEmbedding), Buffer.from(normalEmbedding));
    assert.deepEqual(
      [Buffer.from(nativeEmbedding).readFloatLE(0), Buffer.from(nativeEmbedding).readFloatLE(4)],
      [1, 2]
    );
    assert.deepEqual(native.prepare("SELECT vec_version() AS version").get(), normalVersion);
    assert.throws(
      () =>
        (native!.raw as { loadExtension(filename: string): void }).loadExtension(
          "untrusted-extension"
        ),
      /extension loading is not allowed/i
    );
  } finally {
    native?.close();
    normal?.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test(
  "selected native source reads bounded chunks from a real 512 MiB SQLite row",
  {
    skip:
      process.env.RUN_SQLITE_OFFLINE_MAX_BLOB !== "1"
        ? "requires explicit 512 MiB local scratch fixture"
        : false,
  },
  () => {
    const { directory, filename } = scratch();
    let normal: SqliteAdapter | undefined, native: SqliteAdapter | undefined;
    try {
      const writer = new DatabaseSync(filename);
      try {
        writer.exec(
          "CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT,PRIMARY KEY(namespace,key)); CREATE TABLE files(id TEXT PRIMARY KEY,content BLOB)"
        );
        writer
          .prepare("INSERT INTO files VALUES('maximum',zeroblob(?))")
          .run(NATIVE_BLOB_LIMITS.fileBytes);
      } finally {
        writer.close();
      }
      normal = openOfflineSqliteSource(filename);
      native = openOfflineSqliteSource(filename, { largeFiles: true });
      const query = "SELECT substr(content,1,?) AS chunk FROM files WHERE id='maximum'";
      if (normal.driver === "better-sqlite3")
        assert.throws(
          () => normal!.prepare(query).get(NATIVE_BLOB_LIMITS.chunkBytes),
          (error: unknown) => (error as { code?: unknown })?.code === "SQLITE_TOOBIG"
        );
      const chunk = (
        native.prepare(query).get(NATIVE_BLOB_LIMITS.chunkBytes) as { chunk: Uint8Array }
      ).chunk;
      assert.equal(chunk.byteLength, NATIVE_BLOB_LIMITS.chunkBytes);
      assert.equal(
        chunk.every((value) => value === 0),
        true
      );
      assert.equal(
        (native.prepare("SELECT length(content) AS bytes FROM files").get() as { bytes: number })
          .bytes,
        NATIVE_BLOB_LIMITS.fileBytes
      );
    } finally {
      native?.close();
      normal?.close();
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
