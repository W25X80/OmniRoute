import assert from "node:assert/strict";
import { constants as bufferLimits } from "node:buffer";
import { createRequire } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import type { PostgresTransaction } from "../../../src/lib/db/postgres/database.ts";

const directory = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-file-content-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.DISABLE_SQLITE_AUTO_BACKUP = "true";
const native = await import("../../../src/lib/db/postgres/serviceFiles.ts");
const legacy = await import("../../../src/lib/db/files.ts");
const core = await import("../../../src/lib/db/core.ts");
const { NATIVE_BLOB_LIMITS } = await import("../../../src/lib/db/postgres/nativeBlobCarrier.ts");
const require = createRequire(import.meta.url);
const pgTypes = require("pg-types") as { getTypeParser(oid: number): (input: string) => Buffer };
const { BufferReader } = require(
  path.join(process.cwd(), "node_modules/pg-protocol/dist/buffer-reader.js")
) as {
  BufferReader: new () => {
    setBuffer(offset: number, buffer: Buffer): void;
    string(size: number): string;
  };
};

function readerFixture(
  content: Buffer | null,
  options: {
    absent?: boolean;
    deleted?: boolean;
    size?: unknown;
    decoded?: (bytes: Buffer, index: number) => unknown;
  } = {}
) {
  const queries: Array<{ text: string; values: unknown[] }> = [];
  let index = 0;
  const transaction: PostgresTransaction = {
    async query<Row>(text: string, values: unknown[] = []) {
      queries.push({ text, values });
      let rows: unknown[];
      if (options.absent || options.deleted) rows = [];
      else if (text.includes("octet_length"))
        rows = [
          {
            content_length: Object.hasOwn(options, "size")
              ? options.size
              : (content?.byteLength ?? null),
          },
        ];
      else {
        const offset = Number(values[1]) - 1,
          length = Number(values[2]);
        const bytes = content?.subarray(offset, offset + length) ?? Buffer.alloc(0);
        // Exercise the actual installed pg-protocol text path and OID17 decoder.
        const hex = Buffer.from(`\\x${bytes.toString("hex")}`);
        const protocol = new BufferReader();
        protocol.setBuffer(0, hex);
        const decoded = pgTypes.getTypeParser(17)(protocol.string(hex.byteLength));
        rows = [{ content: options.decoded ? options.decoded(decoded, index++) : decoded }];
      }
      return { rows: rows as Row[], rowCount: rows.length, command: "SELECT" };
    },
  };
  return { transaction, queries };
}

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});

test("chunked native bytes match the SQLite Buffer contract across a chunk boundary", async () => {
  const input = Buffer.allocUnsafe(NATIVE_BLOB_LIMITS.chunkBytes + 137);
  for (let index = 0; index < input.length; index++) input[index] = index % 251;
  const saved = legacy.createFile({
    bytes: 0,
    filename: "binary",
    purpose: "batch",
    content: input,
  });
  const fixture = readerFixture(input);
  const actual = await native.getFileContent(fixture.transaction, saved.id);
  assert.deepEqual(actual, legacy.getFileContent(saved.id));
  assert.ok(Buffer.isBuffer(actual));
  assert.match(fixture.queries[0].text, /FOR SHARE/);
  assert.deepEqual(
    fixture.queries.slice(1).map(({ values }) => values.slice(1)),
    [
      [1, NATIVE_BLOB_LIMITS.chunkBytes],
      [NATIVE_BLOB_LIMITS.chunkBytes + 1, 137],
    ]
  );
  assert.ok(
    fixture.queries
      .slice(1)
      .every(({ values }) => 2 * Number(values[2]) + 2 < bufferLimits.MAX_STRING_LENGTH)
  );
});

test("empty content remains a Buffer and does not issue a chunk query", async () => {
  const fixture = readerFixture(Buffer.alloc(0));
  const actual = await native.getFileContent(fixture.transaction, "empty");
  assert.ok(Buffer.isBuffer(actual));
  assert.equal(actual?.byteLength, 0);
  assert.equal(fixture.queries.length, 1);
});

test("missing, deleted and SQL-null content preserve null semantics", async () => {
  for (const fixture of [
    readerFixture(null),
    readerFixture(Buffer.from("x"), { absent: true }),
    readerFixture(Buffer.from("x"), { deleted: true }),
  ]) {
    assert.equal(await native.getFileContent(fixture.transaction, "missing"), null);
    assert.equal(fixture.queries.length, 1);
  }
});

test("an incomplete final decoded chunk rejects instead of returning partial bytes", async () => {
  const input = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 2, 255);
  const fixture = readerFixture(input, {
    decoded: (bytes, index) => (index ? bytes.subarray(0, 1) : bytes),
  });
  await assert.rejects(
    native.getFileContent(fixture.transaction, "partial"),
    /incomplete or invalid/
  );
});

test("an unexpected bytea decoder result fails closed", async () => {
  for (const decoded of [() => null, () => new Uint8Array([1]), () => "\\x01"]) {
    const fixture = readerFixture(Buffer.from([1]), { decoded });
    await assert.rejects(
      native.getFileContent(fixture.transaction, "decoder"),
      /incomplete or invalid/
    );
  }
});

test("invalid length metadata cannot allocate or return an uninitialized Buffer", async () => {
  for (const size of [-1, 1.5, "invalid", Number.MAX_SAFE_INTEGER + 1]) {
    const fixture = readerFixture(Buffer.from([1]), { size });
    await assert.rejects(native.getFileContent(fixture.transaction, "length"));
    assert.equal(fixture.queries.length, 1);
  }
});
