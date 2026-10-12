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
const pgTypes = require("pg-types") as { getTypeParser(oid: number): (input: string) => string };
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
    decoded?: (text: string, index: number) => unknown;
  } = {}
) {
  const queries: Array<{ text: string; values: unknown[] }> = [];
  const chunkQueries: typeof queries = [];
  const transaction: PostgresTransaction = {
    async query<Row>(text: string, values: unknown[] = []) {
      queries.push({ text, values });
      let rows: unknown[];
      if (text.includes("FOR SHARE OF f")) {
        assert.match(text, /octet_length\(f\.content\) AS inline_length/);
        rows =
          options.absent || options.deleted
            ? []
            : [
                {
                  inline_length: Object.hasOwn(options, "size")
                    ? options.size
                    : (content?.byteLength ?? null),
                  stored_file_id: null,
                  size_bytes: null,
                  chunk_count: null,
                  checksum: null,
                },
              ];
      } else if (text.includes("SELECT EXISTS") && text.includes("files_content_chunks")) {
        rows = [{ present: false }];
      } else {
        assert.match(text, /replace\(encode\(substr\(content/);
        assert.equal(values.length, 4);
        assert.equal(values[3], chunkQueries.length);
        chunkQueries.push({ text, values });
        const offset = Number(values[1]) - 1,
          length = Number(values[2]);
        const bytes = content?.subarray(offset, offset + length) ?? Buffer.alloc(0);
        // The current native query returns base64 TEXT, decoded as OID25 by pg.
        const wire = Buffer.from(bytes.toString("base64"));
        const protocol = new BufferReader();
        protocol.setBuffer(0, wire);
        const decoded = pgTypes.getTypeParser(25)(protocol.string(wire.byteLength));
        assert.equal(decoded, bytes.toString("base64"));
        rows = [
          {
            chunk_index: values[3],
            content: options.decoded ? options.decoded(decoded, chunkQueries.length - 1) : decoded,
            checksum: null,
          },
        ];
      }
      return { rows: rows as Row[], rowCount: rows.length, command: "SELECT" };
    },
  };
  return { transaction, queries, chunkQueries };
}

test.after(() => {
  core.resetDbInstance();
  fs.rmSync(directory, { recursive: true, force: true });
});

test("legacy inline native bytes match the SQLite Buffer contract across a chunk boundary", async () => {
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
    fixture.chunkQueries.map(({ values }) => values.slice(1)),
    [
      [1, NATIVE_BLOB_LIMITS.chunkBytes, 0],
      [NATIVE_BLOB_LIMITS.chunkBytes + 1, 137, 1],
    ]
  );
  assert.ok(
    fixture.chunkQueries.every(
      ({ values }) => 4 * Math.ceil(Number(values[2]) / 3) < bufferLimits.MAX_STRING_LENGTH
    )
  );
});

test("empty content remains a Buffer and does not issue a chunk query", async () => {
  const fixture = readerFixture(Buffer.alloc(0));
  const actual = await native.getFileContent(fixture.transaction, "empty");
  assert.ok(Buffer.isBuffer(actual));
  assert.equal(actual?.byteLength, 0);
  assert.equal(fixture.queries.length, 2);
  assert.equal(fixture.chunkQueries.length, 0);
});

test("missing, deleted and SQL-null content preserve null semantics", async () => {
  for (const [fixture, queries] of [
    [readerFixture(null), 2],
    [readerFixture(Buffer.from("x"), { absent: true }), 1],
    [readerFixture(Buffer.from("x"), { deleted: true }), 1],
  ] as const) {
    assert.equal(await native.getFileContent(fixture.transaction, "missing"), null);
    assert.equal(fixture.queries.length, queries);
    assert.equal(fixture.chunkQueries.length, 0);
  }
});

test("an incomplete final decoded chunk rejects instead of returning partial bytes", async () => {
  const input = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 2, 255);
  const fixture = readerFixture(input, {
    decoded: (text, index) =>
      index ? Buffer.from(text, "base64").subarray(0, 1).toString("base64") : text,
  });
  await assert.rejects(
    native.getFileContent(fixture.transaction, "partial"),
    /incomplete or invalid/
  );
  assert.equal(fixture.chunkQueries.length, 2);
  assert.equal(fixture.chunkQueries[1].values[3], 1);
});

test("an unexpected base64 TEXT decoder result fails closed", async () => {
  for (const value of [null, Buffer.from([1]), new Uint8Array([1]), "\\x01"]) {
    const fixture = readerFixture(Buffer.from([1]), { decoded: () => value });
    await assert.rejects(
      native.getFileContent(fixture.transaction, "decoder"),
      /incomplete or invalid/
    );
    assert.equal(fixture.chunkQueries.length, 1);
  }
});

test("invalid length metadata cannot allocate or return an uninitialized Buffer", async () => {
  const control = readerFixture(Buffer.from([1]), { size: 1 });
  assert.deepEqual(await native.getFileContent(control.transaction, "length"), Buffer.from([1]));
  assert.equal(control.chunkQueries.length, 1);
  for (const size of [-1, 1.5, "invalid", Number.MAX_SAFE_INTEGER + 1]) {
    const fixture = readerFixture(Buffer.from([1]), { size });
    let reserved = false;
    await assert.rejects(
      native.getFileContent(fixture.transaction, "length", () => {
        reserved = true;
      }),
      {
        message:
          size === -1
            ? "Stored file content is incomplete or invalid"
            : "Stored integer exceeds the supported range",
      }
    );
    assert.equal(reserved, false);
    assert.equal(fixture.queries.length, 1);
    assert.equal(fixture.chunkQueries.length, 0);
  }
});
