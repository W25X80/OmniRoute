import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import test from "node:test";
import type { PostgresResult, PostgresTransaction } from "../../src/lib/db/postgres/database";
import { PostgresError } from "../../src/lib/db/postgres/database";
import {
  NATIVE_BLOB_LIMITS,
  NativeBlobCarrierError,
} from "../../src/lib/db/postgres/nativeBlobCarrier";
import {
  POSTGRES_FILE_CONTENT_SCHEMA_STATEMENTS,
  POSTGRES_FILE_CONTENT_TABLES,
} from "../../src/lib/db/postgres/fileContentSchema";
import {
  openStoredFileContent,
  replaceStoredFileContent,
  getStoredFileContentBounds,
} from "../../src/lib/db/postgres/serviceFileContent";
import {
  createFile,
  getFileContent,
  deleteFileOwnedBy,
  pruneExpiredFiles,
} from "../../src/lib/db/postgres/serviceFiles";
const sha = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const encoded = (bytes: Buffer) => bytes.toString("base64");

interface Call {
  text: string;
  values: unknown[];
}
type Reply = { rows?: unknown[]; rowCount?: number | null };
function controlled(handle: (call: Call) => Reply | Promise<Reply>) {
  const calls: Call[] = [];
  const transaction: PostgresTransaction = {
    async query<Row>(text: string, values: unknown[] = []): Promise<PostgresResult<Row>> {
      const call = { text, values };
      calls.push(call);
      const result = await handle(call);
      return {
        rows: (result.rows ?? []) as Row[],
        rowCount: result.rowCount ?? 0,
        command: text.trim().split(/\s+/, 1)[0],
      };
    },
  };
  return { transaction, calls };
}
function reader(
  bytes: Buffer,
  options: {
    mapped?: boolean;
    header?: Record<string, unknown>;
    stats?: Record<string, unknown>;
    chunk?: Record<string, unknown>;
    chunkRows?: unknown[];
    orphans?: boolean;
    missing?: boolean;
  } = {}
) {
  const mapped = options.mapped !== false;
  return controlled(({ text, values }) => {
    if (text.includes("FOR SHARE OF f"))
      return {
        rows: options.missing
          ? []
          : [
              {
                inline_length: mapped ? null : bytes.length,
                stored_file_id: mapped ? "file-id" : null,
                size_bytes: mapped ? String(bytes.length) : null,
                chunk_count: mapped
                  ? Math.ceil(bytes.length / NATIVE_BLOB_LIMITS.chunkBytes)
                  : null,
                checksum: mapped ? sha(bytes) : null,
                ...options.header,
              },
            ],
      };
    if (text.includes("COUNT(*)::text AS count"))
      return {
        rows: [
          {
            count: String(Math.ceil(bytes.length / NATIVE_BLOB_LIMITS.chunkBytes)),
            total: String(bytes.length),
            first: bytes.length ? 0 : null,
            last: bytes.length ? Math.ceil(bytes.length / NATIVE_BLOB_LIMITS.chunkBytes) - 1 : null,
            complete: bytes.length ? true : null,
            ...options.stats,
          },
        ],
      };
    if (text.includes(" AS present")) return { rows: [{ present: options.orphans ?? false }] };
    const index = mapped ? Number(values[1]) : Number(values[3]);
    const chunk = bytes.subarray(
      index * NATIVE_BLOB_LIMITS.chunkBytes,
      (index + 1) * NATIVE_BLOB_LIMITS.chunkBytes
    );
    return {
      rows: options.chunkRows ?? [
        {
          chunk_index: index,
          content: encoded(chunk),
          checksum: mapped ? sha(chunk) : null,
          ...options.chunk,
        },
      ],
    };
  });
}

test("mapped binary content is exact and admission precedes bytes reads", async () => {
  const input = Buffer.from([0, 255, 93, 1]);
  const f = reader(input);
  let reserved = -1;
  const out = await getFileContent(f.transaction, "file-id", (size) => {
    reserved = size;
    assert.equal(
      f.calls.some((c) => c.text.includes("encode(content")),
      false
    );
  });
  assert.deepEqual(out, input);
  assert.equal(reserved, input.length);
  assert.match(f.calls[0].text, /FOR SHARE OF f/);
  assert.match(f.calls[0].text, /f.deleted_at IS NULL/);
});
test("rejected synchronous admission performs no content read", async () => {
  const f = reader(Buffer.from([1]));
  const denied = new Error("admission");
  await assert.rejects(
    getFileContent(f.transaction, "file-id", () => {
      throw denied;
    }),
    (e) => e === denied
  );
  assert.equal(
    f.calls.some((c) => c.text.includes("encode(content")),
    false
  );
});
test("NULL, empty, missing and legacy binary preserve distinct logical values", async () => {
  const empty = reader(Buffer.alloc(0));
  assert.deepEqual(await getFileContent(empty.transaction, "file-id"), Buffer.alloc(0));
  const nil = reader(Buffer.alloc(0), { mapped: false, header: { inline_length: null } });
  assert.equal(await getFileContent(nil.transaction, "file-id"), null);
  const missing = reader(Buffer.alloc(0), { missing: true });
  assert.equal(await getFileContent(missing.transaction, "file-id"), null);
  const legacy = reader(Buffer.from([2, 0, 255]), { mapped: false });
  assert.deepEqual(await getFileContent(legacy.transaction, "file-id"), Buffer.from([2, 0, 255]));
});
test("mapped and legacy SQL emits canonical base64 across padding and line boundaries", async () => {
  for (const mapped of [true, false]) {
    for (const length of [1, 2, 3, 56, 57, 58]) {
      const bytes = Buffer.alloc(length, 253);
      const f = reader(bytes, { mapped });
      assert.deepEqual(await getFileContent(f.transaction, "file-id"), bytes);
      const query = f.calls.find((call) => call.text.includes(" AS content"));
      assert.ok(query);
      assert.ok(query.text.includes("replace(encode("));
      assert.ok(query.text.includes("E'\\n', '') AS content"));
    }
  }
});
test("rejects PostgreSQL line-wrapped base64 instead of normalizing it on the owner", async () => {
  const bytes = Buffer.alloc(58, 253);
  const canonical = encoded(bytes);
  const wrapped = canonical.slice(0, 76) + "\n" + canonical.slice(76);
  assert.deepEqual(Buffer.from(wrapped, "base64"), bytes);
  for (const mapped of [true, false]) {
    const f = reader(bytes, { mapped, chunk: { content: wrapped } });
    await assert.rejects(getFileContent(f.transaction, "file-id"));
  }
});
test("rejects same-length base64 linefeed that a permissive decoder silently ignores", async () => {
  const bytes = Buffer.alloc(4);
  const value = encoded(bytes).slice(0, -1) + "\n";
  assert.equal(value.length, encoded(bytes).length);
  assert.deepEqual(Buffer.from(value, "base64"), bytes);
  for (const mapped of [true, false]) {
    const f = reader(bytes, { mapped, chunk: { content: value } });
    await assert.rejects(getFileContent(f.transaction, "file-id"));
  }
});
test("maintenance includes deleted content and uses captured file ID", async () => {
  const f = reader(Buffer.from([7]));
  const stored = await openStoredFileContent(f.transaction, "file-id");
  assert.ok(stored);
  assert.equal(f.calls[0].text.includes("deleted_at IS NULL"), false);
  assert.deepEqual(await stored.readChunk(0), Buffer.from([7]));
  assert.equal(f.calls.at(-1)?.values[0], "file-id");
});
for (const [name, header] of Object.entries({
  dual: { inline_length: 4 },
  wrongCount: { chunk_count: 2 },
  missingLength: { size_bytes: null },
  badHash: { checksum: "bad" },
  overCap: { size_bytes: String(NATIVE_BLOB_LIMITS.fileBytes + 1) },
})) {
  test(`rejects mapped header ${name} before admission`, async () => {
    const f = reader(Buffer.alloc(4), { header });
    let admitted = false;
    await assert.rejects(
      getFileContent(f.transaction, "file-id", () => {
        admitted = true;
      })
    );
    assert.equal(admitted, false);
  });
}
for (const [name, stats] of Object.entries({
  missing: { count: "0" },
  extra: { count: "2" },
  wrongTotal: { total: "3" },
  gap: { last: 2 },
  wrongLengths: { complete: false },
})) {
  test(`rejects physical chunk metadata ${name} before allocation`, async () => {
    const f = reader(Buffer.alloc(4), { stats });
    let admitted = false;
    await assert.rejects(
      getFileContent(f.transaction, "file-id", () => {
        admitted = true;
      })
    );
    assert.equal(admitted, false);
  });
}
test("mapped empty content validates its whole checksum without readChunk", async () => {
  const f = reader(Buffer.alloc(0), { header: { checksum: "0".repeat(64) } });
  await assert.rejects(openStoredFileContent(f.transaction, "file-id"));
});
test("unmapped chunks are rejected instead of legacy/NULL fallback", async () => {
  const f = reader(Buffer.alloc(0), { mapped: false, orphans: true });
  await assert.rejects(openStoredFileContent(f.transaction, "file-id"));
});
for (const [name, chunk] of Object.entries({
  gap: { chunk_index: 1 },
  stringOrdinal: { chunk_index: "0" },
  wrongPerChunkHash: { checksum: "0".repeat(64) },
  short: { content: "AA==" },
  alphabet: { content: "AAAA-A==" },
  padding: { content: "AAAAAA" },
  padBits: { content: "AAAAAB==" },
})) {
  test(`rejects invalid chunk ${name}`, async () => {
    const f = reader(Buffer.alloc(4), { chunk });
    await assert.rejects(getFileContent(f.transaction, "file-id"));
  });
}
for (const [name, chunkRows] of Object.entries({
  missing: [],
  duplicate: [{ chunk_index: 0 }, { chunk_index: 0 }],
})) {
  test(`rejects ${name} physical bytes row`, async () => {
    const f = reader(Buffer.alloc(4), { chunkRows });
    await assert.rejects(getFileContent(f.transaction, "file-id"));
  });
}
test("whole checksum is verified before final chunk is returned", async () => {
  const f = reader(Buffer.alloc(4), { header: { checksum: "0".repeat(64) } });
  const stored = await openStoredFileContent(f.transaction, "file-id");
  assert.ok(stored);
  await assert.rejects(stored.readChunk(0));
});
test("multi-chunk read is sequential and exact across a nonzero offset", async () => {
  const bytes = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 113, 253);
  bytes[NATIVE_BLOB_LIMITS.chunkBytes] = 17;
  const f = reader(bytes);
  assert.deepEqual(await getFileContent(f.transaction, "file-id"), bytes);
  const again = reader(bytes);
  const stored = await openStoredFileContent(again.transaction, "file-id");
  assert.ok(stored);
  await assert.rejects(stored.readChunk(1));
});
test("native writer binds bounded private binary parameters and finalizes marker last", async () => {
  const input = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 17, 37);
  const original = sha(input);
  const copied: Buffer[] = [];
  const f = controlled(({ text, values }) => {
    if (text.includes("FOR UPDATE")) return { rows: [{ id: "file-id" }] };
    if (text.includes("INSERT INTO omniroute.files_content_chunks")) {
      const bytes = values[2] as Buffer;
      assert.ok(Buffer.isBuffer(bytes));
      assert.ok(bytes.length <= NATIVE_BLOB_LIMITS.chunkBytes);
      assert.equal(values[3], sha(bytes));
      copied.push(Buffer.from(bytes));
      input.fill(99, 0, NATIVE_BLOB_LIMITS.chunkBytes);
      assert.equal(values[3], sha(bytes));
      return { rowCount: 1 };
    }
    return { rowCount: 1 };
  });
  await replaceStoredFileContent(f.transaction, "file-id", input);
  const marker = f.calls.find((c) => c.text.includes("INSERT INTO omniroute.files_content_state"));
  assert.ok(marker);
  assert.equal(marker.values[3], original);
  assert.equal(sha(Buffer.concat(copied)), original);
  const markerIndex = f.calls.indexOf(marker);
  assert.ok(
    f.calls
      .slice(0, markerIndex)
      .filter((c) => c.text.includes("INSERT INTO omniroute.files_content_chunks")).length === 2
  );
  assert.equal(f.calls.at(-1)?.text, "UPDATE omniroute.files SET content = NULL WHERE id = $1");
  assert.equal(
    f.calls.some((c) => c.text.includes("advisory")),
    false
  );
});
test("carrier corruption propagates before marker and does not replay/cleanup", async () => {
  const input = Buffer.from([1, 2, 3]);
  const reference = {
    kind: "blob-reference" as const,
    id: randomUUID(),
    sizeBytes: 3,
    checksum: sha(input),
    chunks: [{ sizeBytes: 3, checksum: sha(input) }],
  };
  const f = controlled(({ text }) =>
    text.includes("FOR UPDATE") ? { rows: [{ id: "file-id" }] } : { rowCount: 1 }
  );
  await assert.rejects(
    replaceStoredFileContent(f.transaction, "file-id", reference, {
      async readChunk() {
        return Buffer.from([1, 2, 4]);
      },
    }),
    (error) => error instanceof NativeBlobCarrierError && error.kind === "verification"
  );
  assert.equal(
    f.calls.some((c) => c.text.includes("INSERT INTO")),
    false
  );
  assert.equal(f.calls.filter((c) => c.text.startsWith("DELETE")).length, 1);
});
test("accepted SQL failure propagates without final marker or replay", async () => {
  const failure = new PostgresError("query_failed", "57014");
  const f = controlled(({ text }) => {
    if (text.includes("FOR UPDATE")) return { rows: [{ id: "file-id" }] };
    if (text.includes("INSERT INTO omniroute.files_content_chunks")) throw failure;
    return { rowCount: 1 };
  });
  await assert.rejects(
    replaceStoredFileContent(f.transaction, "file-id", Buffer.from([1])),
    (e) => e === failure
  );
  assert.equal(f.calls.at(-1)?.text.includes("INSERT INTO omniroute.files_content_chunks"), true);
});
test("NULL replacement removes mapped state; empty has a finalized zero marker", async () => {
  for (const content of [null, Buffer.alloc(0)]) {
    const f = controlled(({ text }) =>
      text.includes("FOR UPDATE") ? { rows: [{ id: "file-id" }] } : { rowCount: 1 }
    );
    await replaceStoredFileContent(f.transaction, "file-id", content);
    const marker = f.calls.find((c) =>
      c.text.includes("INSERT INTO omniroute.files_content_state")
    );
    if (content === null) assert.equal(marker, undefined);
    else {
      assert.ok(marker);
      assert.deepEqual(marker.values, ["file-id", 0, 0, sha(Buffer.alloc(0))]);
    }
  }
});
test("metadata bytes stay literal and no whole file Buffer is sent in metadata INSERT", async () => {
  const input = Buffer.from([0, 255]);
  const f = controlled(({ text, values }) =>
    text.includes("FOR UPDATE") ? { rows: [{ id: values[0] }] } : { rowCount: 1 }
  );
  const file = await createFile(f.transaction, {
    bytes: 0,
    filename: "source",
    purpose: "batch",
    content: input,
  });
  assert.equal(file.bytes, 0);
  assert.equal(file.content, input);
  const metadata = f.calls.find((c) => c.text.includes("INSERT INTO omniroute.files\n"));
  assert.ok(metadata);
  assert.equal(metadata.values[1], 0);
  assert.equal(metadata.values[5], null);
});
test("oversize writer rejects before any query without allocating max input", async () => {
  const bytes = new Proxy(Buffer.alloc(1), {
    get(target, key) {
      return key === "byteLength"
        ? NATIVE_BLOB_LIMITS.fileBytes + 1
        : Reflect.get(target, key, target);
    },
  });
  const f = controlled(() => ({ rowCount: 1 }));
  await assert.rejects(replaceStoredFileContent(f.transaction, "file-id", bytes));
  await assert.rejects(
    createFile(f.transaction, { bytes: 0, filename: "source", purpose: "batch", content: bytes })
  );
  assert.equal(f.calls.length, 0);
});
test("foreign-owned delete cannot clear content chunks", async () => {
  const f = controlled(() => ({ rowCount: 0 }));
  assert.equal(await deleteFileOwnedBy(f.transaction, "file-id", "foreign"), false);
  assert.equal(
    f.calls.some((c) => c.text.startsWith("DELETE")),
    false
  );
});
test("pruning returns source affected metadata count and clears state in same statement", async () => {
  const f = controlled(({ text }) =>
    text.includes("WITH expired") ? { rows: [{ count: "4" }] } : {}
  );
  assert.equal(await pruneExpiredFiles(f.transaction, 123), 4);
  assert.match(f.calls.at(-1)!.text, /DELETE FROM omniroute.files_content_state/);
});
test("bounds preserve large decimal total and reject unaccounted physical content", async () => {
  const f = controlled(() => ({ rows: [{ maximum: 17, total: "10000000000", invalid: false }] }));
  assert.deepEqual(await getStoredFileContentBounds(f.transaction), {
    maximum: 17,
    total: "10000000000",
  });
  assert.match(f.calls[0].text, /WHERE f.id IS NULL/);
  assert.match(f.calls[0].text, /WHERE s.file_id IS NULL/);
  const corrupt = controlled(() => ({ rows: [{ maximum: 17, total: "17", invalid: true }] }));
  await assert.rejects(getStoredFileContentBounds(corrupt.transaction));
});
test("schema owns finite chunk bounds and deferred finalized-marker referential integrity", () => {
  assert.deepEqual(POSTGRES_FILE_CONTENT_TABLES, ["files_content_state", "files_content_chunks"]);
  const ddl = POSTGRES_FILE_CONTENT_SCHEMA_STATEMENTS.join("\n");
  assert.match(ddl, /DEFERRABLE INITIALLY DEFERRED/);
  assert.match(ddl, /ON DELETE CASCADE/);
  assert.match(ddl, new RegExp(`size_bytes BETWEEN 0 AND ${NATIVE_BLOB_LIMITS.fileBytes}`));
  assert.ok(ddl.includes(`octet_length(content) BETWEEN 1 AND ${NATIVE_BLOB_LIMITS.chunkBytes})`));
});
