import assert from "node:assert/strict";
import test from "node:test";
import { createHash } from "node:crypto";
import Database from "better-sqlite3";
import { createBetterSqliteAdapter } from "../../../src/lib/db/adapters/betterSqliteAdapter.ts";
import {
  captureSqliteApplicationSnapshot,
  applicationSnapshotDigest,
} from "../../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  readApplicationHistory,
  checkedApplicationRowCarrier,
  ApplicationRowCarrierError,
} from "../../../src/lib/db/postgres/applicationRowCarrier.ts";

test("opt-in history carrier preserves source rowids without retaining inline rows", () => {
  const raw = new Database(":memory:");
  const source = createBetterSqliteAdapter(raw);
  try {
    source.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT);
      CREATE TABLE conversation_turn_nodes(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,
        parent_id TEXT,role TEXT NOT NULL,content_hash TEXT NOT NULL DEFAULT '',
        last_correlation_id TEXT,first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL);
      INSERT INTO key_value VALUES('settings','requireLogin','true');
      INSERT INTO conversation_turn_nodes(rowid,id,conversation_id,role,first_seen_at,last_seen_at)
        VALUES(-3,'first','conversation','user','2026-10-12','2026-10-12'),
          (7,'second','conversation','assistant','2026-10-12','2026-10-12');`);
    source.pragma("query_only = ON");
    const inline = captureSqliteApplicationSnapshot(source);
    const chunks: Uint8Array[] = [];
    const carried = captureSqliteApplicationSnapshot(source, {
      rowSink: {
        writeChunk(_id: string, index: number, bytes: Uint8Array) {
          chunks[index] = Buffer.from(bytes);
        },
      },
    });
    const table = carried.tables.find((entry) => entry.name === "conversation_turn_nodes")!;
    assert.deepEqual(table.rows, []);
    assert.equal(table.rowCarrier?.rowCount, 2);
    assert.equal(chunks.length, 1);
    assert.deepEqual(
      JSON.parse(Buffer.from(chunks[0]).toString()),
      inline.tables.find((entry) => entry.name === table.name)!.rows
    );
    assert.notEqual(applicationSnapshotDigest(carried), applicationSnapshotDigest(inline));
    assert.equal(raw.prepare("SELECT count(*) AS n FROM conversation_turn_nodes").get().n, 2);
  } finally {
    source.close();
  }
});

test("history chunks reject altered bytes, false counts and reordered rowids", async () => {
  const rows = [
    { rowid: "1", values: ["one", "c", null, "user", "h", null, "a", "b"] },
    { rowid: "2", values: ["two", "c", null, "assistant", "h", null, "a", "b"] },
  ];
  const bytes = Buffer.from(JSON.stringify(rows));
  const carrier = checkedApplicationRowCarrier({
    format: "omniroute-turn-history-carrier-v1",
    id: "147b120d-4c9e-4b75-a6d2-589f180ff074",
    rowCount: 2,
    chunks: [
      { rows: 2, bytes: bytes.length, sha256: createHash("sha256").update(bytes).digest("hex") },
    ],
  });
  const collect = async (value = carrier, body = bytes) => {
    const result = [];
    for await (const batch of readApplicationHistory(value, {
      async readChunk() {
        return body;
      },
    }))
      result.push(...batch);
    return result;
  };
  assert.deepEqual(await collect(), rows);
  await assert.rejects(collect(carrier, Buffer.from(bytes).fill(32)), ApplicationRowCarrierError);
  assert.throws(
    () => checkedApplicationRowCarrier({ ...carrier, rowCount: 1 }),
    ApplicationRowCarrierError
  );
  const reordered = Buffer.from(JSON.stringify(rows.toReversed()));
  await assert.rejects(
    collect(
      {
        ...carrier,
        chunks: [
          {
            rows: 2,
            bytes: reordered.length,
            sha256: createHash("sha256").update(reordered).digest("hex"),
          },
        ],
      },
      reordered
    ),
    ApplicationRowCarrierError
  );
});

test("opt-in carrier crosses the unchanged 250000 inline limit in bounded chunks", () => {
  const raw = new Database(":memory:");
  const source = createBetterSqliteAdapter(raw);
  try {
    source.exec(`CREATE TABLE key_value(namespace TEXT,key TEXT,value TEXT);
      CREATE TABLE conversation_turn_nodes(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,parent_id TEXT,
        role TEXT NOT NULL,content_hash TEXT NOT NULL DEFAULT '',last_correlation_id TEXT,
        first_seen_at TEXT NOT NULL,last_seen_at TEXT NOT NULL);
      WITH RECURSIVE n(v) AS (SELECT 1 UNION ALL SELECT v+1 FROM n WHERE v<250001)
      INSERT INTO conversation_turn_nodes SELECT printf('node-%d',v),'c',NULL,'user','h',NULL,'a','b' FROM n;`);
    source.pragma("query_only = ON");
    assert.throws(() => captureSqliteApplicationSnapshot(source));
    let count = 0,
      maxBytes = 0;
    const snapshot = captureSqliteApplicationSnapshot(source, {
      rowSink: {
        writeChunk(_id, _index, bytes) {
          count += JSON.parse(Buffer.from(bytes).toString()).length;
          maxBytes = Math.max(maxBytes, bytes.byteLength);
        },
      },
    });
    assert.equal(count, 250001);
    assert.equal(snapshot.tables.find((t) => t.rowCarrier)?.rowCarrier?.rowCount, count);
    assert.ok(maxBytes < 8 * 1024 * 1024);
  } finally {
    source.close();
  }
});
