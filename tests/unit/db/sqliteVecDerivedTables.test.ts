import assert from "node:assert/strict";
import test from "node:test";
import {
  ApplicationImportError,
  applicationSnapshotDigest,
  captureSqliteApplicationSnapshot,
  checkedApplicationSnapshot,
  type ApplicationStorageSnapshot,
} from "../../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  qualifySqliteVecDerivedStorage,
  SqliteVecQualificationError,
  SQLITE_VEC_CAPTURE_DECODER,
} from "../../../src/lib/db/postgres/sqliteVecDerivedTables.ts";
import { withSqliteVectorSource } from "../../helpers/persistence/sqliteVecSourceFixture.ts";

const schemaFailure = (error: unknown) =>
  error instanceof ApplicationImportError && error.kind === "schema";
const capture = (largeFiles = false) =>
  withSqliteVectorSource({ largeFiles }, captureSqliteApplicationSnapshot);
function metadata(snapshot: ApplicationStorageSnapshot) {
  return snapshot.metadataTables.find((table) => table.name === "vec_memories_info")!;
}

test("actual pinned vector layout captures logical embeddings, bytes, definitions and producer metadata on both drivers", () => {
  // The extension has a shared SQLite API binding; do not load both drivers concurrently.
  const better = capture(),
    native = capture(true);
  assert.equal(applicationSnapshotDigest(better), applicationSnapshotDigest(native));
  const vectors = native.tables.find((table) => table.name === "memory_vectors")!;
  assert.deepEqual(
    vectors.rows.map((row) => row.values),
    [
      ["first", "fixture-signature", { kind: "vector", value: [1, 2] }],
      ["second", "fixture-signature", { kind: "vector", value: [-0.5, 4.25] }],
    ]
  );
  const raw = native.metadataTables.find((table) => table.name === "vec_memories")!;
  assert.deepEqual(
    raw.rows.map((row) => row.rowid),
    ["7", "21"]
  );
  assert.deepEqual(
    raw.rows.map((row) => row.values),
    [
      [
        {
          kind: "bytes",
          value: Buffer.from(new Float32Array([1, 2]).buffer).toString("base64"),
        },
      ],
      [
        {
          kind: "bytes",
          value: Buffer.from(new Float32Array([-0.5, 4.25]).buffer).toString("base64"),
        },
      ],
    ]
  );
  assert.deepEqual(
    metadata(native).rows.map((row) => row.values),
    [
      ["CREATE_VERSION", SQLITE_VEC_CAPTURE_DECODER],
      ["CREATE_VERSION_MAJOR", { kind: "integer", value: "0" }],
      ["CREATE_VERSION_MINOR", { kind: "integer", value: "1" }],
      ["CREATE_VERSION_PATCH", { kind: "integer", value: "9" }],
    ]
  );
  const names = ["vec_memories_chunks", "vec_memories_rowids", "vec_memories_vector_chunks00"];
  assert.deepEqual(
    native.excluded.filter((row) => names.includes(row.name)).map((row) => row.name),
    names
  );
  for (const name of [...names, "vec_memories_info", "vec_memories"]) {
    assert.ok(native.definitions.some((row) => row.name === name && row.sql));
    assert.equal(
      native.tables.some((row) => row.name === name),
      false
    );
  }
  assert.ok(
    native.excluded
      .filter((row) => names.includes(row.name))
      .every(
        (row) =>
          row.reason.includes("sqlite-vec v0.1.9") && row.reason.includes("source producer v0.1.9")
      )
  );
});

test("self-consistent old producer metadata is preserved while the current decoder reads embeddings", () => {
  for (const largeFiles of [false, true]) {
    const snapshot = withSqliteVectorSource(
      {
        largeFiles,
        mutate(writer) {
          writer.exec(
            "UPDATE vec_memories_info SET value='v0.1.8-alpha.1+source' WHERE key='CREATE_VERSION'; UPDATE vec_memories_info SET value=8 WHERE key='CREATE_VERSION_PATCH'"
          );
        },
      },
      captureSqliteApplicationSnapshot
    );
    assert.equal(metadata(snapshot).rows[0].values[1], "v0.1.8-alpha.1+source");
    assert.deepEqual(
      snapshot.tables.find((table) => table.name === "memory_vectors")?.rows[0].values[2],
      { kind: "vector", value: [1, 2] }
    );
    assert.ok(
      snapshot.excluded.some((row) => row.reason.includes("source producer v0.1.8-alpha.1+source"))
    );
  }
});

test("vector-prefixed user tables are never treated as derived storage", () => {
  for (const withoutParent of [false, true])
    for (const largeFiles of [false, true]) {
      assert.throws(
        () =>
          withSqliteVectorSource(
            {
              largeFiles,
              withoutParent,
              mutate(writer) {
                writer.exec(
                  "CREATE TABLE vec_memories_user_data(id TEXT PRIMARY KEY,content TEXT); INSERT INTO vec_memories_user_data VALUES('private','must not disappear')"
                );
              },
            },
            captureSqliteApplicationSnapshot
          ),
        (error: unknown) =>
          schemaFailure(error) &&
          (error as ApplicationImportError).table === "vec_memories_user_data"
      );
    }
});

test("producer metadata rejects extra keys, wrong storage classes and inconsistent or malformed versions", () => {
  const mutations = [
    "INSERT INTO vec_memories_info VALUES('UNRECOGNIZED',1)",
    "UPDATE vec_memories_info SET value=CAST('9' AS BLOB) WHERE key='CREATE_VERSION_PATCH'",
    "UPDATE vec_memories_info SET value=8 WHERE key='CREATE_VERSION_PATCH'",
    "UPDATE vec_memories_info SET value='v0.1.09' WHERE key='CREATE_VERSION'",
    "UPDATE vec_memories_info SET value='v0.1.9-alpha.01' WHERE key='CREATE_VERSION'",
    "DELETE FROM vec_memories_info WHERE key='CREATE_VERSION_MINOR'",
    "UPDATE vec_memories_info SET value='v0.1.9+' || printf('%0130d',1) WHERE key='CREATE_VERSION'",
  ];
  for (const largeFiles of [false, true])
    for (const mutation of mutations) {
      assert.throws(
        () =>
          withSqliteVectorSource(
            {
              largeFiles,
              mutate(writer) {
                writer.exec(mutation);
              },
            },
            captureSqliteApplicationSnapshot
          ),
        schemaFailure
      );
    }
});

test("known derived names require exact physical definitions and recognized parent declaration", () => {
  for (const largeFiles of [false, true]) {
    assert.throws(
      () =>
        withSqliteVectorSource(
          {
            largeFiles,
            mutate(writer) {
              writer.exec("ALTER TABLE vec_memories_info ADD COLUMN extra TEXT");
            },
          },
          captureSqliteApplicationSnapshot
        ),
      schemaFailure
    );
    assert.throws(
      () =>
        withSqliteVectorSource(
          { largeFiles, declaration: "embedding float[2], extra text" },
          captureSqliteApplicationSnapshot
        ),
      schemaFailure
    );
    assert.throws(
      () =>
        withSqliteVectorSource(
          {
            largeFiles,
            withoutParent: true,
            mutate(writer) {
              writer.exec('CREATE TABLE "vec_memories_info" (key text primary key, value any)');
              writer.exec("INSERT INTO vec_memories_info VALUES('CREATE_VERSION','v0.1.9')");
            },
          },
          captureSqliteApplicationSnapshot
        ),
      schemaFailure
    );
  }
});

test("qualified ownership does not bypass existing orphan and int8 vector rejection", () => {
  for (const largeFiles of [false, true]) {
    assert.throws(
      () =>
        withSqliteVectorSource(
          {
            largeFiles,
            mutate(writer) {
              writer
                .prepare("INSERT INTO vec_memories(rowid,embedding) VALUES(99,?)")
                .run(new Float32Array([2, 3]));
            },
          },
          captureSqliteApplicationSnapshot
        ),
      (error: unknown) =>
        error instanceof ApplicationImportError &&
        error.kind === "verification" &&
        error.table === "vec_memories"
    );
    assert.throws(
      () =>
        withSqliteVectorSource(
          { largeFiles, declaration: "embedding int8[2]" },
          captureSqliteApplicationSnapshot
        ),
      (error: unknown) =>
        error instanceof ApplicationImportError &&
        error.kind === "value" &&
        error.column === "embedding"
    );
  }
});

test("JSON metadata admission requires the evidenced parent, exact layout and four consistent fields", () => {
  const original = capture(true);
  assert.deepEqual(checkedApplicationSnapshot(original), original);
  const mutations: ((snapshot: ApplicationStorageSnapshot) => void)[] = [
    (snapshot) => {
      snapshot.definitions = snapshot.definitions.filter((row) => row.name !== "vec_memories");
    },
    (snapshot) => {
      snapshot.definitions = snapshot.definitions.filter(
        (row) => row.name !== "vec_memories_chunks"
      );
    },
    (snapshot) => {
      snapshot.definitions.find((row) => row.name === "vec_memories_info")!.sql += " ";
    },
    (snapshot) => {
      snapshot.definitions.find((row) => row.name === "vec_memories")!.sql =
        "CREATE VIRTUAL TABLE vec_memories USING vec0(embedding float[2], extra text)";
    },
    (snapshot) => {
      metadata(snapshot).rows[3].values[1] = { kind: "integer", value: "8" };
    },
    (snapshot) => {
      metadata(snapshot).rows.push({
        rowid: "5",
        values: ["unknown", "value"],
      });
    },
    (snapshot) => {
      metadata(snapshot).columns = ["value", "key"];
    },
  ];
  for (const mutate of mutations) {
    const snapshot = structuredClone(original);
    mutate(snapshot);
    assert.throws(() => checkedApplicationSnapshot(snapshot), schemaFailure);
  }
});

test("physical proof does not accept missing children or an unqualified current decoder", () => {
  withSqliteVectorSource({ largeFiles: true }, (source) => {
    const definitions = source
      .prepare("SELECT name,type,sql FROM sqlite_schema")
      .all() as ApplicationStorageSnapshot["definitions"];
    const tables = source.pragma("table_list") as {
      name: string;
      type: string;
      wr: number;
    }[];
    assert.throws(
      () =>
        qualifySqliteVecDerivedStorage(
          source,
          definitions,
          tables.filter((row) => row.name !== "vec_memories_chunks")
        ),
      SqliteVecQualificationError
    );
    (source.raw as { function(name: string, callback: () => string): void }).function(
      "vec_version",
      () => "v0.2.0"
    );
    assert.throws(
      () => qualifySqliteVecDerivedStorage(source, definitions, tables),
      SqliteVecQualificationError
    );
  });
});
