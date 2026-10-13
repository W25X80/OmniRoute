import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { setImmediate as settle } from "node:timers/promises";
import test from "node:test";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { NATIVE_BLOB_LIMITS } from "../../src/lib/db/postgres/nativeBlobCarrier.ts";
import * as files from "../../src/lib/db/postgres/serviceFiles.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
const options = { skip: !postgresTestsEnabled };
test.before(async () => {
  if (!postgresTestsEnabled) return;
  database = await createTestPostgresDatabase({ maxConnections: 3, lockTimeoutMs: 3000 });
  await migratePostgresSchema(database);
});
test.after(async () => {
  await database?.close();
});

async function saved(content: Buffer | null) {
  return files.createFile(database, {
    bytes: 0,
    filename: "content.bin",
    purpose: "batch",
    content,
  });
}
async function remove(id: string) {
  await database.transaction((transaction) =>
    transaction.query("DELETE FROM omniroute.files WHERE id = $1", [id])
  );
}

test(
  "native PostgreSQL reads preserve empty, null, binary and deleted content",
  options,
  async () => {
    const payload = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 113);
    for (let index = 0; index < payload.length; index++) payload[index] = index % 251;
    for (const expected of [null, Buffer.alloc(0), payload]) {
      const file = await saved(expected);
      try {
        assert.deepEqual(await files.getFileContent(database, file.id), expected);
        await database.transaction((transaction) => files.deleteFile(transaction, file.id));
        assert.equal(await files.getFileContent(database, file.id), null);
      } finally {
        await remove(file.id);
      }
    }
    assert.equal(await files.getFileContent(database, "missing-content-fixture"), null);
  }
);

test(
  "the complete chunk read retains its row version against concurrent soft delete",
  options,
  async (t) => {
    const payload = Buffer.alloc(NATIVE_BLOB_LIMITS.chunkBytes + 17, 255);
    const file = await saved(payload);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let locked!: () => void;
    const hasLock = new Promise<void>((resolve) => {
      locked = resolve;
    });
    let readerPid = 0;
    let writerPid = 0;
    let writerReady!: () => void;
    const writerEntered = new Promise<void>((resolve) => {
      writerReady = resolve;
    });
    let reader: Promise<Buffer | null> | undefined;
    let writer: Promise<boolean> | undefined;
    t.after(() => release());
    try {
      const observedDatabase = new Proxy(database, {
        get(target, property, receiver) {
          if (property !== "transaction") return Reflect.get(target, property, receiver);
          return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
            target.transaction(async (transaction) => {
              readerPid = (
                await transaction.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")
              ).rows[0].pid;
              const observed: PostgresTransaction = {
                async query<Row>(text: string, values?: unknown[]) {
                  const result = await transaction.query<Row>(text, values);
                  if (text.includes("octet_length")) {
                    locked();
                    await gate;
                  }
                  return result;
                },
              };
              return body(observed);
            });
        },
      });
      reader = files.getFileContent(observedDatabase, file.id);
      void reader.catch(() => {});
      await hasLock;
      writer = database.transaction(async (transaction) => {
        writerPid = (await transaction.query<{ pid: number }>("SELECT pg_backend_pid() AS pid"))
          .rows[0].pid;
        writerReady();
        return files.deleteFile(transaction, file.id);
      });
      void writer.catch(() => {});
      await writerEntered;
      let blocked = false;
      const deadline = Date.now() + 1500;
      while (!blocked && Date.now() < deadline) {
        blocked = (
          await database.transaction((transaction) =>
            transaction.query<{ blockers: number[] }>(
              "SELECT pg_blocking_pids($1::integer) AS blockers",
              [writerPid]
            )
          )
        ).rows[0].blockers.includes(readerPid);
        if (!blocked) await settle();
      }
      assert.equal(blocked, true, "the writer must wait for the reader's row lock");
      release();
      assert.deepEqual(await reader, payload);
      assert.equal(await writer, true);
      assert.equal(await files.getFileContent(database, file.id), null);
    } finally {
      release();
      await Promise.allSettled([reader, writer].filter((work) => work !== undefined));
      await remove(file.id);
    }
  }
);

test(
  "a committed raw snapshot is owned even when deletion commits before materialization",
  options,
  async () => {
    const original = Buffer.from([19, 0, 255]);
    const file = await saved(original);
    let removed = false;
    const captured = new Proxy(database, {
      get(target, property, receiver) {
        if (property !== "transaction") return Reflect.get(target, property, receiver);
        return async <T>(body: (tx: PostgresTransaction) => Promise<T>) => {
          const snapshot = await target.transaction(body);
          removed = await database.transaction((tx) => files.deleteFile(tx, file.id));
          return snapshot;
        };
      },
    });
    try {
      assert.deepEqual(await files.getFileContent(captured, file.id), original);
      assert.equal(removed, true);
      assert.equal(await files.getFileContent(database, file.id), null);
    } finally {
      await remove(file.id);
    }
  }
);

test(
  "actual 512 MiB native read uses the unchanged runtime transaction budget",
  {
    skip:
      !postgresTestsEnabled || process.env.RUN_PG_IMPORT_MAX_BLOB !== "1"
        ? "requires serial isolated TLS PostgreSQL and explicit maximum-file memory qualification"
        : false,
  },
  async () => {
    // This repository oracle does not assert HTTP multipart admission, Response
    // memory usage or a production throughput guarantee.
    const writer = await createTestPostgresDatabase({
      statementTimeoutMs: 60000,
      queryTimeoutMs: 90000,
      idleInTransactionTimeoutMs: 90000,
      transactionTimeoutMs: 300000,
    });
    const reader = await createTestPostgresDatabase();
    let id: string | undefined;
    try {
      const input = Buffer.alloc(NATIVE_BLOB_LIMITS.fileBytes);
      const file = await files.createFile(writer, {
        bytes: input.byteLength,
        filename: "maximum-content.bin",
        purpose: "batch",
        content: input,
      });
      id = file.id;
      const expected = createHash("sha256").update(input).digest("hex");
      let turns = 0,
        running = true;
      const heartbeat = (async () => {
        while (running) {
          await settle();
          turns++;
        }
      })();
      let content: Buffer | null;
      try {
        content = await files.getFileContent(reader, file.id);
      } finally {
        running = false;
        await heartbeat;
      }
      assert.ok(
        turns >= NATIVE_BLOB_LIMITS.fileBytes / NATIVE_BLOB_LIMITS.chunkBytes,
        "maximum read must allow other event-loop owners to progress between chunks"
      );
      assert.ok(Buffer.isBuffer(content));
      assert.equal(content.byteLength, NATIVE_BLOB_LIMITS.fileBytes);
      assert.equal(createHash("sha256").update(content).digest("hex"), expected);
    } finally {
      try {
        if (id)
          await writer.transaction((transaction) =>
            transaction.query("DELETE FROM omniroute.files WHERE id = $1", [id])
          );
      } finally {
        await Promise.allSettled([reader.close(), writer.close()]);
      }
    }
  }
);
