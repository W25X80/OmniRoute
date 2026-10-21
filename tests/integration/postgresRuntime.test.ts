import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createConnection, createServer, type Socket } from "node:net";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import {
  parsePostgresConfiguration,
  postgresPoolConfiguration,
  type PostgresOptions,
} from "../../src/lib/db/postgres/config.ts";
import { PostgresDatabase, PostgresError } from "../../src/lib/db/postgres/database.ts";

// scripts/test/postgres-runtime.mjs supplies an isolated, disposable loopback server.
const port = Number(process.env.OMNIROUTE_TEST_POSTGRES_PORT);
const caFile = process.env.OMNIROUTE_TEST_POSTGRES_CA;
const enabled = Number.isInteger(port) && port > 0 && port < 65_536 && Boolean(caFile);
const integration = (name: string, body: () => Promise<void>) =>
  test(name, { skip: !enabled, timeout: 15_000 }, body);
const logger = { warn() {} };

async function database(overrides: Partial<PostgresOptions> = {}) {
  return createPostgresDatabase(
    {
      host: "localhost",
      port,
      database: "omniroute_test",
      user: "omniroute_test",
      password: "omniroute_test",
      tls: { mode: "verify-full", ca: await readFile(caFile!, "utf8") },
      ...overrides,
    },
    logger
  );
}

function tableName() {
  return `runtime_${randomUUID().replaceAll("-", "")}`;
}

integration("PostgreSQL 18.6 applies TLS and server-side timeouts", async () => {
  const db = await database();
  try {
    const { rows } = await db.transaction((tx) =>
      tx.query(`SELECT
      current_setting('server_version_num') AS version,
      current_setting('statement_timeout') AS statement,
      current_setting('lock_timeout') AS lock,
      current_setting('transaction_timeout') AS transaction,
      current_setting('TimeZone') AS timezone,
      (SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()) AS tls`)
    );
    assert.deepEqual(rows[0], {
      version: "180006",
      statement: "5s",
      lock: "1s",
      transaction: "15s",
      timezone: "UTC",
      tls: true,
    });
  } finally {
    await db.close();
  }
});

integration("TLS rejects both an untrusted certificate and a hostname mismatch", async () => {
  for (const overrides of [{ tls: { mode: "verify-full" as const } }, { host: "127.0.0.1" }]) {
    const db = await database(overrides);
    try {
      await assert.rejects(
        db.transaction((tx) => tx.query("SELECT 1")),
        (error: PostgresError) => error.kind === "unavailable"
      );
    } finally {
      await db.close();
    }
  }
});

integration(
  "real writes commit, callback errors roll back, and caught SQL errors cannot commit",
  async () => {
    const db = await database();
    const table = tableName();
    try {
      await db.transaction((tx) => tx.query(`CREATE TABLE ${table} (id integer PRIMARY KEY)`));
      await db.transaction((tx) => tx.query(`INSERT INTO ${table} VALUES ($1)`, [1]));
      await assert.rejects(
        db.transaction(async (tx) => {
          await tx.query(`INSERT INTO ${table} VALUES ($1)`, [2]);
          throw new Error("domain error");
        }),
        /domain error/
      );
      await assert.rejects(
        db.transaction(async (tx) => {
          await tx.query(`INSERT INTO ${table} VALUES ($1)`, [3]);
          await tx.query(`INSERT INTO ${table} VALUES ($1)`, [1]).catch(() => {});
        }),
        (error: PostgresError) => error.kind === "query_failed" && error.sqlState === "23505"
      );
      const { rows } = await db.transaction((tx) =>
        tx.query(`SELECT id FROM ${table} ORDER BY id`)
      );
      assert.deepEqual(rows, [{ id: 1 }]);
    } finally {
      await db.transaction((tx) => tx.query(`DROP TABLE IF EXISTS ${table}`));
      await db.close();
    }
  }
);

integration("a server wait does not block the Node event loop or another connection", async () => {
  const db = await database({ maxConnections: 2 });
  let complete = false;
  let ticks = 0;
  const timer = setInterval(() => ticks++, 10);
  try {
    let started!: () => void;
    const ready = new Promise<void>((resolve) => {
      started = resolve;
    });
    const slow = db.transaction(async (tx) => {
      const waiting = tx.query("SELECT pg_sleep(0.5)");
      started();
      await waiting;
      complete = true;
    });
    await ready;
    const { rows } = await db.transaction((tx) => tx.query("SELECT 42 AS answer"));
    assert.deepEqual(rows, [{ answer: 42 }]);
    assert.equal(complete, false);
    await slow;
    assert.ok(ticks > 0);
  } finally {
    clearInterval(timer);
    await db.close();
  }
});

integration("lock timeout rolls back a contender and permits later progress", async () => {
  const db = await database({ maxConnections: 2, lockTimeoutMs: 100 });
  const table = tableName();
  let release!: () => void;
  let locked!: () => void;
  const hold = new Promise<void>((resolve) => {
    release = resolve;
  });
  const ready = new Promise<void>((resolve) => {
    locked = resolve;
  });
  let owner: Promise<void> | undefined;
  try {
    await db.transaction((tx) =>
      tx.query(`CREATE TABLE ${table} (id integer PRIMARY KEY, n integer)`)
    );
    await db.transaction((tx) => tx.query(`INSERT INTO ${table} VALUES (1, 0)`));
    owner = db.transaction(async (tx) => {
      await tx.query(`UPDATE ${table} SET n = n + 1 WHERE id = 1`);
      locked();
      await hold;
    });
    await ready;
    await assert.rejects(
      db.transaction((tx) => tx.query(`UPDATE ${table} SET n = n + 1 WHERE id = 1`)),
      (error: PostgresError) => error.sqlState === "55P03"
    );
    release();
    await owner;
    await db.transaction((tx) => tx.query(`UPDATE ${table} SET n = n + 1 WHERE id = 1`));
    const { rows } = await db.transaction((tx) => tx.query(`SELECT n FROM ${table}`));
    assert.deepEqual(rows, [{ n: 2 }]);
  } finally {
    release();
    await owner;
    await db.transaction((tx) => tx.query(`DROP TABLE IF EXISTS ${table}`));
    await db.close();
  }
});

integration("statement timeout cancels SQL while leaving the event loop responsive", async () => {
  const db = await database({ statementTimeoutMs: 150, lockTimeoutMs: 50, queryTimeoutMs: 1_000 });
  try {
    await assert.rejects(
      db.transaction((tx) => tx.query("SELECT pg_sleep(5)")),
      (error: PostgresError) => error.sqlState === "57014"
    );
    assert.equal((await db.transaction((tx) => tx.query("SELECT 1 AS n"))).rows[0].n, 1);
  } finally {
    await db.close();
  }
});

integration(
  "extended protocol rejects multi-statement text before any statement executes",
  async () => {
    const db = await database();
    const table = tableName();
    try {
      await db.transaction((tx) => tx.query(`CREATE TABLE ${table} (id integer)`));
      await assert.rejects(
        db.transaction((tx) => tx.query(`INSERT INTO ${table} VALUES (1); SELECT 2`)),
        (error: PostgresError) => error.sqlState === "42601"
      );
      assert.deepEqual(
        (await db.transaction((tx) => tx.query(`SELECT id FROM ${table}`))).rows,
        []
      );
    } finally {
      await db.transaction((tx) => tx.query(`DROP TABLE IF EXISTS ${table}`));
      await db.close();
    }
  }
);

// Forward PostgreSQL frames until a committed transaction's acknowledgement is about to
// reach the client, then drop the connection. The independent direct connection is the oracle.
async function loseCommitReply() {
  const sockets = new Set<Socket>();
  let dropped = false;
  const server = createServer((client) => {
    const upstream = createConnection({ host: "127.0.0.1", port });
    sockets.add(client);
    sockets.add(upstream);
    for (const socket of [client, upstream]) {
      socket.on("error", () => {
        client.destroy();
        upstream.destroy();
      });
      socket.on("close", () => {
        sockets.delete(socket);
        client.destroy();
        upstream.destroy();
      });
    }
    client.pipe(upstream);
    let buffered = Buffer.alloc(0);
    upstream.on("data", (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      while (buffered.length >= 5) {
        const length = buffered.readInt32BE(1) + 1;
        if (buffered.length < length) return;
        const frame = buffered.subarray(0, length);
        buffered = buffered.subarray(length);
        if (frame[0] === 67 && frame.subarray(5).toString() === "COMMIT\0") {
          dropped = true;
          client.destroy();
          upstream.destroy();
          return;
        }
        client.write(frame);
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== "string");
  return {
    port: address.port,
    dropped: () => dropped,
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve()))
      );
    },
  };
}

integration(
  "lost COMMIT acknowledgement is unknown even though the row exists, with no replay",
  async () => {
    const direct = await database();
    const proxy = await loseCommitReply();
    const uncertain = await database({
      host: "127.0.0.1",
      port: proxy.port,
      tls: { mode: "disable-local" },
    });
    const table = tableName();
    let attempts = 0;
    try {
      await direct.transaction((tx) => tx.query(`CREATE TABLE ${table} (id integer)`));
      await assert.rejects(
        uncertain.transaction(async (tx) => {
          attempts++;
          await tx.query(`INSERT INTO ${table} VALUES (1)`);
        }),
        (error: PostgresError) => error.kind === "commit_unknown"
      );
      assert.equal(proxy.dropped(), true);
      assert.equal(attempts, 1);
      const { rows } = await direct.transaction((tx) => tx.query(`SELECT id FROM ${table}`));
      assert.deepEqual(rows, [{ id: 1 }]);
    } finally {
      await uncertain.close();
      await proxy.close();
      await direct.transaction((tx) => tx.query(`DROP TABLE IF EXISTS ${table}`));
      await direct.close();
    }
  }
);

integration(
  "idle transaction termination is handled without an unhandled client error",
  async () => {
    const db = await database({ idleInTransactionTimeoutMs: 100 });
    try {
      await assert.rejects(
        db.transaction(async () => {
          await delay(350);
        }),
        PostgresError
      );
      assert.equal((await db.transaction((tx) => tx.query("SELECT 1 AS n"))).rows[0].n, 1);
    } finally {
      await db.close();
    }
  }
);

integration("idle termination during a pending COMMIT proves rollback without replay", async () => {
  const { Pool } = await import("pg");
  const config = parsePostgresConfiguration({
    host: "localhost",
    port,
    database: "omniroute_test",
    user: "omniroute_test",
    password: "omniroute_test",
    idleInTransactionTimeoutMs: 100,
    tls: { mode: "verify-full", ca: await readFile(caFile!, "utf8") },
  });
  const pool = new Pool(postgresPoolConfiguration(config));
  let commitAttempts = 0;
  let destroyed = false;
  // Suppress only outbound COMMIT frames, after pg has installed its active query.
  // The real TLS server is then idle in an open transaction and sends FATAL 25P03
  // to that pending query. This is the dual of the lost-acknowledgement test above:
  // a separate direct connection must observe no write, rather than a committed row.
  const db = new PostgresDatabase(
    {
      on: (event, listener) => pool.on(event, listener),
      end: () => pool.end(),
      async connect() {
        const client = await pool.connect();
        const wire = (
          client as unknown as {
            connection: { _send: (buffer: Buffer) => boolean };
          }
        ).connection;
        const send = wire._send;
        let withholding = false;
        wire._send = function (buffer) {
          return withholding ? true : send.call(this, buffer);
        };
        return {
          query(query) {
            if (query.text === "COMMIT") {
              withholding = true;
              commitAttempts++;
            }
            return client.query(query);
          },
          on: (event, listener) => client.on(event, listener),
          removeListener: (event, listener) => client.removeListener(event, listener),
          release(destroy) {
            destroyed ||= destroy === true;
            wire._send = send;
            client.release(destroy);
          },
        };
      },
    },
    config,
    logger
  );
  const direct = await database();
  const table = tableName();
  let callbacks = 0;
  try {
    await direct.transaction((tx) => tx.query(`CREATE TABLE ${table} (id integer)`));
    await assert.rejects(
      db.transaction(async (tx) => {
        callbacks++;
        assert.equal(
          (await tx.query("SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()")).rows[0].ssl,
          true
        );
        await tx.query(`INSERT INTO ${table} VALUES (1)`);
      }),
      (error: PostgresError) => error.kind === "transaction_failed" && error.sqlState === "25P03"
    );
    assert.equal(commitAttempts, 1);
    assert.equal(callbacks, 1);
    assert.equal(destroyed, true);
    assert.deepEqual(
      (await direct.transaction((tx) => tx.query(`SELECT id FROM ${table}`))).rows,
      []
    );
  } finally {
    await db.close();
    await direct.transaction((tx) => tx.query(`DROP TABLE IF EXISTS ${table}`));
    await direct.close();
  }
});
