import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import {
  PostgresDatabase,
  PostgresError,
  type PostgresClient,
  type PostgresPool,
  type PostgresResult,
  type PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function fixture(query?: (text: string) => Promise<PostgresResult>, timeout = 500) {
  const calls: string[] = [];
  const releases: boolean[] = [];
  const warnings: unknown[] = [];
  const events = new EventEmitter();
  const poolEvents = new EventEmitter();
  let connections = 0;
  let ended = 0;
  const client: PostgresClient = {
    async query<Row>(request: { text: string; values?: unknown[]; queryMode: "extended" }) {
      assert.equal(request.queryMode, "extended");
      const { text } = request;
      calls.push(text);
      const result = query ? await query(text) : { command: text, rows: [], rowCount: 0 };
      return result as PostgresResult<Row>;
    },
    release(destroy = false) {
      releases.push(destroy);
    },
    on: events.on.bind(events),
    removeListener: events.removeListener.bind(events),
  };
  const pool: PostgresPool = {
    async connect() {
      connections++;
      return client;
    },
    async end() {
      ended++;
    },
    on: poolEvents.on.bind(poolEvents),
  };
  const database = new PostgresDatabase(
    pool,
    { maxConnections: 1, transactionTimeoutMs: timeout },
    {
      warn: (...args: unknown[]) => {
        warnings.push(args);
      },
    }
  );
  return {
    database,
    pool,
    calls,
    releases,
    warnings,
    events,
    poolEvents,
    connections: () => connections,
    ended: () => ended,
  };
}

function result(command: string): PostgresResult {
  return { command, rows: [], rowCount: 0 };
}

function errorKind(kind: PostgresError["kind"]) {
  return (error: unknown) => error instanceof PostgresError && error.kind === kind;
}

test("commits once on the leased client and revokes the escaped scope", async () => {
  const f = fixture();
  let escaped!: PostgresTransaction;
  assert.equal(
    await f.database.transaction(async (tx) => {
      escaped = tx;
      await tx.query("SELECT 1");
      return 42;
    }),
    42
  );
  assert.deepEqual(f.calls, ["BEGIN", "SELECT 1", "COMMIT"]);
  await assert.rejects(escaped.query("SELECT 2"), errorKind("invalid_scope"));
  assert.equal(f.connections(), 1);
  assert.deepEqual(f.releases, [false]);
  await f.database.close();
});

test("callback error rolls back and preserves the domain error", async () => {
  const f = fixture();
  const failure = new Error("domain conflict");
  await assert.rejects(
    f.database.transaction(async () => {
      throw failure;
    }),
    (error) => error === failure
  );
  assert.deepEqual(f.calls, ["BEGIN", "ROLLBACK"]);
  assert.deepEqual(f.releases, [false]);
  await f.database.close();
});

test("a swallowed SQL failure still aborts the transaction and redacts driver details", async () => {
  const f = fixture(async (text) => {
    if (text === "INSERT")
      throw Object.assign(new Error("secret-sentinel"), {
        code: "23505",
        severity: "ERROR",
        detail: "private row contents",
        query: "sensitive SQL",
      });
    return result(text);
  });
  await assert.rejects(
    f.database.transaction(async (tx) => {
      await tx.query("INSERT").catch(() => {});
      return "must not succeed";
    }),
    (error: PostgresError) => {
      assert.equal(error.kind, "query_failed");
      assert.equal(error.sqlState, "23505");
      assert.equal(error.cause, undefined);
      assert.doesNotMatch(JSON.stringify(error), /sentinel|private|sensitive/);
      return true;
    }
  );
  assert.deepEqual(f.calls, ["BEGIN", "INSERT", "ROLLBACK"]);
  await f.database.close();
});

for (const [code, expected] of [
  [undefined, "commit_unknown"],
  ["08006", "commit_unknown"],
  ["57P01", "commit_unknown"],
  ["40003", "commit_unknown"],
  ["40001", "transaction_failed"],
  ["23503", "transaction_failed"],
] as const) {
  test(`COMMIT failure ${code ?? "without SQLSTATE"} is ${expected} and never retried`, async () => {
    const f = fixture(async (text) => {
      if (text === "COMMIT") throw Object.assign(new Error("secret"), { code, severity: "ERROR" });
      return result(text);
    });
    let executions = 0;
    await assert.rejects(
      f.database.transaction(async () => {
        executions++;
      }),
      errorKind(expected)
    );
    assert.equal(executions, 1);
    assert.deepEqual(f.calls, ["BEGIN", "COMMIT"]);
    assert.deepEqual(f.releases, [true]);
    await f.database.close();
  });
}

test("a ROLLBACK command tag from COMMIT never acknowledges success", async () => {
  const f = fixture(async (text) => result(text === "COMMIT" ? "ROLLBACK" : text));
  await assert.rejects(
    f.database.transaction(async () => 1),
    errorKind("transaction_failed")
  );
  assert.deepEqual(f.releases, [true]);
  await f.database.close();
});

for (const phase of ["BEGIN", "SELECT 1"]) {
  test(`a ${phase} read timeout destroys the client without queuing ROLLBACK`, async () => {
    const f = fixture(async (text) => {
      if (text === phase) throw new Error("Query read timeout");
      return result(text);
    });
    await assert.rejects(
      f.database.transaction((tx) => tx.query("SELECT 1")),
      errorKind("query_failed")
    );
    assert.equal(f.calls.includes("ROLLBACK"), false);
    assert.deepEqual(f.releases, [true]);
    await f.database.close();
  });
}

test("unfinished query cannot outlive a successful transaction", async () => {
  const pending = deferred<PostgresResult>();
  const f = fixture(async (text) => (text === "SLOW" ? pending.promise : result(text)));
  await assert.rejects(
    f.database.transaction(async (tx) => {
      void tx.query("SLOW");
    }),
    errorKind("invalid_scope")
  );
  assert.deepEqual(f.calls, ["BEGIN", "SLOW"]);
  assert.deepEqual(f.releases, [true]);
  pending.resolve(result("SELECT"));
  await f.database.close();
});

test("EPIPE is a transport failure, not a five-character SQLSTATE", async () => {
  const f = fixture(async (text) => {
    if (text === "WRITE") throw Object.assign(new Error("broken pipe"), { code: "EPIPE" });
    return result(text);
  });
  await assert.rejects(
    f.database.transaction((tx) => tx.query("WRITE")),
    (error: PostgresError) => error.kind === "query_failed" && error.sqlState === undefined
  );
  assert.deepEqual(f.calls, ["BEGIN", "WRITE"]);
  assert.deepEqual(f.releases, [true]);
  await f.database.close();
});

test("a FATAL response destroys the connection even when it has a SQLSTATE", async () => {
  const f = fixture(async (text) => {
    if (text === "WRITE")
      throw Object.assign(new Error("terminating connection"), {
        code: "57P01",
        severity: "FATAL",
      });
    return result(text);
  });
  await assert.rejects(
    f.database.transaction((tx) => tx.query("WRITE")),
    errorKind("query_failed")
  );
  assert.deepEqual(f.calls, ["BEGIN", "WRITE"]);
  assert.deepEqual(f.releases, [true]);
  await f.database.close();
});

test("concurrent queries cannot enqueue unbounded work on one client", async () => {
  const pending = deferred<PostgresResult>();
  const f = fixture(async (text) => (text === "SLOW" ? pending.promise : result(text)));
  await assert.rejects(
    f.database.transaction(async (tx) => {
      const first = tx.query("SLOW");
      await assert.rejects(tx.query("SECOND"), errorKind("invalid_scope"));
      pending.resolve(result("SELECT"));
      await first;
    }),
    errorKind("invalid_scope")
  );
  assert.equal(f.calls.includes("SECOND"), false);
  assert.equal(f.calls.includes("COMMIT"), false);
  await f.database.close();
});

test("callback deadline revokes late work and allows close to drain", async () => {
  const wake = deferred<void>();
  const late = deferred<void>();
  const f = fixture(undefined, 25);
  const transaction = f.database.transaction(async (tx) => {
    await wake.promise;
    await assert.rejects(tx.query("TOO LATE"), errorKind("invalid_scope"));
    late.resolve();
  });
  await assert.rejects(transaction, errorKind("transaction_timeout"));
  await f.database.close();
  wake.resolve();
  await late.promise;
  assert.deepEqual(f.calls, ["BEGIN"]);
  assert.deepEqual(f.releases, [true]);
});

test("admission is bounded during connect and close drains before ending the pool", async () => {
  const f = fixture();
  const originalConnect = f.pool.connect;
  const connect = deferred<void>();
  f.pool.connect = async () => {
    await connect.promise;
    return originalConnect();
  };
  const operation = f.database.transaction(async () => 42);
  await assert.rejects(
    f.database.transaction(async () => 0),
    errorKind("overloaded")
  );
  const close = f.database.close();
  assert.equal(f.database.close(), close);
  await assert.rejects(
    f.database.transaction(async () => 0),
    errorKind("closed")
  );
  assert.equal(f.ended(), 0);
  connect.resolve();
  assert.equal(await operation, 42);
  await close;
  assert.equal(f.ended(), 1);
});

test("connection rejection releases admission and never exposes credentials", async () => {
  const f = fixture();
  const original = f.pool.connect;
  f.pool.connect = async () => {
    throw new Error("secret-sentinel");
  };
  await assert.rejects(
    f.database.transaction(async () => 1),
    errorKind("unavailable")
  );
  f.pool.connect = original;
  assert.equal(await f.database.transaction(async () => 2), 2);
  await f.database.close();
});

test("checked-out and idle errors are handled without logging driver details", async () => {
  const f = fixture();
  await assert.rejects(
    f.database.transaction(async () => {
      f.events.emit("error", new Error("private credentials"));
      await delay(1);
    }),
    errorKind("query_failed")
  );
  assert.deepEqual(f.releases, [true]);
  f.poolEvents.emit(
    "error",
    Object.assign(new Error("private credentials"), {
      code: "57P01",
      severity: "FATAL",
    })
  );
  assert.match(JSON.stringify(f.warnings), /57P01/);
  assert.doesNotMatch(JSON.stringify(f.warnings), /private credentials/);
  await f.database.close();
});
