import assert from "node:assert/strict";
import test from "node:test";
import {
  PostgresDatabase,
  PostgresError,
  type PostgresClient,
  type PostgresPool,
  type PostgresResult,
} from "../../src/lib/db/postgres/database.ts";
import {
  parsePostgresConfiguration,
  postgresPoolConfiguration,
} from "../../src/lib/db/postgres/config.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function withDeadline<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Client-termination oracle deadline exceeded")),
          6000
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

test(
  "independent backend termination after awaited SQL is a connection failure, not invalid scope",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  async () => {
    const configuration = parsePostgresConfiguration(
      await testPostgresOptions({
        maxConnections: 1,
        idleInTransactionTimeoutMs: 20000,
        transactionTimeoutMs: 25000,
      })
    );
    const { Pool } = await import("pg");
    const driver = new Pool(postgresPoolConfiguration(configuration));
    const clientError = deferred<Error>();
    const commands: string[] = [];
    const releases: boolean[] = [];
    const pool: PostgresPool = {
      async connect() {
        const client = await driver.connect();
        client.once("error", (error: Error) => clientError.resolve(error));
        const observed: PostgresClient = {
          async query<Row>(request) {
            commands.push(request.text);
            return (await client.query(request)) as PostgresResult<Row>;
          },
          release(destroy = false) {
            releases.push(destroy);
            client.release(destroy);
          },
          on(event, listener) {
            return client.on(event, listener);
          },
          removeListener(event, listener) {
            return client.removeListener(event, listener);
          },
        };
        return observed;
      },
      end() {
        return driver.end();
      },
      on(event, listener) {
        return driver.on(event, listener);
      },
    };
    const database = new PostgresDatabase(pool, configuration, { warn() {} });
    const administrator = await createTestPostgresDatabase();
    const entered = deferred<number>();
    const release = deferred<void>();
    let executions = 0;
    const transaction = database.transaction(async (tx) => {
      executions++;
      const result = await tx.query<{ pid: number }>("SELECT pg_backend_pid() AS pid");
      entered.resolve(result.rows[0].pid);
      await release.promise;
      return "must not commit";
    });
    void transaction.catch(() => {});
    try {
      const pid = await withDeadline(entered.promise);
      const termination = await administrator.transaction((tx) =>
        tx.query<{ terminated: boolean }>("SELECT pg_terminate_backend($1) AS terminated", [pid])
      );
      assert.equal(termination.rows[0].terminated, true);
      const raw = (await withDeadline(clientError.promise)) as Error & {
        code?: unknown;
        severity?: unknown;
      };
      assert.equal(raw.code, "57P01");
      assert.equal(raw.severity, "FATAL");
      release.resolve();
      await assert.rejects(transaction, (error: unknown) => {
        assert.ok(error instanceof PostgresError);
        assert.equal(error.kind, "query_failed");
        assert.equal(error.sqlState, "57P01");
        assert.equal(error.message, "PostgreSQL query failed");
        assert.equal(error.cause, undefined);
        assert.ok(!JSON.stringify(error).includes(raw.message));
        return true;
      });
      assert.equal(executions, 1);
      assert.deepEqual(commands, ["BEGIN", "SELECT pg_backend_pid() AS pid"]);
      assert.deepEqual(releases, [true]);
      assert.equal(
        (await database.transaction((tx) => tx.query<{ answer: number }>("SELECT 42 AS answer")))
          .rows[0].answer,
        42,
        "destroying the failed client releases admission for a new healthy connection"
      );
    } finally {
      release.resolve();
      await Promise.allSettled([transaction]);
      await Promise.all([database.close(), administrator.close()]);
    }
  }
);
