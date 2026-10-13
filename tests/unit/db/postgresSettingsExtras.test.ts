import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import Database from "better-sqlite3";
import {
  PostgresDatabase,
  type PostgresClient,
  type PostgresPool,
  type PostgresResult,
} from "../../../src/lib/db/postgres/database.ts";
import {
  readPostgresKeyValue,
  writePostgresKeyValue,
  deletePostgresKeyValue,
  withPostgresKeyValueMutation,
} from "../../../src/lib/db/postgres/keyValueRepository.ts";
import {
  cacheTrendHour,
  readPostgresCacheMetrics,
  readPostgresCacheTrend,
} from "../../../src/lib/db/postgres/cacheMetricsRepository.ts";

function fixture(reply: (text: string, values?: unknown[]) => PostgresResult) {
  const calls: { text: string; values?: unknown[] }[] = [];
  const releases: boolean[] = [];
  const events = new EventEmitter();
  const client: PostgresClient = {
    async query<Row>(request: { text: string; values?: unknown[]; queryMode: "extended" }) {
      assert.equal(request.queryMode, "extended");
      calls.push({ text: request.text, values: request.values });
      return (
        request.text === "BEGIN" || request.text === "COMMIT" || request.text === "ROLLBACK"
          ? { command: request.text, rows: [], rowCount: 0 }
          : reply(request.text, request.values)
      ) as PostgresResult<Row>;
    },
    release(destroy = false) {
      releases.push(destroy);
    },
    on: events.on.bind(events),
    removeListener: events.removeListener.bind(events),
  };
  const pool: PostgresPool = {
    async connect() {
      return client;
    },
    async end() {},
    on: events.on.bind(events),
  };
  const database = new PostgresDatabase(
    pool,
    { maxConnections: 1, transactionTimeoutMs: 500 },
    { warn() {} }
  );
  return { database, calls, releases };
}

function result(rows: Record<string, unknown>[] = [], rowCount = rows.length): PostgresResult {
  return { command: "SELECT", rows, rowCount };
}

test("KV absent-row mutation uses one leased transaction, lock 20 and parameterized values", async () => {
  const f = fixture(() => result());
  const namespace = "settings'; DROP TABLE key_value; --";
  const key = "key-$1";
  const value = '{"literal":"secret-sentinel"}';
  assert.equal(
    await withPostgresKeyValueMutation(f.database, async (transaction) => {
      assert.equal(await readPostgresKeyValue(transaction, namespace, key), undefined);
      await writePostgresKeyValue(transaction, namespace, key, value);
      return deletePostgresKeyValue(transaction, namespace, key);
    }),
    false
  );
  assert.equal(f.calls[0].text, "BEGIN");
  assert.equal(f.calls[1].text, "SELECT pg_advisory_xact_lock(18765,$1)");
  assert.deepEqual(f.calls[1].values, [20]);
  assert.deepEqual(f.calls[2].values, [namespace, key]);
  assert.deepEqual(f.calls[3].values, [namespace, key, value]);
  assert.deepEqual(f.calls[4].values, [namespace, key]);
  assert.equal(f.calls[5].text, "COMMIT");
  assert.ok(f.calls.every((call) => !call.text.includes(namespace) && !call.text.includes(value)));
  assert.deepEqual(f.releases, [false]);
  await f.database.close();
});

test("KV callback failure rolls back without replay", async () => {
  const f = fixture(() => result());
  const failure = new Error("domain-conflict");
  await assert.rejects(
    withPostgresKeyValueMutation(f.database, async (transaction) => {
      await writePostgresKeyValue(transaction, "settings", "one", "1");
      throw failure;
    }),
    (error) => error === failure
  );
  assert.deepEqual(
    f.calls.map((call) => (call.text === "BEGIN" || call.text === "ROLLBACK" ? call.text : "DATA")),
    ["BEGIN", "DATA", "DATA", "ROLLBACK"]
  );
  await f.database.close();
});

test("native cache aggregate decodes PostgreSQL numeric strings and preserves source savings policy", async () => {
  let query = 0;
  const f = fixture(() => {
    switch (++query) {
      case 1:
        return result([
          {
            totalRequests: "2",
            totalInputTokens: "700",
            totalCachedTokens: "1000000",
            totalCacheCreationTokens: null,
          },
        ]);
      case 2:
        return result([{ totalRequests: "4" }]);
      case 3:
        return result([
          {
            provider: "p",
            totalRequests: "3",
            cachedRequests: "2",
            inputTokens: "700",
            cachedTokens: "1000000",
            cacheCreationTokens: null,
          },
        ]);
      default:
        return result([
          {
            strategy: "direct",
            requests: "2",
            inputTokens: "700",
            cachedTokens: "1000000",
            cacheCreationTokens: null,
          },
        ]);
    }
  });
  const actual = await readPostgresCacheMetrics(f.database);
  assert.equal(actual.totalRequests, 4);
  assert.equal(actual.requestsWithCacheControl, 2);
  assert.equal(actual.tokensSaved, 1000000);
  assert.equal(actual.estimatedCostSaved, 2.7);
  assert.deepEqual(actual.byProvider.p, {
    requests: 2,
    totalRequests: 3,
    cachedRequests: 2,
    inputTokens: 700,
    cachedTokens: 1000000,
    cacheCreationTokens: 0,
  });
  assert.deepEqual(actual.byStrategy.direct, {
    requests: 2,
    inputTokens: 700,
    cachedTokens: 1000000,
    cacheCreationTokens: 0,
  });
  assert.equal(f.calls.at(-1)?.text, "COMMIT");
  await f.database.close();
});

test("native cache trend retains lexical cutoff, null malformed bucket and aggregate numbers", async () => {
  const f = fixture(() =>
    result([
      {
        timestamp: "2026-10-11T03:23:00Z",
        tokens_input: "2",
        tokens_cache_read: "5",
        tokens_cache_creation: null,
      },
      {
        timestamp: "2026-10-11T03:59:00Z",
        tokens_input: null,
        tokens_cache_read: "0",
        tokens_cache_creation: "7",
      },
      {
        timestamp: "not-a-date",
        tokens_input: "8",
        tokens_cache_read: "0",
        tokens_cache_creation: "0",
      },
    ])
  );
  const actual = await readPostgresCacheTrend(f.database, 1.5);
  assert.deepEqual(actual, [
    {
      timestamp: null,
      requests: 1,
      cachedRequests: 0,
      inputTokens: 8,
      cachedTokens: 0,
      cacheCreationTokens: 0,
    },
    {
      timestamp: "2026-10-11T03:00:00Z",
      requests: 2,
      cachedRequests: 2,
      inputTokens: 2,
      cachedTokens: 5,
      cacheCreationTokens: 7,
    },
  ]);
  assert.match(f.calls[1].text, /timestamp >= to_char/);
  assert.doesNotMatch(f.calls[1].text, /timestamp::timestamptz/);
  assert.deepEqual(f.calls[1].values, [1.5]);
  await f.database.close();
});

test("cache trend sums native bigint tokens before converting to the source number result", async () => {
  const f = fixture(() =>
    result([
      {
        timestamp: "2026-10-11T03:00:00Z",
        tokens_input: "9007199254740992",
        tokens_cache_read: "0",
        tokens_cache_creation: "0",
      },
      {
        timestamp: "2026-10-11T03:01:00Z",
        tokens_input: "1",
        tokens_cache_read: "0",
        tokens_cache_creation: "0",
      },
      {
        timestamp: "2026-10-11T03:02:00Z",
        tokens_input: "1",
        tokens_cache_read: "0",
        tokens_cache_creation: "0",
      },
    ])
  );
  assert.equal(
    (await readPostgresCacheTrend(f.database, 1))[0].inputTokens,
    Number(9007199254740994n)
  );
  await f.database.close();
});

test("invalid source trend modifiers do not acquire a connection", async () => {
  const f = fixture(() => result());
  for (const hours of [-1, NaN, Infinity])
    assert.deepEqual(await readPostgresCacheTrend(f.database, hours), []);
  assert.deepEqual(f.calls, []);
  await f.database.close();
});

test("UTC hour projection agrees with independent SQLite strftime over stored date forms", () => {
  const sqlite = new Database(":memory:");
  try {
    const query = sqlite.prepare("SELECT strftime('%Y-%m-%dT%H:00:00Z', ?) AS hour");
    const inputs = [
      "2026-10-11",
      "2026-10-11 03:25",
      "2026-10-11T03:25:19.123Z",
      "2026-10-11T03:25:19+02:00",
      "2026-10-11 03:25:19 -03:30",
      "2026-02-30 00:00:00",
      "2024-02-29T23:59:59Z",
      "2026-10-11T24:00:00Z",
      "03:25",
      "03:25:11.123+02:00",
      "2460400.5",
      "0",
      "5373484.5",
      "not-a-date",
      "2026-13-11",
      "2026-10-32",
      "",
      "2026-10-11T03:25:60Z",
      "2026-10-11T03:25:19.9999Z",
      "2026-10-29T24:01Z",
      "2026-10-11T24:59:59Z",
      "-0001-01-01",
      "0000-01-01",
      "2026-10-11T03:25+15:00",
    ];
    for (const day of [28, 29, 30, 31])
      for (const hour of [0, 23, 24])
        for (const offset of ["Z", "+00:00", "+02:00", "-03:30"])
          inputs.push(`2026-02-${day}T${String(hour).padStart(2, "0")}:45:59.9999${offset}`);
    for (const input of inputs) {
      const expected = query.get(input) as { hour: string | null };
      assert.equal(cacheTrendHour(input), expected.hour, input);
    }
  } finally {
    sqlite.close();
  }
});
