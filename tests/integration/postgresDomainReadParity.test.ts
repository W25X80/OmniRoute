import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { PostgresControlPlaneBinding } from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import type { ProxyLogEntry } from "../../src/lib/proxyLogger.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

async function collect<T>(source: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const row of source) result.push(row);
  return result;
}

test(
  "PostgreSQL domain reads preserve SQLite query and JSON contracts",
  { skip: !postgresTestsEnabled, timeout: 60000 },
  async (t) => {
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
    };
    const directory = mkdtempSync(join(tmpdir(), "omniroute-domain-read-parity-"));
    process.env.DATA_DIR = directory;
    delete process.env.OMNIROUTE_DATABASE_BACKEND;
    process.env.STORAGE_ENCRYPTION_KEY = "postgres-domain-read-parity-fixture-key";
    let database: PostgresDatabase | undefined;
    let core: typeof import("../../src/lib/db/core.ts") | undefined;
    try {
      database = await createTestPostgresDatabase();
      const pg = database;
      const { migratePostgresSchema } = await import("../../src/lib/db/postgres/migrations.ts");
      await migratePostgresSchema(pg);
      core = await import("../../src/lib/db/core.ts");
      const sqlite = core.getDbInstance();
      const { withPostgresControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
      const binding = {
        database: pg,
        databaseIdentity: {},
        assertReady() {},
      } as unknown as PostgresControlPlaneBinding;
      const native = <T>(body: () => T): T => withPostgresControlPlane(binding, body);

      await t.test("search analytics executes native SQL with both provider guards", async () => {
        const stats = await import("../../src/lib/db/callLogStats.ts");
        const { setFeatureFlagOverride } = await import("../../src/lib/db/featureFlags.ts");
        const flag = "SEARCH_STATS_HIDE_DELETED_CONNECTIONS";
        const today = "2026-10-11T00:00:00.000Z";
        async function setGuard(value: string) {
          setFeatureFlagOverride(flag, value);
          await pg.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.key_value(namespace,key,value) " +
                "VALUES('feature_flags',$1,$2) ON CONFLICT(namespace,key) " +
                "DO UPDATE SET value=excluded.value",
              [flag, value]
            )
          );
        }
        await setGuard("false");
        const empty = { total: 0, today: 0, errors: 0, avg_duration: null, cached: 0 };
        assert.deepEqual(await stats.getSearchAggregateStatsAsync(today), empty);
        assert.deepEqual(await native(() => stats.getSearchAggregateStatsAsync(today)), empty);
        const now = "2026-10-11T10:00:00.000Z";
        sqlite
          .prepare(
            "INSERT INTO provider_connections(id,provider,created_at,updated_at) VALUES(?,?,?,?)"
          )
          .run("read-parity-provider", "tavily-search", now, now);
        await pg.transaction((tx) =>
          tx.query(
            "INSERT INTO omniroute.provider_connections(id,provider,created_at,updated_at) " +
              "VALUES($1,$2,$3,$3)",
            ["read-parity-provider", "tavily-search", now]
          )
        );
        const fixtures: Array<[string, string, string | null, string, number, number, string]> = [
          ["search-live", now, "tavily-search", "search", 200, 10, "upstream"],
          ["search-keyless", now, "duckduckgo-free", "search", 200, 20, "semantic"],
          ["search-deleted", now, "read-parity-deleted", "search", 500, 30, "upstream"],
          [
            "search-yesterday",
            "2026-10-10T23:59:59.999Z",
            "duckduckgo-free",
            "search",
            200,
            30,
            "upstream",
          ],
          ["search-other-type", now, "duckduckgo-free", "chat", 500, 999, "semantic"],
          ["search-null-provider", now, null, "search", 500, 999, "semantic"],
          ["search-sentinel-provider", now, "-", "search", 500, 999, "semantic"],
        ];
        const insert = sqlite.prepare(
          "INSERT INTO call_logs(id,timestamp,provider,request_type,status,duration,cache_source) " +
            "VALUES(?,?,?,?,?,?,?)"
        );
        for (const row of fixtures) insert.run(...row);
        await pg.transaction(async (tx) => {
          for (const row of fixtures)
            await tx.query(
              "INSERT INTO omniroute.call_logs" +
                "(id,timestamp,provider,request_type,status,duration,cache_source) " +
                "VALUES($1,$2,$3,$4,$5,$6,$7)",
              row
            );
        });
        for (const [enabled, expected] of [
          ["false", { total: 4, today: 3, errors: 1, avg_duration: 22.5, cached: 1 }],
          ["true", { total: 3, today: 2, errors: 0, avg_duration: 20, cached: 1 }],
        ] as const) {
          await setGuard(enabled);
          const source = await stats.getSearchAggregateStatsAsync(today);
          assert.deepEqual(source, expected);
          assert.deepEqual(await native(() => stats.getSearchAggregateStatsAsync(today)), source);
        }
      });

      await t.test(
        "provider request totals survive partial and complete native archive",
        async () => {
          const analytics = await import("../../src/lib/db/usageAnalytics.ts");
          const { PostgresUsageHistoryRepository } =
            await import("../../src/lib/db/postgres/usageHistoryRepository.ts");
          const { PostgresUsageAggregationRepository } =
            await import("../../src/lib/db/postgres/usageAggregationRepository.ts");
          const history = new PostgresUsageHistoryRepository(pg);
          const archive = new PostgresUsageAggregationRepository(pg);
          const options = {
            sinceIso: "2026-10-11T00:00:00.000Z",
            untilIso: "2026-10-11T23:59:59.999Z",
            rawCutoffDate: "2026-01-01",
            apiKeyWhere: "",
            apiKeyParams: {},
          };
          const read = () => {
            const source = analytics.buildUnifiedSource(options);
            return analytics.getProviderDailyUsageRowsAsync(
              source.unifiedSource,
              source.unifiedParams
            );
          };
          for (const [hour, input] of [
            ["10", 1],
            ["11", 2],
            ["12", 3],
          ] as const) {
            const timestamp = "2026-10-11T" + hour + ":00:00.000Z";
            sqlite
              .prepare(
                "INSERT INTO usage_history(provider,model,timestamp,api_key_id,tokens_input,tokens_output) " +
                  "VALUES(?,?,?,?,?,?)"
              )
              .run("openai", "read-parity-model", timestamp, "read-parity-key", input, 0);
            await history.save({
              provider: "openai",
              model: "read-parity-model",
              timestamp,
              apiKeyId: "read-parity-key",
              tokens: { input, output: 0 },
            });
          }
          // Keep SQLite's original events as the oracle; its inherited summary COUNT is not fixed here.
          const source = await read();
          assert.deepEqual(source, [
            {
              date: "2026-10-11",
              provider: "openai",
              requests: 3,
              promptTokens: 6,
              completionTokens: 0,
              totalTokens: 6,
            },
          ]);
          assert.deepEqual(await native(read), source);
          assert.equal((await archive.prune("2026-10-11T11:30:00.000Z", async () => 0)).deleted, 2);
          assert.deepEqual(await native(read), source, "two archived events plus one raw event");
          const owned = await native(() => {
            const selected = analytics.buildUnifiedSource({
              ...options,
              apiKeyWhere: "api_key_id = @apiKey",
              apiKeyParams: { apiKey: "read-parity-key" },
            });
            return analytics.getProviderDailyUsageRowsAsync(
              selected.unifiedSource,
              selected.unifiedParams
            );
          });
          assert.equal(owned[0].requests, 1, "unattributed archive must not enter a key filter");
          assert.equal((await archive.prune("2026-10-12", async () => 0)).deleted, 1);
          assert.deepEqual(
            await native(read),
            source,
            "all three events now share one archive row"
          );
          assert.equal((await archive.prune("2026-10-12", async () => 0)).deleted, 0);
          assert.deepEqual(await native(read), source, "repeat retention cannot increase counts");
          const empty = await native(() => {
            const selected = analytics.buildUnifiedSource({
              ...options,
              sinceIso: "2026-10-12T00:00:00.000Z",
              untilIso: "2026-10-12T23:59:59.999Z",
            });
            return analytics.getProviderDailyUsageRowsAsync(
              selected.unifiedSource,
              selected.unifiedParams
            );
          });
          assert.deepEqual(empty, []);
        }
      );

      await t.test("proxy exports retain numeric timings and opaque text identities", async () => {
        const proxy = await import("../../src/lib/db/proxyLogs.ts");
        const { PostgresProxyLogStore } =
          await import("../../src/lib/db/postgres/proxyLogStore.ts");
        const store = new PostgresProxyLogStore(pg);
        const values: Array<[string, number | null, number | null]> = [
          ["00042", 2, null],
          ["00043", 0, 0],
          ["00044", null, null],
        ];
        for (const [index, [id, headersMs, firstChunkMs]] of values.entries()) {
          const timestamp = "2026-10-11T00:00:0" + index + ".000Z";
          const entry: ProxyLogEntry = {
            id,
            timestamp,
            status: "success",
            proxy: null,
            level: "direct",
            levelId: null,
            provider: "openai",
            targetUrl: null,
            clientIp: null,
            egressIp: null,
            latencyMs: 1.5,
            error: null,
            connectionId: "0006",
            comboId: null,
            account: "0008",
            rotationAccount: null,
            correlationId: "0007",
            tlsFingerprint: false,
            upstreamStatus: 200,
            attemptNumber: 1,
            attemptIssue: "served",
            headersMs,
            firstChunkMs,
          };
          await store.append([entry]);
          sqlite
            .prepare(
              "INSERT INTO proxy_logs" +
                "(id,timestamp,connection_id,correlation_id,account,latency_ms,headers_ms,first_chunk_ms) " +
                "VALUES(?,?,?,?,?,?,?,?)"
            )
            .run(id, timestamp, "0006", "0007", "0008", 1.5, headersMs, firstChunkMs);
        }
        assert.equal(await proxy.updateAttemptTimingAsync("00042", { firstChunkMs: 9 }), true);
        assert.equal(
          await native(() => proxy.updateAttemptTimingAsync("00042", { firstChunkMs: 9 })),
          true
        );
        const project = (rows: Record<string, unknown>[]) =>
          rows.map((row) => ({
            id: row.id,
            connection_id: row.connection_id,
            correlation_id: row.correlation_id,
            account: row.account,
            latency_ms: row.latency_ms,
            headers_ms: row.headers_ms,
            first_chunk_ms: row.first_chunk_ms,
          }));
        const since = "2026-10-11T00:00:00.000Z";
        const expected = project(await proxy.exportProxyLogsSinceAsync(since));
        assert.deepEqual(expected, [
          {
            id: "00044",
            connection_id: "0006",
            correlation_id: "0007",
            account: "0008",
            latency_ms: 1.5,
            headers_ms: null,
            first_chunk_ms: null,
          },
          {
            id: "00043",
            connection_id: "0006",
            correlation_id: "0007",
            account: "0008",
            latency_ms: 1.5,
            headers_ms: 0,
            first_chunk_ms: 0,
          },
          {
            id: "00042",
            connection_id: "0006",
            correlation_id: "0007",
            account: "0008",
            latency_ms: 1.5,
            headers_ms: 2,
            first_chunk_ms: 9,
          },
        ]);
        const direct = await native(() => proxy.exportProxyLogsSinceAsync(since));
        const streamed = await native(() => collect(proxy.iterateProxyLogsSinceAsync(since, 10)));
        for (const result of [direct, streamed]) {
          assert.deepEqual(project(result), expected);
          assert.deepEqual(JSON.parse(JSON.stringify(project(result))), expected);
        }
        const capped = await native(() => collect(proxy.iterateProxyLogsSinceAsync(since, 1)));
        assert.deepEqual(project(capped), expected.slice(0, 1));
      });

      await t.test(
        "memory LIKE preserves literal backslashes and supported wildcards",
        async () => {
          const { listMemories } = await import("../../src/lib/memory/store.ts");
          const owner = "read-parity-memory";
          const fixtures = [
            { id: "path-slash", key: "path", content: "C:\\temp", type: "factual" },
            { id: "path-plain", key: "plain", content: "C:temp", type: "episodic" },
            { id: "key-slash", key: "C:\\temp", content: "saved path", type: "semantic" },
            { id: "trailing", key: "tail", content: "trailing\\", type: "procedural" },
            { id: "wild-one", key: "one", content: "axb", type: "factual" },
            { id: "wild-two", key: "two", content: "a_b", type: "episodic" },
            { id: "private", key: "other", content: "C:\\temp", type: "factual" },
          ];
          const insert = sqlite.prepare(
            "INSERT INTO memories" +
              "(id,api_key_id,session_id,type,key,content,metadata,created_at,updated_at) " +
              "VALUES(?,?,?,?,?,?,?,?,?)"
          );
          for (const [index, row] of fixtures.entries()) {
            const timestamp = "2026-10-11T00:00:0" + index + ".000Z";
            const values = [
              row.id,
              row.id === "private" ? "other-owner" : owner,
              "session",
              row.type,
              row.key,
              row.content,
              "{}",
              timestamp,
              timestamp,
            ];
            insert.run(...values);
            await pg.transaction((tx) =>
              tx.query(
                "INSERT INTO omniroute.memories" +
                  "(id,api_key_id,session_id,type,key,content,metadata,created_at,updated_at) " +
                  "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                values
              )
            );
          }
          const cases: Array<[string, string[]]> = [
            ["C:\\temp", ["path-slash", "key-slash"]],
            ["\\", ["path-slash", "key-slash", "trailing"]],
            ["C:\\%", ["path-slash", "key-slash"]],
            ["a_b", ["wild-one", "wild-two"]],
            ["%", fixtures.filter((row) => row.id !== "private").map((row) => row.id)],
          ];
          for (const [query, ids] of cases) {
            const filters = { apiKeyId: owner, query, limit: 20 };
            const source = await listMemories(filters);
            assert.deepEqual(new Set(source.data.map((row) => row.id)), new Set(ids));
            assert.deepEqual(await native(() => listMemories(filters)), source, query);
            const page = { ...filters, limit: 1, offset: 1 };
            const sourcePage = await listMemories(page);
            assert.equal(sourcePage.total, ids.length);
            assert.deepEqual(
              await native(() => listMemories(page)),
              sourcePage,
              "filtered counts and byType must cover rows beyond the page"
            );
          }
        }
      );
    } finally {
      await database?.close();
      core?.resetDbInstance();
      rmSync(directory, { recursive: true, force: true });
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
