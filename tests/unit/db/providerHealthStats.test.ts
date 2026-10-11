import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import {
  HEALTH_STATS_CUTOFF,
  healthStatsFixtureRows,
  insertHealthStatsSqliteRows,
} from "../../helpers/persistence/providerHealthStatsFixture.ts";

const previousDataDir = process.env.DATA_DIR;
const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
const directory = mkdtempSync(join(tmpdir(), "omniroute-health-stats-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
const core = await import("../../../src/lib/db/core.ts");
const stats = await import("../../../src/lib/db/providerHealthStats.ts");
const db = core.getDbInstance();
const rows = healthStatsFixtureRows("fixture");
insertHealthStatsSqliteRows(db, rows);
test.after(() => {
  core.resetDbInstance();
  rmSync(directory, { recursive: true, force: true });
  for (const [name, value] of [
    ["DATA_DIR", previousDataDir],
    ["OMNIROUTE_DATABASE_BACKEND", previousBackend],
  ]) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
});

test("health target stats retain source cutoff, statuses, nulls, ties, grouping and rounding", async () => {
  const result = await stats.getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, null);
  assert.equal(result.length, 8);
  const primary = result.filter((row) => row.provider === "fixture-primary");
  assert.equal(primary.length, 5);
  assert.deepEqual(
    primary.find((row) => row.model === "fallback"),
    {
      provider: "fixture-primary",
      connectionId: null,
      model: "fallback",
      requests: 3,
      successes: 1,
      avgLatencyMs: 2,
      lastRequestAt: "2026-09-10T00:00:01.000Z",
      lastErrorAt: HEALTH_STATS_CUTOFF,
      lastStatus: null,
      lastErrorStatus: 503,
    }
  );
  const negative = primary.find((row) => row.model === "negative")!;
  assert.equal(negative.requests, 2);
  assert.equal(negative.successes, 1);
  assert.equal(negative.avgLatencyMs, -2);
  assert.equal(negative.lastStatus, 399);
  assert.equal(negative.lastErrorStatus, 199);
  const noDuration = primary.find((row) => row.model === "null-duration")!;
  assert.equal(noDuration.avgLatencyMs, null);
  assert.equal(noDuration.successes, 1);
  assert.equal(noDuration.lastErrorStatus, 399);
  assert.equal(noDuration.lastErrorAt, HEALTH_STATS_CUTOFF);
  assert.equal(primary.find((row) => row.connectionId === "unknown-model")?.model, "unknown");
  assert.equal(primary.find((row) => row.connectionId === "empty-model")?.model, "unknown");
  assert.equal(primary.find((row) => row.connectionId === "empty-model")?.avgLatencyMs, 1);
  assert.equal(
    result.find((row) => row.provider === "unknown")?.connectionId,
    "fixture-empty-provider"
  );
  assert.ok(result.every((row) => row.provider !== "-"));
  assert.ok(
    result.every(
      (row) => !Object.hasOwn(row, "errorSummary") && !Object.hasOwn(row, "error_summary")
    )
  );
});

test("raw provider filtering binds the literal provider instead of interpreting SQL", async () => {
  const literal = "fixture' OR TRUE --";
  const result = await stats.getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, literal);
  assert.equal(result.length, 1);
  assert.equal(result[0].provider, literal);
  assert.equal(result[0].requests, 1);
  assert.deepEqual(
    await stats.getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, "missing"),
    []
  );
});

test("native facade awaits its selected query and applies the same source row normalization", async () => {
  let release!: (rows: Record<string, unknown>[]) => void;
  const result = new Promise<Record<string, unknown>[]>((done) => {
    release = done;
  });
  let entered!: () => void;
  const started = new Promise<void>((done) => {
    entered = done;
  });
  let queryText = "",
    queryValues: unknown[] = [];
  const database = {
    transaction: async (body: (tx: unknown) => Promise<unknown>) =>
      body({
        query: async (text: string, values: unknown[]) => {
          queryText = text;
          queryValues = values;
          entered();
          return { rows: await result };
        },
      }),
  } as unknown as PostgresDatabase;
  const binding = {
    database,
    databaseIdentity: {},
    assertReady() {},
  } as unknown as PostgresControlPlaneBinding;
  const work = withPostgresControlPlane(binding, () =>
    stats.getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, "literal'provider")
  );
  let settled = false;
  void work.then(() => {
    settled = true;
  });
  await started;
  assert.equal(settled, false);
  assert.ok(queryText.includes("FROM omniroute.call_logs"));
  assert.ok(queryText.includes("ROUND(AVG(duration)::numeric)"));
  assert.ok(!queryText.includes("literal'provider"));
  assert.deepEqual(queryValues, [HEALTH_STATS_CUTOFF, "literal'provider"]);
  release([
    {
      provider: " raw ",
      connectionId: " ",
      model: "",
      requests: "2",
      successes: "1",
      avgLatencyMs: "2",
      lastRequestAt: HEALTH_STATS_CUTOFF,
      lastErrorAt: null,
      lastStatus: "399",
      lastErrorStatus: null,
    },
  ]);
  assert.deepEqual(await work, [
    {
      provider: "raw",
      connectionId: null,
      model: "unknown",
      requests: 2,
      successes: 1,
      avgLatencyMs: 2,
      lastRequestAt: HEALTH_STATS_CUTOFF,
      lastErrorAt: null,
      lastStatus: 399,
      lastErrorStatus: null,
    },
  ]);
});

test("full SQLite matrix preserves the source behavior for colliding normalized raw groups", async () => {
  const matrixUrl = new URL("../../../src/lib/monitoring/providerHealthMatrix.ts", import.meta.url)
    .href;
  const fixtureUrl = new URL(
    "../../helpers/persistence/providerHealthMatrixSignalsFixture.ts",
    import.meta.url
  ).href;
  const signals = new Set([
    "@/lib/db/models",
    "@/lib/db/providers",
    "@/shared/utils/circuitBreaker",
    "@omniroute/open-sse/services/accountFallback",
    "@omniroute/open-sse/services/model",
    "@omniroute/open-sse/services/webSessionPoolHealth",
  ]);
  const hooks = registerHooks({
    resolve(specifier, context, nextResolve) {
      if (context.parentURL === matrixUrl && signals.has(specifier))
        return { url: fixtureUrl, shortCircuit: true };
      return nextResolve(specifier, context);
    },
  });
  const originalNow = Date.now;
  const { matrixSignalsFixture } =
    await import("../../helpers/persistence/providerHealthMatrixSignalsFixture.ts");
  try {
    const provider = "fixture-collision",
      alias = "fixture-alias";
    matrixSignalsFixture.aliases.set(alias, provider);
    insertHealthStatsSqliteRows(db, [
      {
        ...rows[0],
        id: "collision-space",
        provider: ` ${provider} `,
        model: "colliding",
        status: 503,
        duration: 10,
      },
      {
        ...rows[0],
        id: "collision-alias",
        provider: alias,
        model: "colliding",
        status: 200,
        duration: 100,
      },
      {
        ...rows[0],
        id: "collision-direct",
        provider,
        model: "colliding",
        status: 429,
        duration: 1000,
      },
    ]);
    Date.now = () => Date.parse("2026-09-11T00:00:00.000Z");
    const { buildProviderHealthMatrix } =
      await import("../../../src/lib/monitoring/providerHealthMatrix.ts");
    const report = await buildProviderHealthMatrix({
      provider,
      range: "24h",
      includeHealthy: true,
    });
    assert.equal(report.providers.length, 1);
    assert.equal(report.providers[0].requests, 3);
    const model = report.providers[0].accounts[0].models.find(
      (model) => model.model === "colliding"
    )!;
    assert.equal(model.lastStatus, 429);
    assert.equal(model.avgLatencyMs, 1000);
  } finally {
    Date.now = originalNow;
    hooks.deregister();
    matrixSignalsFixture.aliases.clear();
  }
});
