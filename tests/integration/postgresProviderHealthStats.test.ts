import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import {
  HEALTH_STATS_CUTOFF,
  healthStatsFixtureRows,
  insertHealthStatsSqliteRows,
} from "../helpers/persistence/providerHealthStatsFixture.ts";
import { matrixSignalsFixture } from "../helpers/persistence/providerHealthMatrixSignalsFixture.ts";

test(
  "native health stats match SQLite aggregates and full health matrix with colliding normalized groups",
  { skip: !postgresTestsEnabled, timeout: 30000 },
  async () => {
    const matrixUrl = new URL("../../src/lib/monitoring/providerHealthMatrix.ts", import.meta.url)
      .href;
    const fixtureUrl = new URL(
      "../helpers/persistence/providerHealthMatrixSignalsFixture.ts",
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
    const previousDataDir = process.env.DATA_DIR,
      previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
    const directory = mkdtempSync(join(tmpdir(), "omniroute-native-health-stats-"));
    process.env.DATA_DIR = directory;
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    const originalNow = Date.now;
    let database: import("../../src/lib/db/postgres/database.ts").PostgresDatabase | undefined;
    let core: typeof import("../../src/lib/db/core.ts") | undefined;
    const prefix = `health-${randomUUID()}`;
    const provider = `${prefix}-collision`,
      alias = `${prefix}-alias`;
    const rows = healthStatsFixtureRows(prefix);
    rows.push(
      {
        ...rows[0],
        id: `${prefix}-collision-space`,
        provider: ` ${provider} `,
        model: "colliding",
        status: 503,
        duration: 10,
      },
      {
        ...rows[0],
        id: `${prefix}-collision-alias`,
        provider: alias,
        model: "colliding",
        status: 200,
        duration: 100,
      },
      {
        ...rows[0],
        id: `${prefix}-collision-direct`,
        provider,
        model: "colliding",
        status: 429,
        duration: 1000,
      }
    );
    const owns = (row: { provider: string; connectionId: string | null }) =>
      row.provider.startsWith(prefix) || row.connectionId?.startsWith(prefix);
    const sorted = <T>(values: T[]) => values.map((value) => JSON.stringify(value)).sort();
    try {
      database = await createTestPostgresDatabase();
      const { migratePostgresSchema } = await import("../../src/lib/db/postgres/migrations.ts");
      await migratePostgresSchema(database);
      core = await import("../../src/lib/db/core.ts");
      const { getProviderHealthTargetStatsAsync } =
        await import("../../src/lib/db/providerHealthStats.ts");
      const { withPostgresControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
      const { buildProviderHealthMatrix } =
        await import("../../src/lib/monitoring/providerHealthMatrix.ts");
      insertHealthStatsSqliteRows(core.getDbInstance(), rows);
      await database.transaction(async (tx) => {
        for (const row of rows)
          await tx.query(
            `INSERT INTO omniroute.call_logs
        (id,timestamp,status,model,requested_model,provider,connection_id,duration,error_summary)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
            [
              row.id,
              row.timestamp,
              row.status,
              row.model,
              row.requestedModel,
              row.provider,
              row.connectionId,
              row.duration,
              row.errorSummary,
            ]
          );
      });
      const sqlite = (await getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, null)).filter(
        owns
      );
      const binding = {
        database,
        databaseIdentity: {},
        assertReady() {},
      } as unknown as import("../../src/lib/db/controlPlaneContext.ts").PostgresControlPlaneBinding;
      await withPostgresControlPlane(binding, async () => {
        const native = (await getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, null)).filter(
          owns
        );
        assert.deepEqual(
          sorted(native),
          sorted(sqlite),
          "compare every source aggregate, not row order"
        );
        const literal = `${prefix}' OR TRUE --`;
        const filtered = await getProviderHealthTargetStatsAsync(HEALTH_STATS_CUTOFF, literal);
        assert.equal(filtered.length, 1);
        assert.equal(filtered[0].provider, literal);
        assert.equal(filtered[0].requests, 1);
      });
      matrixSignalsFixture.aliases.set(alias, provider);
      Date.now = () => Date.parse("2026-09-11T00:00:00.000Z");
      const sqliteReport = await buildProviderHealthMatrix({
        provider,
        range: "24h",
        includeHealthy: true,
      });
      const nativeReport = await withPostgresControlPlane(binding, () =>
        buildProviderHealthMatrix({ provider, range: "24h", includeHealthy: true })
      );
      assert.deepEqual(
        nativeReport,
        sqliteReport,
        "full reports must agree even when normalized raw groups collide"
      );
      assert.ok(
        sqliteReport.providers[0]?.accounts[0]?.models.some((model) => model.model === "colliding")
      );
      assert.equal(sqliteReport.providers[0]?.requests, 3);
    } finally {
      Date.now = originalNow;
      await database
        ?.transaction((tx) =>
          tx.query("DELETE FROM omniroute.call_logs WHERE id=ANY($1::text[])", [
            rows.map((row) => row.id),
          ])
        )
        .catch(() => {});
      await database?.close();
      hooks.deregister();
      matrixSignalsFixture.aliases.clear();
      core?.resetDbInstance();
      rmSync(directory, { recursive: true, force: true });
      for (const [name, value] of [
        ["DATA_DIR", previousDataDir],
        ["OMNIROUTE_DATABASE_BACKEND", previousBackend],
      ]) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
    }
  }
);
