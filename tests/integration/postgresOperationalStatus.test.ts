import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

test(
  "PostgreSQL operational status and readiness use their selected authority",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "omniroute-pg-status-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
      INITIAL_PASSWORD: process.env.INITIAL_PASSWORD,
    };
    process.env.DATA_DIR = directory;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-pg-control-plane-encryption-key";
    process.env.JWT_SECRET = "isolated-status-jwt-secret";
    process.env.API_KEY_SECRET = "isolated-status-api-key";
    delete process.env.INITIAL_PASSWORD;
    let plane: PostgresControlPlane | undefined;
    let seeded = false;
    const ids = ["healthy", "unknown", "disabled"].map((kind) => `status-${kind}-${randomUUID()}`);
    const fixture = await createTestPostgresDatabase();
    try {
      const { initializePostgresControlPlane } =
        await import("../../src/lib/db/postgres/controlPlane.ts");
      const { withPostgresControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
      const core = await import("../../src/lib/db/core.ts");
      const { buildOmniRouteStatus } = await import("../../src/lib/omnirouteStatus.ts");
      const { GET: ping } = await import("../../src/app/api/health/ping/route.ts");
      const { GET: operationalStatus } =
        await import("../../src/app/api/omniroute/status/route.ts");
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions(),
        logger: { warn() {} },
        runtime: {
          applyRuntimeSettings() {},
          scheduleCatalogRevalidationAfterInit() {},
        },
      });
      await fixture.transaction(async (transaction) => {
        const now = "2026-10-11T12:00:00.000Z";
        for (const [index, active, status, error] of [
          [0, 1, "active", null],
          [1, null, null, "private-upstream-error"],
          [2, 0, "active", null],
        ] as const) {
          await transaction.query(
            `INSERT INTO omniroute.provider_connections
               (id, provider, is_active, test_status, last_error, api_key, access_token,
                refresh_token, id_token, email, created_at, updated_at)
             VALUES ($1, 'openai', $2, $3, $4, 'enc:v1:invalid-key', 'private-access-token',
                'private-refresh-token', 'private-id-token', 'private@example.com', $5, $5)`,
            [ids[index], active, status, error, now]
          );
        }
      });
      seeded = true;

      await t.test(
        "status reads a credential-free provider projection and native pools",
        async () => {
          await plane!.run(async (binding) => {
            await (
              await import("../../src/shared/utils/circuitBreaker.ts")
            ).initializeCircuitBreakerPersistence(binding.database!);
            assert.equal(await core.pingDbAsync(), true);
            assert.throws(() => core.getDbInstance(), /not been ported/);
            const status = await buildOmniRouteStatus();
            assert.equal(status.gateway, "healthy");
            const connections = new Map(
              status.providers.connections.map((connection) => [connection.id, connection])
            );
            assert.deepEqual(connections.get(ids[1]), {
              id: ids[1],
              provider: "openai",
              active: true,
              health: "unknown",
              failureState: "recent_error",
            });
            assert.equal(connections.get(ids[0])?.health, "healthy");
            assert.equal(connections.get(ids[2])?.health, "disabled");
            const encoded = JSON.stringify(status);
            for (const secret of [
              "enc:v1:invalid-key",
              "private-access-token",
              "private-refresh-token",
              "private-id-token",
              "private@example.com",
              "private-upstream-error",
            ]) {
              assert.equal(encoded.includes(secret), false);
            }
            // Direct invocation uses the existing trusted-local test branch; HTTP auth is separate.
            const operationalResponse = await operationalStatus(undefined as unknown as Request);
            assert.equal(operationalResponse.status, 200);
            const operationalBody = await operationalResponse.json();
            assert.equal(operationalBody.gateway, "healthy");
            assert.equal(operationalBody.liveRequestExecuted, false);
            const response = await ping();
            assert.equal(response.status, 200);
            const body = await response.json();
            assert.equal(body.status, "ok");
            assert.match(response.headers.get("Cache-Control") ?? "", /no-store/);
          });
        }
      );

      await t.test(
        "a closed selected native database cannot report healthy or open SQLite",
        async () => {
          const closed = await createTestPostgresDatabase();
          await closed.close();
          await plane!.run((binding) =>
            withPostgresControlPlane({ ...binding, database: closed }, async () => {
              assert.equal(await core.pingDbAsync(), false);
              const operationalResponse = await operationalStatus(undefined as unknown as Request);
              assert.equal(operationalResponse.status, 500);
              assert.deepEqual(await operationalResponse.json(), {
                error: "Failed to build OmniRoute status",
              });
              const response = await ping();
              assert.equal(response.status, 503);
              assert.deepEqual(await response.json(), {
                status: "error",
                error: "db_query_failed",
              });
            })
          );
        }
      );

      await t.test(
        "readiness rejects operations retained after their admitted scope ended",
        async () => {
          let release!: () => void;
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          let response: Promise<Response> | undefined;
          let status: Promise<Awaited<ReturnType<typeof buildOmniRouteStatus>>> | undefined;
          await plane!.run(async () => {
            response = gate.then(() => ping());
            status = gate.then(() => buildOmniRouteStatus());
          });
          const rejected = assert.rejects(status!, /scope has ended/);
          release();
          const result = await response!;
          assert.equal(result.status, 503);
          assert.deepEqual(await result.json(), { status: "error", error: "db_query_failed" });
          await rejected;
        }
      );
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      if (seeded)
        await fixture.transaction(async (transaction) => {
          await transaction.query(
            "DELETE FROM omniroute.provider_connections WHERE id=ANY($1::text[])",
            [ids]
          );
        });
      await plane?.close();
      await fixture.close();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    }
  }
);
