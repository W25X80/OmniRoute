import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const directory = mkdtempSync(join(tmpdir(), "omniroute-status-"));
process.env.DATA_DIR = directory;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
process.env.API_KEY_SECRET = "isolated-status-api-key";

const core = await import("../../src/lib/db/core.ts");
core.resetDbInstance();
const { buildOmniRouteStatus } = await import("../../src/lib/omnirouteStatus.ts");
const { GET: ping } = await import("../../src/app/api/health/ping/route.ts");
const { GET: operationalStatus } = await import("../../src/app/api/omniroute/status/route.ts");

test.after(() => {
  core.resetDbInstance();
  rmSync(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});

test("SQLite operational status keeps active/null/error policy without reading credentials", async () => {
  const database = core.getDbInstance();
  const insert = database.prepare(
    `INSERT INTO provider_connections
      (id, provider, is_active, test_status, last_error, api_key, access_token,
       refresh_token, id_token, email, created_at, updated_at)
     VALUES (?, 'openai', ?, ?, ?, 'enc:v1:invalid-key', 'private-access-token',
       'private-refresh-token', 'private-id-token', 'private@example.com', ?, ?)`
  );
  const now = "2026-10-11T12:00:00.000Z";
  for (const [id, active, status, error] of [
    ["healthy", 1, "active", null],
    ["unknown", null, null, "private-upstream-error"],
    ["disabled", 0, "active", null],
  ]) {
    insert.run(id, active, status, error, now, now);
  }

  const status = await buildOmniRouteStatus();
  assert.equal(status.gateway, "healthy");
  assert.equal(status.providers.configured, 3);
  assert.equal(status.providers.active, 2);
  assert.equal(status.providers.healthy, 1);
  assert.equal(status.providers.disabled, 1);
  const connections = Object.fromEntries(
    status.providers.connections.map((connection) => [String(connection.id), connection])
  );
  assert.deepEqual(connections.unknown, {
    id: "unknown",
    provider: "openai",
    active: true,
    health: "unknown",
    failureState: "recent_error",
  });
  assert.equal(connections.disabled.health, "disabled");
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
  assert.equal((await operationalResponse.json()).gateway, "healthy");
  assert.equal(await core.pingDbAsync(), core.pingDb());
  assert.equal((await ping()).status, 200);
});

test("an unready selected PostgreSQL backend fails readiness without using initialized SQLite", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  try {
    assert.equal(await core.pingDbAsync(), false);
    const response = await ping();
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { status: "error", error: "db_query_failed" });
    await assert.rejects(buildOmniRouteStatus(), /PostgreSQL application persistence is not ready/);
    const operationalResponse = await operationalStatus(undefined as unknown as Request);
    assert.equal(operationalResponse.status, 500);
    assert.deepEqual(await operationalResponse.json(), {
      error: "Failed to build OmniRoute status",
    });
  } finally {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  }
  assert.equal((await ping()).status, 200);
});
