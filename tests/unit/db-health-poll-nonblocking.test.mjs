import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { stripTypeScriptTypes } from "node:module";
import test from "node:test";
import vm from "node:vm";

import { createDbHealthCoordinator } from "../../src/lib/db/healthCheckRunner.ts";
import { createPostgresOperationsRepository } from "../../src/lib/db/postgres/operationsRepository.ts";
import { persistenceCapabilityResponse } from "../../src/lib/db/persistenceCapabilities.ts";

// The managed health check is loaded from the REAL core.ts source (sliced, not
// reimplemented) so this test cannot drift away from production. #13717 moved the
// body behind a coordinator, so the slice starts at the coordinator construction.
const core = readFileSync(new URL("../../src/lib/db/core.ts", import.meta.url), "utf8");
const managedCheck = core
  .slice(
    core.indexOf("const healthShutdown = new AbortController();"),
    core.indexOf("export function getDbInstance()")
  )
  .replace("export function", "function");
const route = readFileSync(new URL("../../src/app/api/db/health/route.ts", import.meta.url), "utf8")
  .replace(/^import .*;\n/gm, "")
  .replaceAll("export async function", "async function");

// Keep the selected facade itself in the fixture. Only its module loading is redirected
// so the SQLite branch uses the real coordinator above, not a second implementation.
const health = readFileSync(new URL("../../src/lib/db/healthCheck.ts", import.meta.url), "utf8");
const facadeStart = health.indexOf("export async function runManagedDatabaseHealthCheck(");
assert.ok(facadeStart >= 0, "the managed health facade must be present");
const managedFacade = health
  .slice(facadeStart)
  .replace("export async function", "async function")
  .replace(/import\(("\.\/(?:core|postgres\/operationsRepository)")\)/g, "loadHealthModule($1)");

function loadCheck(skipEnv, authenticated = true, backend = "sqlite") {
  const calls = [];
  const nativeQueries = [];
  const database = {
    transaction: async (body) =>
      body({
        query: async (text) => {
          nativeQueries.push(text);
          assert.ok(text === "SELECT 1" || text.includes("FROM omniroute.quota_snapshots"));
          return { rows: [], rowCount: 0, command: "SELECT" };
        },
      }),
  };
  const context = vm.createContext({
    process: { env: { OMNIROUTE_SKIP_DB_HEALTHCHECK: skipEnv } },
    AbortController,
    createDbHealthCoordinator,
    createPostgresOperationsRepository,
    persistenceCapabilityResponse,
    getSelectedPostgresDatabase: () => (backend === "postgres" ? database : undefined),
    loadHealthModule: async (specifier) => {
      if (specifier === "./core") {
        return { runManagedDbHealthCheck: context.runManagedDbHealthCheck };
      }
      assert.equal(specifier, "./postgres/operationsRepository");
      return { createPostgresOperationsRepository };
    },
    getPagerCorruption: () => null,
    invalidateDbCache: () => {},
    path: { join: (...parts) => parts.join("/") },
    DB_BACKUPS_DIR: "/tmp/db_backups",
    DATA_DIR: "/tmp",
    writeManagedDbBackup: () => assert.fail("read-only polling must not create backups"),
    runDbHealthInChild: () => assert.fail("stub DB must stay on the owning-connection path"),
    // No `name`/`driver`: core takes the in-process branch, never forking a child.
    getDbInstance: () => {
      assert.equal(backend, "sqlite", "native health must not open SQLite");
      return {};
    },
    runDbHealthCheck: (_db, options) => {
      calls.push(options);
      return { status: "healthy", repairedCount: 0 };
    },
    isAuthenticated: async () => authenticated,
    NextResponse: { json: (body, options) => ({ body, status: options?.status ?? 200 }) },
    sanitizeErrorMessage: (message) => message,
    console,
  });
  vm.runInContext(stripTypeScriptTypes(managedCheck + managedFacade + route), context);
  return { context, calls, nativeQueries };
}

test("dashboard GET skips integrity scans even without a deployment opt-out", async () => {
  const { context, calls } = loadCheck(undefined);
  for (let poll = 0; poll < 3; poll++) {
    const response = await vm.runInContext("GET({})", context);
    assert.equal(response.status, 200, JSON.stringify(response.body));
  }
  // Three real calls: a skipped scan is never replayed from the diagnosis cache,
  // otherwise a later full diagnosis would be silently downgraded.
  assert.equal(calls.length, 3);
  for (const options of calls) {
    assert.equal(options.autoRepair, false);
    assert.equal(options.skipIntegrityCheck, true);
  }
});

test("managed checks honor the deployment opt-out, including manual repair", async () => {
  const { context, calls } = loadCheck("1");
  await vm.runInContext("runManagedDbHealthCheck({skipIntegrityCheck:false})", context);
  const response = await vm.runInContext("POST({})", context);
  assert.equal(response.status, 200, JSON.stringify(response.body));
  assert.equal(calls.length, 2);
  assert.equal(calls[0].skipIntegrityCheck, true);
  assert.equal(calls[1].skipIntegrityCheck, true);
  assert.equal(calls[1].autoRepair, true);
});

test("explicit repair retains integrity scans when the deployment allows them", async () => {
  const { context, calls } = loadCheck("0");
  const response = await vm.runInContext("POST({})", context);
  assert.equal(response.status, 200, JSON.stringify(response.body));
  assert.equal(Boolean(calls[0].skipIntegrityCheck), false);
  assert.equal(calls[0].autoRepair, true);
});

test("a full diagnosis is still cached, a skipped one is not", async () => {
  const { context, calls } = loadCheck(undefined);
  await vm.runInContext("runManagedDbHealthCheck({})", context);
  await vm.runInContext("runManagedDbHealthCheck({})", context);
  assert.equal(calls.length, 1, "full diagnosis is served from the 60s cache");
  assert.equal(Boolean(calls[0].skipIntegrityCheck), false);
  await vm.runInContext("GET({})", context);
  // The cached FULL result satisfies a caller willing to skip the scan.
  assert.equal(calls.length, 1);
});

test("unauthenticated polling and repair do not touch the database", async () => {
  for (const backend of ["sqlite", "postgres"]) {
    const { context, calls, nativeQueries } = loadCheck(undefined, false, backend);
    for (const method of ["GET", "POST"]) {
      const response = await vm.runInContext(`${method}({})`, context);
      assert.equal(response.status, 401);
    }
    assert.equal(calls.length, 0);
    assert.equal(nativeQueries.length, 0);
  }
});

test("native diagnosis uses its owning repository without SQLite integrity work", async () => {
  const { context, calls, nativeQueries } = loadCheck(undefined, true, "postgres");
  const response = await vm.runInContext("GET({})", context);
  assert.equal(response.status, 200, JSON.stringify(response.body));
  assert.equal(response.body.isHealthy, true);
  assert.equal(response.body.driver.name, "postgresql");
  assert.equal(response.body.integrityScope, "connectivity-and-application-references");
  assert.equal(calls.length, 0);
  assert.equal(nativeQueries.length, 2);
});

test("native manual repair returns the canonical capability response before SQL", async () => {
  const { context, calls, nativeQueries } = loadCheck(undefined, true, "postgres");
  const response = await vm.runInContext("POST({})", context);
  assert.equal(response.status, 501);
  assert.deepEqual(response.body, {
    error: {
      message:
        "PersistenceCapabilityError: The PostgreSQL backend does not support sqlite-integrity-repair",
      code: "persistence_capability_unavailable",
      backend: "postgresql",
      capability: "sqlite-integrity-repair",
    },
  });
  assert.equal(calls.length, 0);
  assert.equal(nativeQueries.length, 0);
});
