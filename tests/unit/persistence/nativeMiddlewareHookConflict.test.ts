import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import { registerHooks } from "node:module";
import { setImmediate as nextTurn } from "node:timers/promises";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import {
  MiddlewareConflictPool,
  managementFixture,
  barrier,
} from "../../helpers/persistence/middlewareConflictFixture.ts";
import { clearAllHooks, getAllHooks } from "../../../src/lib/middleware/registry.ts";
import { ensureDbInitialized, resetDbInstance } from "../../../src/lib/db/core.ts";

const fixtureUrl = new URL(
  "../../helpers/persistence/middlewareConflictFixture.ts",
  import.meta.url
).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      (context.parentURL?.endsWith("/src/lib/api/requireManagementAuth.ts") &&
        specifier === "@/sse/services/auth") ||
      (context.parentURL?.endsWith("/src/shared/utils/apiAuth.ts") &&
        specifier === "@/lib/db/settings") ||
      (context.parentURL?.endsWith("/src/lib/api/requireManagementAuth.ts") &&
        specifier === "@/lib/db/apiKeys")
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const { POST } = await import("../../../src/app/api/middleware/hooks/route.ts");
const savedBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
test.after(() => {
  hooks.deregister();
  clearAllHooks();
  resetDbInstance();
  if (savedBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
  else process.env.OMNIROUTE_DATABASE_BACKEND = savedBackend;
});
test.beforeEach(() => {
  clearAllHooks();
  managementFixture.checks = 0;
  managementFixture.validScopes = ["manage"];
});
function request(name = "same-name", key = "fixture-managed-key") {
  return new Request("http://localhost/api/middleware/hooks", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
    body: JSON.stringify({ name, code: "return {};" }),
  });
}
function binding(pool: MiddlewareConflictPool): PostgresControlPlaneBinding {
  return {
    database: pool.database,
    databaseIdentity: {},
    assertReady() {},
  } as PostgresControlPlaneBinding;
}
async function concurrentCreates(pool: MiddlewareConflictPool) {
  pool.blockTwoReads = true;
  const first = POST(request());
  const second = POST(request());
  await pool.readsEntered.promise;
  pool.releaseReads.resolve();
  return Promise.all([first, second]);
}
test("authenticated concurrent native hook creation yields one 201 and one 409 after ACK", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const pool = new MiddlewareConflictPool();
  await withPostgresControlPlane(binding(pool), async () => {
    const responses = await concurrentCreates(pool);
    assert.deepEqual(responses.map((response) => response.status).sort(), [201, 409]);
    assert.equal(pool.inserts, 2);
    assert.equal(pool.rows.size, 1);
    assert.equal(pool.commits, 4);
    assert.equal(getAllHooks().length, 1);
    assert.equal(managementFixture.checks, 2);
    const conflict = responses.find((response) => response.status === 409)!;
    assert.deepEqual(await conflict.json(), { error: "Hook already exists" });
  });
  await pool.database.close();
});
test("confirmed empty INSERT does not become a 409 before its COMMIT is acknowledged", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const pool = new MiddlewareConflictPool();
  pool.conflictCommit = barrier();
  await withPostgresControlPlane(binding(pool), async () => {
    let complete = false;
    const responses = concurrentCreates(pool).then((result) => {
      complete = true;
      return result;
    });
    await pool.conflictCommitEntered.promise;
    await nextTurn();
    assert.equal(complete, false);
    pool.conflictCommit!.resolve();
    assert.deepEqual((await responses).map((response) => response.status).sort(), [201, 409]);
  });
  await pool.database.close();
});
test("lost COMMIT reply after a duplicate outcome remains 500 and is never replayed", async (t) => {
  t.mock.method(console, "error", () => {});
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const pool = new MiddlewareConflictPool();
  pool.loseConflictCommit = true;
  await withPostgresControlPlane(binding(pool), async () => {
    const responses = await concurrentCreates(pool);
    assert.deepEqual(responses.map((response) => response.status).sort(), [201, 500]);
    const body = await responses.find((response) => response.status === 500)!.json();
    assert.match(body.error, /commit outcome is unknown/);
    assert.equal(pool.inserts, 2);
    assert.equal(pool.rows.size, 1);
    assert.equal(getAllHooks().length, 1);
  });
  await pool.database.close();
});
for (const state of ["23503", "23505", "23514"]) {
  test(`unrelated native SQLSTATE ${state} stays a sanitized 500`, async (t) => {
    t.mock.method(console, "error", () => {});
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    const pool = new MiddlewareConflictPool();
    pool.insertSqlState = state;
    await withPostgresControlPlane(binding(pool), async () => {
      const response = await POST(request());
      assert.equal(response.status, 500);
      const body = await response.json();
      assert.match(body.error, /PostgreSQL query failed/);
      assert.ok(!body.error.includes("private driver detail"));
      assert.ok(!body.error.includes("unrelated_constraint"));
      assert.equal(pool.inserts, 1);
      assert.equal(getAllHooks().length, 0);
    });
    await pool.database.close();
  });
}
test("unknown INSERT commit never registers the hook or replays create", async (t) => {
  t.mock.method(console, "error", () => {});
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const pool = new MiddlewareConflictPool();
  pool.loseInsertedCommit = true;
  await withPostgresControlPlane(binding(pool), async () => {
    const response = await POST(request());
    assert.equal(response.status, 500);
    assert.match((await response.json()).error, /commit outcome is unknown/);
    assert.equal(pool.inserts, 1);
    assert.equal(getAllHooks().length, 0);
  });
  await pool.database.close();
});
test("default SQLite concurrent creates preserve the same 201/409 semantics", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  await ensureDbInitialized();
  const responses = await Promise.all([
    POST(request("sqlite-same-name")),
    POST(request("sqlite-same-name")),
  ]);
  assert.deepEqual(responses.map((response) => response.status).sort(), [201, 409]);
  // The atomic create outcome uses this message; the preflight-only branch quotes the name.
  assert.deepEqual(await responses.find((response) => response.status === 409)!.json(), {
    error: "Hook already exists",
  });
  assert.equal(getAllHooks().filter((hook) => hook.name === "sqlite-same-name").length, 1);
});
test("management authentication and scope denial still precede persistence", async () => {
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const pool = new MiddlewareConflictPool();
  await withPostgresControlPlane(binding(pool), async () => {
    assert.equal((await POST(request("invalid", "wrong"))).status, 403);
    managementFixture.validScopes = ["read"];
    assert.equal((await POST(request("denied"))).status, 403);
    assert.equal(pool.inserts, 0);
    assert.equal(getAllHooks().length, 0);
  });
  await pool.database.close();
});
