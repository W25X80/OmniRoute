import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import test from "node:test";
import { registerHooks } from "node:module";
import { realpathSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { NextRequest } from "next/server";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { PostgresError } from "../../../src/lib/db/postgres/database.ts";

const fixtureUrl = new URL("../../helpers/persistence/a2aRouteFailureFixture.ts", import.meta.url)
  .href;
const routeUrl = pathToFileURL(
  realpathSync(new URL("../../../src/app/a2a/route.ts", import.meta.url))
).href;
const dependencies = new Set([
  "@/lib/a2a/taskManager",
  "@/lib/a2a/routingLogger",
  "@/lib/a2a/streaming",
  "@/lib/a2a/taskExecution",
  "@/lib/db/settings",
  "@/lib/db/persistenceRuntime",
]);
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      (context.parentURL === routeUrl && dependencies.has(specifier)) ||
      (context.parentURL?.endsWith("/src/lib/a2a/authenticate.ts") &&
        specifier === "@/sse/services/auth") ||
      (context.parentURL?.endsWith("/src/shared/utils/apiAuth.ts") &&
        specifier === "@/lib/db/settings") ||
      (context.parentURL?.endsWith("/src/shared/utils/featureFlags.ts") &&
        specifier === "@/lib/db/featureFlags")
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const { a2aRouteFixture } = await import(fixtureUrl);
const { POST } = await import(routeUrl);
const savedEnv = new Map(
  ["OMNIROUTE_DATABASE_BACKEND", "OMNIROUTE_API_KEY", "REQUIRE_API_KEY"].map((key) => [
    key,
    process.env[key],
  ])
);
process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
process.env.OMNIROUTE_API_KEY = "fixture-a2a-key";
process.env.REQUIRE_API_KEY = "false";
const nativeBinding = {
  database: {},
  databaseIdentity: {},
  assertReady() {},
} as PostgresControlPlaneBinding;
test.after(() => {
  hooks.deregister();
  a2aRouteFixture.reset();
  for (const [key, value] of savedEnv) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
});
test.beforeEach(() => a2aRouteFixture.reset());
function request(method: string, id: string | number = 0, key = "fixture-a2a-key") {
  return new NextRequest("http://localhost/a2a", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
    body: JSON.stringify({
      jsonrpc: "2.0",
      id,
      method,
      params: { skill: "fixture", messages: [{ role: "user", content: "hello" }] },
    }),
  });
}
for (const method of ["message/send", "message/stream", "SendMessage", "SendStreamingMessage"]) {
  for (const state of ["submitted", "working"] as const) {
    for (const kind of ["overloaded", "query_failed", "commit_unknown"] as const) {
      test(`${method} preserves RPC id and blocks skill after ${state} ${kind}`, async (t) => {
        t.mock.method(console, "error", () => {});
        await withPostgresControlPlane(nativeBinding, async () => {
          a2aRouteFixture.initialize();
          a2aRouteFixture.failureState = state;
          a2aRouteFixture.failure = new PostgresError(kind);
          const response = await POST(request(method));
          assert.equal(response.status, 500);
          const body = await response.json();
          assert.equal(body.jsonrpc, "2.0");
          assert.equal(body.id, 0);
          assert.equal(body.error.code, -32603);
          assert.equal(typeof body.error.message, "string");
          assert.equal(a2aRouteFixture.handlerCalls, 0);
          assert.equal(a2aRouteFixture.streamCalls, 0);
          assert.deepEqual(
            a2aRouteFixture.writes,
            state === "submitted" ? ["submitted"] : ["submitted", "working"]
          );
        });
      });
    }
  }
}
test("completion commit_unknown never triggers a replacement failed write or repeats a skill", async (t) => {
  t.mock.method(console, "error", () => {});
  await withPostgresControlPlane(nativeBinding, async () => {
    a2aRouteFixture.initialize();
    a2aRouteFixture.failureState = "completed";
    a2aRouteFixture.failure = new PostgresError("commit_unknown");
    const response = await POST(request("message/send", "completion-id"));
    assert.equal(response.status, 500);
    const body = await response.json();
    assert.equal(body.id, "completion-id");
    assert.equal(body.error.code, -32603);
    assert.match(body.error.message, /commit outcome is unknown/);
    assert.equal(a2aRouteFixture.handlerCalls, 1);
    assert.deepEqual(a2aRouteFixture.writes, ["submitted", "working", "completed"]);
  });
});
test("a secondary failed-state ACK failure remains inside the correlated RPC boundary", async (t) => {
  t.mock.method(console, "error", () => {});
  await withPostgresControlPlane(nativeBinding, async () => {
    a2aRouteFixture.initialize();
    a2aRouteFixture.handlerFailure = new Error("skill failed");
    a2aRouteFixture.failureState = "failed";
    a2aRouteFixture.failure = new PostgresError("commit_unknown");
    const response = await POST(request("message/send", "secondary-id"));
    assert.equal(response.status, 500);
    const body = await response.json();
    assert.equal(body.id, "secondary-id");
    assert.equal(body.error.code, -32603);
    assert.match(body.error.message, /commit outcome is unknown/);
    assert.equal(a2aRouteFixture.handlerCalls, 1);
    assert.deepEqual(a2aRouteFixture.writes, ["submitted", "working", "failed"]);
  });
});
test("ordinary skill failures stay sanitized and acknowledge one failed transition", async (t) => {
  t.mock.method(console, "error", () => {});
  await withPostgresControlPlane(nativeBinding, async () => {
    a2aRouteFixture.initialize();
    a2aRouteFixture.handlerFailure = new Error(
      "failed at /private/app/secret.ts:1\n    at run (/private/app/secret.ts:1:1)"
    );
    const response = await POST(request("message/send", "sanitized-id"));
    const body = await response.json();
    assert.equal(body.id, "sanitized-id");
    assert.equal(body.error.code, -32603);
    assert.ok(!body.error.message.includes("/private/app"));
    assert.ok(!body.error.message.includes("at run"));
    assert.deepEqual(a2aRouteFixture.writes, ["submitted", "working", "failed"]);
  });
});
test("authenticated success retains all durable ACKs before returning the task", async () => {
  await withPostgresControlPlane(nativeBinding, async () => {
    a2aRouteFixture.initialize();
    const response = await POST(request("message/send", "success-id"));
    const body = await response.json();
    assert.equal(body.id, "success-id");
    assert.equal(body.result.task.state, "completed");
    assert.equal(a2aRouteFixture.handlerCalls, 1);
    assert.deepEqual(a2aRouteFixture.writes, ["submitted", "working", "completed"]);
  });
});
test("invalid authentication still rejects before task creation", async () => {
  await withPostgresControlPlane(nativeBinding, async () => {
    a2aRouteFixture.initialize();
    const response = await POST(request("message/send", 7, "wrong"));
    assert.equal((await response.json()).error.code, -32600);
    assert.equal(a2aRouteFixture.handlerCalls, 0);
    assert.deepEqual(a2aRouteFixture.writes, []);
  });
});
