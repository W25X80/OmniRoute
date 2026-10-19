import assert from "node:assert/strict";
import test from "node:test";
import { getClaudeUsage } from "../../open-sse/services/usage/claude.ts";
import {
  isClaudeOauthUsageCoolingDown,
  _resetClaudeOauthUsageCooldown,
} from "../../open-sse/services/claudeUsageCooldown.ts";
import { _resetClaudeUsagePollingState } from "../../open-sse/services/usage/claude.ts";

test.beforeEach(() => {
  _resetClaudeUsagePollingState();
  _resetClaudeOauthUsageCooldown();
});

const turn = () => new Promise<void>((resolve) => setImmediate(resolve));
const stalledJson = () => {
  const response = new Response("pending");
  response.json = () => new Promise(() => {});
  return response;
};

for (const stalled of ["oauth", "settings", "organization", "bootstrap"]) {
  test(`Claude ${stalled} body consumption has a complete operation deadline`, async (t) => {
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const signals: AbortSignal[] = [];
    t.mock.method(
      globalThis,
      "fetch",
      async (input: string | URL | Request, init?: RequestInit) => {
        const url = String(input);
        assert.ok(init?.signal, "every usage fetch, including legacy, carries a deadline signal");
        signals.push(init.signal);
        if (url.includes("bootstrap"))
          return stalled === "bootstrap" ? stalledJson() : Response.json({});
        if (url.endsWith("oauth/usage")) {
          if (stalled === "oauth") return stalledJson();
          return stalled === "bootstrap" ? Response.json({}) : new Response(null, { status: 404 });
        }
        if (url.endsWith("settings"))
          return stalled === "settings"
            ? stalledJson()
            : Response.json({ organization_id: "test-org" });
        return stalledJson();
      }
    );
    let settled = false;
    const result = getClaudeUsage(`deadline-${stalled}`).then((value) => {
      settled = true;
      return value;
    });
    await turn();
    assert.equal(settled, false);
    t.mock.timers.tick(10_001);
    await turn();
    assert.equal(settled, true, "headers alone must not clear the operation deadline");
    assert.ok(signals.some((signal) => signal.aborted));
    assert.ok(await result);
  });
}

test("concurrent polling of one token shares bootstrap and OAuth requests", async (t) => {
  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  let calls = 0;
  t.mock.method(globalThis, "fetch", async () => {
    calls++;
    await gate;
    return Response.json({});
  });
  const first = getClaudeUsage("same-token");
  const second = getClaudeUsage("same-token");
  await turn();
  assert.equal(calls, 2);
  release();
  assert.deepEqual(await first, await second);
});

test("OAuth Retry-After is honored without a legacy fallback", async (t) => {
  let legacyCalls = 0;
  t.mock.method(globalThis, "fetch", async (input: string | URL | Request) => {
    const url = String(input);
    if (url.includes("bootstrap")) return Response.json({});
    if (url.endsWith("oauth/usage"))
      return new Response(null, { status: 429, headers: { "retry-after": "900" } });
    legacyCalls++;
    return new Response(null, { status: 403 });
  });
  await getClaudeUsage("retry-after-token");
  assert.equal(legacyCalls, 0);
  assert.equal(isClaudeOauthUsageCoolingDown("retry-after-token", Date.now() + 600_000), true);
});
