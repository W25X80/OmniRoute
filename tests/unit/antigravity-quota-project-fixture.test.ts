import assert from "node:assert/strict";
import test from "node:test";
import { getAntigravityUsage } from "../../open-sse/services/usage/antigravity.ts";

const MODEL = "gemini-3.7-flash-high";
const RESET = "2026-10-11T12:00:00.000Z";
const PROJECT = "l61-primary-quota-project";

test("Antigravity live quota requires a project and parses the primary POST buckets response", async () => {
  const originalFetch = globalThis.fetch;
  const oldMode = process.env.ANTIGRAVITY_CREDITS;
  const requests: Array<{ path: string; method?: string; project: unknown }> = [];
  const unexpected: string[] = [];
  globalThis.fetch = async (input, options) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const path = new URL(url).pathname;
    const body = JSON.parse(String(options?.body || "{}")) as Record<string, unknown>;
    requests.push({ path, method: options?.method, project: body.project });
    switch (path) {
      case "/v1internal:loadCodeAssist":
        return Response.json({});
      case "/v1internal:fetchAvailableModels":
        return Response.json({
          models: { [MODEL]: { quotaInfo: { remainingFraction: 1, resetTime: RESET } } },
        });
      case "/v1internal:retrieveUserQuota":
        return Response.json({
          buckets: [{ modelId: MODEL, remainingFraction: 0.25, resetTime: RESET }],
        });
      case "/v1internal:retrieveUserQuotaSummary":
        return new Response("{}", { status: 404 });
      default:
        unexpected.push(path);
        throw new Error("Unexpected Antigravity fixture endpoint");
    }
  };
  process.env.ANTIGRAVITY_CREDITS = "off";
  try {
    const missing = await getAntigravityUsage(
      "antigravity",
      "l61-no-project-token",
      {},
      undefined,
      undefined,
      { forceRefresh: true }
    );
    assert.ok(missing.quotas);
    assert.equal(missing.quotas[MODEL].quotaSource, "fetchAvailableModels");
    assert.equal(
      requests.filter((request) => request.path === "/v1internal:retrieveUserQuota").length,
      0
    );

    const firstRequest = requests.length;
    const live = await getAntigravityUsage(
      "antigravity",
      "l61-explicit-project-token",
      {},
      PROJECT,
      undefined,
      { forceRefresh: true }
    );
    assert.ok(live.quotas);
    assert.equal(live.quotas[MODEL].quotaSource, "retrieveUserQuota");
    assert.equal(live.quotas[MODEL].used, 750);
    assert.equal(live.quotas[MODEL].remainingPercentage, 25);
    const calls = requests
      .slice(firstRequest)
      .filter((request) => request.path === "/v1internal:retrieveUserQuota");
    assert.equal(calls.length, 1);
    assert.equal(calls[0].method, "POST");
    assert.equal(calls[0].project, PROJECT);
    assert.deepEqual(unexpected, []);
  } finally {
    globalThis.fetch = originalFetch;
    if (oldMode === undefined) delete process.env.ANTIGRAVITY_CREDITS;
    else process.env.ANTIGRAVITY_CREDITS = oldMode;
  }
});
