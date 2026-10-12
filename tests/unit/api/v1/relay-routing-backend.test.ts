import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  getBifrostRoutingConfig,
  getRoutingFallbackHeader,
  getRoutingFallbackReasonHeader,
  resolveRelayRoutingBackend,
  shouldTryBifrost,
  shouldTryBifrostForRequest,
} from "../../../../src/app/api/v1/relay/chat/completions/routingBackend.ts";

test("relay routing backend defaults to TypeScript without bifrost", () => {
  const env = {};

  assert.equal(resolveRelayRoutingBackend(env), "ts");
  assert.equal(getBifrostRoutingConfig(env), null);
});

test("relay routing backend auto-enables bifrost when base URL is configured", () => {
  const env = {
    BIFROST_BASE_URL: "http://127.0.0.1:8080/",
    OMNIROUTE_BIFROST_KEY: "sidecar-key",
    BIFROST_TIMEOUT_MS: "250",
  };

  const config = getBifrostRoutingConfig(env);

  assert.equal(resolveRelayRoutingBackend(env), "auto");
  assert.equal(config?.baseUrl, "http://127.0.0.1:8080");
  assert.equal(config?.apiKey, "sidecar-key");
  assert.equal(config?.timeoutMs, 250);
  assert.equal(config?.streamingEnabled, true);
  assert.equal(config?.enabled, true);
  assert.equal(shouldTryBifrost("auto", config), true);
});

test("relay routing backend honors explicit TS and strict bifrost modes", () => {
  const env = {
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
    OMNIROUTE_RELAY_BACKEND: "ts",
  };
  const config = getBifrostRoutingConfig(env);

  assert.equal(resolveRelayRoutingBackend(env), "ts");
  assert.equal(shouldTryBifrost("ts", config), false);

  env.OMNIROUTE_RELAY_BACKEND = "bifrost";
  assert.equal(resolveRelayRoutingBackend(env), "bifrost");
  assert.equal(shouldTryBifrost("bifrost", config), true);
});

test("relay routing backend respects bifrost killswitch", () => {
  const env = {
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
    BIFROST_ENABLED: "0",
  };
  const config = getBifrostRoutingConfig(env);

  assert.equal(resolveRelayRoutingBackend(env), "ts");
  assert.equal(config?.enabled, false);
  assert.equal(shouldTryBifrost("auto", config), false);
});

test("relay routing backend falls back on invalid timeout values", () => {
  const env = {
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
    BIFROST_TIMEOUT_MS: "not-a-number",
  };

  assert.equal(getBifrostRoutingConfig(env)?.timeoutMs, 30000);
});

test("relay routing backend exposes TS fallback header only for enabled auto bifrost fallback", () => {
  const config = getBifrostRoutingConfig({
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
  });

  assert.equal(getRoutingFallbackHeader("auto", config), "bifrost");
  assert.equal(getRoutingFallbackHeader("ts", config), undefined);
  assert.equal(getRoutingFallbackHeader("bifrost", config), undefined);
  assert.equal(
    getRoutingFallbackHeader(
      "auto",
      getBifrostRoutingConfig({
        BIFROST_BASE_URL: "http://127.0.0.1:8080",
        BIFROST_ENABLED: "0",
      })
    ),
    undefined
  );
  assert.equal(getRoutingFallbackHeader("auto", null), undefined);
});

test("relay routing backend keeps strict bifrost failures out of auto fallback accounting", () => {
  const config = getBifrostRoutingConfig({
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
  });

  assert.equal(shouldTryBifrost("bifrost", config), true);
  assert.equal(getRoutingFallbackHeader("bifrost", config), undefined);
  assert.equal(resolveRelayRoutingBackend({ OMNIROUTE_RELAY_BACKEND: "bifrost" }), "bifrost");
});

test("relay routing backend auto mode tries bifrost for manifest-eligible providers", () => {
  const config = getBifrostRoutingConfig({
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
  });

  assert.deepEqual(
    shouldTryBifrostForRequest("auto", config, { model: "openai/gpt-4.1" }, () => ({
      eligible: true,
      reasons: [],
    })),
    { tryBifrost: true }
  );
});

test("relay routing backend auto mode skips bifrost for manifest-ineligible providers", () => {
  const config = getBifrostRoutingConfig({
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
  });

  assert.deepEqual(
    shouldTryBifrostForRequest("auto", config, { model: "cw/claude-sonnet-4.6" }, () => ({
      eligible: false,
      reasons: ["custom executor: claude-web"],
    })),
    { tryBifrost: false, fallbackReason: "bifrost-ineligible" }
  );
});

test("relay routing backend auto mode keeps unknown providers on TS fallback", () => {
  const config = getBifrostRoutingConfig({
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
  });

  assert.deepEqual(
    shouldTryBifrostForRequest("auto", config, { model: "unknown/model" }, () => null),
    { tryBifrost: false, fallbackReason: "bifrost-provider-unknown" }
  );
});

test("relay routing backend strict bifrost bypasses manifest eligibility", () => {
  const config = getBifrostRoutingConfig({
    BIFROST_BASE_URL: "http://127.0.0.1:8080",
  });

  assert.deepEqual(
    shouldTryBifrostForRequest("bifrost", config, { model: "cw/claude-sonnet-4.6" }, () => ({
      eligible: false,
      reasons: ["custom executor: claude-web"],
    })),
    { tryBifrost: true }
  );
});

test("automatic relay keeps the Bifrost timeout active until an SSE stream finalizes", () => {
  const routeSource = readFileSync(
    new URL("../../../../src/app/api/v1/relay/chat/completions/route.ts", import.meta.url),
    "utf8"
  );
  const forwardStart = routeSource.indexOf("async function forwardToBifrost");
  const forwardEnd = routeSource.indexOf("export async function OPTIONS", forwardStart);
  assert.ok(forwardStart >= 0 && forwardEnd > forwardStart, "Bifrost function boundaries exist");
  const forwardToBifrost = routeSource.slice(forwardStart, forwardEnd);
  const streamStart = forwardToBifrost.indexOf("if (wantsStream && upstream.body)");
  const nonStreamStart = forwardToBifrost.indexOf(
    "clearTimeout(tid);\n    await recordUsage(",
    streamStart
  );
  assert.ok(streamStart >= 0 && nonStreamStart > streamStart, "stream boundaries exist");
  const streamBranch = forwardToBifrost.slice(streamStart, nonStreamStart);
  const nonStreamBranch = forwardToBifrost.slice(nonStreamStart);

  assert.match(
    forwardToBifrost,
    /const tid = setTimeout\(\(\) => \{\s*timedOut = true;\s*ac\.abort\(\);\s*\}, config\.timeoutMs\)/
  );
  assert.match(forwardToBifrost, /signal: ac\.signal/);
  assert.match(
    streamBranch,
    /if \(wantsStream && upstream\.body\) \{\s*const stream = finalizeReadableStream\(upstream\.body, async \(error\) => \{\s*clearTimeout\(tid\)/
  );
  assert.match(streamBranch, /const statusCode = timedOut \? 504 : upstream\.status/);
  assert.match(streamBranch, /error && backend === "auto"/);
  assert.match(streamBranch, /recordBifrostFailure\(/);
  assert.match(streamBranch, /await recordUsage\(/);
  assert.match(streamBranch, /return new Response\(stream,/);
  assert.match(nonStreamBranch, /^clearTimeout\(tid\);\s*await recordUsage\(/);
});

test("relay routing fallback reason header strips dynamic cooldown detail to the stable code", () => {
  assert.equal(
    getRoutingFallbackReasonHeader("bifrost-cooldown; remaining=1500"),
    "bifrost-cooldown"
  );
});

test("relay routing fallback reason header passes already-stable reasons through unchanged", () => {
  assert.equal(getRoutingFallbackReasonHeader("bifrost-error"), "bifrost-error");
  assert.equal(getRoutingFallbackReasonHeader("bifrost-ineligible"), "bifrost-ineligible");
  assert.equal(
    getRoutingFallbackReasonHeader("bifrost-provider-unknown"),
    "bifrost-provider-unknown"
  );
});

test("relay routing fallback reason header stays unset for the bare static legacy value", () => {
  assert.equal(getRoutingFallbackReasonHeader("bifrost"), undefined);
});

test("relay routing fallback reason header stays unset for null/undefined/unrecognized input", () => {
  assert.equal(getRoutingFallbackReasonHeader(null), undefined);
  assert.equal(getRoutingFallbackReasonHeader(undefined), undefined);
  assert.equal(getRoutingFallbackReasonHeader("something-unrecognized"), undefined);
});
