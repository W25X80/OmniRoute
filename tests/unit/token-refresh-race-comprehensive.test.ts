import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "fs/promises";
import path from "path";
import {
  controlledBarrier,
  controlledOwnerModule,
} from "../helpers/persistence/controlledOwnerModule.ts";

const root = path.resolve(import.meta.dirname, "../..");

const read = (rel: string) => readFile(path.join(root, rel), "utf8");

test("Fix A: getAccessToken accepts an onPersist parameter", async () => {
  const src = await read("open-sse/services/tokenRefresh.ts");
  // Accepts either the legacy `(result: any) => Promise<void>` or the
  // refactored named-type form (RefreshPersistFn) — the latter is preferred
  // because it satisfies the t11 any-budget.
  assert.match(
    src,
    /export async function getAccessToken\([\s\S]{0,500}onPersist\?:\s*(?:RefreshPersistFn|\(result:\s*\w+\)\s*=>\s*Promise<void>)/,
    "getAccessToken must declare onPersist as the 5th parameter"
  );
});

test("getAccessToken shares the mutex until its persistence callback settles", async () => {
  const entered = controlledBarrier(),
    finish = controlledBarrier();
  const mutex = new Map<string, { waiters: number }>();
  let refreshes = 0,
    persists = 0,
    casChecks = 0;
  type Service = typeof import("../../open-sse/services/tokenRefresh.ts");
  const service = await controlledOwnerModule<Pick<Service, "getAccessToken">>(
    new URL("../../open-sse/services/tokenRefresh.ts", import.meta.url),
    {},
    {
      connectionRefreshMutex: mutex,
      refreshMutexMaxMs: 5000,
      getActiveOnPersist: () => undefined,
      // This oracle owns mutex ordering; native lifetime has a separate real-runtime oracle.
      runNativeRefreshWork: (body: () => Promise<unknown>) => body(),
      _getAccessTokenWithStalenessCheck: async () => {
        refreshes++;
        return { accessToken: "new-access", refreshToken: "new-refresh" };
      },
      casGuardShouldSkipPersist: async () => {
        casChecks++;
        return false;
      },
      setTimeout,
      clearTimeout,
    },
    (source) =>
      source.slice(
        source.indexOf("export async function getAccessToken("),
        source.indexOf("async function _getAccessTokenWithStalenessCheck")
      )
  );
  const pending: Promise<unknown>[] = [];
  try {
    const credentials = { connectionId: "mutex-persist", refreshToken: "old-refresh" };
    const first = service.getAccessToken("codex", credentials, undefined, null, async () => {
      persists++;
      entered.release();
      await finish.promise;
    });
    pending.push(first);
    await entered.promise;
    assert.ok(mutex.has(credentials.connectionId), "persistence must remain inside the mutex");
    const second = service.getAccessToken("codex", credentials, undefined, null, async () => {
      assert.fail("a waiter must share the leader's callback");
    });
    pending.push(second);
    let settled = false;
    const result = Promise.all([first, second]).then((values) => {
      settled = true;
      return values;
    });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(settled, false, "neither caller may observe success before persistence");
    assert.equal(refreshes, 1);
    assert.equal(persists, 1);
    assert.equal(casChecks, 1);
    assert.equal(mutex.get(credentials.connectionId)?.waiters, 1);
    finish.release();
    assert.deepEqual(
      (await result).map((value) => value?.accessToken),
      ["new-access", "new-access"]
    );
    assert.equal(mutex.size, 0, "the shared mutex releases after persistence");
  } finally {
    finish.release();
    await Promise.allSettled(pending);
  }
});

test("Fix A: runWithOnPersist + getActiveOnPersist are exported for executor plumbing", async () => {
  const src = await read("open-sse/services/tokenRefresh.ts");
  assert.match(src, /export function runWithOnPersist\b/);
  assert.match(src, /export function getActiveOnPersist\b/);
  assert.match(src, /AsyncLocalStorage/);
});

test("Fix A: chatCore.ts wraps executor.refreshCredentials with runWithOnPersist", async () => {
  const src = await read("open-sse/handlers/chatCore.ts");
  assert.match(
    src,
    /runWithOnPersist\([\s\S]{0,200}executor\.refreshCredentials/,
    "chatCore.ts reactive 401 path must wrap refreshCredentials with runWithOnPersist"
  );
});

test("Fix A: base.ts proactive refresh also wraps refreshCredentials with runWithOnPersist", async () => {
  const src = await read("open-sse/executors/base.ts");
  assert.match(
    src,
    /runWithOnPersist\([\s\S]{0,200}this\.refreshCredentials/,
    "base.ts proactive needsRefresh branch must wrap refreshCredentials with runWithOnPersist"
  );
});

test("Fix B: refreshOAuthToken in test/route.ts includes connectionId in credentials", async () => {
  const src = await read("src/app/api/providers/[id]/test/route.ts");
  const fnIdx = src.indexOf("async function refreshOAuthToken(");
  assert.ok(fnIdx >= 0, "refreshOAuthToken function must exist");
  const fnSlice = src.slice(fnIdx, fnIdx + 2500);
  assert.match(
    fnSlice,
    /connectionId:\s*connection\.id/,
    "refreshOAuthToken credentials must include connectionId so Layer 1 (per-connection mutex) is used instead of Layer 2 (token-hash dedup)"
  );
  assert.match(
    fnSlice,
    /onPersist|async\s*\(refreshed\)\s*=>/,
    "refreshOAuthToken must pass an onPersist callback so the DB write is atomic with the network refresh"
  );
});

test("Fix C reverted: codexAuthImport does NOT refresh tokens on import (avoids family revocation on stale auth.json)", async () => {
  const src = await read("src/lib/oauth/utils/codexAuthImport.ts");
  assert.doesNotMatch(
    src,
    /refreshConnectionTokensOnImport\(/,
    "Fix C was reverted because auth.json files exported from Codex CLI are often partially rotated; refreshing with a stale refresh_token caused upstream to invalidate the entire token family"
  );
  assert.doesNotMatch(
    src,
    /import\s*\{[^}]*getAccessToken[^}]*\}\s*from\s*"@omniroute\/open-sse\/services\/tokenRefresh/,
    "codexAuthImport should not import getAccessToken (refresh-on-import was reverted)"
  );
});

test("codexAuthImport public surface keeps only consumed auth import types", async () => {
  const src = await read("src/lib/oauth/utils/codexAuthImport.ts");
  assert.doesNotMatch(src, /export interface CodexAuthFileInput\b/);
  assert.match(src, /export interface ParsedCodexAuth\b/);
  assert.match(src, /export interface CreateConnectionOptions\b/);
});

test("Fix D: staleness fallback returns absolute expiresAt, not raw expiresIn", async () => {
  const src = await read("open-sse/services/tokenRefresh.ts");
  // Locate the actual return statement, not the JSDoc mention.
  // The anchor is the log line "DB token is still valid. Skipping OAuth refresh."
  // which only appears in the code path (the JSDoc says "DB token is still valid it is").
  const idx = src.indexOf("DB token is still valid. Skipping");
  assert.ok(idx >= 0, "Staleness-valid log line not found");
  const slice = src.slice(idx, idx + 800);
  assert.match(
    slice,
    /expiresAt:\s*dbConnection\.expiresAt/,
    "Staleness fallback must return absolute expiresAt"
  );
  assert.doesNotMatch(
    slice,
    /expiresIn:\s*dbConnection\.expiresIn(?!\s*\/\/)/,
    "Staleness fallback must NOT return raw expiresIn (causes downstream lifetime extension)"
  );
});

test("Fix D: src/sse wrapper prefers expiresAt over expiresIn when recomputing", async () => {
  const src = await read("src/sse/services/tokenRefresh.ts");
  // The recompute block in checkAndRefreshToken
  assert.match(
    src,
    /expiresAt:\s*newCredentials\.expiresAt\s*\?\s*newCredentials\.expiresAt/,
    "checkAndRefreshToken must prefer the absolute expiresAt before falling back to expiresIn arithmetic"
  );
});

test("Fix E: chatCore.ts moves credentials mutation INSIDE the mutex closure via persistFn", async () => {
  const src = await read("open-sse/handlers/chatCore.ts");
  // The persistFn closure must do BOTH the Object.assign AND the user callback
  const persistFnMatch = src.match(
    /const\s+persistFn\s*=[\s\S]{0,800}Object\.assign\(credentials,[\s\S]{0,300}onCredentialsRefreshed/
  );
  assert.ok(
    persistFnMatch,
    "chatCore.ts must build persistFn that both Object.assigns credentials and calls onCredentialsRefreshed, so both happen INSIDE the mutex"
  );
});

test("Fix F removed: no over-eager skip that would bypass legitimate refreshes", async () => {
  const src = await read("open-sse/services/tokenRefresh.ts");
  // Fix F was intentionally removed because it skipped refreshes that the
  // caller (checkAndRefreshToken) had already decided were needed. Verify the
  // dead-code branch is gone (no `credentials.accessToken === dbConnection.accessToken`
  // skip path remains).
  assert.doesNotMatch(
    src,
    /credentials\.accessToken\s*===\s*dbConnection\.accessToken[\s\S]{0,300}Skipping OAuth refresh/,
    "Fix F was removed because it caused legitimate refreshes to be skipped"
  );
});

test("Mutex consolidation: src/sse wrapper no longer holds its own connectionRefreshMutex map", async () => {
  const src = await read("src/sse/services/tokenRefresh.ts");
  // It's OK to keep a withConnectionRefreshMutex shim for back-compat, but
  // the actual Map should not be re-instantiated here (use the open-sse one).
  const hasOwnMap = /const\s+connectionRefreshMutex\s*=\s*new\s+Map/.test(src);
  if (hasOwnMap) {
    // If a Map remains, the file must clearly document that it is a deprecated
    // shim — otherwise it's a regression that recreates the dual-mutex bug.
    assert.match(
      src,
      /deprecated|legacy|shim|redundant|kept for back-compat/i,
      "If src/sse keeps its own connectionRefreshMutex Map, the file must document it as deprecated/legacy"
    );
  }
});

test("Imports: chatCore.ts imports runWithOnPersist from open-sse tokenRefresh", async () => {
  const src = await read("open-sse/handlers/chatCore.ts");
  assert.match(src, /runWithOnPersist/);
  assert.match(src, /from\s+"\.\.\/services\/tokenRefresh\.ts"/);
});

test("Imports: base.ts imports runWithOnPersist from open-sse tokenRefresh", async () => {
  const src = await read("open-sse/executors/base.ts");
  assert.match(src, /runWithOnPersist/);
  assert.match(src, /from\s+"\.\.\/services\/tokenRefresh\.ts"/);
});

test("serialized refresh re-checks rotation inside the lane, not before waiting", async () => {
  const src = await read("open-sse/services/tokenRefresh.ts");
  const start = src.indexOf("async function _getAccessTokenWithStalenessCheck");
  const inner = src.indexOf("async function _refreshWithFreshCredentials");
  assert.ok(start >= 0 && inner > start, "staleness helper must wrap the freshness re-check");
  const wrapper = src.slice(start, inner);
  assert.match(
    wrapper,
    // Whitespace-tolerant: #15002 added a `log` argument, so prettier wraps the call.
    /serializeRefresh\(\s*provider,\s*\(\)\s*=>/,
    "the network POST must stay behind serializeRefresh"
  );
  assert.match(wrapper, /_refreshWithFreshCredentials/);
  assert.doesNotMatch(
    wrapper,
    /lookupRotation/,
    "lookupRotation before serializeRefresh is the race that burns a Claude refresh token"
  );
  const body = src.slice(inner, inner + 2500);
  assert.match(body, /lookupRotation\(/);
  assert.match(body, /recordRotation\(/);
  assert.match(body, /_getAccessTokenInternal\(/);
});
