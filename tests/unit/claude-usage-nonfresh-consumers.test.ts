import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test, { type TestContext } from "node:test";

const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), "omniroute-claude-nonfresh-"));
process.env.DATA_DIR = dataDir;
process.env.API_KEY_SECRET = "claude-nonfresh-test-secret";
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";

const core = await import("../../src/lib/db/core.ts");
const providersDb = await import("../../src/lib/db/providers.ts");
const usageService = await import("../../open-sse/services/usage.ts");
const usageLeaf = await import("../../open-sse/services/usage/claude.ts");
const cooldown = await import("../../open-sse/services/claudeUsageCooldown.ts");
const provenance = await import("../../open-sse/services/usage/claudeUsageProvenance.ts");
const freeAccess = await import("../../open-sse/services/autoCombo/freeAccessQuota.ts");
const saturation = await import("../../src/lib/quota/saturationSignals.ts");
const buckets = await import("../../src/lib/quota/accountBuckets.ts");
const providerLimits = await import("../../src/lib/usage/providerLimits.ts");
const providerLimitsDb = await import("../../src/lib/db/providerLimits.ts");
const quotaSnapshots = await import("../../src/lib/db/quotaSnapshots.ts");
const quotaCache = await import("../../src/domain/quotaCache.ts");

// Capture the fully installed production wrapper after every production import.
// Individual tests replace only the transport and restore this exact function.
const productionFetch = globalThis.fetch;

type UsageFixture = {
  queued: boolean;
  resetAt: string;
  bootstrapAccountUuid: string;
};

type AnthropicMock = {
  calls: string[];
  restore: () => void;
};

function installAnthropicMock(
  t: TestContext,
  fixtures: Record<string, UsageFixture>
): AnthropicMock {
  const calls: string[] = [];
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const rawUrl = input instanceof Request ? input.url : String(input);
      const url = new URL(rawUrl);
      calls.push(`${url.pathname}${url.search}`);
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected test transport URL: ${url.origin}${url.pathname}`);
      }

      const requestHeaders = new Headers(
        init?.headers ?? (input instanceof Request ? input.headers : undefined)
      );
      const token = requestHeaders.get("authorization")?.replace(/^Bearer\s+/i, "");
      const fixture = token ? fixtures[token] : undefined;
      if (!fixture) throw new Error(`Unexpected Anthropic test credential for ${url.pathname}`);

      if (url.pathname === "/api/oauth/usage") {
        return Response.json({
          tier: "Claude Max",
          five_hour: { utilization: 0, resets_at: fixture.resetAt },
          seven_day: { utilization: 15, resets_at: fixture.resetAt },
          extra_usage: { queued: fixture.queued },
        });
      }
      if (url.pathname === "/api/claude_cli/bootstrap") {
        return Response.json({
          oauth_account: {
            account_uuid: fixture.bootstrapAccountUuid,
            organization_uuid: `${fixture.bootstrapAccountUuid}-org`,
            organization_name: `${fixture.bootstrapAccountUuid} organization`,
            organization_type: "claude_max",
            organization_rate_limit_tier: "max-new",
          },
        });
      }
      throw new Error(`Unexpected Anthropic test transport path: ${url.pathname}`);
    }
  );

  return {
    calls,
    restore: () => {
      fetchMock.mock.restore();
      assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
    },
  };
}

function freshResetAt(): string {
  return new Date(Date.now() + 10 * 60_000).toISOString();
}

async function waitUntil(predicate: () => boolean, message: string): Promise<void> {
  for (let i = 0; i < 100; i += 1) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setTimeout(resolve, 5));
  }
  assert.fail(message);
}

async function waitUntilImmediate(predicate: () => boolean, message: string): Promise<void> {
  for (let i = 0; i < 100; i += 1) {
    if (predicate()) return;
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.fail(message);
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((accept) => {
    resolve = accept;
  });
  return { promise, resolve };
}

function getResetEventCount(connectionId: string): number {
  const db = core.getDbInstance() as unknown as {
    prepare: (sql: string) => { get: (id: string) => { count: number } | undefined };
  };
  return (
    db
      .prepare("SELECT COUNT(*) AS count FROM provider_quota_reset_events WHERE connection_id = ?")
      .get(connectionId)?.count ?? 0
  );
}

function assertNoPolicyFields(value: Record<string, unknown>, source: string): void {
  for (const field of ["quotas", "modelQuotas", "plan", "extraUsage", "bootstrap"]) {
    assert.equal(field in value, false, `${source} must not expose ${field} to policy consumers`);
  }
}

test.beforeEach(() => {
  usageLeaf._resetClaudeUsagePollingState();
  cooldown._resetClaudeOauthUsageCooldown();
  freeAccess.__testing.cache.clear();
  saturation._clearSaturationCache();
  buckets._clearBucketsForTest();
  quotaCache.__clearForTests();
  saturation.__setAnthropicSaturationDepsForTests(null);
});

test.after(async () => {
  saturation.__setAnthropicSaturationDepsForTests(null);
  quotaCache.stopBackgroundRefresh();
  core.resetDbInstance();
  fs.rmSync(dataDir, { recursive: true, force: true });
});

test("default dispatch redacts memo and stale-429 authority while provider-limit display keeps it", async (t) => {
  const token = "claude-dispatch-provenance-token";
  const mock = installAnthropicMock(t, {
    [token]: {
      queued: true,
      resetAt: "2099-09-10T15:59:59.522Z",
      bootstrapAccountUuid: "dispatch-account",
    },
  });
  try {
    const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    const freshProvenance = provenance.getClaudeUsageProvenance(fresh);
    assert.equal(freshProvenance?.source, "oauth");
    assert.equal(provenance.isFreshClaudeUsage(fresh), true);
    assert.ok(fresh.quotas);
    assert.deepEqual(fresh.extraUsage, { queued: true });
    assert.equal(fresh.bootstrap?.organization_uuid, "dispatch-account-org");

    const memo = await usageService.getUsageForProvider({ provider: "claude", accessToken: token });
    assert.equal(provenance.getClaudeUsageProvenance(memo)?.source, "success-memo");
    assertNoPolicyFields(memo, "default success-memo dispatch");
    assert.equal(memo._cached, true);

    const memoDisplay = await usageService.getUsageForProvider(
      { provider: "claude", accessToken: token },
      { allowStaleDisplay: true }
    );
    assert.equal(provenance.getClaudeUsageProvenance(memoDisplay)?.source, "success-memo");
    assert.ok(memoDisplay.quotas, "only the provider-limit display opt-in receives cached quotas");

    cooldown.markClaudeOauthUsage429(token, Date.now(), 180_000);
    const stale = await usageService.getUsageForProvider({
      provider: "claude",
      accessToken: token,
    });
    assert.equal(provenance.getClaudeUsageProvenance(stale)?.source, "stale-429");
    assertNoPolicyFields(stale, "default stale-429 dispatch");
    assert.equal(stale._stale, true);
    assert.equal(stale._staleSince, new Date(freshProvenance!.observedAt!).toISOString());

    const staleDisplay = await usageService.getUsageForProvider(
      { provider: "claude", accessToken: token },
      { allowStaleDisplay: true }
    );
    assert.equal(provenance.getClaudeUsageProvenance(staleDisplay)?.source, "stale-429");
    assert.ok(staleDisplay.quotas);
    assert.deepEqual(staleDisplay.extraUsage, { queued: true });
    assert.equal(staleDisplay.bootstrap?.organization_uuid, "dispatch-account-org");
    assert.equal(mock.calls.filter((call) => call === "/api/oauth/usage").length, 1);
    assert.equal(mock.calls.filter((call) => call === "/api/claude_cli/bootstrap").length, 1);
  } finally {
    mock.restore();
  }
});

test("missing provenance and reset-generation results cannot carry Claude policy fields", async (t) => {
  const token = "claude-generation-fence-token";
  const mock = installAnthropicMock(t, {
    [token]: {
      queued: false,
      resetAt: "2099-09-10T15:59:59.522Z",
      bootstrapAccountUuid: "generation-fence-account",
    },
  });
  try {
    const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    assert.equal(provenance.isFreshClaudeUsage(fresh), true);

    const clonedWithoutProvenance = { ...fresh };
    assert.equal(provenance.isFreshClaudeUsage(clonedWithoutProvenance), false);
    assertNoPolicyFields(
      provenance.redactClaudeUsageForPolicy(clonedWithoutProvenance),
      "unprovenanced Claude clone"
    );

    usageLeaf._resetClaudeUsagePollingState();
    assert.equal(provenance.isFreshClaudeUsage(fresh), false);
    assertNoPolicyFields(
      provenance.redactClaudeUsageForPolicy(fresh),
      "pre-reset OAuth generation"
    );
    assert.equal(mock.calls.filter((call) => call === "/api/oauth/usage").length, 1);
  } finally {
    mock.restore();
  }
});

test("unconsumed OAuth error bodies are cancelled and status errors do not fall back", async (t) => {
  let cancelCount = 0;
  let legacyCalls = 0;
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL): Promise<Response> => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected status-test transport URL: ${url.origin}${url.pathname}`);
      }
      if (url.pathname === "/api/oauth/usage") {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new Uint8Array([1]));
          },
          cancel() {
            cancelCount += 1;
          },
        });
        return new Response(body, { status: 403 });
      }
      if (url.pathname === "/api/claude_cli/bootstrap") return Response.json({});
      legacyCalls += 1;
      throw new Error(`Legacy fallback must not run for status 403: ${url.pathname}`);
    }
  );
  try {
    const result = await usageLeaf.getClaudeUsage("claude-status-cancel-token", {
      forceRefresh: true,
    });
    assert.match(result.message ?? "", /HTTP 403/);
    assert.equal(cancelCount, 1);
    assert.equal(legacyCalls, 0);
  } finally {
    fetchMock.mock.restore();
    assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
  }
});

test("free-access routing stays UNKNOWN when only a Claude success memo is available", async (t) => {
  const token = "claude-free-access-memo-token";
  const mock = installAnthropicMock(t, {
    [token]: {
      queued: false,
      resetAt: "2099-09-10T15:59:59.522Z",
      bootstrapAccountUuid: "free-access-account",
    },
  });
  try {
    const connection = await providersDb.createProviderConnection({
      provider: "claude",
      authType: "oauth",
      name: "Nonfresh free-access oracle",
      email: "claude-nonfresh-free-access@example.test",
      accessToken: token,
      isActive: true,
    });
    const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    assert.equal(provenance.isFreshClaudeUsage(fresh), true);

    const connectionId = connection.id as string;
    const cacheKey = `claude::${connectionId}`;
    assert.equal(freeAccess.resolveFreeAccessState("claude", connectionId), undefined);
    await waitUntil(
      () => freeAccess.__testing.cache.has(cacheKey),
      "free-access refresh did not settle"
    );
    const entry = freeAccess.__testing.cache.get(cacheKey);
    assert.equal(entry?.state.status, "UNKNOWN");
    assert.equal(entry?.state.remainingFreeAllowance, null);
    assert.notEqual(entry?.state.status, "SAFE");
    assert.equal(mock.calls.filter((call) => call === "/api/oauth/usage").length, 1);
  } finally {
    mock.restore();
  }
});

test("stale Claude usage cannot clear an active saturation bucket", async (t) => {
  const token = "claude-saturation-stale-token";
  const mock = installAnthropicMock(t, {
    [token]: {
      queued: false,
      resetAt: "2099-09-10T15:59:59.522Z",
      bootstrapAccountUuid: "saturation-account",
    },
  });
  try {
    const connectionId = "claude-stale-saturation-oracle";
    const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    assert.equal(provenance.isFreshClaudeUsage(fresh), true);
    cooldown.markClaudeOauthUsage429(token, Date.now(), 180_000);
    const resetAt = freshResetAt();
    buckets.recordUsage(connectionId, "5h", 100, resetAt);
    assert.equal(buckets.isBucketSaturated(connectionId, "5h"), true);

    saturation.__setAnthropicSaturationDepsForTests({
      loadConnection: async () => ({
        id: connectionId,
        provider: "claude",
        authType: "oauth",
        accessToken: token,
      }),
      fetchUsage: async (connection) =>
        usageService.getUsageForProvider(
          connection as Parameters<typeof usageService.getUsageForProvider>[0]
        ),
    });
    saturation._clearSaturationCache();
    const result = await saturation.getSaturation(connectionId, "claude", {
      unit: "percent",
      window: "5h",
    });
    assert.equal(result, 0, "redacted stale quotas use the existing header fallback");
    assert.equal(
      buckets.isBucketSaturated(connectionId, "5h"),
      true,
      "missing fresh quota data cannot clear the active bucket"
    );
    assert.equal(mock.calls.filter((call) => call === "/api/oauth/usage").length, 1);
  } finally {
    mock.restore();
  }
});

test("reset fences an old Claude result without its finally clearing a newer pending poll", async (t) => {
  let releaseOld!: (value: Record<string, unknown>) => void;
  let releaseNew!: (value: Record<string, unknown>) => void;
  const oldJson = new Promise<Record<string, unknown>>((resolve) => {
    releaseOld = resolve;
  });
  const newJson = new Promise<Record<string, unknown>>((resolve) => {
    releaseNew = resolve;
  });
  let oauthCalls = 0;
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL): Promise<Response> => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected reset-fence transport URL: ${url.origin}${url.pathname}`);
      }
      if (url.pathname === "/api/oauth/usage") {
        oauthCalls += 1;
        const body = oauthCalls === 1 ? oldJson : newJson;
        if (oauthCalls > 2) throw new Error("Unexpected third Claude OAuth poll");
        const response = Response.json({});
        response.json = () => body;
        return response;
      }
      if (url.pathname === "/api/claude_cli/bootstrap") {
        return Response.json({ oauth_account: { organization_rate_limit_tier: "max" } });
      }
      throw new Error(`Unexpected reset-fence transport path: ${url.pathname}`);
    }
  );
  try {
    const oldPoll = usageLeaf.getClaudeUsage("reset-fence-token", { forceRefresh: true });
    await waitUntil(() => oauthCalls === 1, "old poll did not start");
    usageLeaf._resetClaudeUsagePollingState();

    const newPoll = usageLeaf.getClaudeUsage("reset-fence-token", { forceRefresh: true });
    await waitUntil(() => oauthCalls === 2, "new poll did not start");
    releaseOld({ five_hour: { utilization: 95, resets_at: "2099-09-10T15:59:59.522Z" } });
    const oldResult = await oldPoll;
    assert.equal(provenance.getClaudeUsageProvenance(oldResult)?.source, "superseded");
    assert.equal(provenance.isFreshClaudeUsage(oldResult), false);

    const joinedAfterOldSettles = usageLeaf.getClaudeUsage("reset-fence-token");
    assert.equal(oauthCalls, 2, "the old finally must leave the newer promise in the map");
    releaseNew({ five_hour: { utilization: 20, resets_at: "2099-09-10T15:59:59.522Z" } });
    const [newResult, joinedResult] = await Promise.all([newPoll, joinedAfterOldSettles]);
    assert.equal(newResult.quotas?.["session (5h)"].used, 20);
    assert.equal(provenance.isFreshClaudeUsage(newResult), true);
    assert.deepEqual(joinedResult, newResult);
    assert.equal(oauthCalls, 2);
  } finally {
    fetchMock.mock.restore();
    assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
  }
});

test("Claude memo and stale provider-limit displays cannot publish quota, recovery, extra-usage, or bootstrap state", async (t) => {
  for (const queued of [false, true]) {
    const token = `provider-limits-stale-${queued}`;
    const mock = installAnthropicMock(t, {
      [token]: {
        queued,
        resetAt: "2099-09-10T15:59:59.522Z",
        bootstrapAccountUuid: `account-new-${queued}`,
      },
    });
    try {
      const connection = await providersDb.createProviderConnection({
        provider: "claude",
        authType: "oauth",
        name: `Nonfresh provider limits ${queued}`,
        email: `nonfresh-provider-limits-${queued}@example.test`,
        accessToken: token,
        refreshToken: "unused-refresh-token",
        expiresAt: new Date(Date.now() + 60 * 60_000).toISOString(),
        testStatus: "unavailable",
        isActive: true,
        lastError: "Claude quota is temporarily unavailable",
        lastErrorAt: new Date(Date.now() - 60_000).toISOString(),
        lastErrorType: "quota_exhausted",
        lastErrorSource: "extra_usage",
        errorCode: 429,
        rateLimitedUntil: "2099-09-10T15:00:00.000Z",
        backoffLevel: 2,
        providerSpecificData: {
          accountUUID: `account-old-${queued}`,
          organizationUUID: `org-old-${queued}`,
          organizationName: "Old organization",
          organizationType: "claude_pro",
          organizationRateLimitTier: "old-tier",
          blockExtraUsage: true,
        },
      });
      const connectionId = connection.id as string;
      const before = (await providersDb.getProviderConnectionById(connectionId)) as Record<
        string,
        unknown
      >;
      const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
      const observation = provenance.getClaudeUsageProvenance(fresh);
      assert.equal(observation?.source, "oauth");
      assert.equal(provenance.isFreshClaudeUsage(fresh), true);

      const existingCache = queued
        ? {
            quotas: {
              "session (5h)": {
                used: 35,
                total: 100,
                remaining: 65,
                remainingPercentage: 65,
                resetAt: "2099-09-10T15:59:59.522Z",
                unlimited: false,
              },
            },
            modelQuotas: {},
            plan: "Newer saved observation",
            message: null,
            fetchedAt: "2099-09-10T16:00:00.000Z",
            source: "test-newer",
          }
        : null;
      if (existingCache) {
        await providerLimitsDb.setProviderLimitsCacheAsync(connectionId, existingCache);
      }

      quotaCache.setQuotaCache(connectionId, "claude", {
        "weekly (7d)": { remainingPercentage: 0, resetAt: "2099-09-10T15:00:00.000Z" },
      });
      quotaCache.setQuotaCache(connectionId, "claude", {
        "weekly (7d)": { remainingPercentage: 0, resetAt: "2099-09-17T15:00:00.000Z" },
      });
      const activeQuotaEntry = quotaCache.getQuotaCache(connectionId);
      assert.ok(activeQuotaEntry?.exhausted, "precondition: a real exhausted quota epoch exists");
      const snapshotsBefore =
        await quotaSnapshots.getLatestQuotaSnapshotsForConnection(connectionId);
      const resetEventsBefore = getResetEventCount(connectionId);
      assert.ok(resetEventsBefore > 0, "precondition: the quota epoch has a persisted reset event");

      const memoResult = await providerLimits.fetchAndPersistProviderLimits(
        connectionId,
        "scheduled"
      );
      assert.equal(memoResult.usage._cached, true);
      assert.equal(provenance.getClaudeUsageProvenance(memoResult.usage)?.source, "success-memo");
      assert.ok(memoResult.usage.quotas, "provider-limits may return a display-only memo");

      cooldown.markClaudeOauthUsage429(token, Date.now(), 180_000);
      const staleResult = await providerLimits.fetchAndPersistProviderLimits(
        connectionId,
        "manual"
      );
      assert.equal(staleResult.usage._stale, true);
      assert.equal(provenance.getClaudeUsageProvenance(staleResult.usage)?.source, "stale-429");
      assert.ok(staleResult.usage.quotas, "provider-limits may return a display-only stale quota");
      if (existingCache) {
        assert.equal(
          staleResult.usage._staleSince,
          existingCache.fetchedAt,
          "display prefers the newer stored observation time"
        );
      } else {
        assert.equal(
          staleResult.usage._staleSince,
          new Date(observation!.observedAt!).toISOString(),
          "stale display retains the original network observation time"
        );
      }

      assert.deepEqual(
        await providerLimitsDb.getProviderLimitsCacheAsync(connectionId),
        existingCache,
        "memo and stale results cannot create or replace the durable provider-limits cache"
      );
      assert.strictEqual(
        quotaCache.getQuotaCache(connectionId),
        activeQuotaEntry,
        "nonfresh quota cannot replace an active exhausted in-memory quota epoch"
      );
      assert.deepEqual(
        await quotaSnapshots.getLatestQuotaSnapshotsForConnection(connectionId),
        snapshotsBefore,
        "nonfresh quota cannot publish quota snapshots or a reset transition"
      );
      assert.equal(
        getResetEventCount(connectionId),
        resetEventsBefore,
        "nonfresh quota cannot publish a quota reset event"
      );

      const after = (await providersDb.getProviderConnectionById(connectionId)) as Record<
        string,
        unknown
      >;
      for (const field of [
        "testStatus",
        "lastError",
        "lastErrorAt",
        "lastErrorType",
        "lastErrorSource",
        "errorCode",
        "rateLimitedUntil",
        "backoffLevel",
        "providerSpecificData",
      ]) {
        assert.deepEqual(
          after[field],
          before[field],
          `memo/stale extraUsage=${queued} changed ${field}`
        );
      }
      assert.equal(mock.calls.filter((call) => call === "/api/oauth/usage").length, 1);
      assert.equal(mock.calls.filter((call) => call === "/api/claude_cli/bootstrap").length, 1);
    } finally {
      mock.restore();
    }
  }
});

test("bulk sync preserves a concurrent durable cache update when Claude replay is display-only", async (t) => {
  const token = "claude-syncall-stale-race-token";
  const fixture: UsageFixture = {
    queued: false,
    resetAt: "2099-09-10T15:59:59.522Z",
    bootstrapAccountUuid: "syncall-stale-account",
  };
  const activeConnections = await providersDb.getProviderConnections({ isActive: true });
  for (const active of activeConnections) {
    const activeId = (active as Record<string, unknown>).id;
    if (typeof activeId === "string") {
      await providersDb.updateProviderConnection(activeId, { isActive: false });
    }
  }
  const connection = await providersDb.createProviderConnection({
    provider: "claude",
    authType: "oauth",
    name: "syncAll stale cache race",
    email: "claude-syncall-stale-race@example.test",
    accessToken: token,
    expiresAt: new Date(Date.now() + 60 * 60_000).toISOString(),
    isActive: true,
  });
  const connectionId = connection.id as string;
  const cacheA = {
    quotas: { "session (5h)": { used: 10, remainingPercentage: 90 } },
    modelQuotas: {},
    plan: "Cache A",
    message: null,
    fetchedAt: "2026-10-19T09:00:00.000Z",
    source: "race-A",
  };
  const cacheB = {
    quotas: { "session (5h)": { used: 70, remainingPercentage: 30 } },
    modelQuotas: {},
    plan: "Cache B",
    message: null,
    fetchedAt: "2026-10-19T10:00:00.000Z",
    source: "race-B",
  };
  const primer = installAnthropicMock(t, { [token]: fixture });
  try {
    const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    assert.equal(provenance.isFreshClaudeUsage(fresh), true);
  } finally {
    primer.restore();
  }
  await providerLimitsDb.setProviderLimitsCacheAsync(connectionId, cacheA);

  let oauthCalls = 0;
  let bootstrapSignal: AbortSignal | undefined;
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected syncAll test URL: ${url.origin}${url.pathname}`);
      }
      if (url.pathname === "/api/claude_cli/bootstrap") {
        bootstrapSignal = init?.signal ?? undefined;
        return Response.json({
          oauth_account: { organization_rate_limit_tier: "max" },
        });
      }
      if (url.pathname === "/api/oauth/usage") {
        oauthCalls += 1;
        assert.equal(oauthCalls, 1, "the manual bulk poll issues one OAuth request");
        // fetchOne has completed its real SELECT/decode of A before it reaches
        // this transport. Publish B through the real repository before the
        // stale result can return to recordResult's second real SELECT/decode.
        await providerLimitsDb.setProviderLimitsCacheAsync(connectionId, cacheB);
        return new Response(null, { status: 429, headers: { "retry-after": "900" } });
      }
      throw new Error(`Unexpected syncAll test path: ${url.pathname}`);
    }
  );
  try {
    const result = await providerLimits.syncAllProviderLimits({ source: "manual", concurrency: 1 });
    assert.equal(oauthCalls, 1);
    assert.ok(bootstrapSignal?.aborted, "the unneeded parallel bootstrap is canceled on 429");
    assert.equal(result.total, 1);
    assert.equal(
      result.caches[connectionId]?.plan,
      "Cache A",
      "the read cache remains a display result"
    );
    assert.deepEqual(
      await providerLimitsDb.getProviderLimitsCacheAsync(connectionId),
      cacheB,
      "the newer real repository write survives the nonfresh bulk result"
    );
    assert.equal(result.succeeded, 0, "display-only results are excluded from batch publication");
  } finally {
    fetchMock.mock.restore();
    assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
  }
});

test("an old-generation OAuth 429 cannot install cooldown after newer success", async (t) => {
  const token = "claude-old-429-reset-token";
  const oldResponse = deferred<Response>();
  let oauthCalls = 0;
  let oldBodyCancelled = false;
  let oldBootstrapSignal: AbortSignal | undefined;
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected reset-429 URL: ${url.origin}${url.pathname}`);
      }
      if (url.pathname === "/api/claude_cli/bootstrap") {
        if (!oldBootstrapSignal) oldBootstrapSignal = init?.signal ?? undefined;
        return Response.json({ oauth_account: { organization_rate_limit_tier: "max" } });
      }
      if (url.pathname === "/api/oauth/usage") {
        oauthCalls += 1;
        if (oauthCalls === 1) return oldResponse.promise;
        if (oauthCalls === 2 || oauthCalls === 3) {
          return Response.json({ five_hour: { utilization: oauthCalls * 10 } });
        }
        throw new Error("Unexpected fourth reset-429 OAuth request");
      }
      throw new Error(`Unexpected reset-429 path: ${url.pathname}`);
    }
  );
  try {
    const oldPoll = usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    await waitUntil(() => oauthCalls === 1, "old 429 poll did not start");
    usageLeaf._resetClaudeUsagePollingState();

    const newPoll = usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    await waitUntil(() => oauthCalls === 2, "new generation poll did not start");
    const newResult = await newPoll;
    assert.equal(provenance.isFreshClaudeUsage(newResult), true);

    const oldBody = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1]));
      },
      cancel() {
        oldBodyCancelled = true;
      },
    });
    oldResponse.resolve(new Response(oldBody, { status: 429, headers: { "retry-after": "900" } }));
    const oldResult = await oldPoll;
    assert.equal(provenance.getClaudeUsageProvenance(oldResult)?.source, "superseded");
    assert.equal(oldBodyCancelled, true);
    assert.equal(oldBootstrapSignal?.aborted, true);
    assert.equal(cooldown.getClaudeOauthUsageGate(token), null);

    const afterOld429 = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    assert.equal(oauthCalls, 3, "a force read after obsolete 429 still reaches OAuth");
    assert.equal(provenance.isFreshClaudeUsage(afterOld429), true);
  } finally {
    fetchMock.mock.restore();
    assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
  }
});

test("a deadline-aborted late OAuth 429 cannot install cooldown", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const token = "claude-late-429-deadline-token";
  const lateResponse = deferred<Response>();
  let oauthCalls = 0;
  let requestSignal: AbortSignal | undefined;
  let bootstrapSignal: AbortSignal | undefined;
  let bodyCancelled = false;
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected late-429 URL: ${url.origin}${url.pathname}`);
      }
      if (url.pathname === "/api/claude_cli/bootstrap") {
        bootstrapSignal = init?.signal ?? undefined;
        return Response.json({ oauth_account: { organization_rate_limit_tier: "max" } });
      }
      if (url.pathname === "/api/oauth/usage") {
        oauthCalls += 1;
        requestSignal = init?.signal ?? undefined;
        return lateResponse.promise;
      }
      throw new Error(`Unexpected late-429 path: ${url.pathname}`);
    }
  );
  try {
    const poll = usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    await waitUntilImmediate(() => oauthCalls === 1, "deadline poll did not start");
    t.mock.timers.tick(10_001);
    const deadlineResult = await poll;
    assert.equal(provenance.getClaudeUsageProvenance(deadlineResult)?.source, "error");
    assert.equal(requestSignal?.aborted, true);

    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new Uint8Array([1]));
      },
      cancel() {
        bodyCancelled = true;
      },
    });
    lateResponse.resolve(new Response(body, { status: 429, headers: { "retry-after": "900" } }));
    await waitUntilImmediate(() => bodyCancelled, "late 429 body did not settle cleanup");
    assert.equal(bootstrapSignal?.aborted, true);
    assert.equal(cooldown.getClaudeOauthUsageGate(token), null);
  } finally {
    fetchMock.mock.restore();
    assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
  }
});

test("a current in-deadline OAuth 429 still records Retry-After and returns stale usage", async (t) => {
  const token = "claude-current-429-positive-token";
  const primer = installAnthropicMock(t, {
    [token]: {
      queued: false,
      resetAt: "2099-09-10T15:59:59.522Z",
      bootstrapAccountUuid: "current-429-account",
    },
  });
  let originalObservedAt: number | null = null;
  try {
    const fresh = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    originalObservedAt = provenance.getClaudeUsageProvenance(fresh)?.observedAt ?? null;
    assert.equal(provenance.isFreshClaudeUsage(fresh), true);
  } finally {
    primer.restore();
  }

  let bodyCancelled = false;
  let bootstrapSignal: AbortSignal | undefined;
  const fetchMock = t.mock.method(
    globalThis,
    "fetch",
    async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      if (url.origin !== "https://api.anthropic.com") {
        throw new Error(`Unexpected positive-429 URL: ${url.origin}${url.pathname}`);
      }
      if (url.pathname === "/api/claude_cli/bootstrap") {
        bootstrapSignal = init?.signal ?? undefined;
        return Response.json({});
      }
      if (url.pathname === "/api/oauth/usage") {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new Uint8Array([1]));
          },
          cancel() {
            bodyCancelled = true;
          },
        });
        return new Response(body, { status: 429, headers: { "retry-after": "900" } });
      }
      throw new Error(`Unexpected positive-429 path: ${url.pathname}`);
    }
  );
  try {
    const stale = await usageLeaf.getClaudeUsage(token, { forceRefresh: true });
    const staleProvenance = provenance.getClaudeUsageProvenance(stale);
    assert.equal(staleProvenance?.source, "stale-429");
    assert.equal(staleProvenance?.observedAt, originalObservedAt);
    assert.equal(stale._staleSince, new Date(originalObservedAt!).toISOString());
    assert.equal(bodyCancelled, true);
    assert.equal(bootstrapSignal?.aborted, true);
    assert.equal(cooldown.getClaudeOauthUsageGate(token), "cooldown");
    assert.equal(
      cooldown.getClaudeOauthUsageGate(token, Date.now() + 600_000),
      "cooldown",
      "Retry-After delta-seconds remains effective after the response settles"
    );
  } finally {
    fetchMock.mock.restore();
    assert.equal(globalThis.fetch, productionFetch, "the production fetch wrapper is restored");
  }
});

test("fresh Claude usage retains provider-limit persistence and recovery writes", async (t) => {
  const token = "claude-provider-limits-fresh-positive";
  const mock = installAnthropicMock(t, {
    [token]: {
      queued: false,
      resetAt: "2099-09-10T15:59:59.522Z",
      bootstrapAccountUuid: "fresh-positive-account",
    },
  });
  try {
    const connection = await providersDb.createProviderConnection({
      provider: "claude",
      authType: "oauth",
      name: "Fresh provider limits positive control",
      email: "claude-fresh-positive@example.test",
      accessToken: token,
      refreshToken: "unused-refresh-token",
      expiresAt: new Date(Date.now() + 60 * 60_000).toISOString(),
      testStatus: "unavailable",
      isActive: true,
      lastError: "Claude extra usage is blocked",
      lastErrorAt: new Date(Date.now() - 60_000).toISOString(),
      lastErrorType: "quota_exhausted",
      lastErrorSource: "extra_usage",
      errorCode: 429,
      rateLimitedUntil: "2099-09-10T15:00:00.000Z",
      backoffLevel: 2,
      providerSpecificData: {
        accountUUID: "fresh-positive-old-account",
        organizationUUID: "fresh-positive-old-org",
        organizationName: "Old organization",
        organizationType: "claude_pro",
        organizationRateLimitTier: "old-tier",
        blockExtraUsage: true,
      },
    });
    const connectionId = connection.id as string;

    const result = await providerLimits.fetchAndPersistProviderLimits(connectionId, "manual");
    assert.equal(provenance.getClaudeUsageProvenance(result.usage)?.source, "oauth");
    assert.equal(provenance.isFreshClaudeUsage(result.usage), true);
    assert.ok(result.usage.quotas);

    const storedLimits = await providerLimitsDb.getProviderLimitsCacheAsync(connectionId);
    assert.ok(
      storedLimits?.quotas,
      "fresh usage is persisted through the existing provider-limits owner"
    );
    const storedQuota = quotaCache.getQuotaCache(connectionId);
    assert.ok(storedQuota, "fresh usage is published through the existing quota-cache owner");
    assert.equal(storedQuota?.exhausted, false);

    const updated = (await providersDb.getProviderConnectionById(connectionId)) as Record<
      string,
      unknown
    >;
    assert.equal(updated.testStatus, "active");
    assert.equal(updated.lastErrorSource ?? null, null);
    assert.equal(updated.rateLimitedUntil ?? null, null);
    assert.equal(updated.backoffLevel, 0);
    assert.equal(
      (updated.providerSpecificData as Record<string, unknown>).organizationUUID,
      "fresh-positive-account-org"
    );
    assert.equal(mock.calls.filter((call) => call === "/api/oauth/usage").length, 1);
    assert.equal(mock.calls.filter((call) => call === "/api/claude_cli/bootstrap").length, 1);
  } finally {
    mock.restore();
  }
});
