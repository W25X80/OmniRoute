/**
 * Claude OAuth usage polling and the legacy organization-usage fallback.
 * The leaf owns bounded polling state; consumers use the non-enumerable
 * provenance helper before publishing observations into routing or persistence.
 */

import {
  getClaudeCodeVersion,
  fetchClaudeBootstrap,
  type ClaudeBootstrap,
} from "../../executors/claudeIdentity.ts";
import {
  computeClaudeUsageRetryMs,
  getClaudeOauthUsageGate,
  hashClaudeCredential,
  markClaudeOauthUsage429,
} from "../claudeUsageCooldown.ts";
import { toRecord } from "./scalars.ts";
import { normalizeClaudeUsageQuotas } from "./claudeQuota.ts";
import { withOperationDeadline } from "../../utils/operationDeadline.ts";
import {
  advanceClaudeUsageGeneration,
  beginClaudeUsageProducer,
  currentClaudeUsageGeneration,
  setClaudeUsageProvenance,
  type ClaudeUsageProvenance,
} from "./claudeUsageProvenance.ts";

const CLAUDE_CONFIG = {
  oauthUsageUrl: "https://api.anthropic.com/api/oauth/usage",
  usageUrl: "https://api.anthropic.com/v1/organizations/{org_id}/usage",
  settingsUrl: "https://api.anthropic.com/v1/settings",
  apiVersion: "2023-06-01",
};

export const CLAUDE_USAGE_SUCCESS_TTL_MS = 65_000;
const CLAUDE_USAGE_CACHE_MAX = 128;
const CLAUDE_USAGE_INFLIGHT_MAX = 128;
const RATE_LIMITED_MESSAGE = "Claude usage is rate limited (429). Retry later.";
const RATE_LIMITED_STALE_REASON =
  "Claude usage endpoint is rate limited (429); showing the last known quota.";
const CAPACITY_MESSAGE = "Claude usage refresh is busy. Retry shortly.";

type ClaudeUsageResult = {
  plan?: unknown;
  organization?: unknown;
  quotas?: unknown;
  modelQuotas?: unknown;
  extraUsage?: unknown;
  message?: string;
  bootstrap: ClaudeBootstrap | null;
  _stale?: boolean;
  _staleSince?: string;
  _staleReason?: string;
  _cached?: boolean;
  _cachedAt?: string;
};

interface CachedClaudeUsage {
  value: ClaudeUsageResult;
  observedAt: number;
  producerId: number;
  generation: number;
}

interface LegacyFetchResult {
  value: ClaudeUsageResult;
  observedAt: number | null;
  hasFreshSettings: boolean;
}

const successfulUsage = new Map<string, CachedClaudeUsage>();
const pendingUsage = new Map<string, Promise<ClaudeUsageResult>>();

export function getClaudePlanLabel(...candidates: Array<string | null | undefined>): string | null {
  for (const candidate of candidates) {
    if (typeof candidate !== "string") continue;
    const trimmed = candidate.trim();
    if (
      !trimmed ||
      trimmed.toLowerCase() === "claude code" ||
      trimmed.toLowerCase() === "unknown"
    ) {
      continue;
    }
    return trimmed;
  }
  return null;
}

export function getClaudeUsage(
  accessToken?: string,
  options: { forceRefresh?: boolean } = {}
): Promise<ClaudeUsageResult> {
  if (!accessToken) {
    return Promise.resolve(
      tagged({ message: "Claude connected. Access token not available.", bootstrap: null }, "error")
    );
  }

  const key = hashClaudeCredential(accessToken);
  const gate = getClaudeOauthUsageGate(accessToken);
  if (gate === "cooldown") return Promise.resolve(cooldownResult(key));
  if (gate === "capacity") {
    return Promise.resolve(tagged({ message: CAPACITY_MESSAGE, bootstrap: null }, "error"));
  }

  const pending = pendingUsage.get(key);
  if (pending) return pending;

  const cached = readSuccessfulUsage(key);
  if (
    cached &&
    !options.forceRefresh &&
    Date.now() - cached.observedAt >= 0 &&
    Date.now() - cached.observedAt < CLAUDE_USAGE_SUCCESS_TTL_MS
  ) {
    return Promise.resolve(memoResult(cached));
  }

  if (pendingUsage.size >= CLAUDE_USAGE_INFLIGHT_MAX) {
    return Promise.resolve(tagged({ message: CAPACITY_MESSAGE, bootstrap: null }, "error"));
  }

  const producerId = beginClaudeUsageProducer();
  const generation = currentClaudeUsageGeneration();
  let request: Promise<ClaudeUsageResult>;
  request = withOperationDeadline(10_000, (signal) =>
    fetchClaudeUsage(accessToken, key, signal, producerId, generation)
  )
    .catch((error: unknown) => {
      return tagged(
        { message: unableToFetchMessage(error), bootstrap: null },
        "error",
        null,
        producerId,
        generation
      );
    })
    .finally(() => {
      if (pendingUsage.get(key) === request) pendingUsage.delete(key);
    });
  pendingUsage.set(key, request);
  return request;
}

function tagged(
  result: ClaudeUsageResult,
  source: ClaudeUsageProvenance["source"],
  observedAt: number | null = null,
  producerId: number | null = null,
  generation: number = currentClaudeUsageGeneration()
): ClaudeUsageResult {
  return setClaudeUsageProvenance(result, { source, observedAt, producerId, generation });
}

function readSuccessfulUsage(key: string): CachedClaudeUsage | undefined {
  const cached = successfulUsage.get(key);
  if (!cached) return undefined;
  successfulUsage.delete(key);
  successfulUsage.set(key, cached);
  return cached;
}

function cacheSuccessfulUsage(
  key: string,
  value: ClaudeUsageResult,
  observedAt: number,
  producerId: number,
  generation: number
): void {
  successfulUsage.delete(key);
  successfulUsage.set(key, {
    value: cloneAndFreeze(value),
    observedAt,
    producerId,
    generation,
  });
  while (successfulUsage.size > CLAUDE_USAGE_CACHE_MAX) {
    const oldest = successfulUsage.keys().next().value;
    if (oldest === undefined) break;
    successfulUsage.delete(oldest);
  }
}

function cloneAndFreeze<T>(value: T): T {
  const copy = structuredClone(value);
  const freeze = (entry: unknown): void => {
    if (!entry || typeof entry !== "object" || Object.isFrozen(entry)) return;
    for (const child of Object.values(entry)) freeze(child);
    Object.freeze(entry);
  };
  freeze(copy);
  return copy;
}

function memoResult(cached: CachedClaudeUsage): ClaudeUsageResult {
  return tagged(
    {
      ...structuredClone(cached.value),
      _cached: true,
      _cachedAt: new Date(cached.observedAt).toISOString(),
    },
    "success-memo",
    cached.observedAt,
    cached.producerId,
    cached.generation
  );
}

function staleResult(cached: CachedClaudeUsage): ClaudeUsageResult {
  return tagged(
    {
      ...structuredClone(cached.value),
      _stale: true,
      _staleSince: new Date(cached.observedAt).toISOString(),
      _staleReason: RATE_LIMITED_STALE_REASON,
    },
    "stale-429",
    cached.observedAt,
    cached.producerId,
    cached.generation
  );
}

function cooldownResult(key: string): ClaudeUsageResult {
  const cached = readSuccessfulUsage(key);
  return cached
    ? staleResult(cached)
    : tagged({ message: RATE_LIMITED_MESSAGE, bootstrap: null }, "stale-429");
}

function supersededResult(
  observedAt: number | null,
  producerId: number,
  generation: number
): ClaudeUsageResult {
  return tagged(
    {
      message: "Claude usage refresh was superseded.",
      bootstrap: null,
      ...(observedAt !== null
        ? { _stale: true, _staleSince: new Date(observedAt).toISOString() }
        : {}),
    },
    "superseded",
    observedAt,
    producerId,
    generation
  );
}

function unableToFetchMessage(error: unknown): string {
  const value = error as { message?: unknown; code?: unknown; cause?: { code?: unknown } } | null;
  const message = typeof value?.message === "string" ? value.message : "";
  const code = typeof value?.code === "string" ? value.code : "";
  const causeCode = typeof value?.cause?.code === "string" ? value.cause.code : "";
  const marker = [message, code, causeCode].find((part) =>
    [
      "fetch failed",
      "ECONNREFUSED",
      "ETIMEDOUT",
      "Proxy unreachable",
      "UND_ERR_CONNECT_TIMEOUT",
    ].some((known) => part.includes(known))
  );
  const safeDetail = marker?.match(
    /fetch failed|ECONNREFUSED|ETIMEDOUT|Proxy unreachable|UND_ERR_CONNECT_TIMEOUT/
  )?.[0];
  const timedOut = (error as { name?: unknown } | null)?.name === "TimeoutError";
  return timedOut
    ? "Claude connected. Unable to fetch usage: fetch failed (deadline exceeded)."
    : safeDetail
      ? `Claude connected. Unable to fetch usage: ${safeDetail}`
      : "Claude connected. Unable to fetch usage: request failed.";
}

async function cancelBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // Cancellation is best-effort; the shared operation signal remains the hard bound.
  }
}

async function fetchClaudeUsage(
  accessToken: string,
  key: string,
  signal: AbortSignal,
  producerId: number,
  generation: number
): Promise<ClaudeUsageResult> {
  const bootstrapController = new AbortController();
  const bootstrapSignal = AbortSignal.any([signal, bootstrapController.signal]);
  const bootstrapPromise = fetchClaudeBootstrap(accessToken, bootstrapSignal).catch(() => null);
  let observedAt: number | null = null;

  try {
    signal.throwIfAborted();
    const oauthResponse = await fetch(CLAUDE_CONFIG.oauthUsageUrl, {
      method: "GET",
      headers: {
        Accept: "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, compress, deflate, br",
        Authorization: `Bearer ${accessToken}`,
        "Content-Type": "application/json",
        "User-Agent": `claude-code/${getClaudeCodeVersion()}`,
        "anthropic-beta": "oauth-2025-04-20",
      },
      signal,
    });

    if (oauthResponse.ok) {
      const data = toRecord(await oauthResponse.json());
      observedAt = Date.now();
      signal.throwIfAborted();
      const { quotas, modelQuotas } = normalizeClaudeUsageQuotas(data);
      const bootstrap = await bootstrapPromise;
      signal.throwIfAborted();
      if (generation !== currentClaudeUsageGeneration()) {
        return supersededResult(observedAt, producerId, generation);
      }

      const plan =
        getClaudePlanLabel(
          typeof data.tier === "string" ? data.tier : null,
          typeof data.plan === "string" ? data.plan : null,
          typeof data.subscription_type === "string" ? data.subscription_type : null,
          bootstrap?.organization_rate_limit_tier
        ) ?? undefined;
      const value: ClaudeUsageResult = {
        ...(plan ? { plan } : {}),
        quotas,
        modelQuotas,
        extraUsage: data.extra_usage ?? null,
        bootstrap,
      };
      cacheSuccessfulUsage(key, value, observedAt, producerId, generation);
      return tagged(value, "oauth", observedAt, producerId, generation);
    }

    if (oauthResponse.status === 429) {
      const observed429At = Date.now();
      const retryMs = computeClaudeUsageRetryMs(oauthResponse.headers, observed429At);
      bootstrapController.abort();
      await cancelBody(oauthResponse);
      await bootstrapPromise;
      if (signal.aborted || generation !== currentClaudeUsageGeneration()) {
        return supersededResult(observedAt, producerId, generation);
      }
      markClaudeOauthUsage429(accessToken, observed429At, retryMs);
      const cached = readSuccessfulUsage(key);
      if (cached) return staleResult(cached);
      return tagged(
        { message: RATE_LIMITED_MESSAGE, bootstrap: null },
        "error",
        null,
        producerId,
        generation
      );
    }

    await cancelBody(oauthResponse);
    if (oauthResponse.status === 401) {
      bootstrapController.abort();
      await bootstrapPromise;
      return tagged(
        {
          message: "Claude authentication expired (401). Re-authorize or refresh the connection.",
          bootstrap: null,
        },
        "error",
        null,
        producerId,
        generation
      );
    }

    if (oauthResponse.status === 404 || oauthResponse.status === 405) {
      console.warn(
        `[Claude Usage] OAuth endpoint returned ${oauthResponse.status}, falling back to legacy`
      );
      const legacy = await getClaudeUsageLegacy(accessToken, signal);
      if (!legacy.hasFreshSettings || legacy.observedAt === null) {
        bootstrapController.abort();
        await bootstrapPromise;
        return tagged(legacy.value, "error", null, producerId, generation);
      }
      const bootstrap = await bootstrapPromise;
      signal.throwIfAborted();
      if (generation !== currentClaudeUsageGeneration()) {
        return supersededResult(legacy.observedAt, producerId, generation);
      }
      const value = { ...legacy.value, bootstrap };
      return tagged(value, "legacy", legacy.observedAt, producerId, generation);
    }

    bootstrapController.abort();
    await bootstrapPromise;
    return tagged(
      {
        message: `Claude connected. Usage endpoint returned HTTP ${oauthResponse.status}.`,
        bootstrap: null,
      },
      "error",
      null,
      producerId,
      generation
    );
  } catch (error) {
    bootstrapController.abort();
    await bootstrapPromise;
    if (signal.aborted || generation !== currentClaudeUsageGeneration()) {
      return supersededResult(observedAt, producerId, generation);
    }
    return tagged(
      { message: unableToFetchMessage(error), bootstrap: null },
      "error",
      null,
      producerId,
      generation
    );
  }
}

/** Fetch legacy settings/org quota only when the OAuth endpoint is absent. */
async function getClaudeUsageLegacy(
  accessToken: string,
  signal: AbortSignal
): Promise<LegacyFetchResult> {
  let settingsResponse: Response | null = null;
  let usageResponse: Response | null = null;
  try {
    signal.throwIfAborted();
    settingsResponse = await fetch(CLAUDE_CONFIG.settingsUrl, {
      method: "GET",
      signal,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "anthropic-version": CLAUDE_CONFIG.apiVersion,
      },
    });
    if (!settingsResponse.ok) {
      const status = settingsResponse.status;
      await cancelBody(settingsResponse);
      return {
        value: {
          message: `Claude connected. Usage API requires admin permissions (HTTP ${status}).`,
          bootstrap: null,
        },
        observedAt: null,
        hasFreshSettings: false,
      };
    }

    const settings = toRecord(await settingsResponse.json());
    const settingsObservedAt = Date.now();
    signal.throwIfAborted();
    const organizationId =
      typeof settings.organization_id === "string" ? settings.organization_id : "";
    if (!organizationId) {
      return {
        value: {
          plan: settings.plan || "Unknown",
          organization: settings.organization_name,
          message: "Claude connected. Usage details require admin access.",
          bootstrap: null,
        },
        observedAt: settingsObservedAt,
        hasFreshSettings: true,
      };
    }

    usageResponse = await fetch(CLAUDE_CONFIG.usageUrl.replace("{org_id}", organizationId), {
      method: "GET",
      signal,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "anthropic-version": CLAUDE_CONFIG.apiVersion,
      },
    });
    if (!usageResponse.ok) {
      const status = usageResponse.status;
      await cancelBody(usageResponse);
      return {
        value: {
          plan: settings.plan || "Unknown",
          organization: settings.organization_name,
          message: `Claude connected. Usage details require admin access (HTTP ${status}).`,
          bootstrap: null,
        },
        observedAt: settingsObservedAt,
        hasFreshSettings: true,
      };
    }

    const usage = await usageResponse.json();
    const observedAt = Date.now();
    signal.throwIfAborted();
    return {
      value: {
        plan: settings.plan || "Unknown",
        organization: settings.organization_name,
        quotas: usage,
        bootstrap: null,
      },
      observedAt,
      hasFreshSettings: true,
    };
  } catch (error) {
    if (settingsResponse && !settingsResponse.ok) await cancelBody(settingsResponse);
    if (usageResponse && !usageResponse.ok) await cancelBody(usageResponse);
    return {
      value: { message: unableToFetchMessage(error), bootstrap: null },
      observedAt: null,
      hasFreshSettings: false,
    };
  }
}

/** Reset the bounded memo and invalidate late or already-returned publications. */
export function _resetClaudeUsagePollingState(): void {
  advanceClaudeUsageGeneration();
  successfulUsage.clear();
  pendingUsage.clear();
}
