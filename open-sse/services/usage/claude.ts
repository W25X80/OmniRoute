/**
 * usage/claude.ts — Claude (Anthropic OAuth + legacy org) usage fetcher + plan-label helper.
 *
 * Extracted from services/usage.ts (god-file decomposition): the Claude family — the API
 * config, the plan-label picker, the OAuth usage fetcher (getClaudeUsage) with its legacy
 * settings/org fallback (getClaudeUsageLegacy). Depends only on the sibling scalar/quota
 * leaves + Claude identity/cooldown helpers + safePercentage — no host coupling — so it
 * lives as a co-located provider leaf. usage.ts imports getClaudeUsage (dispatcher) +
 * getClaudePlanLabel (__testing). Behavior-preserving move.
 */

import { getClaudeCodeVersion, fetchClaudeBootstrap } from "../../executors/claudeIdentity.ts";
import { isClaudeOauthUsageCoolingDown, markClaudeOauthUsage429 } from "../claudeUsageCooldown.ts";
import { toRecord } from "./scalars.ts";
import { normalizeClaudeUsageQuotas } from "./claudeQuota.ts";
import { withOperationDeadline } from "../../utils/operationDeadline.ts";

// Claude API config
const CLAUDE_CONFIG = {
  oauthUsageUrl: "https://api.anthropic.com/api/oauth/usage",
  usageUrl: "https://api.anthropic.com/v1/organizations/{org_id}/usage",
  settingsUrl: "https://api.anthropic.com/v1/settings",
  apiVersion: "2023-06-01",
};

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

/**
 * Claude Usage - Try to fetch from Anthropic API
 */
const pendingUsage = new Map<
  string,
  Promise<Awaited<ReturnType<typeof fetchClaudeUsage>> | { message: string; bootstrap: null }>
>();

export async function getClaudeUsage(accessToken?: string) {
  if (!accessToken) {
    return { message: "Claude connected. Access token not available.", bootstrap: null };
  }
  const existing = pendingUsage.get(accessToken);
  if (existing) return existing;
  const work = withOperationDeadline(10_000, (signal) =>
    fetchClaudeUsage(accessToken, signal)
  ).catch(() => ({
    message: "Claude connected. Usage operation deadline exceeded.",
    bootstrap: null,
  }));
  pendingUsage.set(accessToken, work);
  try {
    return await work;
  } finally {
    if (pendingUsage.get(accessToken) === work) pendingUsage.delete(accessToken);
  }
}

async function fetchClaudeUsage(accessToken: string, signal: AbortSignal) {
  // Refresh bootstrap in parallel; best-effort, failure non-fatal.
  const bootstrapPromise = fetchClaudeBootstrap(accessToken, signal).catch(() => null);
  // Skip OAuth usage call while this token is cooling down from a recent 429
  // (chat with the same token still works — only the quota endpoint is throttled).
  if (isClaudeOauthUsageCoolingDown(accessToken)) {
    const legacy = await getClaudeUsageLegacy(accessToken, signal);
    return { ...legacy, bootstrap: await bootstrapPromise };
  }
  try {
    // Real CLI uses axios here, not Stainless — UA is `claude-code/<version>`
    // (not `claude-cli/...`) and the shape is simpler than /v1/messages.
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
      const { quotas, modelQuotas } = normalizeClaudeUsageQuotas(data);

      const bootstrap = await bootstrapPromise;
      const plan =
        getClaudePlanLabel(
          typeof data.tier === "string" ? data.tier : null,
          typeof data.plan === "string" ? data.plan : null,
          typeof data.subscription_type === "string" ? data.subscription_type : null,
          bootstrap?.organization_rate_limit_tier
        ) ?? undefined;

      return {
        ...(plan ? { plan } : {}),
        quotas,
        modelQuotas,
        extraUsage: data.extra_usage ?? null,
        bootstrap,
      };
    }

    // Cool down OAuth usage polling after a 429 (quota endpoint only — chat is unaffected).
    if (oauthResponse.status === 429) {
      const retryAfter = oauthResponse.headers.get("retry-after");
      const seconds = retryAfter === null ? NaN : Number(retryAfter);
      const milliseconds = Number.isFinite(seconds)
        ? seconds * 1000
        : Date.parse(retryAfter ?? "") - Date.now();
      markClaudeOauthUsage429(
        accessToken,
        Date.now(),
        Number.isFinite(milliseconds) && milliseconds > 0 ? milliseconds : undefined
      );
    }

    // Fallback: OAuth endpoint returned non-OK, try legacy settings/org endpoint
    console.warn(
      `[Claude Usage] OAuth endpoint returned ${oauthResponse.status}, falling back to legacy`
    );
    await oauthResponse.body?.cancel();
    const legacy = await getClaudeUsageLegacy(accessToken, signal);
    return { ...legacy, bootstrap: await bootstrapPromise };
  } catch (error) {
    return {
      message: `Claude connected. Unable to fetch usage: ${(error as Error).message}`,
      bootstrap: await bootstrapPromise,
    };
  }
}

/**
 * Legacy Claude usage fetcher for API key / org admin users.
 * Uses /v1/settings + /v1/organizations/{org_id}/usage endpoints.
 */
async function getClaudeUsageLegacy(accessToken: string, signal: AbortSignal) {
  try {
    signal.throwIfAborted();
    const settingsResponse = await fetch(CLAUDE_CONFIG.settingsUrl, {
      method: "GET",
      signal,
      headers: {
        Authorization: `Bearer ${accessToken}`,
        "anthropic-version": CLAUDE_CONFIG.apiVersion,
      },
    });

    if (settingsResponse.ok) {
      const settings = toRecord(await settingsResponse.json());

      const organizationId =
        typeof settings.organization_id === "string" ? settings.organization_id : "";
      if (organizationId) {
        signal.throwIfAborted();
        const usageResponse = await fetch(
          CLAUDE_CONFIG.usageUrl.replace("{org_id}", organizationId),
          {
            method: "GET",
            signal,
            headers: {
              Authorization: `Bearer ${accessToken}`,
              "anthropic-version": CLAUDE_CONFIG.apiVersion,
            },
          }
        );

        if (usageResponse.ok) {
          const usage = await usageResponse.json();
          return {
            plan: settings.plan || "Unknown",
            organization: settings.organization_name,
            quotas: usage,
          };
        }
        await usageResponse.body?.cancel();
      }

      return {
        plan: settings.plan || "Unknown",
        organization: settings.organization_name,
        message: "Claude connected. Usage details require admin access.",
      };
    }

    await settingsResponse.body?.cancel();
    return { message: "Claude connected. Usage API requires admin permissions." };
  } catch (error) {
    return { message: `Claude connected. Unable to fetch usage: ${(error as Error).message}` };
  }
}
