import { requestBodyLimitMbFromEnv } from "@/shared/constants/bodySize";
import { DEFAULT_RESPONSES_PREVIOUS_RESPONSE_ID_MODE } from "@/shared/constants/responsesPreviousResponseId";
import { toRecord } from "./shared";

export type SettingsRecord = Record<string, unknown>;

/** Environment values that already participate in SQLite settings reads. */
export interface SettingsEnvironment {
  [name: string]: string | undefined;
  OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS?: string;
  MAX_BODY_SIZE_BYTES?: string;
  INITIAL_PASSWORD?: string;
}

/** Internal key_value row — not exposed as a user-facing settings field. */
export const SETTINGS_REVISION_KEY = "_settingsRevision";

export class SettingsRevisionConflictError extends Error {
  readonly code = "SETTINGS_REVISION_CONFLICT" as const;

  constructor(public readonly currentRevision: number) {
    super("Settings revision mismatch");
    this.name = "SettingsRevisionConflictError";
  }
}

/** Corrupt or absent internal revision rows have always read as revision zero. */
export function parseSettingsRevision(rawValue: unknown): number {
  if (typeof rawValue !== "string" || !rawValue) return 0;
  try {
    const parsed = JSON.parse(rawValue) as unknown;
    return typeof parsed === "number" && Number.isInteger(parsed) && parsed >= 0 ? parsed : 0;
  } catch {
    return 0;
  }
}

function isTruthyEnvFlag(value: string | undefined): boolean {
  return typeof value === "string" && /^(1|true|yes|on)$/i.test(value.trim());
}

/**
 * #7274: read-fallback for the codexSessionAffinityTtlMs -> sessionAffinityTtlMs
 * rename. Migration 124 already backfills the new key from any pre-existing
 * old-key row for the common case, but this covers callers reading settings
 * before that migration has had a chance to run (or any drift between the
 * two). Only applies when the generic key was never explicitly persisted —
 * an operator-set `sessionAffinityTtlMs` (including an explicit 0) always wins.
 * Extracted to a leaf helper so `getSettings()` stays under the max-lines-per-
 * function ratchet.
 */
function applySessionAffinityLegacyFallback(settings: Record<string, unknown>): void {
  if (settings.sessionAffinityTtlMs === undefined) {
    settings.sessionAffinityTtlMs =
      typeof settings.codexSessionAffinityTtlMs === "number"
        ? settings.codexSessionAffinityTtlMs
        : 0;
  }
}

/** A fresh object is returned so one read cannot mutate another read's defaults. */
export function createSettingsDefaults(environment: SettingsEnvironment): SettingsRecord {
  return {
    cloudEnabled: false,
    tailscaleEnabled: false,
    tailscaleUrl: "",
    stickyRoundRobinLimit: 3,
    disableSessionStickiness: false,
    // Global connection-aware expansion fallback for group-B combo strategies is opt-in.
    connectionAwareExpansion: false,
    promptCacheAffinityEnabled: true,
    comboStrategy: "fallback",
    comboStickyRoundRobinLimit: null, // null = inherit stickyRoundRobinLimit (a literal default here shadows the documented batched-rotation default of 3 — #6678 regression caught by the v3.8.47 release CI)
    providerStrategies: {},
    // Per-operator quota row visibility (dashboard usage tab). Keyed by
    // provider id → { hidden: [<quota visibility key>] }. Independent of the
    // model catalog's isHidden flag (collectHiddenQuotaModelIds in
    // ProviderLimits/utils.tsx) — this is a personal view preference, not an
    // admin model-catalog edit. Ported from upstream decolua/9router#2371.
    quotaVisibility: {},
    requestRetry: 3,
    maxRetryIntervalSec: 30,
    antigravitySignatureCacheMode: "enabled",
    requireLogin: true,
    oidcEnabled: false,
    oidcDisablePasswordLogin: false,
    oidcIssuer: "",
    oidcClientId: "",
    oidcClientSecret: "",
    oidcScopes: ["openid", "profile", "email"],
    oidcRedirectPath: "/api/auth/oidc/callback",
    oidcAllowedSubjects: [], // optional sub or email whitelist
    mcpEnabled: false,
    a2aEnabled: false,
    hiddenSidebarItems: [],
    hiddenSidebarGroupLabels: [],
    sidebarSectionOrder: [],
    sidebarItemOrder: {},
    sidebarActivePreset: null,
    hideEndpointCloudflaredTunnel: false,
    hideEndpointTailscaleFunnel: false,
    hideEndpointNgrokTunnel: false,
    preferClaudeCodeForUnprefixedClaudeModels: isTruthyEnvFlag(
      environment.OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS
    ),
    // Opt-in (default "off"): short-circuits Claude Code's `--permission-mode auto`
    // internal security-classifier request with a synthetic `<block>no</block>` ALLOW
    // response, without calling the upstream provider. See
    // open-sse/handlers/chatCore/claudeClassifierCompat.ts for the detector + builder.
    claudeClassifierCompat: "off",
    autoRefreshProviderQuota: false,
    credentialRedactionEnabled: false,
    autoRefreshProviderQuotaInterval: 180,
    comboConfigMode: "guided",
    comboAutoPromoteEnabled: false,
    codexServiceTier: { enabled: false },
    claudeFastMode: {
      enabled: false,
      supportedModels: [
        "claude-fable-5",
        "claude-opus-5-5",
        "claude-opus-5",
        "claude-opus-4-8",
        "claude-opus-4-7",
        "claude-opus-4-6",
      ],
    },
    // #7274: renamed from codexSessionAffinityTtlMs — session affinity now
    // applies to any provider, not just Codex. No default here on purpose:
    // the read-fallback below only kicks in while `sessionAffinityTtlMs` has
    // never been explicitly persisted, so it can tell "never configured"
    // apart from "operator explicitly set 0" on the new key.
    responsesPreviousResponseIdMode: DEFAULT_RESPONSES_PREVIOUS_RESPONSE_ID_MODE,
    alwaysPreserveClientCache: "auto",
    idempotencyWindowMs: 5000,
    wsAuth: false,
    maxBodySizeMb: requestBodyLimitMbFromEnv(environment.MAX_BODY_SIZE_BYTES),
    // #10312: opt-in only — a fresh install (or one missing the persisted key)
    // must not run in debug mode; installs that persisted `true` keep it.
    debugMode: false,
    // Opt-in diagnostic: when true, the chat handler emits a `log.debug("TOOLS", …)`
    // line per request summarizing tool count + MCP/hosted/client source breakdown.
    logToolSources: false,
    // LOCAL_ONLY manage-scope bypass policy defaults (T-011 / spec §Data Model).
    // Preserves PR #2473 behaviour on migration — the bypass starts ENABLED
    // for `/api/mcp/` so existing manage-scope Bearer clients keep working.
    // Operators flip the kill-switch to false (or drop the prefix) via the
    // Settings UI; the change hot-reloads through `applyRuntimeSettings` →
    // `applyAuthzBypassSection` → `getAuthzBypassSnapshot()`.
    localOnlyManageScopeBypassEnabled: true,
    localOnlyManageScopeBypassPrefixes: ["/api/mcp/"],
    customBannedSignals: [],
    autoDisableBannedScope: "all",
    proxyEnabled: true,
    perKeyProxyEnabled: false,
    customSystemPromptEnabled: false,
    customSystemPrompt: "",
    // #6316: Opt-in filter that hides paid-only models from the /v1/models catalog.
    // Uses isFreeModel() from src/shared/utils/freeModels.ts to detect free entries
    // (`:free` suffix, zero-price pricing, or FREE_MODEL_BUDGETS membership). Default
    // false preserves prior behaviour; opt-in only.
    hidePaidModels: false,
    // Opt-in, default off: same shape as hidePaidModels above, but requires a
    // live hard-stop-guaranteed quota check for non-keyless free candidates.
    // See open-sse/services/autoCombo/strictZeroCostFilter.ts.
    freeAccessPolicy: "off",
    // #15059: ON by default — providers whose curated verdict is `tos: "avoid"` (e.g. Antigravity)
    // must not receive `auto` traffic on a fresh install. Opt out with PATCH /api/settings.
    excludeTosAvoid: true,
    // #9418: Opt-in filter that hides auto/* virtual combos from the /v1/models catalog.
    // User-defined combos are unaffected; routing still works for hidden ids sent explicitly.
    hideAutoCombos: false,
    // #9418: Opt-in filter that hides no-think/* gateway variants from the /v1/models catalog.
    // Routing still works for hidden ids sent explicitly.
    hideNoThinkVariants: false,
    // #11481: Opt-in explicit model exposure allow/deny list, mirrored into the
    // auto/* combo candidate pool (open-sse/services/autoCombo/modelExposureFilter.ts)
    // so a denied model can't sneak back in via combo routing — the same trap
    // #6512 already fixed once for hidePaidModels. See
    // src/shared/utils/modelExposureList.ts for the matching predicate. Empty
    // arrays preserve prior behaviour; opt-in only.
    modelVisibilityAllowlist: [],
    modelVisibilityDenylist: [],
    // #6977: Opt-in per-connection auto-ping that warms a Codex OAuth connection's
    // quota window right after it resets, so the first real request doesn't land in
    // a cold window. `connections` maps connection id -> enabled. Default empty map
    // (nobody opted in) — the scheduler is a no-op until an operator flips a
    // connection on, since pinging burns a small amount of real quota (Hard Rule #20
    // spirit: never mutate/consume on the operator's behalf by default).
    codexAutoPing: { connections: {} },
    // #8848: opt-in per-connection Claude proactive warmup (empty = off for everyone).
    claudeWarmup: { connections: {} },
  };
}

/** Storage interpretation only; onboarding writes and encryption ownership stay explicit. */
export function interpretSettingsRows(
  rows: readonly unknown[],
  environment: SettingsEnvironment,
  decryptSecret: (value: string) => string | null | undefined
): SettingsRecord {
  const settings = createSettingsDefaults(environment);
  for (const row of rows) {
    const record = toRecord(row);
    const key = typeof record.key === "string" ? record.key : null;
    const rawValue = typeof record.value === "string" ? record.value : null;
    if (!key || rawValue === null || key.startsWith("_")) continue;
    try {
      settings[key] = JSON.parse(rawValue);
    } catch {
      settings[key] = rawValue;
    }
  }

  if (typeof settings.oidcClientSecret === "string") {
    settings.oidcClientSecret = decryptSecret(settings.oidcClientSecret) ?? "";
  }
  applySessionAffinityLegacyFallback(settings);

  return settings;
}

export function needsHeadlessOnboarding(
  settings: SettingsRecord,
  environment: SettingsEnvironment
): boolean {
  return !settings.setupComplete && Boolean(environment.INITIAL_PASSWORD);
}
