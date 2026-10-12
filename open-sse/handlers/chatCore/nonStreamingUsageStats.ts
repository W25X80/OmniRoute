import { sanitizeErrorMessage } from "../../utils/error.ts";
import { getSelectedUsageHistoryRepository } from "@/lib/db/postgres/usageHistoryRepository";
import { saveRequestUsageWithReceipt, type UsageEntry } from "@/lib/usage/usageHistory";
import { recordKeyQuotaUsageAsync } from "@/domain/keyQuota";
import { recordTokenUsageAsync } from "../../services/tokenLimitCounter.ts";
/**
 * chatCore non-streaming usage-stats persistence (Quality Gate v2 / Fase 9 — chatCore god-file
 * decomposition, #3501 — response-handling slice of executeProviderRequest).
 *
 * Extracted from handleChatCore's non-streaming success path: records per-request usage analytics
 * for a successful non-streaming response — an optional trace console line, the fire-and-forget
 * `saveRequestUsage` row, and the per-api-key billable-token counter. Side-effect only (no handler
 * state is mutated, nothing is returned); best-effort, every write swallows its own errors. The
 * per-request context is threaded via `ctx` so the call site stays byte-identical; behaviour is
 * unchanged.
 */

import { saveRequestUsage } from "@/lib/usageDb";
import { recordKeyQuotaUsage } from "@/domain/keyQuota";
import { formatUsageLog } from "@/lib/usage/tokenAccounting";
import { COLORS } from "../../utils/stream.ts";
import { recordTokenUsage } from "../../services/tokenLimitCounter.ts";
import { computeBillableTokens } from "./upstreamTimeouts.ts";
import { type EffectiveServiceTier } from "./serviceTier.ts";

export type RecordNonStreamingUsageStatsContext = {
  traceEnabled: boolean;
  provider: string | null | undefined;
  connectionId: string | null | undefined;
  model: string | null | undefined;
  startTime: number;
  apiKeyInfo: { id?: string | null; name?: string | null } | null | undefined;
  effectiveServiceTier: EffectiveServiceTier;
  isCombo: boolean;
  comboStrategy: string | null | undefined;
  endpoint?: string | null | undefined;
  cpaAuthIndex?: string | null | undefined;
};

function logUsageTrace(
  usage: object,
  provider: string | null | undefined,
  connectionId: string | null | undefined
): void {
  const msg = `[${new Date().toLocaleTimeString("en-US", { hour12: false, hour: "2-digit", minute: "2-digit" })}] 📊 [USAGE] ${provider?.toUpperCase()} | ${formatUsageLog(usage)}${connectionId ? ` | account=${connectionId.slice(0, 8)}...` : ""}`;
  console.log(`${COLORS.green}${msg}${COLORS.reset}`);
}

function persistUsageRow(usage: object, ctx: RecordNonStreamingUsageStatsContext): void {
  const { provider, connectionId, model, startTime, apiKeyInfo, effectiveServiceTier } = ctx;
  saveRequestUsage({
    provider: provider || "unknown",
    model: model || "unknown",
    tokens: usage,
    status: "200",
    success: true,
    latencyMs: Date.now() - startTime,
    timeToFirstTokenMs: Date.now() - startTime,
    errorCode: null,
    timestamp: new Date().toISOString(),
    connectionId: connectionId || undefined,
    apiKeyId: apiKeyInfo?.id || undefined,
    apiKeyName: apiKeyInfo?.name || undefined,
    serviceTier: effectiveServiceTier,
    comboStrategy: ctx.isCombo ? ctx.comboStrategy || undefined : undefined,
    endpoint: ctx.endpoint || undefined,
    cpaAuthIndex: ctx.cpaAuthIndex || undefined,
  }).catch((err) => {
    console.error("Failed to save usage stats:", err.message);
  });
}

function recordBillableTokens(
  usage: object,
  apiKeyInfo: RecordNonStreamingUsageStatsContext["apiKeyInfo"],
  provider: string | null | undefined,
  model: string | null | undefined
): void {
  if (!apiKeyInfo?.id) return;
  try {
    const billable = computeBillableTokens(usage);
    // Key-quota tpm/rpm counters advance for every successful call regardless
    // of token count (rpm always +1), so the gate in apiKeyPolicy sees current
    // usage. Mirrors recordTokenUsage's swallow-and-continue.
    recordKeyQuotaUsage(apiKeyInfo.id, billable);
    if (billable > 0)
      recordTokenUsage(apiKeyInfo.id, provider || "unknown", model || "unknown", billable);
  } catch {
    // never block the response on counter recording
  }
}

export function recordNonStreamingUsageStats(
  usage: unknown,
  ctx: RecordNonStreamingUsageStatsContext
): void {
  if (!usage || typeof usage !== "object") return;

  if (ctx.traceEnabled) logUsageTrace(usage, ctx.provider, ctx.connectionId);
  persistUsageRow(usage, ctx);
  recordBillableTokens(usage, ctx.apiKeyInfo, ctx.provider, ctx.model);
}

export async function recordNonStreamingUsageStatsAsync(
  usage: unknown,
  ctx: RecordNonStreamingUsageStatsContext
): Promise<void> {
  if (!getSelectedUsageHistoryRepository()) {
    recordNonStreamingUsageStats(usage, ctx);
    return;
  }
  if (!usage || typeof usage !== "object") return;
  const { provider, connectionId, model, startTime, apiKeyInfo, effectiveServiceTier } = ctx;
  if (ctx.traceEnabled) logUsageTrace(usage, ctx.provider, ctx.connectionId);
  const entry: UsageEntry = {
    provider: provider || "unknown",
    model: model || "unknown",
    tokens: usage,
    status: "200",
    success: true,
    latencyMs: Date.now() - startTime,
    timeToFirstTokenMs: Date.now() - startTime,
    errorCode: null,
    timestamp: new Date().toISOString(),
    connectionId: connectionId || undefined,
    apiKeyId: apiKeyInfo?.id || undefined,
    apiKeyName: apiKeyInfo?.name || undefined,
    serviceTier: effectiveServiceTier,
    comboStrategy: ctx.isCombo ? ctx.comboStrategy || undefined : undefined,
    endpoint: ctx.endpoint || undefined,
    cpaAuthIndex: ctx.cpaAuthIndex || undefined,
  };
  let receipt: Awaited<ReturnType<typeof saveRequestUsageWithReceipt>>;
  try {
    receipt = await saveRequestUsageWithReceipt(entry);
  } catch (error) {
    try {
      console.warn(
        "[USAGE] Failed to persist completed-request usage:",
        sanitizeErrorMessage(error) || "Usage persistence failed"
      );
    } catch {
      // Optional diagnostics cannot discard an already successful provider response.
    }
    // No acknowledged history identity: do not replay a possibly committed event or
    // seed its receipt-dependent counters as though the history write had rolled back.
    return;
  }
  if (ctx.apiKeyInfo?.id) {
    const billable = computeBillableTokens(usage);
    if (receipt?.inserted) await recordKeyQuotaUsageAsync(ctx.apiKeyInfo.id, billable);
    if (billable > 0)
      await recordTokenUsageAsync(
        ctx.apiKeyInfo.id,
        ctx.provider || "unknown",
        ctx.model || "unknown",
        billable,
        receipt?.id
      );
  }
}
