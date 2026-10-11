import type { StreamCompletionClock } from "./streamingCompletion.ts";
import { getSelectedUsageHistoryRepository } from "@/lib/db/postgres/usageHistoryRepository";
import { saveRequestUsageWithReceipt, type UsageEntry } from "@/lib/usage/usageHistory";
import { recordKeyQuotaUsageAsync } from "@/domain/keyQuota";
import { recordTokenUsageAsync } from "../../services/tokenLimitCounter.ts";
/**
 * chatCore streaming usage-stats persistence (Quality Gate v2 / Fase 9 — chatCore god-file
 * decomposition, #3501).
 *
 * Extracted from handleChatCore's onStreamComplete: records per-request usage analytics for a
 * completed streaming response — the fire-and-forget `saveRequestUsage` row and the per-api-key
 * billable-token counter (only on a 200 stream). Side-effect only (no handler state is mutated,
 * nothing is returned); best-effort, every write swallows its own errors. The per-request context
 * is threaded via `ctx` so the call site stays byte-identical; behaviour is unchanged. The
 * compression usage-receipt attach stays in the handler (it is a handler-bound closure).
 */

import { saveRequestUsage } from "@/lib/usageDb";
import { recordKeyQuotaUsage } from "@/domain/keyQuota";
import { recordTokenUsage } from "../../services/tokenLimitCounter.ts";
import { computeBillableTokens } from "./upstreamTimeouts.ts";
import { type EffectiveServiceTier } from "./serviceTier.ts";

export type RecordStreamingUsageStatsContext = {
  provider: string | null | undefined;
  model: string | null | undefined;
  streamStatus: number;
  startTime: number;
  ttft: number | undefined;
  streamErrorCode: string | null | undefined;
  connectionId: string | null | undefined;
  apiKeyInfo: { id?: string | null; name?: string | null } | null | undefined;
  effectiveServiceTier: EffectiveServiceTier;
  isCombo: boolean;
  comboStrategy: string | null | undefined;
  endpoint?: string | null | undefined;
  cpaAuthIndex?: string | null | undefined;
  completionClock?: StreamCompletionClock;
};

function persistStreamingUsageRow(usage: object, ctx: RecordStreamingUsageStatsContext): void {
  const { provider, model, streamStatus, startTime, ttft, streamErrorCode } = ctx;
  saveRequestUsage({
    provider: provider || "unknown",
    model: model || "unknown",
    tokens: usage,
    status: String(streamStatus),
    success: streamStatus === 200,
    latencyMs: Date.now() - startTime,
    timeToFirstTokenMs: ttft,
    errorCode: streamStatus === 200 ? null : streamErrorCode || String(streamStatus),
    timestamp: new Date().toISOString(),
    connectionId: ctx.connectionId || undefined,
    apiKeyId: ctx.apiKeyInfo?.id || undefined,
    apiKeyName: ctx.apiKeyInfo?.name || undefined,
    serviceTier: ctx.effectiveServiceTier,
    comboStrategy: ctx.isCombo ? ctx.comboStrategy || undefined : undefined,
    endpoint: ctx.endpoint || undefined,
    cpaAuthIndex: ctx.cpaAuthIndex || undefined,
  }).catch((err) => {
    console.error("Failed to save usage stats:", err.message);
  });
}

function recordStreamingBillableTokens(usage: object, ctx: RecordStreamingUsageStatsContext): void {
  if (!ctx.apiKeyInfo?.id || ctx.streamStatus !== 200) return;
  try {
    const billable = computeBillableTokens(usage);
    // Key-quota tpm/rpm counters advance on every completed stream.
    recordKeyQuotaUsage(ctx.apiKeyInfo.id, billable);
    if (billable > 0)
      recordTokenUsage(
        ctx.apiKeyInfo.id,
        ctx.provider || "unknown",
        ctx.model || "unknown",
        billable
      );
  } catch {
    // never block the stream on counter recording
  }
}

export function recordStreamingUsageStats(
  usage: unknown,
  ctx: RecordStreamingUsageStatsContext
): void {
  if (!usage || typeof usage !== "object") return;
  persistStreamingUsageRow(usage, ctx);
  recordStreamingBillableTokens(usage, ctx);
}

export async function recordStreamingUsageStatsAsync(
  usage: unknown,
  ctx: RecordStreamingUsageStatsContext
): Promise<void> {
  if (!getSelectedUsageHistoryRepository()) {
    recordStreamingUsageStats(usage, ctx);
    return;
  }
  if (!usage || typeof usage !== "object") return;
  const { provider, model, streamStatus, startTime, ttft, streamErrorCode, completionClock } = ctx;
  const entry: UsageEntry = {
    provider: provider || "unknown",
    model: model || "unknown",
    tokens: usage,
    status: String(streamStatus),
    success: streamStatus === 200,
    latencyMs: completionClock?.latencyMs ?? Date.now() - startTime,
    timeToFirstTokenMs: ttft,
    errorCode: streamStatus === 200 ? null : streamErrorCode || String(streamStatus),
    timestamp: completionClock?.timestamp ?? new Date().toISOString(),
    connectionId: ctx.connectionId || undefined,
    apiKeyId: ctx.apiKeyInfo?.id || undefined,
    apiKeyName: ctx.apiKeyInfo?.name || undefined,
    serviceTier: ctx.effectiveServiceTier,
    comboStrategy: ctx.isCombo ? ctx.comboStrategy || undefined : undefined,
    endpoint: ctx.endpoint || undefined,
    cpaAuthIndex: ctx.cpaAuthIndex || undefined,
  };
  const receipt = await saveRequestUsageWithReceipt(entry);
  if (ctx.streamStatus !== 200) return;
  if (ctx.apiKeyInfo?.id) {
    const billable = computeBillableTokens(usage);
    if (receipt?.inserted)
      await recordKeyQuotaUsageAsync(
        ctx.apiKeyInfo.id,
        billable,
        completionClock ? { now: () => completionClock.completedAt } : {}
      );
    if (billable > 0)
      await recordTokenUsageAsync(
        ctx.apiKeyInfo.id,
        ctx.provider || "unknown",
        ctx.model || "unknown",
        billable,
        receipt?.id,
        completionClock?.completedAt
      );
  }
}
