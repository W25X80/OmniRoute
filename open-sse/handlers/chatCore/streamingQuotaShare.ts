import { validateQuotaEventTimeMs } from "@/lib/quota/eventTime";

/**
 * chatCore streaming quota-share consumption POST-hook (Quality Gate v2 / Fase 9 — chatCore
 * god-file decomposition, #3501).
 *
 * Extracted from handleChatCore's onStreamComplete (B/F7): records shared-quota consumption for a
 * completed streaming response, resolving the real per-request cost via the injected calculateCost
 * so USD-unit pools accrue on streaming traffic too. onStreamComplete is synchronous, so this is a
 * sync fire-and-forget that drives the work through import().then().catch() and never throws to the
 * caller. Behaviour is byte-identical to the previous inline block.
 */

type LoggerLike = { warn?: (...args: unknown[]) => void } | null | undefined;
type CostResolver = (
  provider: string,
  model: string,
  usage: Record<string, number | undefined> | null | undefined,
  options: { serviceTier?: string }
) => Promise<number>;

export function scheduleStreamingQuotaShareConsumption(args: {
  apiKeyId: string | null | undefined;
  connectionId: string | null | undefined;
  provider: string | null | undefined;
  model: string | null | undefined;
  streamUsage: unknown;
  streamStatus: number;
  serviceTier?: string;
  calculateCost: CostResolver;
  log?: LoggerLike;
}): void {
  if (!args.apiKeyId || !args.connectionId || args.streamStatus !== 200) return;

  const quotaApiKeyId = args.apiKeyId;
  const quotaConnectionId = args.connectionId;
  // onStreamComplete is sync — use .then() (fire-and-forget, fail-open) instead of await
  import("@/lib/quota/spendRecorder")
    .then(({ recordStreamingConsumption }) =>
      recordStreamingConsumption(
        {
          apiKeyId: quotaApiKeyId,
          connectionId: quotaConnectionId,
          provider: args.provider,
          model: args.model,
          streamUsage: args.streamUsage,
          streamStatus: args.streamStatus,
          serviceTier: args.serviceTier,
        },
        { calculateCost: args.calculateCost, log: args.log }
      )
    )
    .catch(() => {
      // Outer fail-open — never throws to caller
    });
}

export async function scheduleStreamingQuotaShareConsumptionAsync(
  args: Parameters<typeof scheduleStreamingQuotaShareConsumption>[0] & { completedAtMs?: number }
): Promise<void> {
  if (!args.apiKeyId || !args.connectionId || args.streamStatus !== 200) return;
  try {
    const eventTimeMs = validateQuotaEventTimeMs(
      args.completedAtMs === undefined ? Date.now() : args.completedAtMs
    );
    const { apiKeyId, connectionId, provider, model, serviceTier, calculateCost } = args;
    const streamUsage = args.streamUsage;
    const tokenUsage =
      streamUsage && typeof streamUsage === "object"
        ? {
            prompt_tokens: (streamUsage as Record<string, unknown>).prompt_tokens,
            completion_tokens: (streamUsage as Record<string, unknown>).completion_tokens,
          }
        : null;
    const [{ buildConsumptionCost }, { recordConsumption }] = await Promise.all([
      import("@/lib/quota/spendRecorder"),
      import("@/lib/quota/enforce"),
    ]);
    let estimatedCost = 0;
    if (streamUsage && typeof streamUsage === "object") {
      try {
        estimatedCost = await calculateCost(
          provider ?? "unknown",
          model ?? "unknown",
          streamUsage as Record<string, number | undefined>,
          { serviceTier }
        );
      } catch {
        // Preserve quota share's declared fail-open cost fallback.
      }
    }
    await recordConsumption(
      {
        apiKeyId,
        connectionId,
        provider: provider ?? "unknown",
        model: model || undefined,
        cost: buildConsumptionCost(tokenUsage, estimatedCost),
      },
      eventTimeMs
    );
  } catch (error) {
    args.log?.warn?.("QUOTA_SHARE", error instanceof Error ? error.name : "Error");
  }
}
