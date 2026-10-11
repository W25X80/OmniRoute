import { isExplicitNoLogApiKey } from "../compliance/noLog";
import { withSelectedContinuationPrivacy } from "../db/postgres/continuationPrivacy";
import {
  clearOwnedPendingContinuationState,
  seedOwnedPendingContinuationState,
  type PendingContinuationSeed,
  type ContinuationPipeline,
} from "../db/responsesContinuationStore";
import { protectPipelinePayloads } from "./callLogs/format";

export interface ResponsesContinuationPreparationContext {
  apiKeyId: string | null;
  noLog: boolean;
  detailedLoggingEnabled: boolean;
  videoContentRemoved: boolean;
  getPipelinePayloads: () => { clientRawRequest?: unknown } | null | undefined;
}

/** Prepare only the existing protected bridge; no artifact or accounting owner starts here. */
export async function prepareResponsesContinuationBeforeTerminal(
  clientResponse: unknown,
  context: ResponsesContinuationPreparationContext,
  signal: AbortSignal
): Promise<PendingContinuationSeed | undefined> {
  if (
    signal.aborted ||
    context.noLog ||
    !context.detailedLoggingEnabled ||
    context.videoContentRemoved ||
    isExplicitNoLogApiKey(context.apiKeyId)
  )
    return undefined;
  const summary =
    clientResponse && typeof clientResponse === "object"
      ? (clientResponse as { summary?: { id?: unknown } }).summary
      : undefined;
  const responseId = summary?.id;
  if (typeof responseId !== "string" || !responseId) return undefined;
  let seed: PendingContinuationSeed | undefined;
  try {
    await withSelectedContinuationPrivacy(
      context.apiKeyId,
      () => {
        if (signal.aborted || isExplicitNoLogApiKey(context.apiKeyId)) return;
        const captured = context.getPipelinePayloads();
        const pipeline = protectPipelinePayloads({
          clientRawRequest: captured?.clientRawRequest,
          clientResponse,
        }) as ContinuationPipeline | null;
        seed = seedOwnedPendingContinuationState(responseId, context.apiKeyId, pipeline, false);
      },
      { requirePipelineLogging: true }
    );
  } catch (error) {
    if (seed) clearOwnedPendingContinuationState(seed);
    throw error;
  }
  if (signal.aborted && seed) {
    clearOwnedPendingContinuationState(seed);
    return undefined;
  }
  return seed;
}
