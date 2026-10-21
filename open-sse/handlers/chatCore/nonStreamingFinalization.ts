import { createErrorResult } from "../../utils/error.ts";
import {
  isIngressRequestDeadline,
  throwIfRequestCancelled,
  INGRESS_REQUEST_DEADLINE_CODE,
  INGRESS_REQUEST_DEADLINE_TYPE,
} from "../../utils/requestDeadline.ts";
import { getSelectedControlPlane } from "@/lib/db/controlPlaneContext";
/**
 * Request-level finalization for non-streaming chat.
 * Success and failure each write usage/cost/quota/attempt/pending once.
 */

import type {
  ChatCoreErrorResult,
  ProviderLegUsage,
  ServerOwnedToolLoopResult,
} from "@/lib/skills/toolLoopTypes.ts";
import type { PersistAttemptLogsArgs } from "./attemptLogging.ts";
import type { PendingRequestMetadata } from "@/lib/usage/usageHistory.ts";
import { type FailureUsageAggregate, toFailureUsageAggregate } from "./failureUsage.ts";

export type NonStreamingFinalizationPlan =
  | {
      kind: "success";
      usage: ProviderLegUsage | null;
      totalCostUsd: number;
      receiptCount: number;
    }
  | {
      kind: "failure";
      error: ChatCoreErrorResult;
      usage: ProviderLegUsage | null;
      totalCostUsd: number;
      receiptCount: number;
    };

export interface NonStreamingFinalizationDeps {
  writeUsage: (plan: NonStreamingFinalizationPlan) => void | Promise<void>;
  writeCost: (totalCostUsd: number) => void;
  scheduleQuota: (plan: NonStreamingFinalizationPlan) => void | Promise<void>;
  writeAttempt: (plan: NonStreamingFinalizationPlan) => void | Promise<void>;
  finalizePending: (plan: NonStreamingFinalizationPlan) => void;
}

function missingError(): ChatCoreErrorResult {
  return {
    success: false,
    status: 500,
    response: new Response(null, { status: 500 }),
    error: "Missing tool-loop error result",
    errorCode: "internal_error",
  };
}

export function buildNonStreamingFinalizationPlan(
  loop: ServerOwnedToolLoopResult
): NonStreamingFinalizationPlan {
  const usage = loop.cumulativeUsage;
  const totalCostUsd = loop.totalCostUsd;
  const receiptCount = loop.receipts.length;
  if (loop.kind === "error") {
    return {
      kind: "failure",
      error: loop.errorResult ?? missingError(),
      usage,
      totalCostUsd,
      receiptCount,
    };
  }
  return {
    kind: "success",
    usage,
    totalCostUsd,
    receiptCount,
  };
}

export async function finalizeNonStreamingRequest(
  plan: NonStreamingFinalizationPlan,
  deps: NonStreamingFinalizationDeps
): Promise<void> {
  await deps.writeUsage(plan);
  deps.writeCost(plan.totalCostUsd);
  if (plan.kind === "success") {
    await deps.scheduleQuota(plan);
  }
  const attemptPersistence = deps.writeAttempt(plan);
  if (getSelectedControlPlane()) await attemptPersistence;
  deps.finalizePending(plan);
}

export async function finalizeToolLoopError(input: {
  loop: ServerOwnedToolLoopResult;
  model: string;
  provider: string;
  connectionId?: string;
  providerRequest?: Record<string, unknown>;
  persistFailureUsage: (
    status: number,
    errorCode: string,
    usage?: FailureUsageAggregate | null
  ) => void | Promise<void>;
  persistAttemptLogs: (params: PersistAttemptLogsArgs) => void | Promise<void>;
  trackPendingRequest: (
    model: string,
    provider: string,
    connectionId?: string,
    isPending?: boolean,
    metadata?: PendingRequestMetadata,
    pendingRequestId?: string
  ) => void;
  pendingRequestId?: string;
}): Promise<ChatCoreErrorResult> {
  const plan = buildNonStreamingFinalizationPlan(input.loop);
  const err = plan.kind === "failure" ? plan.error : missingError();
  return finalizeNonStreamingFailure({ ...input, error: err, usage: plan.usage });
}

/** Resolve cancellation only before finalization; committed outcomes are never compensated. */
export function resolveNonStreamingCancellation(
  signal?: AbortSignal | null,
  originalError?: unknown
): ChatCoreErrorResult | null {
  let reason = originalError;
  try {
    throwIfRequestCancelled(signal);
  } catch (error) {
    reason = error;
  }
  const localDeadline = isIngressRequestDeadline(reason);
  if (!localDeadline && !signal?.aborted) return null;
  const status = localDeadline ? 504 : 499;
  const result = createErrorResult(
    status,
    localDeadline ? "Gateway request exceeded its configured ingress deadline" : "Request aborted",
    null,
    localDeadline ? INGRESS_REQUEST_DEADLINE_CODE : "client_disconnected",
    localDeadline ? INGRESS_REQUEST_DEADLINE_TYPE : "client_disconnected"
  ) as ChatCoreErrorResult;
  result.originalError = reason;
  return result;
}

export async function finalizeNonStreamingFailure(
  input: Omit<Parameters<typeof finalizeToolLoopError>[0], "loop"> & {
    error: ChatCoreErrorResult;
    usage: ProviderLegUsage | null;
  }
): Promise<ChatCoreErrorResult> {
  const err = input.error;
  const plan: NonStreamingFinalizationPlan = {
    kind: "failure",
    error: err,
    usage: input.usage,
    totalCostUsd: 0,
    receiptCount: 0,
  };
  await finalizeNonStreamingRequest(plan, {
    writeUsage: () => {
      return input.persistFailureUsage(
        err.status,
        err.errorCode || `upstream_${err.status}`,
        toFailureUsageAggregate(plan.usage)
      );
    },
    writeCost: () => {},
    scheduleQuota: () => {},
    writeAttempt: () => {
      return input.persistAttemptLogs({
        status: err.status,
        tokens: plan.usage,
        error: err.error || "Provider request failed",
        providerRequest: input.providerRequest,
        clientResponse: {
          error: {
            message: err.error || "Provider request failed",
            type: err.errorType || "api_error",
          },
        },
        cacheSource: "upstream",
      });
    },
    finalizePending: () => {
      input.trackPendingRequest(
        input.model,
        input.provider,
        input.connectionId,
        false,
        undefined,
        input.pendingRequestId
      );
    },
  });
  return err;
}
