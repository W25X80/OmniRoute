/**
 * Streaming lifecycle deadline signal.
 *
 * Keeps the slow-stream deadline separate from the framework Request object:
 * callers pass the returned signal explicitly through admission/handler/lease
 * cleanup instead of rebuilding a NextRequest/Request just to replace .signal.
 */
import { mergeAbortSignals } from "../executors/base/mergeAbortSignals.ts";

export type StreamDeadlineSignal = {
  signal: AbortSignal;
  deadlineController: AbortController;
};

export function createStreamDeadlineSignal(
  clientSignal?: AbortSignal | null
): StreamDeadlineSignal {
  const deadlineController = new AbortController();
  // Avoid Node's strong registry for live AbortSignal.any composites: listeners
  // can capture the completed request and keep its body reachable indefinitely.
  const signal = clientSignal
    ? mergeAbortSignals(clientSignal, deadlineController.signal)
    : deadlineController.signal;
  return { signal, deadlineController };
}
