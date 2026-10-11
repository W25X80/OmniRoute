import type { PersistenceTailPermit } from "@/lib/db/persistenceRuntime";

export type StreamCompletionWork = void | Promise<unknown>;
export type StreamCompletionOwner = () => StreamCompletionWork;
export type StreamCompletionPriority = "accounting" | "optional";

export interface StreamCompletionClock {
  completedAt: number;
  timestamp: string;
  latencyMs: number;
}

/** Capture terminal event time before a native snapshot or accounting owner can await. */
export function captureStreamCompletionClock(
  startTime: number,
  completedAt = Date.now()
): StreamCompletionClock {
  return {
    completedAt,
    timestamp: new Date(completedAt).toISOString(),
    latencyMs: completedAt - startTime,
  };
}

/** Synchronous terminal claim, followed by the full admitted accounting tail. */
export function createStreamingCompletionCallback<Payload>(args: {
  permit?: PersistenceTailPermit;
  claim: (payload: Payload) => boolean;
  complete: (
    payload: Payload,
    track: (owner: StreamCompletionOwner, priority?: StreamCompletionPriority) => void
  ) => void;
}): (payload: Payload) => void | Promise<void> {
  let completion: Promise<void> | undefined;
  return (payload) => {
    if (!args.claim(payload)) return completion;
    if (!args.permit) {
      args.complete(payload, (owner) => {
        owner();
      });
      return;
    }
    const permit = args.permit;
    let work: Promise<void>;
    try {
      work = permit.run(async () => {
        const accounting: StreamCompletionOwner[] = [];
        const optional: StreamCompletionOwner[] = [];
        const failures: unknown[] = [];
        try {
          args.complete(payload, (owner, priority = "optional") => {
            (priority === "accounting" ? accounting : optional).push(owner);
          });
        } catch (error) {
          failures.push(error);
        }
        // Accounting keeps its registration order and starts before optional network/cache
        // work. Priority changes scheduling, never an owner's source error policy.
        for (const owner of [...accounting, ...optional]) {
          try {
            await owner();
          } catch (error) {
            failures.push(error);
          }
        }
        if (failures.length) throw new AggregateError(failures, "Stream accounting failed");
      });
    } catch (error) {
      work = Promise.reject(error);
    }
    completion = work.finally(() => permit.release());
    return completion;
  };
}

/** Before HTTP handoff, release is mandatory even if abort/cancel itself fails. */
export async function releaseFailedStreamingHandoff(
  permit: PersistenceTailPermit,
  cancel: () => void | Promise<void>
): Promise<void> {
  try {
    await cancel();
  } finally {
    await permit.release();
  }
}

/** Settle every already-started owner before reporting accounting failure. */
export async function settleStreamAccountingWork(
  work: Iterable<StreamCompletionWork>
): Promise<void> {
  const pending = Array.from(work)
    .filter((item) => item !== undefined)
    .map((item) => Promise.resolve(item));
  const outcomes = await Promise.allSettled(pending);
  const failures = outcomes.flatMap((outcome) =>
    outcome.status === "rejected" ? [outcome.reason] : []
  );
  if (failures.length) throw new AggregateError(failures, "Stream accounting failed");
}
