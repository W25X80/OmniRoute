type Waiter = { grant(): void };
type Queue = { waiters: Waiter[] };
type FenceState = { queues: Map<string, Queue>; waiting: number };

const key = Symbol.for("omniroute.conversation-fingerprint-admission.v1");
const root = globalThis as typeof globalThis & { [key]?: WeakMap<object, FenceState> };
const states = (root[key] ??= new WeakMap());
const PER_FINGERPRINT_LIMIT = 64;
const DATABASE_WAITER_LIMIT = 256;

export class ConversationTrackingUnavailableError extends Error {
  constructor(readonly reason: "cancelled" | "deadline" | "capacity" | "conflict") {
    super(`Conversation tracking is unavailable (${reason})`);
    this.name = "ConversationTrackingUnavailableError";
  }
}

export function assertConversationTrackingActive(
  signal: AbortSignal | undefined,
  deadline: number
) {
  if (signal?.aborted) throw new ConversationTrackingUnavailableError("cancelled");
  if (Date.now() >= deadline) throw new ConversationTrackingUnavailableError("deadline");
}

/** Cancellation removes a waiter; only the actual holder can hand off the fence. */
export async function withConversationFingerprint<T>(
  identity: object,
  fingerprint: string,
  body: () => Promise<T>,
  options: { signal?: AbortSignal; deadline: number }
): Promise<T> {
  assertConversationTrackingActive(options.signal, options.deadline);
  let state = states.get(identity);
  if (!state) states.set(identity, (state = { queues: new Map(), waiting: 0 }));
  let queue = state.queues.get(fingerprint);
  if (queue) {
    if (queue.waiters.length >= PER_FINGERPRINT_LIMIT || state.waiting >= DATABASE_WAITER_LIMIT)
      throw new ConversationTrackingUnavailableError("capacity");
    const current = queue,
      owner = state;
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: ConversationTrackingUnavailableError) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        options.signal?.removeEventListener("abort", abort);
        const index = current.waiters.indexOf(waiter);
        if (index >= 0) current.waiters.splice(index, 1);
        owner.waiting--;
        if (error) reject(error);
        else resolve();
      };
      const waiter: Waiter = { grant: () => finish() };
      const abort = () => finish(new ConversationTrackingUnavailableError("cancelled"));
      const timer = setTimeout(
        () => finish(new ConversationTrackingUnavailableError("deadline")),
        Math.max(0, options.deadline - Date.now())
      );
      current.waiters.push(waiter);
      owner.waiting++;
      options.signal?.addEventListener("abort", abort, { once: true });
      if (options.signal?.aborted) abort();
    });
  } else {
    queue = { waiters: [] };
    state.queues.set(fingerprint, queue);
  }
  try {
    assertConversationTrackingActive(options.signal, options.deadline);
    return await body();
  } finally {
    const next = queue.waiters[0];
    if (next) next.grant();
    else state.queues.delete(fingerprint);
  }
}
