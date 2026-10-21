import { mergeAbortSignals } from "../executors/base/mergeAbortSignals.ts";

export const INGRESS_REQUEST_DEADLINE_CODE = "request_ingress_deadline_exceeded";
export const INGRESS_REQUEST_DEADLINE_TYPE = "request_deadline";

interface IngressRequestContext {
  signal: AbortSignal;
  deadlineAt: number;
  mode: string;
  promoteStreaming(): void;
  assertLive(): void;
  remainingMs(): number;
}

function context(): IngressRequestContext | undefined {
  const storage = (globalThis as Record<symbol, unknown>)[
    Symbol.for("omniroute.request-deadline-context.v1")
  ] as { getStore(): IngressRequestContext | undefined } | undefined;
  return storage?.getStore();
}

/** Trusted transport provenance; a matching upstream error string is insufficient. */
export function isIngressRequestDeadline(error: unknown): error is Error {
  return (
    error instanceof Error &&
    (error as unknown as Record<symbol, unknown>)[
      Symbol.for("omniroute.request-ingress-deadline.v1")
    ] === true
  );
}

export function getRequestLifecycleSignal(signal?: AbortSignal | null): AbortSignal | undefined {
  const owned = context()?.signal;
  if (!owned || owned === signal) return signal ?? owned;
  return signal ? mergeAbortSignals(signal, owned) : owned;
}

export function promoteRequestToStreaming(): void {
  context()?.promoteStreaming();
}

export function throwIfRequestCancelled(signal?: AbortSignal | null): void {
  context()?.assertLive();
  signal?.throwIfAborted();
}

/** Cancel a pending body read promptly without awaiting an uncooperative source cancel. */
export function readRequestChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  signal?: AbortSignal | null
): Promise<ReadableStreamReadResult<Uint8Array>> {
  throwIfRequestCancelled(signal);
  if (!signal) return reader.read();
  return new Promise((resolve, reject) => {
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => {
      cleanup();
      try {
        void reader.cancel(signal.reason).catch(() => {});
      } catch {}
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    reader.read().then(
      (value) => {
        cleanup();
        if (signal.aborted) reject(signal.reason);
        else resolve(value);
      },
      (error) => {
        cleanup();
        reject(error);
      }
    );
  });
}
