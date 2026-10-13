/** Bound the complete operation, including response body consumption. */
export async function withOperationDeadline<T>(
  timeoutMs: number,
  body: (signal: AbortSignal) => Promise<T>,
  parent?: AbortSignal
): Promise<T> {
  const controller = new AbortController();
  const signal = parent ? AbortSignal.any([parent, controller.signal]) : controller.signal;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  const cancelled = new Promise<never>((_, reject) => {
    onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    timeout = setTimeout(
      () => controller.abort(new DOMException("Operation deadline exceeded", "TimeoutError")),
      timeoutMs
    );
  });
  void cancelled.catch(() => {});
  try {
    signal.throwIfAborted();
    return await Promise.race([body(signal), cancelled]);
  } finally {
    clearTimeout(timeout);
    if (onAbort) signal.removeEventListener("abort", onAbort);
  }
}
