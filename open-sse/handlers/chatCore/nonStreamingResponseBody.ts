/**
 * chatCore non-streaming response-body reader (Quality Gate v2 / Fase 9 — chatCore god-file
 * decomposition, #3501).
 *
 * Extracted from chatCore: reads an upstream response body to a string. When the upstream is an SSE /
 * NDJSON stream consumed in non-streaming mode, it drains the reader chunk-by-chunk under the body
 * timeout and cancels early once a terminal SSE signal is observed. Native JSON follows the same
 * actual-byte bound and cancellation ownership without treating its text as SSE.
 */

import { FETCH_BODY_TIMEOUT_MS } from "../../config/constants.ts";
import { createBodyTimeoutError } from "./upstreamTimeouts.ts";
import {
  appendNonStreamingSseTerminalSignal,
  type NonStreamingSseTerminalState,
} from "./nonStreamingSse.ts";

/**
 * Thrown when a non-streaming upstream body exceeds the hard cap. Buffering an unbounded
 * SSE/NDJSON or JSON response in non-streaming mode was an OOM path (`rawBody += chunk`
 * with no ceiling): a single multi-hundred-MB upstream response could fill the V8 heap.
 * Callers treat this like any other upstream error rather than crashing the process.
 */
export class NonStreamingResponseTooLargeError extends Error {
  readonly bytesSeen: number;
  readonly maxBytes: number;
  constructor(bytesSeen: number, maxBytes: number) {
    super(
      `Upstream non-streaming response exceeded the ${maxBytes}-byte cap (saw at least ${bytesSeen} bytes)`
    );
    this.name = "NonStreamingResponseTooLargeError";
    this.bytesSeen = bytesSeen;
    this.maxBytes = maxBytes;
  }
}

const DEFAULT_MAX_NONSTREAMING_RESPONSE_BYTES = 64 * 1024 * 1024; // 64 MB

/**
 * Hard cap for a non-streaming response buffered fully into memory. Generous by default so
 * legitimate large completions pass; bounds only pathological/runaway upstream bodies.
 * Override with `OMNIROUTE_MAX_NONSTREAMING_RESPONSE_BYTES`.
 */
export const MAX_NONSTREAMING_RESPONSE_BYTES = (() => {
  const parsed = Number.parseInt(String(process.env.OMNIROUTE_MAX_NONSTREAMING_RESPONSE_BYTES), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_MAX_NONSTREAMING_RESPONSE_BYTES;
})();

export async function readNonStreamingResponseBody(
  response: Response,
  contentType: string,
  upstreamStream: boolean,
  maxBytes: number = MAX_NONSTREAMING_RESPONSE_BYTES,
  options: { signal?: AbortSignal | null; timeoutMs?: number } = {}
): Promise<string> {
  const { signal } = options;
  const timeoutMs = options.timeoutMs ?? FETCH_BODY_TIMEOUT_MS;
  const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
  const reader = response.body?.getReader();
  if (!reader) {
    signal?.throwIfAborted();
    return "";
  }
  let cancelled = false;
  const cancel = (reason: unknown) => {
    if (cancelled) return;
    cancelled = true;
    try {
      void reader.cancel(reason).catch(() => {});
    } catch {
      // Cancellation is best-effort; never replace the original failure.
    }
  };
  const decoder = new TextDecoder();
  const terminalState: NonStreamingSseTerminalState = { currentEvent: "", pendingLine: "" };
  const inspectTerminal =
    upstreamStream &&
    (contentType.includes("text/event-stream") || contentType.includes("application/x-ndjson"));
  let bytesSeen = 0;
  let rawBody = "";
  try {
    signal?.throwIfAborted();
    const declared = Number.parseInt(response.headers.get("content-length") ?? "", 10);
    if (Number.isFinite(declared) && declared > maxBytes) {
      throw new NonStreamingResponseTooLargeError(declared, maxBytes);
    }
    while (true) {
      const next = await readChunk(reader, deadline, timeoutMs, signal);
      signal?.throwIfAborted();
      if (next.done) break;
      if (!next.value) continue;
      bytesSeen += next.value.byteLength;
      if (bytesSeen > maxBytes) {
        throw new NonStreamingResponseTooLargeError(bytesSeen, maxBytes);
      }
      const chunk = decoder.decode(next.value, { stream: true });
      rawBody += chunk;
      if (inspectTerminal && appendNonStreamingSseTerminalSignal(terminalState, chunk)) {
        cancel("non-streaming bridge consumed terminal SSE event");
        break;
      }
    }
    signal?.throwIfAborted();
    return rawBody + decoder.decode();
  } catch (error) {
    cancel(error);
    throw error;
  } finally {
    reader.releaseLock();
  }
}

function readChunk(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  deadline: number,
  timeoutMs: number,
  signal?: AbortSignal | null
): Promise<ReadableStreamReadResult<Uint8Array>> {
  signal?.throwIfAborted();
  const remaining = deadline > 0 ? deadline - Date.now() : 0;
  if (deadline > 0 && remaining <= 0) throw createBodyTimeoutError(timeoutMs);
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    const onAbort = () => {
      cleanup();
      reject(signal?.reason);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (deadline > 0) {
      timer = setTimeout(() => {
        cleanup();
        reject(createBodyTimeoutError(timeoutMs));
      }, remaining);
    }
    reader.read().then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error) => {
        cleanup();
        reject(error);
      }
    );
  });
}
