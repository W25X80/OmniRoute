/**
 * Body Size Guard — E-1 Critical Fix
 *
 * Middleware helper that rejects oversized request bodies
 * before they are parsed, preventing OOM from malicious payloads.
 *
 * Usage:
 *   import { checkBodySize, MAX_BODY_BYTES } from "@/shared/middleware/bodySizeGuard";
 *
 *   const rejection = checkBodySize(request);
 *   if (rejection) return rejection;
 *
 * @module shared/middleware/bodySizeGuard
 */

import {
  normalizeRequestBodyLimitMb,
  parseRequestBodyLimitBytes,
  requestBodyLimitMbToBytes,
} from "../constants/bodySize";

/** Larger limit for backup/import routes: 100 MB */
export const MAX_BODY_BYTES_IMPORT = 100 * 1024 * 1024;

/** Larger limit for audio transcription uploads: 100 MB */
export const MAX_BODY_BYTES_AUDIO = 100 * 1024 * 1024;

/** Larger limit for file uploads: 500 MB */
export const MAX_BODY_BYTES_FILE = 500 * 1024 * 1024;

/** Larger limit for LLM request payloads: 50 MB */
export const MAX_BODY_BYTES_LLM_API = 50 * 1024 * 1024;

/** Fixed limit for the loopback-only Video Bridge extraction broker: 50 MB. */
export const MAX_BODY_BYTES_VIDEO_BRIDGE_BROKER = 50 * 1024 * 1024;

/**
 * Media (image generate / edit / upscale / video) is not capped by OmniRoute.
 * JSON + base64 inflates payloads by roughly 33%, and provider limits vary by model,
 * so the provider should decide whether a media request is too large.
 */
export const MAX_BODY_BYTES_MEDIA = Number.POSITIVE_INFINITY;

/** @deprecated Use MAX_BODY_BYTES_MEDIA — kept as alias for any external imports. */
export const MAX_BODY_BYTES_IMAGE_EDIT = MAX_BODY_BYTES_MEDIA;

/** Configured limit — reads from env or falls back to 10 MB */
export const MAX_BODY_BYTES = parseRequestBodyLimitBytes(process.env.MAX_BODY_SIZE_BYTES);

type BodySizeRule = {
  prefix: string;
  limit: number;
  exactPath?: boolean;
  fixedLimit?: boolean;
};

const ROUTE_LIMITS: BodySizeRule[] = [
  {
    prefix: "/api/modality-bridge/video/extract",
    limit: MAX_BODY_BYTES_VIDEO_BRIDGE_BROKER,
    exactPath: true,
    fixedLimit: true,
  },
  { prefix: "/api/db-backups/import", limit: MAX_BODY_BYTES_IMPORT },
  { prefix: "/api/v1/chat/completions", limit: MAX_BODY_BYTES_LLM_API },
  { prefix: "/api/v1/responses", limit: MAX_BODY_BYTES_LLM_API },
  { prefix: "/api/v1/images", limit: MAX_BODY_BYTES_MEDIA },
  { prefix: "/api/v1/videos", limit: MAX_BODY_BYTES_MEDIA },
  { prefix: "/api/v1/audio/transcriptions", limit: MAX_BODY_BYTES_AUDIO },
  { prefix: "/api/v1/files", limit: MAX_BODY_BYTES_FILE },
];

const PROVIDER_IMAGE_GENERATION_ROUTE = /^\/api\/v1\/providers\/[^/]+\/images\/generations(?:\/|$)/;

export function getConfiguredBodySizeLimitBytes(settings?: Record<string, unknown>): number {
  const configuredMb = normalizeRequestBodyLimitMb(settings?.maxBodySizeMb);
  return configuredMb === null ? MAX_BODY_BYTES : requestBodyLimitMbToBytes(configuredMb);
}

/**
 * Resolve the body size limit for a request path.
 */
export function getBodySizeLimit(pathname: string, settings?: Record<string, unknown>): number {
  const configuredLimit = getConfiguredBodySizeLimitBytes(settings);
  if (PROVIDER_IMAGE_GENERATION_ROUTE.test(pathname)) return MAX_BODY_BYTES_MEDIA;
  const customRule = ROUTE_LIMITS.find((rule) =>
    rule.exactPath ? pathname === rule.prefix : pathname.startsWith(rule.prefix)
  );
  if (!customRule) return configuredLimit;
  return customRule.fixedLimit ? customRule.limit : Math.max(customRule.limit, configuredLimit);
}

/**
 * Check Content-Length header against the configured limit.
 * Returns a 413 Response if the body is too large, or null if OK.
 */
export function checkBodySize(request: Request, limit: number = MAX_BODY_BYTES): Response | null {
  const contentLength = request.headers.get("content-length");

  if (contentLength) {
    const bytes = Number.parseInt(contentLength, 10);
    if (!Number.isNaN(bytes) && bytes > limit) {
      return createBodyTooLargeResponse(limit);
    }
  }

  return null;
}

/** Shared header/stream rejection; callers retain their existing CORS stamping. */
export function createBodyTooLargeResponse(limit: number): Response {
  return new Response(
    JSON.stringify({
      error: {
        message: `Request body too large. Maximum allowed: ${formatBytes(limit)}`,
        type: "payload_too_large",
        code: "PAYLOAD_TOO_LARGE",
      },
    }),
    {
      status: 413,
      headers: {
        "Content-Type": "application/json",
      },
    }
  );
}

export class RequestBodyTooLargeError extends Error {
  constructor(readonly limit: number) {
    super(`Request body exceeds ${formatBytes(limit)}`);
    this.name = "RequestBodyTooLargeError";
  }
}

/**
 * Read a request body while enforcing the actual streamed byte count. This closes the gap left by
 * Content-Length-only admission for chunked or deliberately mislabelled requests.
 */
export async function readRequestBodyWithLimit(
  request: Request,
  limit: number
): Promise<Uint8Array<ArrayBuffer>> {
  const declaredLength = Number.parseInt(request.headers.get("content-length") || "", 10);
  if (!Number.isNaN(declaredLength) && declaredLength > limit) {
    throw new RequestBodyTooLargeError(limit);
  }
  if (!request.body) return new Uint8Array(0);

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > limit) {
        await reader.cancel("request body too large");
        throw new RequestBodyTooLargeError(limit);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}

/** Count bytes before multipart parsing without retaining a second full request buffer. */
export async function readFormDataWithLimit(
  request: Request,
  limit: number,
  observe?: { beforeForward(total: number): void; beforeComplete(total: number): void }
): Promise<FormData> {
  const declared = Number.parseInt(request.headers.get("content-length") || "", 10);
  if (!Number.isNaN(declared) && declared > limit) {
    await request.body?.cancel("request body too large").catch(() => {});
    throw new RequestBodyTooLargeError(limit);
  }
  if (!request.body) return request.formData();
  const reader = request.body.getReader();
  let total = 0;
  let stopped = false;
  let consumerCanceled = false;
  let released = false;
  let cancellation: Promise<void> | undefined;
  let controller: ReadableStreamDefaultController<Uint8Array>;
  const release = () => {
    if (released) return;
    released = true;
    request.signal.removeEventListener("abort", abort);
    reader.releaseLock();
  };
  const cancelInput = (reason: unknown): Promise<void> => {
    stopped = true;
    if (released) return Promise.resolve();
    return (cancellation ??= reader
      .cancel(reason)
      .catch(() => {})
      .then(release));
  };
  const abort = () => {
    if (stopped) return;
    const reason = request.signal.reason ?? new DOMException("Request aborted", "AbortError");
    controller.error(reason);
    void cancelInput(reason);
  };
  const stream = new ReadableStream<Uint8Array>(
    {
      start(value) {
        controller = value;
        if (request.signal.aborted) abort();
        else request.signal.addEventListener("abort", abort, { once: true });
      },
      async pull(value) {
        if (stopped) return;
        try {
          const chunk = await reader.read();
          if (stopped) return;
          if (chunk.done) {
            observe?.beforeComplete(total);
            stopped = true;
            release();
            value.close();
            return;
          }
          total += chunk.value.byteLength;
          if (total > limit) {
            const error = new RequestBodyTooLargeError(limit);
            await cancelInput(error);
            if (!consumerCanceled) value.error(error);
            return;
          }
          observe?.beforeForward(total);
          value.enqueue(chunk.value);
        } catch (error) {
          if (!stopped) {
            stopped = true;
            release();
            value.error(error);
          }
        }
      },
      async cancel(reason) {
        consumerCanceled = true;
        await cancelInput(reason);
      },
    },
    { highWaterMark: 0 }
  );
  try {
    const bounded = new Request(request, { body: stream, duplex: "half" } as RequestInit & {
      duplex: "half";
    });
    return await bounded.formData();
  } catch (error) {
    await cancelInput(error);
    throw error;
  }
}

/** Format bytes as human-readable string */
function formatBytes(bytes: number): string {
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${bytes} bytes`;
}
