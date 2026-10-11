import {
  CALL_LOG_SIZE_LIMIT_REASON as SIZE_LIMIT_EXCEEDED_REASON,
  CALL_LOG_BODY_OMITTED_FOR_SIZE_LIMIT as OMITTED_FOR_SIZE_LIMIT,
  CALL_LOG_STREAM_CHUNKS_OMITTED_FOR_SIZE_LIMIT as STREAM_CHUNKS_OMITTED_FOR_SIZE_LIMIT,
} from "@/shared/constants/callLogSizeLimitMarkers";
import type { CallLogArtifact } from "./callLogArtifacts.ts";
import { getCallLogPipelineMaxSizeBytes, isChatDebugFileEnabled } from "../logEnv";

export const MAX_CALL_LOG_ARTIFACT_BYTES = 512 * 1024;

// The error is the only field that says *why* a request failed, and it is
// typically ~90 bytes next to the multi-hundred-KB bodies that trip the cap.
// Dropping it made a size-limited row undiagnosable: a provider outage, a local
// timeout and an upstream 400 all rendered as the same omission marker. It is
// kept at every fallback stage instead, truncated rather than discarded.
const MAX_PRESERVED_ERROR_BYTES = 4 * 1024;
const ERROR_TRUNCATED_FOR_SIZE_LIMIT = "[truncated: call log artifact size limit exceeded]";

function truncateUtf8(text: string, maxBytes: number): string {
  const buffer = Buffer.from(text, "utf8");
  if (buffer.length <= maxBytes) return text;
  // Cut back off a partial multi-byte sequence so the tail is not a U+FFFD.
  let end = maxBytes;
  while (end > 0 && (buffer[end] & 0xc0) === 0x80) end--;
  return buffer.subarray(0, end).toString("utf8");
}

/**
 * Keep the error through a size-limit fallback, truncating it if it is itself
 * large. Returns the value unchanged when it already fits, so a normal-sized
 * error is byte-identical to what a non-truncated artifact would carry.
 */
function preserveErrorForSizeLimit(error: unknown): unknown {
  if (error === null || error === undefined) return null;
  let serialized: string;
  try {
    serialized = typeof error === "string" ? error : (JSON.stringify(error) ?? String(error));
  } catch {
    // A circular or unserializable error must not take the whole artifact down.
    serialized = String(error);
  }
  if (Buffer.byteLength(serialized, "utf8") <= MAX_PRESERVED_ERROR_BYTES) return error;
  return `${truncateUtf8(serialized, MAX_PRESERVED_ERROR_BYTES)} ${ERROR_TRUNCATED_FOR_SIZE_LIMIT}`;
}

export function computeArtifactChecksum(serialized: string): string {
  const bytes = Buffer.from(serialized);
  let hash = 0x811c9dc5;
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

function truncateArtifactForStorage(artifact: CallLogArtifact): CallLogArtifact {
  const pipeline = artifact.pipeline;
  if (!pipeline?.streamChunks) return artifact;

  return {
    ...artifact,
    pipeline: {
      ...pipeline,
      streamChunks: {
        provider: pipeline.streamChunks.provider?.length
          ? [STREAM_CHUNKS_OMITTED_FOR_SIZE_LIMIT]
          : undefined,
        openai: pipeline.streamChunks.openai?.length
          ? [STREAM_CHUNKS_OMITTED_FOR_SIZE_LIMIT]
          : undefined,
        client: pipeline.streamChunks.client?.length
          ? [STREAM_CHUNKS_OMITTED_FOR_SIZE_LIMIT]
          : undefined,
      },
    },
  };
}

function omitOversizedPipeline(artifact: CallLogArtifact): CallLogArtifact {
  if (!artifact.pipeline) return artifact;

  return {
    ...artifact,
    pipeline: {
      error: {
        _omniroute_truncated: true,
        reason: SIZE_LIMIT_EXCEEDED_REASON,
      },
    },
  };
}

// Test-only export: the real byte budget for an artifact, so budget
// assertions measure against the same cap the ladder enforces instead of a
// hardcoded byte count.
export function getArtifactMaxBytesForTest(artifact: CallLogArtifact): number {
  return getArtifactMaxBytes(artifact);
}

function getArtifactMaxBytes(artifact: CallLogArtifact): number {
  return artifact.pipeline ? getCallLogPipelineMaxSizeBytes() : MAX_CALL_LOG_ARTIFACT_BYTES;
}

function buildMinimalArtifactForSizeLimit(artifact: CallLogArtifact) {
  return {
    schemaVersion: artifact.schemaVersion,
    summary: artifact.summary,
    requestBody: OMITTED_FOR_SIZE_LIMIT,
    responseBody: OMITTED_FOR_SIZE_LIMIT,
    // Never drop the error: it is the only field that says WHY the request
    // failed (e.g. "Fetch timeout after 110000ms on https://..."). Diagnosing
    // provider outages from a log row that shows only an omission marker is
    // impossible; the error string is tiny next to the request/response bodies.
    error: preserveErrorForSizeLimit(artifact.error),
    pipeline: {
      error: {
        _omniroute_truncated: true,
        reason: SIZE_LIMIT_EXCEEDED_REASON,
      },
    },
  };
}

/**
 * Fallback ladder for an artifact that does not fit its byte budget, ordered
 * from "keeps the most" to "keeps the least": the first stage that fits wins.
 *
 * Ordering rule: drop the payload that most plausibly tripped the cap, and
 * drop a payload that is *duplicated elsewhere in the artifact* before one
 * that is unique. `pipeline` carries both sides of the exchange already
 * translated (`clientRawRequest`/`providerRequest`/`providerResponse`/
 * `clientResponse`), so evicting it to keep `requestBody` traded the whole
 * upstream exchange -- including the only record of what the provider
 * actually answered -- for a raw client prompt the pipeline already holds a
 * translated copy of. The request body goes first on its own now (it is the
 * usual cap-tripper and the least diagnostic side), then both bodies, and
 * `pipeline` survives one stage longer; the previous order is still reached
 * when dropping the request body alone is not enough.
 *
 * Two consumers depend on that ordering, not just human diagnosis:
 * `resolvePreviousResponseState` (db/responsesContinuationStore.ts) rebuilds
 * `previous_response_id` history from `pipeline.clientRawRequest` /
 * `pipeline.clientResponse` and returns null -- forcing the client to resend
 * full history -- for any artifact whose pipeline was omitted; and
 * `maybeEnrichCompletedDetail` (usage/completedRequestDetails.ts) reads
 * `pipeline.providerResponse` in preference to `responseBody`.
 */
function buildSizeLimitStages(artifact: CallLogArtifact): Array<() => unknown> {
  // One parametrized helper for both body-omission stages: request-only keeps
  // the response verbatim, both-bodies drops it too. Single spread + single
  // error-preservation call, so the two stages cannot drift apart.
  const omitBodies = <T extends object>(value: T, keepResponse = false) => ({
    ...value,
    requestBody: OMITTED_FOR_SIZE_LIMIT,
    responseBody: keepResponse
      ? (value as { responseBody: unknown }).responseBody
      : OMITTED_FOR_SIZE_LIMIT,
    error: preserveErrorForSizeLimit(artifact.error),
  });

  return [
    () => truncateArtifactForStorage(artifact),
    // Request body alone: the usual cap-tripper (multi-hundred-KB client
    // prompts) and the least diagnostic once the pipeline holds a translated
    // copy of it. Worth a stage only when there is a pipeline to keep in
    // exchange -- without one it produces the same bytes as a later stage, so
    // it is left out rather than costing a redundant stringify.
    ...(artifact.pipeline ? [() => omitBodies(artifact, true)] : []),
    // Bodies alone: worth a stage only when there is a pipeline to keep in
    // exchange. Without one it produces the same bytes as the stage two lines
    // below, so it is left out rather than costing a redundant stringify.
    ...(artifact.pipeline ? [() => omitBodies(artifact)] : []),
    () => omitOversizedPipeline(artifact),
    () => omitBodies(omitOversizedPipeline(artifact)),
    // The summary alone exceeded the cap (pathological). Keep the error so the
    // row stays diagnosable, drop everything else including the summary body.
    () => buildMinimalArtifactForSizeLimit(artifact),
  ];
}

export function serializeArtifactForStorage(artifact: CallLogArtifact): string {
  // Debug mode: write full untruncated payload
  if (isChatDebugFileEnabled()) {
    return JSON.stringify(artifact, null, 2);
  }

  const maxBytes = getArtifactMaxBytes(artifact);
  // Single-pass, non-pretty serialization on the hot path. Artifacts are machine-read via
  // JSON.parse (readCallArtifact), so pretty-printing only doubled the bytes and CPU of
  // serializing large request/response bodies on every request — a contributor to the
  // CPU-runaway. The debug path above keeps pretty output for human inspection.
  const serialized = JSON.stringify(artifact);
  if (Buffer.byteLength(serialized) <= maxBytes) {
    return serialized;
  }

  for (const buildStage of buildSizeLimitStages(artifact)) {
    const candidate = JSON.stringify(buildStage());
    if (Buffer.byteLength(candidate) <= maxBytes) {
      return candidate;
    }
  }

  // Last resort: not even the summary fit. The error still rides along --
  // without it this row says only "something was too big", which is the state
  // the size-limit fallbacks exist to remove.
  return JSON.stringify({
    schemaVersion: artifact.schemaVersion,
    _omniroute_truncated: true,
    reason: SIZE_LIMIT_EXCEEDED_REASON,
    error: preserveErrorForSizeLimit(artifact.error),
  });
}
