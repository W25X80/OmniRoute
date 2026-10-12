import { acquireNativeFileConsumer } from "@/lib/db/nativeFileWork";
import { v4 as uuidv4 } from "uuid";
import { parseDatabaseBackend } from "@/lib/db/backendSelection";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { PostgresError } from "@/lib/db/postgres/database";
import {
  type BatchItemCheckpoint,
  type BatchRecord,
  countBatchItemCheckpointsAsync,
  ensureBatchItemCheckpointsAsync,
  getBatchAsync,
  getPendingBatchesAsync,
  getTerminalBatchesAsync,
  isFileReferencedByOtherBatchAsync,
  listBatchItemCheckpointsAsync,
  markBatchItemErrorAsync,
  markBatchItemProcessingAsync,
  markBatchItemResultAsync,
  updateBatchAsync,
  transitionBatchAsync,
  completeBatchWithFilesAsync,
  type BatchOutputFile,
} from "@/lib/db/batches";
import {
  deleteFileAsync,
  getFileContentAsync,
  getFileContentForConsumerAsync,
  listFilesAsync,
} from "@/lib/db/files";
import { getApiKeyById } from "@/lib/db/apiKeys";
import { dispatch } from "@/lib/batches/dispatch";
import type { SupportedBatchEndpoint } from "@/shared/constants/batchEndpoints";
import { DEFAULT_BATCH_EXPIRATION_SECONDS } from "@/shared/constants/batch";

interface BatchProcessorRuntime {
  processing: boolean;
  pollInterval: NodeJS.Timeout | null;
  generation: number;
  closing: boolean;
  identity?: object;
  activePolls: Set<Promise<void>>;
  activeProcesses: Set<Promise<void>>;
  activeBatches: Set<string>;
  close?: Promise<void>;
}
function createProcessorRuntime(): BatchProcessorRuntime {
  return {
    processing: false,
    pollInterval: null,
    generation: 0,
    closing: false,
    activePolls: new Set(),
    activeProcesses: new Set(),
    activeBatches: new Set(),
  };
}
declare global {
  var __omnirouteNativeBatchProcessor: BatchProcessorRuntime | undefined;
}
// Native server bundles share one scheduler; SQLite retains its module-local runtime.
const processor =
  parseDatabaseBackend() === "postgres"
    ? (globalThis.__omnirouteNativeBatchProcessor ??= createProcessorRuntime())
    : createProcessorRuntime();
const activeProcesses = processor.activeProcesses;
const activeBatches = processor.activeBatches;

function captureAuthority(): object | undefined {
  const identity = getSelectedPersistenceIdentity();
  if (processor.identity && processor.identity !== identity) {
    throw new Error("Batch processor persistence authority changed");
  }
  processor.identity ??= identity;
  return identity;
}
function assertAuthority(identity: object | undefined): void {
  if (identity !== getSelectedPersistenceIdentity()) {
    throw new Error("Batch processor persistence authority changed");
  }
}
const DEFAULT_BATCH_WINDOW_SECONDS: number = 24 * 60 * 60;
const BATCH_RETRY_DURATION_MS: number =
  Number.parseInt(process.env.BATCH_RETRY_DURATION_MS ?? "", 10) || 24 * 60 * 60 * 1_000;
const BATCH_BACKOFF_BASE_MS: number =
  Number.parseInt(process.env.BATCH_BACKOFF_BASE_MS ?? "", 10) || 5_000;
const BATCH_BACKOFF_MAX_MS: number =
  Number.parseInt(process.env.BATCH_BACKOFF_MAX_MS ?? "", 10) || 3_600_000;
const BATCH_MAX_CONCURRENT: number =
  Number.parseInt(process.env.BATCH_MAX_CONCURRENT ?? "", 10) || 1;

interface BatchRequestItem {
  body: Record<string, unknown>;
  customId: string | null;
  lineNumber: number;
  method: "POST";
  url: SupportedBatchEndpoint;
}

export function initBatchProcessor() {
  if (processor.closing) throw new Error("Batch processor is closing");
  captureAuthority();
  if (processor.pollInterval) return processor.pollInterval;
  const generation = processor.generation;
  console.log("[BATCH] Initializing batch processor polling...");

  processor.pollInterval = setInterval(() => {
    if (processor.closing || generation !== processor.generation || processor.processing) return;
    processor.processing = true;
    const work = processPendingBatches();
    void work.then(
      () => {
        processor.processing = false;
      },
      (error: unknown) => {
        processor.processing = false;
        console.error("[BATCH] Polling error:", error);
      }
    );
  }, 10000).unref();
  return processor.pollInterval;
}

/** Legacy stop is restartable; accepted polls and batches continue. */
export function stopBatchProcessor(): void {
  processor.generation++;
  if (processor.pollInterval) {
    clearInterval(processor.pollInterval);
    processor.pollInterval = null;
    console.log("[BATCH] Stopped batch processor polling.");
  }
}

/** Shutdown closes admission before draining accepted polls and their descendants. */
export function stopBatchProcessorAsync(): Promise<void> {
  if (processor.close) return processor.close;
  processor.closing = true;
  stopBatchProcessor();
  processor.close = waitForAllBatches();
  return processor.close;
}

export function processPendingBatches(): Promise<void> {
  if (processor.closing) return Promise.reject(new Error("Batch processor is closing"));
  const work = runPersistenceBackgroundWork(async () => {
    const identity = captureAuthority();
    await processPendingBatchesInScope(identity);
  });
  processor.activePolls.add(work);
  void work.then(
    () => processor.activePolls.delete(work),
    () => processor.activePolls.delete(work)
  );
  return work;
}

async function processPendingBatchesInScope(identity: object | undefined): Promise<void> {
  const pending = await getPendingBatchesAsync();

  // Phase 1: Stale recovery — in_progress/finalizing batches not in activeBatches
  // are from a previous session; reset checkpointed batches to validating so they
  // can be completed without replaying already-dispatched items.
  for (const batch of pending) {
    if (batch.status === "in_progress" || batch.status === "finalizing") {
      if (!activeBatches.has(batch.id)) {
        await recoverStaleBatch(batch);
      }
    }
  }

  // Phase 2: Process actions respecting concurrency limit
  const remaining = await getPendingBatchesAsync(); // re-fetch after recovery updates
  let activeCount = activeBatches.size;

  for (const batch of remaining) {
    if (batch.status === "validating") {
      if (activeCount >= BATCH_MAX_CONCURRENT) {
        console.log(
          `[BATCH] Concurrency limit ${BATCH_MAX_CONCURRENT} reached, deferring batch ${batch.id}`
        );
        continue;
      }
      activeCount++;
      await startBatch(batch, identity);
    } else if (batch.status === "cancelling") {
      await cancelBatch(batch);
    }
  }

  // Cleanup task: delete files for batches completed more than completionWindow ago
  await cleanupExpiredBatches();
}

async function recoverStaleBatch(batch: BatchRecord): Promise<void> {
  const checkpointCount = await countBatchItemCheckpointsAsync(batch.id);
  const hasPotentialExternalEffects =
    batch.requestCountsTotal > 0 ||
    batch.requestCountsCompleted > 0 ||
    batch.requestCountsFailed > 0 ||
    batch.status === "finalizing";

  if (checkpointCount === 0 && hasPotentialExternalEffects) {
    console.warn(
      `[BATCH] Stale batch ${batch.id} has no item checkpoints; failing instead of replaying provider calls`
    );
    await failBatch(
      batch.id,
      "Cannot safely recover stale batch because item checkpoints are unavailable; create a new batch to retry intentionally."
    );
    return;
  }

  console.log(`[BATCH] Recovering stale batch ${batch.id} (${batch.status}) → validating`);

  await transitionBatchAsync(
    batch.id,
    ["in_progress", "finalizing"],
    {
      status: "validating",
      inProgressAt: null,
      finalizingAt: null,
      outputFileId: null,
      errorFileId: null,
      ...(checkpointCount === 0
        ? {
            requestCountsCompleted: 0,
            requestCountsFailed: 0,
          }
        : {}),
    },
    true
  );
}

function parseBatchWindowSeconds(window: string | null | undefined): number {
  if (!window) return DEFAULT_BATCH_WINDOW_SECONDS;
  const match = /^(\d+)([hdm])$/.exec(window);
  if (!match) return DEFAULT_BATCH_WINDOW_SECONDS;

  const value = Number.parseInt(match[1], 10);
  const unit = match[2];
  if (unit === "h") return value * 3600;
  if (unit === "d") return value * 86400;
  if (unit === "m") return value * 60;
  return DEFAULT_BATCH_WINDOW_SECONDS;
}

function getBatchOutputExpiresAt(batch: BatchRecord): number | null {
  if (
    batch.outputExpiresAfterAnchor === "created_at" &&
    typeof batch.outputExpiresAfterSeconds === "number" &&
    batch.outputExpiresAfterSeconds > 0
  ) {
    return batch.createdAt + batch.outputExpiresAfterSeconds;
  }

  const completionTime =
    batch.completedAt || batch.failedAt || batch.cancelledAt || batch.expiredAt;
  if (!completionTime) return null;
  // Default: batch output files expire 30 days after completion
  return completionTime + DEFAULT_BATCH_EXPIRATION_SECONDS;
}

function resolveBatchApiKeyValue(batch: Pick<BatchRecord, "apiKeyId">, apiKeyRow: any): any {
  if (typeof apiKeyRow?.key === "string" && apiKeyRow.key.length > 0) {
    return apiKeyRow.key;
  }
  if (batch.apiKeyId === "env-key") {
    return process.env.OMNIROUTE_API_KEY || process.env.ROUTER_API_KEY || null;
  }
  return null;
}

export function parseBatchItems(
  content: Buffer,
  batchEndpoint: SupportedBatchEndpoint
): { items: BatchRequestItem[]; error: null } | { items: null; error: string } {
  const lines = content
    .toString()
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const items: BatchRequestItem[] = [];
  for (const [index, line] of lines.entries()) {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(line);
    } catch {
      return { items: null, error: `Line ${index + 1} is not valid JSON` };
    }

    const method = String(parsed.method || "POST").toUpperCase();
    const url = parsed.url;
    const body = parsed.body;

    if (method !== "POST") {
      return {
        items: null,
        error: `Line ${index + 1} uses unsupported method ${method}; only POST is supported`,
      };
    }
    if (url !== batchEndpoint) {
      return {
        items: null,
        error: `Line ${index + 1} url ${String(url)} does not match batch endpoint ${batchEndpoint}`,
      };
    }
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      return { items: null, error: `Line ${index + 1} must include a JSON object body` };
    }

    items.push({
      body: body as Record<string, unknown>,
      customId: typeof parsed.custom_id === "string" ? parsed.custom_id : null,
      lineNumber: index + 1,
      method: "POST",
      url: batchEndpoint,
    });
  }

  return { items, error: null };
}

async function cleanupExpiredBatches(): Promise<void> {
  try {
    const now = Math.floor(Date.now() / 1_000);
    const batches = await getTerminalBatchesAsync();

    // Delete files for terminal batches that have exceeded their completion window
    for (const batch of batches) {
      const completionTime =
        batch.completedAt || batch.failedAt || batch.cancelledAt || batch.expiredAt;
      // Input files expire 30 days after batch completion
      const inputExpiresAt =
        completionTime && batch.inputFileId
          ? completionTime + DEFAULT_BATCH_EXPIRATION_SECONDS
          : null;
      const outputExpiresAt = getBatchOutputExpiresAt(batch);

      // #13681: skip the soft-delete when some OTHER batch still references
      // the same file id (e.g. one input file reused across batches) — a
      // terminal batch's own expiry must not null a file a sibling still
      // needs.
      if (
        batch.inputFileId &&
        inputExpiresAt &&
        now > inputExpiresAt &&
        !(await isFileReferencedByOtherBatchAsync(batch.inputFileId, [batch.id]))
      ) {
        await deleteFileAsync(batch.inputFileId);
      }
      if (
        batch.outputFileId &&
        outputExpiresAt &&
        now > outputExpiresAt &&
        !(await isFileReferencedByOtherBatchAsync(batch.outputFileId, [batch.id]))
      ) {
        await deleteFileAsync(batch.outputFileId);
      }
      if (
        batch.errorFileId &&
        outputExpiresAt &&
        now > outputExpiresAt &&
        !(await isFileReferencedByOtherBatchAsync(batch.errorFileId, [batch.id]))
      ) {
        await deleteFileAsync(batch.errorFileId);
      }
    }

    // Expire validating batches that have exceeded their completion window
    for (const batch of await getPendingBatchesAsync()) {
      if (batch.status === "validating") {
        const windowSeconds = parseBatchWindowSeconds(batch.completionWindow);
        if (now - batch.createdAt > windowSeconds) {
          await transitionBatchAsync(batch.id, ["validating"], {
            status: "expired",
            expiredAt: now,
          });
        }
      }
    }

    // Cleanup orphan files (batch-purpose files stuck in validating after 48h)
    // Use asc order so oldest files are processed first; use a high limit to avoid missing old orphans.
    const allFiles = await listFilesAsync({ order: "asc", limit: 100 });
    for (const file of allFiles) {
      if (file.purpose === "batch" && now - file.createdAt > DEFAULT_BATCH_EXPIRATION_SECONDS) {
        await deleteFileAsync(file.id);
      }
    }
  } catch (err) {
    console.error("[BATCH] Cleanup error:", err);
  }
}

async function startBatch(batch: any, identity: object | undefined): Promise<void> {
  console.log(`[BATCH] Starting batch ${batch.id}`);

  const contentOwner = identity ? acquireNativeFileConsumer() : undefined;
  try {
    const content = contentOwner
      ? await getFileContentForConsumerAsync(batch.inputFileId, contentOwner)
      : await getFileContentAsync(batch.inputFileId);
    if (!content) {
      await failBatch(batch.id, "Input file content not found");
      return;
    }

    try {
      const parsedItems = parseBatchItems(content, batch.endpoint);
      if (parsedItems.error || parsedItems.items === null) {
        // Set total count even on validation failure so UI shows correct numbers
        const lines = content
          .toString()
          .split("\n")
          .map((line) => line.trim())
          .filter(Boolean);
        await updateBatchAsync(batch.id, {
          requestCountsTotal: lines.length,
          requestCountsFailed: lines.length, // All failed due to validation error
        });
        await failBatch(batch.id, parsedItems.error);
        return;
      }
      const total = parsedItems.items.length;

      console.log(`[BATCH] Batch ${batch.id} contains (${total} items)`);

      await ensureBatchItemCheckpointsAsync(batch.id, parsedItems.items);

      const started = await transitionBatchAsync(batch.id, ["validating"], {
        status: "in_progress",
        inProgressAt: Math.floor(Date.now() / 1000),
        requestCountsTotal: total,
      });
      if (!started.changed) return;

      activeBatches.add(batch.id);

      // Admission owns the detached worker, failure persistence and underlying dispatches.
      const pendingDispatches = identity ? new Set<Promise<Response>>() : undefined;
      const work = runPersistenceBackgroundWork(async () => {
        assertAuthority(identity);
        try {
          await processBatchItems(batch, parsedItems.items, pendingDispatches);
        } catch (error) {
          console.error(`[BATCH] Critical error in processBatchItems for ${batch.id}:`, error);
          try {
            await failBatch(batch.id, String(error));
          } catch (storageError) {
            console.error(`[BATCH] Unable to persist failure for ${batch.id}:`, storageError);
          }
        } finally {
          // A deadline acknowledgement does not mean the underlying HTTP call has settled.
          while (pendingDispatches?.size) await Promise.allSettled([...pendingDispatches]);
        }
      });
      activeProcesses.add(work);
      const finished = () => {
        activeProcesses.delete(work);
        activeBatches.delete(batch.id);
      };
      void work.then(finished, (error: unknown) => {
        finished();
        console.error(`[BATCH] Background admission failed for ${batch.id}:`, error);
      });
    } catch (err) {
      console.error(`[BATCH] Error starting batch ${batch.id}:`, err);
      await failBatch(batch.id, err instanceof Error ? err.message : String(err));
    }
  } finally {
    contentOwner?.release();
  }
}

let prevHeaders: Headers | null = null;
let prevHeadersTimestamp: number = 0;
const HEADERS_CACHE_TTL_MS = 60_000;

async function processBatchItems(
  batch: BatchRecord,
  items: BatchRequestItem[],
  pendingDispatches?: Set<Promise<Response>>
): Promise<void> {
  const state = createBatchState(batch);
  const checkpoints = new Map<number, BatchItemCheckpoint>(
    (await listBatchItemCheckpointsAsync(batch.id)).map((checkpoint) => [
      checkpoint.lineNumber,
      checkpoint,
    ])
  );

  const apiKey = await resolveApiKey(batch);

  for (const item of items) {
    if (await isBatchCancelled(batch.id)) break;

    const checkpoint = checkpoints.get(item.lineNumber);
    if (checkpoint && (await applyRecoveredCheckpoint(batch.id, item, checkpoint, state))) {
      await maybePersistProgress(batch.id, state);
      continue;
    }

    const cachedHeaders =
      prevHeaders && Date.now() - prevHeadersTimestamp < HEADERS_CACHE_TTL_MS ? prevHeaders : null;
    if (cachedHeaders) {
      const delay = maybeThrottle(cachedHeaders);
      if (delay) {
        await sleep(delay);
      }
    }

    await markBatchItemProcessingAsync(batch.id, item);

    try {
      const response = await processSingleItemWithRetry(item, apiKey, pendingDispatches);
      let responseBody: unknown;
      try {
        responseBody = await response.clone().json();
      } catch {
        responseBody = await response.text();
      }

      // Record the item's result so finalization can emit output/error files.
      // The output file format expects entries like:
      // { id, custom_id, response: { status_code, body } }
      const wrapped = {
        id: `req_${uuidv4().replaceAll("-", "")}`,
        custom_id: item.customId ?? null,
        response: {
          status_code: response.status,
          body: responseBody,
        },
      };

      await markBatchItemResultAsync(batch.id, item, wrapped);
      state.results.push(wrapped);
      applyItemResult(state, response.status, responseBody);
      prevHeaders = response.headers;
      prevHeadersTimestamp = Date.now();
    } catch (exception) {
      if (exception instanceof PostgresError) throw exception;
      // Track processing-level errors separately (items that failed to be processed)
      const error = { custom_id: item.customId ?? null, error: String(exception) };
      await markBatchItemErrorAsync(batch.id, item, error);
      state.errors.push(error);
      state.failed++;
      prevHeaders = null;
      prevHeadersTimestamp = 0;
    }

    await maybePersistProgress(batch.id, state);
  }

  return finalizeBatch(batch.id, state.results, state.errors);
}

async function applyRecoveredCheckpoint(
  batchId: string,
  item: BatchRequestItem,
  checkpoint: BatchItemCheckpoint,
  state: ReturnType<typeof createBatchState>
): Promise<boolean> {
  if (checkpoint.status === "completed" && checkpoint.result) {
    state.results.push(checkpoint.result);
    applyItemResult(
      state,
      checkpoint.result.response?.status_code ?? 500,
      checkpoint.result.response?.body
    );
    return true;
  }

  if (checkpoint.status === "errored" && checkpoint.error) {
    state.errors.push(checkpoint.error);
    state.failed++;
    return true;
  }

  if (checkpoint.status === "processing") {
    const error = {
      custom_id: item.customId ?? null,
      error:
        "Batch item was interrupted before its provider response was recorded; it was not replayed to avoid duplicate provider work.",
    };
    await markBatchItemErrorAsync(batchId, item, error);
    state.errors.push(error);
    state.failed++;
    return true;
  }

  return false;
}

async function isBatchCancelled(batchId: string): Promise<boolean> {
  const current = await getBatchAsync(batchId);

  return !current || current.status === "cancelling" || current.status === "cancelled";
}

async function resolveApiKey(batch: BatchRecord): Promise<any> {
  const apiKeyRow = batch.apiKeyId ? await getApiKeyById(batch.apiKeyId) : null;
  return resolveBatchApiKeyValue(batch, apiKeyRow);
}

async function processSingleItemWithRetry(
  item: BatchRequestItem,
  apiKey: string,
  pendingDispatches?: Set<Promise<Response>>
) {
  // Time-based retry limit: individual batch items can retry for up to 24 hours.
  // This accommodates large batches against heavily rate-limited providers.
  const MAX_RETRY_DURATION_MS = BATCH_RETRY_DURATION_MS;
  const maxRetries = 200; // safety ceiling — time limit should kick in first
  const retryStartedAt = Date.now();

  let response: Response | null = null;
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    // If the previous attempt got a response, check for rate limit headers and throttle if needed before retrying.
    if (response) {
      const delay = maybeThrottle(response.headers);
      if (delay) {
        await sleep(delay);
      }
    }

    response = await processSingleItem(item, apiKey, pendingDispatches);

    if (
      (response.status === 429 || response.status === 502 || response.status === 504) &&
      attempt < maxRetries
    ) {
      // Bail if we've been retrying for longer than the allowed window
      if (Date.now() - retryStartedAt >= MAX_RETRY_DURATION_MS) {
        console.warn(
          `[BATCH] Item ${item.customId ?? "(no id)"} exceeded 24h retry window after ${attempt} attempts — giving up`
        );
        return response;
      }
      const delay = getRetryDelayMs(response.headers) ?? getBackoffDelayMs(attempt);
      await sleep(delay);
      continue;
    }

    return response;
  }
  if (!response) throw new Error("Batch item did not receive a response");
  return response;
}

// G10 (silent-stop fix): individual batch-item dispatches can hang indefinitely
// if the upstream route stalls (no signal/timeout plumbed through). Bound each
// item with a wall-clock timeout so a stuck item fails fast (recorded as an item
// error) instead of freezing the item loop. Native workers retain ownership
// of the underlying dispatch until it settles, including after this deadline.
export const BATCH_ITEM_DISPATCH_TIMEOUT_MS = 120_000;

/**
 * G10: race a promise against a wall-clock deadline. Exported for unit testing
 * (batch dispatch is a module-internal import, so the timeout mechanism itself
 * is verified directly here).
 */
export function withItemDispatchTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
      timeoutMs
    );
  });
  return Promise.race([promise, timeoutPromise]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

async function processSingleItem(
  item: BatchRequestItem,
  apiKey: string,
  pendingDispatches?: Set<Promise<Response>>
) {
  const body = buildRequestBody(item);
  const request = dispatch.dispatchBatchApiRequest({ endpoint: item.url, body, apiKey });
  if (pendingDispatches) {
    pendingDispatches.add(request);
    void request.then(
      () => pendingDispatches.delete(request),
      () => pendingDispatches.delete(request)
    );
  }
  return withItemDispatchTimeout(
    request,
    BATCH_ITEM_DISPATCH_TIMEOUT_MS,
    `Batch item dispatch (${item.url})`
  );
}

export function buildRequestBody(item: BatchRequestItem) {
  const isChatEndpoint = ![
    "/v1/embeddings",
    "/v1/moderations",
    "/v1/images/generations",
    "/v1/images/edits",
    "/v1/videos",
    "/v1/videos/generations",
  ].includes(item.url);

  return {
    ...item.body,
    ...(isChatEndpoint ? { stream: false } : {}),
  };
}

function getBackoffDelayMs(attempt: number) {
  const baseMs = BATCH_BACKOFF_BASE_MS;
  const maxMs = BATCH_BACKOFF_MAX_MS;

  // exponential: 2^attempt * base
  const exp = Math.min(maxMs, baseMs * 2 ** attempt);

  // jitter ±20%
  const jitterFactor = 1 + (Math.random() * 0.4 - 0.2);

  return Math.floor(exp * jitterFactor);
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}
function getRetryDelayMs(headers: Headers): number | null {
  const retryAfter = headers.get("retry-after");
  if (retryAfter) {
    const seconds = Number(retryAfter);
    if (!Number.isNaN(seconds)) {
      return seconds * 1_000;
    }

    // fallback: HTTP-date
    const date = new Date(retryAfter).getTime();
    if (!Number.isNaN(date)) {
      return Math.max(0, date - Date.now());
    }
  }

  return null;
}

export function maybeThrottle(headers: Headers): number | null {
  // Mistral reports these headers from their API
  const remainingReq = toNumber(headers.get("x-ratelimit-remaining-req-minute"));
  const limitReq = toNumber(headers.get("x-ratelimit-limit-req-minute"));

  const remainingTokens = toNumber(headers.get("x-ratelimit-remaining-tokens-minute"));
  const cost = toNumber(headers.get("x-ratelimit-tokens-query-cost"));

  let pressures: number[] = [];

  // Request pressureRemaining
  if (remainingReq !== null && limitReq !== null) {
    if (limitReq > 0) {
      pressures.push(remainingReq / limitReq);
    }
  }

  // Token pressureRemaining
  if (remainingTokens !== null && cost !== null) {
    if (remainingTokens + cost > 0) {
      pressures.push(remainingTokens / (remainingTokens + cost));
    }
  }

  if (pressures.length === 0) {
    console.log("[BATCH] Throttle check - no rate-limit headers present");
    return null;
  } else {
    const tokenTotal = remainingTokens != null && cost != null ? remainingTokens + cost : null;
    console.log(
      `[BATCH] Throttle check - Request pressure: ${remainingReq ?? "n/a"}/${limitReq ?? "n/a"}, Token pressure: ${remainingTokens ?? "n/a"}/${tokenTotal ?? "n/a"}`
    );
  }

  const pressureRemaining = Math.min(...pressures);

  const delay = throttleDelay(pressureRemaining);
  if (delay !== null) {
    console.log(
      `[BATCH] Throttling next request with delay of ${Math.round(delay)}ms (pressure remaining: ${(pressureRemaining * 100).toFixed(2)}%)`
    );
  }
  return delay;
}

function throttleDelay(pressure: number): number | null {
  if (pressure >= 0.2) return null;

  const severity = (0.2 - pressure) / 0.2;

  const delay = Math.pow(severity, 2) * 30_000;

  return 200 + delay + Math.random() * 1000;
}

const toNumber = (v: string | null) => {
  if (v === null || v.trim() === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

function createBatchState(batch: BatchRecord) {
  return {
    results: [] as Array<Record<string, unknown>>,
    errors: [] as Array<Record<string, unknown>>,
    completed: 0,
    failed: 0,
    tokens: {
      input: 0,
      output: 0,
      reasoning: 0,
    },
    model: batch.model || null,
  };
}

function applyItemResult(state: any, statusCode: number, body: any): void {
  if (statusCode >= 400 || body?.error) {
    state.failed++;
  } else {
    state.completed++;

    if (body?.usage) {
      state.tokens.input += body.usage.prompt_tokens || body.usage.input_tokens || 0;
      state.tokens.output += body.usage.completion_tokens || body.usage.output_tokens || 0;
      state.tokens.reasoning += body.usage.completion_tokens_details?.reasoning_tokens || 0;
    }

    if (!state.model && body?.model) {
      state.model = body.model;
    }
  }
}

async function maybePersistProgress(batchId: string, state: any): Promise<void> {
  // Persist basic progress (completed/failed counts + model) on every item so
  // the UI can show up-to-date progress even for small batches.
  try {
    await updateBatchAsync(batchId, {
      requestCountsCompleted: state.completed,
      requestCountsFailed: state.failed,
      model: state.model,
    });
  } catch (err) {
    console.error(`[BATCH] Failed to persist progress for ${batchId}:`, err);
  }

  // Persist richer usage/model info less frequently to avoid excessive writes.
  const total = state.completed + state.failed;
  if (total % 50 !== 0) return;

  try {
    await updateBatchAsync(batchId, {
      requestCountsCompleted: state.completed,
      requestCountsFailed: state.failed,
      model: state.model,
      usage: {
        input_tokens: state.tokens.input,
        output_tokens: state.tokens.output,
        total_tokens: state.tokens.input + state.tokens.output,
        input_tokens_details: { cached_tokens: 0 },
        output_tokens_details: { reasoning_tokens: state.tokens.reasoning },
      },
    });
  } catch (err) {
    console.error(`[BATCH] Failed to persist extended progress for ${batchId}:`, err);
  }
}

async function finalizeBatch(
  batchId: string,
  results: any[],
  itemsWithErrors: any[]
): Promise<void> {
  const begun = await transitionBatchAsync(batchId, ["in_progress"], {
    status: "finalizing",
    finalizingAt: now(),
  });
  if (!begun.changed || !begun.batch) {
    if (begun.batch?.status === "cancelling") await cancelBatch(begun.batch);
    return;
  }
  const current = begun.batch;

  // Compute counts from results
  const successes = results.filter(
    (r) =>
      typeof r.response?.status_code === "number" &&
      r.response.status_code < 400 &&
      !r.response.body?.error
  );
  const failuresFromResults = results.filter(
    (r) =>
      (typeof r.response?.status_code === "number" && r.response.status_code >= 400) ||
      r.response?.body?.error
  );
  const processingErrors = itemsWithErrors || [];

  const completedCount = successes.length;
  const failedCount = failuresFromResults.length + processingErrors.length;
  const totalCount = current?.requestCountsTotal || completedCount + failedCount;

  // Aggregate usage from per-item responses when available
  let inputTokens = 0;
  let outputTokens = 0;
  let reasoningTokens = 0;

  for (const r of results) {
    try {
      const body = r.response?.body;
      if (!body) continue;
      const usage = body.usage || {};
      inputTokens += usage.prompt_tokens ?? usage.input_tokens ?? usage.total_tokens ?? 0;
      outputTokens += usage.completion_tokens ?? 0;
      reasoningTokens += usage.completion_tokens_details?.reasoning_tokens ?? 0;
    } catch (err) {
      console.error("Failed to aggregate usage for batch", batchId, err);
    }
  }

  const model =
    results.find((r) => r.response?.body?.model)?.response?.body?.model || current?.model || null;

  const completionTime = now();

  const completion = {
    requestCountsTotal: totalCount,
    requestCountsCompleted: completedCount,
    requestCountsFailed: failedCount,
    model,
    usage: {
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      total_tokens: inputTokens + outputTokens,
      input_tokens_details: { cached_tokens: 0 },
      output_tokens_details: { reasoning_tokens: reasoningTokens },
    },
    completedAt: completionTime,
  };
  // Serialize outside SQL; only the final transaction allocates durable result files.
  const outputContent = successFileContent(results);
  const errorContent = errorFileContent(results, itemsWithErrors);
  const contentOwner =
    parseDatabaseBackend() === "postgres" ? acquireNativeFileConsumer() : undefined;
  try {
    const bytes = [outputContent, errorContent].reduce(
      (total, content) => total + (content === null ? 0 : Buffer.byteLength(content)),
      0
    );
    contentOwner?.beforeBuffer(bytes);
    const expiresAt = getBatchOutputExpiresAt({ ...current, completedAt: completionTime });
    const file = (kind: string, content: string | null): BatchOutputFile | null =>
      content === null
        ? null
        : { filename: `batch_${batchId}_${kind}.jsonl`, content: Buffer.from(content), expiresAt };
    // A lost COMMIT reply is not permission to delete or recreate any file. The SQL
    // transaction either retained every result/link, or rolled all of them back.
    const completed = await completeBatchWithFilesAsync(
      batchId,
      completion,
      file("output", outputContent),
      file("error", errorContent)
    );
    if (completed.changed && completed.batch?.status === "completed") {
      console.log(
        `[BATCH] Completed batch ${batchId} (${completed.batch.requestCountsTotal} items)`
      );
    }
  } finally {
    contentOwner?.release();
  }
}

function now(): number {
  return Math.floor(Date.now() / 1_000);
}

function successFileContent(results: any[]): string | null {
  const successes = results.filter((r) => r.response.status_code < 400 && !r.response.body?.error);
  return successes.length ? toJsonl(successes) : null;
}

function errorFileContent(results: any[], itemsWithErrors: any[]): string | null {
  const failures = results.filter((r) => r.response.status_code >= 400 || r.response.body?.error);
  const processErrors = itemsWithErrors.map((e) => ({
    id: `batch_req_${uuidv4().replaceAll("-", "")}`,
    custom_id: e.custom_id,
    response: null,
    error: { message: e.error, type: "batch_process_error" },
  }));
  const allFailures = [...failures, ...processErrors];
  return allFailures.length ? toJsonl(allFailures) : null;
}

function toJsonl(items: any[]): string {
  return items.map((i) => JSON.stringify(i)).join("\n");
}

async function cancelBatch(batch: any): Promise<void> {
  const cancelled = await transitionBatchAsync(batch.id, ["cancelling"], {
    status: "cancelled",
    cancelledAt: Math.floor(Date.now() / 1000),
  });
  if (cancelled.changed) console.log(`[BATCH] Cancelled batch ${batch.id}`);
}

async function failBatch(batchId: string, reason: string): Promise<void> {
  const failed = await transitionBatchAsync(batchId, ["validating", "in_progress", "finalizing"], {
    status: "failed",
    failedAt: Math.floor(Date.now() / 1000),
    errors: [{ message: reason }],
  });
  if (failed.batch?.status === "cancelling") await cancelBatch(failed.batch);
  activeBatches.delete(batchId);
}

export async function waitForAllBatches(): Promise<void> {
  // Polls can enqueue a batch after drain starts; resnapshot until both sets are empty.
  while (processor.activePolls.size || activeProcesses.size) {
    await Promise.allSettled([...processor.activePolls, ...activeProcesses]);
  }
}

// Test helpers
export function getCachedHeaders(): { headers: Headers | null; timestamp: number } {
  return { headers: prevHeaders, timestamp: prevHeadersTimestamp };
}
export function resetCachedHeaders(): void {
  prevHeaders = null;
  prevHeadersTimestamp = 0;
}
export function resetBatchProcessorState(): void {
  activeBatches.clear();
  activeProcesses.clear();
  processor.processing = false;
  prevHeaders = null;
  prevHeadersTimestamp = 0;
}
