import { NativeProxyLogBuffer, type ProxyLogBufferStatus } from "./proxyLogBuffer";
import { updateAttemptTimingAsync } from "./db/proxyLogs";
import { parseDatabaseBackend } from "./db/backendSelection";
import { runPersistenceBackgroundWork } from "./db/persistenceRuntime";
import { getSelectedProxyLogStore } from "./db/postgres/proxyLogStore";
/**
 * Proxy Logger — Hybrid in-memory + SQLite persistence
 *
 * Keeps a fast in-memory ring buffer for real-time dashboard AND
 * persists to SQLite so logs survive server restarts.
 *
 * Pattern follows callLogs.js (T-15 decomposition).
 */
import { v4 as uuidv4 } from "uuid";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/errorSanitization.ts";
import { sanitizeTimingMs } from "@omniroute/open-sse/utils/timingMs.ts";
import { getDbInstance, isCloud, isBuildPhase } from "./db/core";
import { ensureProxyLogsColumns } from "./db/schemaColumns";
import { normalizeProxyHostForLog } from "./proxyLogHost";

// Re-exported for existing callers; the helper lives in a zero-import leaf so DB modules
// can use it without loading this module (which hydrates from SQLite at import time).
export { normalizeProxyHostForLog };

const shouldPersistToDisk = !isCloud && !isBuildPhase;

const MAX_IN_MEMORY_ENTRIES = 200;

interface ProxyInfo {
  type: string;
  host: string;
  port: number | string;
  /** Registry name (e.g. `murphy-eu-fr`) — carried by registry resolution so the
   *  proxy log can identify a leg even when many entries share host:port. */
  name?: string;
}

export interface ProxyLogEntry {
  id: string;
  timestamp: string;
  status: string;
  proxy: ProxyInfo | null;
  level: string;
  levelId: string | null;
  provider: string | null;
  targetUrl: string | null;
  clientIp: string | null;
  /** Outbound/egress IP the upstream actually saw (null until probed). The
   * historical clientIp is the INBOUND IP (x-forwarded-for); egressIp answers
   * "by which IP is this account leaving" — critical for rotating providers. */
  egressIp: string | null;
  latencyMs: number;
  error: string | null;
  connectionId: string | null;
  comboId: string | null;
  // `account` is the configured connection id prefix (connectionId slice);
  // `rotationAccount` is the masked id of the rotation account that served the
  // request (multi-account anonymous rotation only, null otherwise). Both stay
  // masked prefixes — never a full account id.
  account: string | null;
  /** Masked serving-account id of the rotation executor (null unless set). */
  rotationAccount: string | null;
  /** Request correlation id shared with call_logs (null unless set). */
  correlationId: string | null;
  tlsFingerprint: boolean;
  /** HTTP status the provider actually returned; null when no response was received. */
  upstreamStatus: number | null;
  /** 1-based position of this row within its request journal; null for unjournaled rows. */
  attemptNumber: number | null;
  /** Outcome of this send within its request journal; null for unjournaled rows. */
  attemptIssue: "served" | "abandoned" | null;
  /** Send start -> response headers received; null when unknown (network throw). */
  headersMs: number | null;
  /** Send start -> first useful body byte; null until the byte arrives. */
  firstChunkMs: number | null;
}

type ProxyLogInput = Partial<ProxyLogEntry> & {
  publicIp?: string | null;
};

interface ProxyLogFilters {
  status?: string;
  type?: string;
  provider?: string;
  level?: string;
  search?: string;
  limit?: number;
}

const proxyLogs: ProxyLogEntry[] = [];

// `public_ip` is the historical SQLite column name; API/UI expose the value as clientIp.

// ──────────────── Startup: hydrate from DB ────────────────

function loadFromDb() {
  if (parseDatabaseBackend() === "postgres") return;
  if (!shouldPersistToDisk) return;
  try {
    const db = getDbInstance();
    // Self-heal the proxy_logs schema before reading/writing (migration 134
    // guarantees egress_ip on every migrated DB; this covers restored/odd states).
    ensureProxyLogsColumns(db);
    const rows = db
      .prepare("SELECT * FROM proxy_logs ORDER BY timestamp DESC LIMIT ?")
      .all(MAX_IN_MEMORY_ENTRIES) as any[];

    for (const row of rows) {
      proxyLogs.push(mapProxyLogRow(row));
    }

    if (proxyLogs.length > 0) {
      console.log(`[proxyLogger] Loaded ${proxyLogs.length} proxy logs from SQLite`);
    }
  } catch (err: any) {
    console.warn(
      "[proxyLogger] Failed to load from DB:",
      sanitizeErrorMessage(err) || "Proxy log hydration failed"
    );
  }
}

loadFromDb();

// Default-off override that restores the verbose [ProxyEgress] console line (raw
// client/egress IPs + account prefix). Kept OFF by default so the process log leaks
// neither IPs nor the account prefix. Deliberately NOT coupled to debugMode
// (src/lib/db/settings.ts defaults debugMode to true) — this verbosity is opt-in only.
// Storage (in-memory ring buffer + SQLite) is untouched and always keeps full IPs.

/** Read at call time so tests can toggle it between imports. */
export function isProxyLogIncludeIps(): boolean {
  return process.env.PROXY_LOG_INCLUDE_IPS === "true" || process.env.PROXY_LOG_INCLUDE_IPS === "1";
}

/**
 * Pure formatter for the [ProxyEgress] process-log line (#10348). At the default level it
 * emits a short, IP/prefix-free summary; when details are opted in it restores the full
 * verbose line including client/egress IPs and the account. Extracted as a separate
 * function so it is unit-testable without patching console.log and so the change never
 * grows logProxyEvent itself.
 */
export function formatProxyEgressConsoleLine(params: {
  provider: string | null;
  account: string | null;
  clientIp: string | null;
  egressIp: string | null;
  level: string;
  proxyHost: string | null | undefined;
  proxyName?: string | null | undefined;
  status: string;
  includeDetails?: boolean;
}): string {
  const provider = params.provider || "-";
  const status = params.status;
  if (!params.includeDetails) {
    return `[ProxyEgress] ${provider} status=${status}`;
  }
  const proxy = params.proxyHost ? `:${params.proxyHost}` : "";
  const name = params.proxyName ? ` name=${params.proxyName}` : "";
  return (
    `[ProxyEgress] ${provider}/${params.account || "-"} ` +
    `in=${params.clientIp || "?"} out=${params.egressIp || "?"} ` +
    `proxy=${params.level}${proxy}${name} status=${status}`
  );
}

// ──────────────── Log a proxy event ────────────────

function clipNativeProxyField(value: string, limit?: number): string;
function clipNativeProxyField(value: string | undefined, limit?: number): string | undefined;
function clipNativeProxyField(
  value: string | null | undefined,
  limit?: number
): string | null | undefined;
function clipNativeProxyField(
  value: string | null | undefined,
  limit = 512
): string | null | undefined {
  return typeof value === "string" ? value.slice(0, limit).replace(/\u0000/g, "") : value;
}

const NATIVE_PROXY_IDENTITY_CHAR_LIMIT = 4096;

export function hasOversizedNativeProxyIdentity(entry: ProxyLogInput): boolean {
  return [
    entry.levelId,
    entry.provider,
    entry.connectionId,
    entry.comboId,
    entry.account,
    entry.rotationAccount,
    entry.correlationId,
    entry.proxy?.host,
  ].some((value) => typeof value === "string" && value.length > NATIVE_PROXY_IDENTITY_CHAR_LIMIT);
}

function nativeTiming(value: unknown): number | null {
  const timing = sanitizeTimingMs(value);
  return timing !== null && Number.isSafeInteger(timing) ? timing : null;
}

export function projectNativeProxyInput(entry: ProxyLogInput): ProxyLogInput {
  const port =
    typeof entry.proxy?.port === "string"
      ? Number(entry.proxy.port.slice(0, 16))
      : entry.proxy?.port;
  return {
    status: clipNativeProxyField(entry.status, 64),
    proxy: entry.proxy
      ? {
          type: clipNativeProxyField(entry.proxy.type, 64),
          host: clipNativeProxyField(entry.proxy.host, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
          port:
            typeof port === "number" && Number.isInteger(port) && port >= 1 && port <= 65535
              ? port
              : 0,
          name: clipNativeProxyField(entry.proxy.name, 256),
        }
      : null,
    level: clipNativeProxyField(entry.level, 64),
    levelId: clipNativeProxyField(entry.levelId, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    provider: clipNativeProxyField(entry.provider, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    targetUrl: clipNativeProxyField(entry.targetUrl, 2048),
    clientIp: clipNativeProxyField(entry.clientIp, 64),
    publicIp: clipNativeProxyField(entry.publicIp, 64),
    egressIp: clipNativeProxyField(entry.egressIp, 64),
    error: clipNativeProxyField(entry.error, 1024),
    connectionId: clipNativeProxyField(entry.connectionId, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    comboId: clipNativeProxyField(entry.comboId, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    account: clipNativeProxyField(entry.account, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    rotationAccount: clipNativeProxyField(entry.rotationAccount, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    correlationId: clipNativeProxyField(entry.correlationId, NATIVE_PROXY_IDENTITY_CHAR_LIMIT),
    latencyMs:
      typeof entry.latencyMs === "number" &&
      Number.isFinite(entry.latencyMs) &&
      Math.abs(entry.latencyMs) <= Number.MAX_SAFE_INTEGER
        ? entry.latencyMs
        : 0,
    tlsFingerprint: entry.tlsFingerprint,
    upstreamStatus:
      typeof entry.upstreamStatus === "number" &&
      Number.isInteger(entry.upstreamStatus) &&
      entry.upstreamStatus >= 100 &&
      entry.upstreamStatus <= 599
        ? entry.upstreamStatus
        : null,
    attemptNumber:
      typeof entry.attemptNumber === "number" &&
      Number.isSafeInteger(entry.attemptNumber) &&
      entry.attemptNumber >= 1
        ? entry.attemptNumber
        : null,
    attemptIssue: entry.attemptIssue,
    headersMs: nativeTiming(entry.headersMs),
    firstChunkMs: nativeTiming(entry.firstChunkMs),
  };
}

export function logProxyEvent(entry: ProxyLogInput) {
  const nativePersistence = parseDatabaseBackend() === "postgres";
  const oversizedIdentity = nativePersistence && hasOversizedNativeProxyIdentity(entry);
  if (nativePersistence) entry = projectNativeProxyInput(entry);
  const safeError =
    entry.error === null || entry.error === undefined || entry.error === ""
      ? null
      : sanitizeErrorMessage(entry.error) || "Proxy request failed";
  const log: ProxyLogEntry = {
    id: uuidv4(),
    timestamp: new Date().toISOString(),
    status: entry.status || "success",
    proxy: entry.proxy
      ? {
          ...entry.proxy,
          host: normalizeProxyHostForLog(entry.proxy.host) ?? entry.proxy.host,
        }
      : null,
    level: entry.level || "direct",
    levelId: entry.levelId || null,
    provider: entry.provider || null,
    targetUrl: entry.targetUrl || null,
    clientIp: entry.clientIp ?? entry.publicIp ?? null,
    egressIp: entry.egressIp ?? null,
    latencyMs: entry.latencyMs || 0,
    error: safeError,
    connectionId: entry.connectionId || null,
    comboId: entry.comboId || null,
    account: entry.account || null,
    rotationAccount: entry.rotationAccount || null,
    correlationId: entry.correlationId || null,
    tlsFingerprint: entry.tlsFingerprint || false,
    upstreamStatus: entry.upstreamStatus ?? null,
    attemptNumber:
      typeof entry.attemptNumber === "number" && Number.isInteger(entry.attemptNumber)
        ? entry.attemptNumber
        : null,
    attemptIssue:
      entry.attemptIssue === "served" || entry.attemptIssue === "abandoned"
        ? entry.attemptIssue
        : null,
    headersMs: sanitizeTimingMs(entry.headersMs),
    firstChunkMs: sanitizeTimingMs(entry.firstChunkMs),
  };

  // Structured egress line so the operator can confirm, in the proxy logs, which
  // IP each account is entering (clientIp) and leaving (egressIp) by.
  if (log.proxy || log.egressIp) {
    console.log(
      formatProxyEgressConsoleLine({
        provider: log.provider,
        account: log.account,
        clientIp: log.clientIp,
        egressIp: log.egressIp,
        level: log.level,
        proxyHost: log.proxy?.host,
        proxyName: log.proxy?.name,
        status: log.status,
        includeDetails: isProxyLogIncludeIps(),
      })
    );
  }

  // 1. In-memory ring buffer (newest first)
  proxyLogs.unshift(log);
  if (proxyLogs.length > MAX_IN_MEMORY_ENTRIES) {
    proxyLogs.length = MAX_IN_MEMORY_ENTRIES;
  }

  // 2. Queue for background batch persistence (SQLite / Redis)
  if (shouldPersistToDisk || (nativePersistence && !isBuildPhase)) {
    enqueueProxyLog(log, oversizedIdentity);
  }

  return log;
}

// ──────────────── Background Batch Persistence ────────────────

const BATCH_FLUSH_INTERVAL_MS = 1000;
const BATCH_SIZE_THRESHOLD = 100;

let pendingLogsQueue: ProxyLogEntry[] = [];
const nativeProxyLogBuffer = new NativeProxyLogBuffer();

/** Process-local diagnostics; unavailable on SQLite, and never a financial accounting counter. */
export function getProxyLogBufferStatus(): ProxyLogBufferStatus | undefined {
  return parseDatabaseBackend() === "postgres" ? nativeProxyLogBuffer.status() : undefined;
}
let batchTimer: NodeJS.Timeout | null = null;
let nativeProxyFlushScheduled = false;

function scheduleProxyLogFlush(): void {
  if (parseDatabaseBackend() !== "postgres") {
    void runPersistenceBackgroundWork(flushProxyLogsAsync).catch(reportProxyFlushFailure);
    return;
  }
  if (nativeProxyFlushScheduled || !nativeProxyLogBuffer.canScheduleFlush(Date.now())) return;
  nativeProxyFlushScheduled = true;
  void runPersistenceBackgroundWork(flushProxyLogsAsync)
    .catch(reportProxyFlushFailure)
    .finally(() => {
      nativeProxyFlushScheduled = false;
    });
}

function ensureBatchTimer() {
  if (batchTimer) return;
  batchTimer = setInterval(scheduleProxyLogFlush, BATCH_FLUSH_INTERVAL_MS);
  if (typeof batchTimer.unref === "function") {
    batchTimer.unref();
  }
}

function enqueueProxyLog(log: ProxyLogEntry, oversizedIdentity = false) {
  if (parseDatabaseBackend() === "postgres") {
    // Projection above bounds serialization work; reservations include batches awaiting COMMIT.
    const bytes = oversizedIdentity
      ? nativeProxyLogBuffer.status().eventByteLimit + 1
      : Buffer.byteLength(JSON.stringify(log), "utf8") + 32;
    if (!nativeProxyLogBuffer.reserve(log.id, bytes)) {
      if (nativeProxyLogBuffer.warningDue(Date.now())) {
        const status = nativeProxyLogBuffer.status();
        console.warn(
          "[proxyLogger] Native telemetry capacity reached; newest diagnostic events were dropped",
          {
            bufferedRows: status.bufferedRows,
            bufferedBytes: status.bufferedBytes,
            droppedEvents: status.droppedEvents,
            oversizedEvents: status.oversizedEvents,
          }
        );
      }
      return;
    }
  }
  pendingLogsQueue.push(log);
  ensureBatchTimer();
  if (pendingLogsQueue.length >= BATCH_SIZE_THRESHOLD) {
    scheduleProxyLogFlush();
  }
}

export function flushProxyLogsSync() {
  if (getSelectedProxyLogStore()) throw new Error("PostgreSQL proxy logs require an awaited flush");
  if (pendingLogsQueue.length === 0) return;
  const batch = pendingLogsQueue;
  pendingLogsQueue = [];

  // 1. If Redis driver is active, asynchronously publish batch to Redis Stream/Channel
  if (process.env.QUOTA_STORE_DRIVER === "redis" || process.env.QUOTA_STORE_REDIS_URL) {
    try {
      import("@/lib/quota/redisQuotaStore")
        .then(({ getRedisQuotaStore }) => {
          const store = getRedisQuotaStore(process.env.QUOTA_STORE_REDIS_URL || "");
          const client = (store as any)?.client;
          if (client && typeof client.publish === "function") {
            for (const entry of batch) {
              client.publish("omniroute:proxy_logs", JSON.stringify(entry)).catch(() => {});
            }
          }
        })
        .catch(() => {});
    } catch {
      /* ignore redis pub errors */
    }
  }

  // 2. Persist to SQLite using a single transaction for high-performance non-blocking write
  try {
    const db = getDbInstance();
    const insertStmt = db.prepare(
      `INSERT INTO proxy_logs (id, timestamp, status, proxy_type, proxy_host, proxy_port, proxy_name,
        level, level_id, provider, target_url, public_ip, egress_ip, latency_ms, error,
        connection_id, combo_id, account, rotation_account, correlation_id, tls_fingerprint, upstream_status,
        attempt_number, attempt_issue, headers_ms, first_chunk_ms)
      VALUES (@id, @timestamp, @status, @proxyType, @proxyHost, @proxyPort, @proxyName,
        @level, @levelId, @provider, @targetUrl, @clientIp, @egressIp, @latencyMs, @error,
        @connectionId, @comboId, @account, @rotationAccount, @correlationId, @tlsFingerprint, @upstreamStatus,
        @attemptNumber, @attemptIssue, @headersMs, @firstChunkMs)`
    );

    const transaction = db.transaction((entries: ProxyLogEntry[]) => {
      for (const item of entries) {
        insertStmt.run({
          id: item.id,
          timestamp: item.timestamp,
          status: item.status,
          proxyType: item.proxy?.type || null,
          proxyHost: item.proxy?.host || null,
          proxyPort: item.proxy?.port ? Number(item.proxy.port) : null,
          proxyName: item.proxy?.name || null,
          level: item.level,
          levelId: item.levelId,
          provider: item.provider,
          targetUrl: item.targetUrl,
          clientIp: item.clientIp,
          egressIp: item.egressIp,
          latencyMs: item.latencyMs,
          error: item.error,
          connectionId: item.connectionId,
          comboId: item.comboId,
          account: item.account,
          rotationAccount: item.rotationAccount,
          correlationId: item.correlationId,
          tlsFingerprint: item.tlsFingerprint ? 1 : 0,
          upstreamStatus: item.upstreamStatus,
          attemptNumber: item.attemptNumber,
          attemptIssue: item.attemptIssue,
          headersMs: item.headersMs,
          firstChunkMs: item.firstChunkMs,
        });
      }
    });

    transaction(batch);
  } catch (err: any) {
    console.warn(
      "[proxyLogger] Failed to write proxy log batch to disk:",
      sanitizeErrorMessage(err) || "Proxy log persistence failed"
    );
  }
}

// ──────────────── Deferred timing patch ────────────────

// Bounded join key for late first-chunk arrivals: log id -> queued entry ref.
// Registered only when the first byte is still unknown at journal time; every
// entry leaves through exactly one path below (notify, settle-without-byte,
// cancel/error, cap eviction, clear). Cap mirrors the ring buffer so the
// registry never retains more than memory already does.
export const TIMING_LINK_CAP = 200;
const pendingFirstChunk = new Map<string, ProxyLogEntry>();

function evictOldestTimingLink(): void {
  const oldest = pendingFirstChunk.keys().next();
  if (!oldest.done) pendingFirstChunk.delete(oldest.value);
}

/**
 * Totest seam: current registry size (bounded by TIMING_LINK_CAP).
 */
export function pendingFirstChunkSizeForTests(): number {
  return pendingFirstChunk.size;
}

/**
 * Link a journaled row to its still-open upstream body. Called by the journal
 * layer right after logProxyEvent returns the entry, when the first byte has
 * not arrived yet. No-ops (no entry) when the timing is already known or when
 * there is no body to wait for.
 */
export function linkPendingFirstChunk(
  id: string,
  entry: ProxyLogEntry,
  timingKnown: boolean,
  hasBody: boolean
): void {
  if (timingKnown || !hasBody) return;
  if (parseDatabaseBackend() === "postgres" && !nativeProxyLogBuffer.has(id)) return;
  if (pendingFirstChunk.size >= TIMING_LINK_CAP) evictOldestTimingLink();
  pendingFirstChunk.set(id, entry);
}

function dropPendingFirstChunk(id: string): void {
  pendingFirstChunk.delete(id);
}

/**
 * Settle a linked row once the first useful body byte arrives (or never does).
 * Before the batch flush the queued object is mutated in place so the INSERT
 * carries the value; after the flush the row is patched by id and the
 * in-memory copy is updated. Every path drops the registry entry.
 * The patch callback keeps this module decoupled from the db writer: the
 * journal/capture layer passes updateAttemptTiming from the owned db module.
 */
export function settlePendingFirstChunk(
  id: string,
  firstChunkMs: number | null,
  patchRow?: (id: string, patch: { firstChunkMs: number | null }) => boolean | Promise<boolean>
): void {
  const entry = pendingFirstChunk.get(id);
  dropPendingFirstChunk(id);
  if (!entry) return;
  const clean =
    parseDatabaseBackend() === "postgres"
      ? nativeTiming(firstChunkMs)
      : sanitizeTimingMs(firstChunkMs);
  entry.firstChunkMs = clean;
  if (!shouldPersistToDisk && parseDatabaseBackend() !== "postgres") return;
  if (pendingLogsQueue.includes(entry) || proxyFlushingEntries.has(entry)) return;
  try {
    const result =
      parseDatabaseBackend() === "postgres" && patchRow
        ? runPersistenceBackgroundWork(async () => {
            await patchRow(id, { firstChunkMs: clean });
          })
        : patchRow?.(id, { firstChunkMs: clean });
    if (result && typeof result === "object")
      void result.catch((error: unknown) => {
        if (parseDatabaseBackend() === "postgres") {
          console.debug(
            "[proxyLogger] Deferred timing patch failed:",
            error instanceof Error ? error.name : "Error"
          );
        }
      });
  } catch {
    // Deferred visibility is best-effort; the in-memory copy above stays correct.
  }
}

// ──────────────── Query ────────────────

/**
 * Get proxy logs with optional filters.
 * Reads from in-memory for speed (already hydrated from DB on startup).
 */
export function getProxyLogs(filters: ProxyLogFilters = {}) {
  return filterProxyLogs(proxyLogs, filters);
}

function filterProxyLogs(entries: ProxyLogEntry[], filters: ProxyLogFilters) {
  let logs = [...entries];

  if (filters.status) {
    if (filters.status === "ok") {
      logs = logs.filter((l) => l.status === "success");
    } else {
      logs = logs.filter((l) => l.status === filters.status);
    }
  }

  if (filters.type) {
    logs = logs.filter((l) => l.proxy?.type === filters.type);
  }

  if (filters.provider) {
    logs = logs.filter((l) => l.provider === filters.provider);
  }

  if (filters.level) {
    logs = logs.filter((l) => l.level === filters.level);
  }

  if (filters.search) {
    const q = filters.search.toLowerCase();
    logs = logs.filter(
      (l) =>
        (l.proxy?.host || "").toLowerCase().includes(q) ||
        (l.proxy?.name || "").toLowerCase().includes(q) ||
        (l.provider || "").toLowerCase().includes(q) ||
        (l.targetUrl || "").toLowerCase().includes(q) ||
        (l.clientIp || "").toLowerCase().includes(q) ||
        (l.egressIp || "").toLowerCase().includes(q) ||
        (l.level || "").toLowerCase().includes(q) ||
        (l.error || "").toLowerCase().includes(q) ||
        (l.account || "").toLowerCase().includes(q)
    );
  }

  const limit = filters.limit || 300;
  return logs.slice(0, limit);
}

// ──────────────── Clear ────────────────

export function clearProxyLogs() {
  proxyLogs.length = 0;
  pendingFirstChunk.clear();

  if (shouldPersistToDisk) {
    try {
      const db = getDbInstance();
      db.prepare("DELETE FROM proxy_logs").run();
    } catch (err: any) {
      console.warn(
        "[proxyLogger] Failed to clear DB:",
        sanitizeErrorMessage(err) || "Proxy log cleanup failed"
      );
    }
  }
}

function mapProxyLogRow(row: ProxyLogRow): ProxyLogEntry {
  return {
    id: row.id,
    timestamp: row.timestamp,
    status: row.status || "success",
    proxy: row.proxy_host
      ? {
          type: row.proxy_type,
          host: row.proxy_host,
          port: row.proxy_port ?? 0,
          name: row.proxy_name || undefined,
        }
      : null,
    level: row.level || "direct",
    levelId: row.level_id || null,
    provider: row.provider || null,
    targetUrl: row.target_url || null,
    clientIp: row.public_ip || null,
    egressIp: row.egress_ip || null,
    latencyMs: row.latency_ms || 0,
    error: row.error || null,
    connectionId: row.connection_id || null,
    comboId: row.combo_id || null,
    account: row.account || null,
    rotationAccount: row.rotation_account || null,
    correlationId: row.correlation_id || null,
    tlsFingerprint: row.tls_fingerprint === 1,
    upstreamStatus: typeof row.upstream_status === "number" ? row.upstream_status : null,
    attemptNumber: typeof row.attempt_number === "number" ? row.attempt_number : null,
    attemptIssue:
      row.attempt_issue === "served" || row.attempt_issue === "abandoned"
        ? row.attempt_issue
        : null,
    headersMs: sanitizeTimingMs(row.headers_ms),
    firstChunkMs: sanitizeTimingMs(row.first_chunk_ms),
  };
}

let proxyFlushInFlight: Promise<void> | undefined;
const proxyFlushingEntries = new Set<ProxyLogEntry>();
function reportProxyFlushFailure(error: unknown): void {
  console.warn(
    "[proxyLogger] Proxy log flush failed:",
    sanitizeErrorMessage(error) || "Proxy log persistence failed"
  );
}
export async function flushProxyLogsAsync(): Promise<void> {
  const store = getSelectedProxyLogStore();
  if (!store) {
    flushProxyLogsSync();
    return;
  }
  if (proxyFlushInFlight) {
    await proxyFlushInFlight;
    return flushProxyLogsAsync();
  }
  if (pendingLogsQueue.length === 0) return;

  // Drain the queue admitted by this call; later events belong to the next flush.
  // The fence covers INSERT, COMMIT and the deferred timing patches together.
  const admittedCount = pendingLogsQueue.length;
  const work = (async () => {
    let remaining = admittedCount;
    while (remaining > 0) {
      const batch = pendingLogsQueue.splice(0, Math.min(BATCH_SIZE_THRESHOLD, remaining));
      if (batch.length === 0) break;
      for (const entry of batch) proxyFlushingEntries.add(entry);
      try {
        await store.append(batch);
        for (const entry of batch) proxyFlushingEntries.delete(entry);
        // A first byte can arrive while INSERT/COMMIT is pending. Patch after commit
        // from the current object so a zero-row early UPDATE cannot lose that byte.
        for (const entry of batch) {
          if (entry.firstChunkMs !== null) {
            await updateAttemptTimingAsync(entry.id, {
              firstChunkMs: entry.firstChunkMs,
            });
          }
        }
        for (const entry of batch) nativeProxyLogBuffer.release(entry.id);
      } catch (error) {
        // Stable row ids permit an explicit later flush without duplicate inserts.
        // There is no transaction retry here, including an unknown COMMIT outcome.
        pendingLogsQueue.unshift(...batch);
        throw error;
      } finally {
        for (const entry of batch) proxyFlushingEntries.delete(entry);
      }
      remaining -= batch.length;
    }
  })();
  proxyFlushInFlight = work;
  try {
    await work;
    nativeProxyLogBuffer.noteFlushSuccess();
  } catch (error) {
    nativeProxyLogBuffer.noteFlushFailure(Date.now());
    throw error;
  } finally {
    if (proxyFlushInFlight === work) proxyFlushInFlight = undefined;
  }
}
export async function getProxyLogsAsync(filters: ProxyLogFilters = {}): Promise<ProxyLogEntry[]> {
  const store = getSelectedProxyLogStore();
  if (!store) return getProxyLogs(filters);
  await flushProxyLogsAsync();
  const rows = await store.list(MAX_IN_MEMORY_ENTRIES);
  return filterProxyLogs(
    rows.map((row) => mapProxyLogRow(row as ProxyLogRow)),
    filters
  );
}
export async function clearProxyLogsAsync(): Promise<void> {
  const store = getSelectedProxyLogStore();
  if (!store) {
    clearProxyLogs();
    return;
  }
  await flushProxyLogsAsync();
  await store.clear();
  proxyLogs.length = 0;
  pendingFirstChunk.clear();
}

type ProxyLogRow = Record<string, unknown> & {
  id: string;
  timestamp: string;
  status: ProxyLogEntry["status"];
  proxy_host: string | null;
  proxy_type: NonNullable<ProxyLogEntry["proxy"]>["type"];
  proxy_port: number | null;
  proxy_name: string | null;
  level: ProxyLogEntry["level"];
  level_id: string | null;
  provider: string | null;
  target_url: string | null;
  public_ip: string | null;
  egress_ip: string | null;
  latency_ms: number | null;
  error: string | null;
  connection_id: string | null;
  combo_id: string | null;
  account: string | null;
  rotation_account: string | null;
  correlation_id: string | null;
  tls_fingerprint: number | null;
  upstream_status: number | null;
  attempt_number: number | null;
  attempt_issue: ProxyLogEntry["attemptIssue"];
  headers_ms: number | null;
  first_chunk_ms: number | null;
};

export async function closeProxyLogSavesAsync(): Promise<void> {
  if (batchTimer) {
    clearInterval(batchTimer);
    batchTimer = null;
  }
  await flushProxyLogsAsync();
}
