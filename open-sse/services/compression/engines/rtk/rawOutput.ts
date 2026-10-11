import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";

import { parseDatabaseBackend } from "../../../../../src/lib/db/backendSelection";

import type { CommandSample } from "./discover.ts";
import { severityWordPattern } from "./severityVocabulary.ts";

export type RtkRawOutputRetention = "never" | "failures" | "always";

export interface RtkRawOutputPointer {
  id: string;
  path: string;
  bytes: number;
  sha256: string;
  redacted: boolean;
}

const SECRET_PATTERNS: Array<[RegExp, string]> = [
  [/\b(sk-[A-Za-z0-9_-]{16,})\b/g, "[REDACTED_OPENAI_KEY]"],
  [/\b(xox[baprs]-[A-Za-z0-9-]{16,})\b/g, "[REDACTED_SLACK_TOKEN]"],
  [/\b(AKIA[0-9A-Z]{16})\b/g, "[REDACTED_AWS_KEY]"],
  // key=value / key: value for common credential field names (flat alternation — no nesting,
  // so no ReDoS). Covers names the bare token/secret/password set misses (private_key, etc).
  [
    /((?:api[_-]?key|api[_-]?token|access[_-]?key|access[_-]?token|client[_-]?secret|auth[_-]?token|private[_-]?key|secret[_-]?key|credentials?|token|secret|password)\s*[:=]\s*)("[^"]+"|'[^']+'|[^\s]+)/gi,
    "$1[REDACTED]",
  ],
  // Authorization / Proxy-Authorization with Bearer OR Basic (curl -v emits Basic <base64>).
  [/((?:Proxy-)?Authorization:\s*(?:Bearer|Basic)\s+)[A-Za-z0-9._~+/-]+=*/gi, "$1[REDACTED]"],
];

function dataDir(): string {
  return process.env.DATA_DIR || path.join(os.homedir(), ".omniroute");
}

function safeId(seed: string): string {
  return crypto.createHash("sha256").update(seed).digest("hex").slice(0, 24);
}

function safeUtf8Slice(value: string, maxBytes: number): string {
  if (maxBytes <= 0 || Buffer.byteLength(value, "utf8") <= maxBytes) return value;
  let bytes = 0;
  let output = "";
  for (const char of value) {
    const len = Buffer.byteLength(char, "utf8");
    if (bytes + len > maxBytes) break;
    output += char;
    bytes += len;
  }
  return `${output}\n\n--- truncated at ${maxBytes} bytes ---`;
}

export function redactRtkRawOutput(value: string): {
  text: string;
  redacted: boolean;
} {
  let redacted = false;
  let text = value;
  for (const [pattern, replacement] of SECRET_PATTERNS) {
    const next = text.replace(pattern, (...args: string[]) => {
      redacted = true;
      return typeof replacement === "string"
        ? replacement.replace("$1", args[1] ?? "")
        : replacement;
    });
    text = next;
  }
  return { text, redacted };
}

export function isLikelyFailureOutput(value: string): boolean {
  return severityWordPattern().test(value);
}

/**
 * #10659: the raw-output store used to grow unbounded and every pointer read did a full
 * readdirSync over the whole store, freezing the event loop with millions of files.
 * New writes now land in id-prefix buckets (`<store>/<id[0:2]>/...`) so reads are O(bucket),
 * and a bounded async purge (see purgeRtkRawOutput) caps total files/age.
 */
const RAW_OUTPUT_BUCKET_LEN = 2;
/** Legacy flat-store entries beyond this size are not synchronously scanned (freeze guard). */
const LEGACY_FLAT_SCAN_GUARD = 100_000;

function rawOutputDir(): string {
  return path.join(dataDir(), "rtk", "raw-output");
}

function bucketDir(id: string): string {
  return path.join(rawOutputDir(), id.slice(0, RAW_OUTPUT_BUCKET_LEN));
}

interface RtkRawOutputOptions {
  retention: RtkRawOutputRetention;
  command?: string | null;
  maxBytes?: number;
  failure?: boolean;
}

function prepareRtkRawOutput(
  raw: string,
  options: RtkRawOutputOptions
): {
  pointer: RtkRawOutputPointer;
  directory: string;
  text: string;
  metadata: string;
} | null {
  if (options.retention === "never") return null;
  const failure = options.failure ?? isLikelyFailureOutput(raw);
  if (options.retention === "failures" && !failure) return null;
  if (raw.trim().length === 0) return null;
  const maxBytes = Math.max(1024, Math.floor(options.maxBytes ?? 1_048_576));
  const redaction = redactRtkRawOutput(safeUtf8Slice(raw, maxBytes));
  const now = Date.now();
  const commandSlug = (options.command || "tool-output")
    .replace(/[^A-Za-z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 48);
  const id = safeId(`${now}:${commandSlug}:${raw.length}:${redaction.text}`);
  const directory = bucketDir(id);
  const filePath = path.join(directory, `${now}-${commandSlug || "tool-output"}-${id}.log`);
  const bytes = Buffer.byteLength(redaction.text, "utf8");
  return {
    pointer: {
      id,
      path: filePath,
      bytes,
      sha256: crypto.createHash("sha256").update(redaction.text).digest("hex"),
      redacted: redaction.redacted,
    },
    directory,
    text: redaction.text,
    metadata: JSON.stringify({
      command: options.command ?? null,
      timestamp: now,
      failure,
      redacted: redaction.redacted,
      bytes,
    }),
  };
}

export function maybePersistRtkRawOutput(
  raw: string,
  options: RtkRawOutputOptions
): RtkRawOutputPointer | null {
  const capture = prepareRtkRawOutput(raw, options);
  if (!capture) return null;
  try {
    fs.mkdirSync(capture.directory, { recursive: true });
    fs.writeFileSync(capture.pointer.path, capture.text);
  } catch {
    // A failed log write never fails compression or advertises an unavailable receipt.
    return null;
  }
  try {
    fs.writeFileSync(capture.pointer.path.replace(/\.log$/, ".meta.json"), capture.metadata);
  } catch {
    // The log remains recoverable when optional command metadata cannot be written.
  }
  return capture.pointer;
}

/** Native inline compression awaits the file owner; the synchronous worker API stays intact. */
export async function maybePersistRtkRawOutputAsync(
  raw: string,
  options: RtkRawOutputOptions
): Promise<RtkRawOutputPointer | null> {
  const capture = prepareRtkRawOutput(raw, options);
  if (!capture) return null;
  try {
    await fsp.mkdir(capture.directory, { recursive: true });
    await fsp.writeFile(capture.pointer.path, capture.text);
  } catch {
    return null;
  }
  try {
    await fsp.writeFile(capture.pointer.path.replace(/\.log$/, ".meta.json"), capture.metadata);
  } catch {
    // Keep the source best-effort sidecar policy and successful log receipt.
  }
  return capture.pointer;
}

export function readRtkRawOutput(pointerId: string): string | null {
  const dir = rawOutputDir();
  if (!fs.existsSync(dir)) return null;

  // Bucketed layout first (new writes): one tiny subdir read instead of a full-store scan.
  const bucket = bucketDir(pointerId);
  if (fs.existsSync(bucket)) {
    const entry = fs
      .readdirSync(bucket)
      .find((file) => file.endsWith(".log") && file.includes(pointerId));
    if (entry) {
      const fullPath = path.join(bucket, entry);
      if (!fullPath.startsWith(dir)) return null;
      return fs.readFileSync(fullPath, "utf8");
    }
  }

  // Legacy flat layout (pre-bucket writes). Guarded: scanning a multi-million-entry flat
  // store synchronously is exactly the event-loop freeze #10659 reports, so refuse once
  // the flat store is pathologically large instead of stalling the gateway.
  const entries = fs.readdirSync(dir);
  if (entries.length > LEGACY_FLAT_SCAN_GUARD) {
    console.warn(
      `[rtk-raw-output] legacy flat store has ${entries.length} entries; skipping O(n) pointer scan for ${pointerId}`
    );
    return null;
  }
  const entry = entries.find((file) => file.endsWith(".log") && file.includes(pointerId));
  if (!entry) return null;
  const fullPath = path.join(dir, entry);
  if (!fullPath.startsWith(dir)) return null;
  return fs.readFileSync(fullPath, "utf8");
}

/** Recover the command for a `.log` from its filename slug (legacy, sidecar-less captures). */
function commandFromSlug(fileName: string): string {
  // `<timestamp>-<slug>-<id>.log` → strip the leading timestamp and the trailing id.
  const slug = fileName
    .replace(/^\d+-/, "")
    .replace(/-[0-9a-f]{24}\.log$/i, "")
    .replace(/\.log$/i, "");
  return slug.replace(/_+/g, " ").trim();
}

/**
 * Collect every `.log` path in the store (legacy flat + buckets). The flat store is
 * guarded so a pathological legacy directory cannot freeze the loop; bucket dirs are
 * small by construction (the purge cap keeps each bucket bounded).
 */
function collectRawOutputLogFiles(dir: string): Array<{ name: string; fullPath: string }> {
  const logs: Array<{ name: string; fullPath: string }> = [];
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return logs;
  }
  if (entries.length <= LEGACY_FLAT_SCAN_GUARD) {
    for (const entry of entries) {
      if (entry.endsWith(".log")) logs.push({ name: entry, fullPath: path.join(dir, entry) });
    }
  } else {
    console.warn(
      `[rtk-raw-output] legacy flat store has ${entries.length} entries; skipping sample scan this run`
    );
  }
  for (const entry of entries) {
    if (entry.length !== RAW_OUTPUT_BUCKET_LEN) continue;
    const subPath = path.join(dir, entry);
    let isDir = false;
    try {
      isDir = fs.statSync(subPath).isDirectory();
    } catch {
      continue;
    }
    if (!isDir) continue;
    let subEntries: string[];
    try {
      subEntries = fs.readdirSync(subPath);
    } catch {
      continue;
    }
    for (const name of subEntries) {
      if (name.endsWith(".log")) logs.push({ name, fullPath: path.join(subPath, name) });
    }
  }
  return logs;
}

/**
 * Read the opt-in RTK raw-output store (`DATA_DIR/rtk/raw-output/*.log`) into
 * `CommandSample[]` for the pure miners `discoverRepeatedNoise()` / `suggestFilter()`.
 *
 * The command comes from the `.meta.json` sidecar when present (exact), else from the
 * filename slug (lossy, for legacy captures). Empty/unreadable entries are skipped.
 * Returns the most-recent-first samples, capped at `opts.limit` (default 500) to bound
 * memory. No throw: a corrupt entry is dropped, not propagated.
 */
export function listRtkCommandSamples(opts: { limit?: number } = {}): CommandSample[] {
  const dir = rawOutputDir();
  if (!fs.existsSync(dir)) return [];
  const limit = Math.max(1, Math.floor(opts.limit ?? 500));

  const logs = collectRawOutputLogFiles(dir);
  // Newest first: the filename is timestamp-prefixed, so a reverse lexical sort works.
  logs.sort((a, b) => (a.name < b.name ? 1 : a.name > b.name ? -1 : 0));

  const samples: CommandSample[] = [];
  for (const { name, fullPath } of logs) {
    if (samples.length >= limit) break;
    let output: string;
    try {
      output = fs.readFileSync(fullPath, "utf8");
    } catch {
      continue;
    }
    if (output.trim().length === 0) continue;
    let command = "";
    try {
      const metaRaw = fs.readFileSync(fullPath.replace(/\.log$/, ".meta.json"), "utf8");
      const meta = JSON.parse(metaRaw) as { command?: unknown };
      if (typeof meta.command === "string" && meta.command.trim()) command = meta.command.trim();
    } catch {
      // No/!invalid sidecar → fall back to the filename slug below.
    }
    if (!command) command = commandFromSlug(name) || "tool-output";
    samples.push({ command, output });
  }
  return samples;
}

export interface RtkRawOutputPurgeOptions {
  maxAgeDays?: number;
  maxFiles?: number;
}

export interface RtkRawOutputPurgeResult {
  skipped: boolean;
  scanned: number;
  deleted: number;
  errors: number;
}

const PURGE_THROTTLE_MS = 60_000;
let lastRawOutputPurgeAt = 0;
const NATIVE_PURGE = Symbol.for("omniroute.rtkRawOutputPurge");
const NATIVE_PURGE_AT = Symbol.for("omniroute.rtkRawOutputPurgeAt");
interface NativePurgeTask {
  timer?: ReturnType<typeof setImmediate>;
  promise: Promise<void>;
  finish(): void;
}
interface NativePurgeOwner {
  stopped: boolean;
  queued?: NativePurgeTask;
  accepted: Set<Promise<void>>;
  closing?: Promise<void>;
}
const nativePurgeGlobals = globalThis as typeof globalThis & {
  [NATIVE_PURGE]?: NativePurgeOwner | false;
  [NATIVE_PURGE_AT]?: number;
};

/** Test hook: clear the purge throttle so a test can exercise two consecutive purges. */
export function resetRtkRawOutputPurgeThrottle(): void {
  lastRawOutputPurgeAt = 0;
  delete nativePurgeGlobals[NATIVE_PURGE_AT];
}

async function mapLimit<T>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<void>
): Promise<void> {
  let index = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (index < items.length) {
      const item = items[index++];
      await fn(item);
    }
  });
  await Promise.all(workers);
}

/**
 * #10659: bounded retention for the raw-output store. Enforces max age and max file count
 * asynchronously (never blocks the event loop), best-effort (never throws into callers),
 * and throttled to once per minute from the scheduler.
 *
 * The legacy flat store is skipped when it is pathologically large (guard) — scanning it
 * synchronously/async with millions of entries is what froze gateways; the operator does
 * a one-off cleanup and the bucketized layout keeps new growth bounded.
 */
export async function purgeRtkRawOutput(
  opts: RtkRawOutputPurgeOptions = {}
): Promise<RtkRawOutputPurgeResult> {
  return purgeRtkRawOutputAt(rawOutputDir(), opts, parseDatabaseBackend() === "postgres");
}

async function purgeRtkRawOutputAt(
  dir: string,
  opts: RtkRawOutputPurgeOptions,
  native: boolean
): Promise<RtkRawOutputPurgeResult> {
  const now = Date.now();
  if (
    now - (native ? (nativePurgeGlobals[NATIVE_PURGE_AT] ?? 0) : lastRawOutputPurgeAt) <
    PURGE_THROTTLE_MS
  ) {
    return { skipped: true, scanned: 0, deleted: 0, errors: 0 };
  }
  if (native) nativePurgeGlobals[NATIVE_PURGE_AT] = now;
  else lastRawOutputPurgeAt = now;

  const maxAgeDays = Math.max(1, Math.floor(opts.maxAgeDays ?? 30));
  const maxFiles = Math.max(1, Math.floor(opts.maxFiles ?? 100_000));
  const maxAgeMs = maxAgeDays * 86_400_000;
  const result: RtkRawOutputPurgeResult = {
    skipped: false,
    scanned: 0,
    deleted: 0,
    errors: 0,
  };
  if (native ? !(await pathExistsAsync(dir)) : !fs.existsSync(dir)) return result;

  try {
    const candidates: Array<{ file: string; meta: string | null; ts: number }> = [];
    const flat = await fsp.readdir(dir);
    if (flat.length > LEGACY_FLAT_SCAN_GUARD) {
      console.warn(
        `[rtk-raw-output] legacy flat store has ${flat.length} entries; purge skips flat scan this run (one-off manual cleanup recommended)`
      );
    } else {
      for (const name of flat) {
        if (!name.endsWith(".log")) continue;
        candidates.push({
          file: path.join(dir, name),
          meta: path.join(dir, name.replace(/\.log$/, ".meta.json")),
          ts: parseInt(name, 10) || 0,
        });
      }
    }
    for (const entry of flat) {
      if (entry.length !== RAW_OUTPUT_BUCKET_LEN) continue;
      const subPath = path.join(dir, entry);
      let isDir = false;
      try {
        isDir = (await fsp.stat(subPath)).isDirectory();
      } catch {
        continue;
      }
      if (!isDir) continue;
      let subEntries: string[];
      try {
        subEntries = await fsp.readdir(subPath);
      } catch {
        continue;
      }
      for (const name of subEntries) {
        if (!name.endsWith(".log")) continue;
        candidates.push({
          file: path.join(subPath, name),
          meta: path.join(subPath, name.replace(/\.log$/, ".meta.json")),
          ts: parseInt(name, 10) || 0,
        });
      }
    }
    result.scanned = candidates.length;

    const agedOut = candidates.filter((c) => c.ts > 0 && now - c.ts > maxAgeMs);
    const aged = new Set(agedOut);
    const remaining = candidates.filter((c) => !aged.has(c));
    remaining.sort((a, b) => b.ts - a.ts || (a.file < b.file ? 1 : -1));
    const keep = new Set(remaining.slice(0, maxFiles).map((c) => c.file));
    const overflow = remaining.filter((c) => !keep.has(c.file));

    await mapLimit([...agedOut, ...overflow], 32, async (c) => {
      try {
        await fsp.unlink(c.file);
        result.deleted++;
      } catch {
        result.errors++;
      }
      if (c.meta) {
        try {
          await fsp.unlink(c.meta);
        } catch {
          // Missing/never-written sidecar is fine.
        }
      }
    });

    if (result.deleted > 0 || result.errors > 0) {
      console.log(
        `[rtk-raw-output] purge: scanned=${result.scanned} deleted=${result.deleted} errors=${result.errors} (maxFiles=${maxFiles}, maxAgeDays=${maxAgeDays})`
      );
    }
  } catch (err) {
    console.warn("[rtk-raw-output] purge failed:", (err as Error).message);
    result.errors++;
  }
  return result;
}

/**
 * Schedule a throttled best-effort purge off the hot path. Safe to call on every write:
 * purgeRtkRawOutput itself throttles to once per minute.
 */
export function scheduleRtkRawOutputPurge(opts: RtkRawOutputPurgeOptions = {}): void {
  if (parseDatabaseBackend() === "postgres") {
    const existing = nativePurgeGlobals[NATIVE_PURGE];
    if (existing === false || existing?.stopped || existing?.queued) return;
    let directory: string;
    try {
      directory = rawOutputDir();
    } catch {
      console.warn("[rtk-raw-output] background purge unavailable.");
      return;
    }
    const options = { ...opts };
    const owner: NativePurgeOwner =
      existing ??
      (nativePurgeGlobals[NATIVE_PURGE] = { stopped: false, accepted: new Set<Promise<void>>() });
    let finish!: () => void;
    const promise = new Promise<void>((resolve) => {
      finish = resolve;
    });
    const task: NativePurgeTask = { promise, finish };
    owner.accepted.add(promise);
    owner.queued = task;
    task.timer = setImmediate(() => {
      if (owner.queued === task) owner.queued = undefined;
      void purgeRtkRawOutputAt(directory, options, true)
        .catch(() => {
          console.warn("[rtk-raw-output] background purge failed.");
        })
        .finally(() => {
          owner.accepted.delete(promise);
          finish();
        });
    });
    return;
  }
  setImmediate(() => {
    void purgeRtkRawOutput(opts).catch(() => {
      /* best-effort */
    });
  });
}

/** Stop optional native production; already-running file maintenance remains owned until close. */
export function stopRtkRawOutputPurgeScheduler(): void {
  const owner = nativePurgeGlobals[NATIVE_PURGE];
  if (!owner) {
    nativePurgeGlobals[NATIVE_PURGE] = false;
    return;
  }
  owner.stopped = true;
  const queued = owner.queued;
  if (queued) {
    if (queued.timer) clearImmediate(queued.timer);
    owner.queued = undefined;
    owner.accepted.delete(queued.promise);
    queued.finish();
  }
}

/** Final sink stage, after accepted requests have completed their retained-log writes. */
export async function closeRtkRawOutputPurgeScheduler(): Promise<void> {
  stopRtkRawOutputPurgeScheduler();
  const owner = nativePurgeGlobals[NATIVE_PURGE];
  if (!owner) return;
  await (owner.closing ??= Promise.all([...owner.accepted]).then(() => {}));
}

async function pathExistsAsync(filename: string): Promise<boolean> {
  try {
    await fsp.access(filename);
    return true;
  } catch {
    return false;
  }
}

/** Async serving reader; synchronous exports remain the compression-worker/SQLite contract. */
export async function readRtkRawOutputAsync(pointerId: string): Promise<string | null> {
  if (parseDatabaseBackend() !== "postgres") return readRtkRawOutput(pointerId);
  const dir = rawOutputDir();
  if (!(await pathExistsAsync(dir))) return null;
  const bucket = bucketDir(pointerId);
  if (await pathExistsAsync(bucket)) {
    const entry = (await fsp.readdir(bucket)).find(
      (file) => file.endsWith(".log") && file.includes(pointerId)
    );
    if (entry) {
      const fullPath = path.join(bucket, entry);
      if (!fullPath.startsWith(dir)) return null;
      return fsp.readFile(fullPath, "utf8");
    }
  }
  const entries = await fsp.readdir(dir);
  if (entries.length > LEGACY_FLAT_SCAN_GUARD) {
    console.warn(
      `[rtk-raw-output] legacy flat store has ${entries.length} entries; skipping O(n) pointer scan for ${pointerId}`
    );
    return null;
  }
  const entry = entries.find((file) => file.endsWith(".log") && file.includes(pointerId));
  if (!entry) return null;
  const fullPath = path.join(dir, entry);
  return fullPath.startsWith(dir) ? fsp.readFile(fullPath, "utf8") : null;
}

async function collectRawOutputLogFilesAsync(
  dir: string
): Promise<Array<{ name: string; fullPath: string }>> {
  const logs: Array<{ name: string; fullPath: string }> = [];
  let entries: string[];
  try {
    entries = await fsp.readdir(dir);
  } catch {
    return logs;
  }
  if (entries.length <= LEGACY_FLAT_SCAN_GUARD) {
    for (const entry of entries) {
      if (entry.endsWith(".log")) logs.push({ name: entry, fullPath: path.join(dir, entry) });
    }
  } else {
    console.warn(
      `[rtk-raw-output] legacy flat store has ${entries.length} entries; skipping sample scan this run`
    );
  }
  for (const entry of entries) {
    if (entry.length !== RAW_OUTPUT_BUCKET_LEN) continue;
    const subPath = path.join(dir, entry);
    try {
      if (!(await fsp.stat(subPath)).isDirectory()) continue;
    } catch {
      continue;
    }
    let subEntries: string[];
    try {
      subEntries = await fsp.readdir(subPath);
    } catch {
      continue;
    }
    for (const name of subEntries) {
      if (name.endsWith(".log")) logs.push({ name, fullPath: path.join(subPath, name) });
    }
  }
  return logs;
}

export async function listRtkCommandSamplesAsync(
  opts: { limit?: number } = {}
): Promise<CommandSample[]> {
  if (parseDatabaseBackend() !== "postgres") return listRtkCommandSamples(opts);
  const dir = rawOutputDir();
  if (!(await pathExistsAsync(dir))) return [];
  const limit = Math.max(1, Math.floor(opts.limit ?? 500));
  const logs = await collectRawOutputLogFilesAsync(dir);
  logs.sort((a, b) => (a.name < b.name ? 1 : a.name > b.name ? -1 : 0));
  const samples: CommandSample[] = [];
  for (const { name, fullPath } of logs) {
    if (samples.length >= limit) break;
    let output: string;
    try {
      output = await fsp.readFile(fullPath, "utf8");
    } catch {
      continue;
    }
    if (!output.trim()) continue;
    let command = "";
    try {
      const meta = JSON.parse(
        await fsp.readFile(fullPath.replace(/\.log$/, ".meta.json"), "utf8")
      ) as {
        command?: unknown;
      };
      if (typeof meta.command === "string" && meta.command.trim()) command = meta.command.trim();
    } catch {
      /* Preserve the legacy filename fallback. */
    }
    if (!command) command = commandFromSlug(name) || "tool-output";
    samples.push({ command, output });
  }
  return samples;
}
