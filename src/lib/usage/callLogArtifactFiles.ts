import fs from "node:fs";
import path from "node:path";
import type { RequestPipelinePayloads } from "@omniroute/open-sse/utils/requestLogger.ts";
import { isSizeLimitOmissionMarker } from "@/shared/constants/callLogSizeLimitMarkers";
import { resolveDataDir } from "../dataPaths";
import { parseDatabaseBackend } from "../db/backendSelection";
import { assertSqliteBackendAllowed } from "../db/controlPlaneContext";
import {
  serializeArtifactForStorage,
  computeArtifactChecksum,
} from "./callLogArtifactSerialization.ts";
export {
  MAX_CALL_LOG_ARTIFACT_BYTES,
  getArtifactMaxBytesForTest,
} from "./callLogArtifactSerialization.ts";

const isCloud = typeof globalThis.caches === "object" && globalThis.caches !== null;
const isBuildPhase =
  process.env.NEXT_PHASE === "phase-production-build" || process.env.OMNIROUTE_BUILDING === "1";
const DATA_DIR = parseDatabaseBackend() === "postgres" ? null : resolveDataDir({ isCloud });

export const CALL_LOGS_DIR = isCloud || DATA_DIR === null ? null : path.join(DATA_DIR, "call_logs");

// Re-exported for backward compatibility: consumers (completedRequestDetails.ts)
// import this marker check from here. Definition now lives in the shared
// constants module so the client-side detail view can use the exact same check
// without importing this fs/path-dependent, server-only module (see #13894).
export { isSizeLimitOmissionMarker };

export type CallLogDetailState = "none" | "ready" | "missing" | "corrupt" | "legacy-inline";

export type CallLogArtifact = {
  schemaVersion: 5;
  summary: {
    id: string;
    timestamp: string;
    method: string;
    path: string;
    status: number;
    model: string;
    requestedModel: string | null;
    provider: string;
    account: string;
    connectionId: string | null;
    duration: number;
    tokens: {
      in: number;
      out: number;
      cacheRead: number | null;
      cacheWrite: number | null;
      reasoning: number | null;
      compressed: number | null;
    };
    requestType: string | null;
    sourceFormat: string | null;
    targetFormat: string | null;
    apiKeyId: string | null;
    apiKeyName: string | null;
    comboName: string | null;
    comboStepId: string | null;
    comboExecutionKey: string | null;
  };
  requestBody: unknown;
  responseBody: unknown;
  error: unknown;
  pipeline?: RequestPipelinePayloads;
};

export type CallLogArtifactWriteResult = {
  relPath: string;
  sizeBytes: number;
  sha256: string;
};

export type PurgeCallLogArtifactDirectoryResult = {
  deletedArtifacts: number;
  errors: number;
};

export function buildArtifactRelativePath(timestamp: string, id: string) {
  const parsed = new Date(timestamp);
  const safeTimestamp = (
    Number.isNaN(parsed.getTime()) ? new Date().toISOString() : parsed.toISOString()
  ).replace(/[:]/g, "-");
  const dateFolder = safeTimestamp.slice(0, 10);
  return path.posix.join(dateFolder, `${safeTimestamp}_${id}.json`);
}

export function writeCallArtifact(
  artifact: CallLogArtifact,
  relativePath = buildArtifactRelativePath(artifact.summary.timestamp, artifact.summary.id)
): CallLogArtifactWriteResult | null {
  assertSqliteBackendAllowed();
  if (!CALL_LOGS_DIR || isBuildPhase) return null;

  const absPath = path.join(CALL_LOGS_DIR, relativePath);
  const tmpPath = `${absPath}.${process.pid}.${Date.now()}.tmp`;

  try {
    const serialized = serializeArtifactForStorage(artifact);
    const sizeBytes = Buffer.byteLength(serialized);
    // Keep the legacy field name for storage compatibility, but use a non-cryptographic checksum
    // so artifact bookkeeping is not treated as password hashing by static analysis.
    const fileChecksum = computeArtifactChecksum(serialized);

    fs.mkdirSync(path.dirname(absPath), { recursive: true });
    fs.writeFileSync(tmpPath, serialized);
    fs.renameSync(tmpPath, absPath);

    return {
      relPath: relativePath,
      sizeBytes,
      sha256: fileChecksum,
    };
  } catch (error) {
    try {
      fs.rmSync(tmpPath, { force: true });
    } catch {
      // Best effort cleanup only.
    }
    console.error("[callLogs] Failed to write request artifact:", (error as Error).message);
    return null;
  }
}

export function readCallArtifact(relativePath: string | null): {
  artifact: CallLogArtifact | null;
  state: "ready" | "missing" | "corrupt";
} {
  assertSqliteBackendAllowed();
  if (!CALL_LOGS_DIR || !relativePath) {
    return { artifact: null, state: "missing" };
  }

  try {
    const absPath = path.join(CALL_LOGS_DIR, relativePath);
    if (!fs.existsSync(absPath)) {
      return { artifact: null, state: "missing" };
    }
    return {
      artifact: JSON.parse(fs.readFileSync(absPath, "utf8")) as CallLogArtifact,
      state: "ready",
    };
  } catch (error) {
    console.error("[callLogs] Failed to read request artifact:", (error as Error).message);
    return { artifact: null, state: "corrupt" };
  }
}

export function deleteCallArtifact(relativePath: string | null, baseDir = CALL_LOGS_DIR): boolean {
  assertSqliteBackendAllowed();
  if (!baseDir || !relativePath) return false;

  try {
    const resolvedBaseDir = path.resolve(baseDir);
    const absPath = path.join(resolvedBaseDir, relativePath);
    if (!fs.existsSync(absPath)) return false;
    fs.rmSync(absPath, { force: true });
    const parentDir = path.dirname(absPath);
    if (parentDir !== resolvedBaseDir) {
      try {
        fs.rmdirSync(parentDir);
      } catch {
        // Directory is non-empty or already gone.
      }
    }
    return true;
  } catch {
    return false;
  }
}

export function cleanupEmptyCallLogDirs(baseDir = CALL_LOGS_DIR) {
  assertSqliteBackendAllowed();
  if (!baseDir || !fs.existsSync(baseDir)) return;

  try {
    for (const entry of fs.readdirSync(baseDir)) {
      const entryPath = path.join(baseDir, entry);
      const stat = fs.statSync(entryPath);
      if (!stat.isDirectory()) continue;
      if (fs.readdirSync(entryPath).length === 0) {
        fs.rmSync(entryPath, { recursive: true, force: true });
      }
    }
  } catch {
    // Best effort only.
  }
}

export function listCallLogArtifactFiles(baseDir = CALL_LOGS_DIR) {
  assertSqliteBackendAllowed();
  if (!baseDir || !fs.existsSync(baseDir)) return [];

  return fs
    .readdirSync(baseDir)
    .flatMap((entry) => {
      const entryPath = path.join(baseDir, entry);
      try {
        const stat = fs.statSync(entryPath);
        if (!stat.isDirectory()) return [];

        return fs
          .readdirSync(entryPath)
          .filter((file) => file.endsWith(".json"))
          .map((file) => {
            const absPath = path.join(entryPath, file);
            const fileStat = fs.statSync(absPath);
            return {
              relativePath: path.posix.join(entry, file),
              absPath,
              mtimeMs: fileStat.mtimeMs,
            };
          });
      } catch {
        return [];
      }
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs);
}

export function purgeCallLogArtifactDirectory(
  baseDir = CALL_LOGS_DIR
): PurgeCallLogArtifactDirectoryResult {
  assertSqliteBackendAllowed();
  const result = { deletedArtifacts: 0, errors: 0 };
  if (!baseDir || !fs.existsSync(baseDir)) return result;

  try {
    result.deletedArtifacts = listCallLogArtifactFiles(baseDir).length;
  } catch {
    result.deletedArtifacts = 0;
  }

  try {
    fs.rmSync(baseDir, { recursive: true, force: true });
  } catch (error) {
    console.error("[callLogArtifacts] Failed to purge call log artifacts:", error);
    result.deletedArtifacts = 0;
    result.errors++;
  }

  return result;
}
