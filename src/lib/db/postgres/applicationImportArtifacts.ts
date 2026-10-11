import { createHash } from "node:crypto";
import type { NativeArtifactStore } from "@/domain/persistence/artifactStore";
import {
  ApplicationImportError,
  APPLICATION_SNAPSHOT_LIMITS,
  checkedApplicationSnapshot,
} from "./applicationSnapshot";

export interface OfflineArtifactMapping {
  sourceReference: string;
  targetReference: string;
  sizeBytes: number;
  checksum: string;
}
/** Caller explicitly supplies offline source reads and a native object-store upload authority. */
export interface OfflineArtifactIO {
  readSource(reference: string): Promise<Uint8Array>;
  target: Pick<NativeArtifactStore, "put" | "get">;
  /** Persist only acknowledged/reconciled mappings, outside every database transaction. */
  checkpoint?(mappings: readonly OfflineArtifactMapping[]): Promise<void>;
}
function sha(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}
function legacyChecksum(bytes: Uint8Array): string {
  let hash = 0x811c9dc5;
  for (const byte of bytes) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16).padStart(8, "0");
}

/** Upload each shared source reference once. Unknown writes are read back, never blindly retried. */
export async function prepareApplicationArtifacts(
  input: unknown,
  io: OfflineArtifactIO,
  existing: readonly OfflineArtifactMapping[] = []
): Promise<OfflineArtifactMapping[]> {
  const snapshot = checkedApplicationSnapshot(input),
    requirements = new Map<string, { size: string | null; checksum: string | null }[]>();
  for (const table of snapshot.tables.filter((table) =>
    ["call_logs", "call_logs_v1_legacy"].includes(table.name)
  )) {
    const path = table.columns.indexOf("artifact_relpath"),
      size = table.columns.indexOf("artifact_size_bytes"),
      checksum = table.columns.indexOf("artifact_sha256");
    if (path < 0) continue;
    for (const row of table.rows) {
      const reference = row.values[path];
      if (reference === null || reference === "") continue;
      if (typeof reference !== "string") throw new ApplicationImportError("artifact");
      const value = row.values[size];
      const length =
        value && typeof value === "object" && value.kind === "integer"
          ? value.value
          : typeof value === "number"
            ? String(value)
            : null;
      const check = row.values[checksum];
      if (check !== null && check !== undefined && typeof check !== "string")
        throw new ApplicationImportError("artifact");
      const entries = requirements.get(reference) ?? [];
      entries.push({
        size: length,
        checksum: typeof check === "string" && check !== "" ? check : null,
      });
      requirements.set(reference, entries);
    }
  }
  const mappings: OfflineArtifactMapping[] = [],
    seen = new Set<string>();
  for (const [reference, expected] of requirements) {
    const bytes = await io.readSource(reference);
    if (!(bytes instanceof Uint8Array) || bytes.byteLength > APPLICATION_SNAPSHOT_LIMITS.blobBytes)
      throw new ApplicationImportError("artifact");
    const checksum = sha(bytes);
    for (const entry of expected) {
      if (entry.size !== null && BigInt(entry.size) !== BigInt(bytes.byteLength))
        throw new ApplicationImportError("artifact");
      if (
        entry.checksum !== null &&
        entry.checksum !== checksum &&
        entry.checksum !== legacyChecksum(bytes)
      )
        throw new ApplicationImportError("artifact");
    }
    const prior = existing.filter((mapping) => mapping.sourceReference === reference);
    if (prior.length > 1) throw new ApplicationImportError("artifact");
    let stored = prior[0]
      ? {
          reference: prior[0].targetReference,
          sizeBytes: prior[0].sizeBytes,
          checksum: prior[0].checksum,
        }
      : undefined;
    if (stored && (stored.sizeBytes !== bytes.byteLength || stored.checksum !== checksum))
      throw new ApplicationImportError("artifact");
    if (!stored) {
      try {
        stored = await io.target.put(bytes);
      } catch (error) {
        const candidate = error as { kind?: unknown; reference?: unknown };
        if (candidate?.kind !== "put_unknown" || typeof candidate.reference !== "string")
          throw new ApplicationImportError("artifact");
        const readback = await io.target.get(candidate.reference);
        if (!readback || sha(readback) !== checksum) throw new ApplicationImportError("artifact");
        stored = { reference: candidate.reference, sizeBytes: bytes.byteLength, checksum };
      }
    }
    if (
      !stored ||
      stored.sizeBytes !== bytes.byteLength ||
      stored.checksum !== checksum ||
      seen.has(stored.reference)
    )
      throw new ApplicationImportError("artifact");
    const readback = await io.target.get(stored.reference);
    if (!readback || readback.byteLength !== bytes.byteLength || sha(readback) !== checksum)
      throw new ApplicationImportError("artifact");
    seen.add(stored.reference);
    mappings.push({
      sourceReference: reference,
      targetReference: stored.reference,
      sizeBytes: stored.sizeBytes,
      checksum,
    });
    await io.checkpoint?.(mappings);
  }
  return mappings;
}

/** Verify every acknowledged object; missing or mutated bytes block restore qualification. */
export async function verifyApplicationArtifacts(
  mappings: readonly OfflineArtifactMapping[],
  store: Pick<NativeArtifactStore, "get">
): Promise<void> {
  for (const mapping of mappings) {
    const bytes = await store.get(mapping.targetReference);
    if (!bytes || bytes.byteLength !== mapping.sizeBytes || sha(bytes) !== mapping.checksum)
      throw new ApplicationImportError("artifact");
  }
}
