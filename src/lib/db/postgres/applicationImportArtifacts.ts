import { createHash } from "node:crypto";
import { z } from "zod";
import type { NativeArtifactStore, PreparedArtifact } from "@/domain/persistence/artifactStore";
import { isArtifactStorageErrorKind } from "@/domain/persistence/artifactStore";
import {
  ApplicationImportError,
  APPLICATION_SNAPSHOT_LIMITS,
  checkedApplicationSnapshot,
  type ApplicationStorageSnapshot,
} from "./applicationSnapshot";

export interface OfflineArtifactMapping {
  sourceReference: string;
  targetReference: string;
  sizeBytes: number;
  checksum: string;
  /** Missing only on pre-journal ready arrays/receipts; never invent one-attempt provenance. */
  storageScope?: string;
  singleAttempt?: true;
  preparedAt?: string;
}
export interface OfflineArtifactCheckpoint extends OfflineArtifactMapping {
  state: "pending" | "ready";
  uploadState: "uncertain" | "acknowledged";
  storageScope: string;
  preparedAt: string;
}
/** Explicit offline source reads and a native object-store upload authority. */
export interface OfflineArtifactIO {
  readSource(reference: string): Promise<Uint8Array>;
  target: Pick<NativeArtifactStore, "put" | "get" | "storageScope">;
  /** Required before new uploads; acknowledge durable pending identity before dispatch. */
  checkpoint?(entries: readonly OfflineArtifactCheckpoint[]): Promise<void>;
}
const referenceSchema = z
  .string()
  .regex(/^s3\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}_[0-9a-f]{64}\.json$/);
const mappingSchema = z
  .object({
    sourceReference: z.string().min(1),
    targetReference: referenceSchema,
    sizeBytes: z.number().int().min(0).max(APPLICATION_SNAPSHOT_LIMITS.blobBytes),
    checksum: z.string().regex(/^[0-9a-f]{64}$/),
    storageScope: z
      .string()
      .regex(/^[0-9a-f]{64}$/)
      .optional(),
    singleAttempt: z.literal(true).optional(),
    preparedAt: z.string().datetime().optional(),
    state: z.enum(["pending", "ready"]).optional(),
    uploadState: z.enum(["uncertain", "acknowledged"]).optional(),
  })
  .strict();
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
/** Settled attached ownership is rewritten with its mapping; unresolved scope must be retained. */
export function assertApplicationArtifactPublicationScope(
  snapshot: ApplicationStorageSnapshot,
  target: Pick<NativeArtifactStore, "storageScope">
): void {
  const table = snapshot.tables.find((entry) => entry.name === "artifact_deletion_queue");
  if (!table) return;
  const logReferences = new Set(
    snapshot.tables
      .filter((entry) => ["call_logs", "call_logs_v1_legacy"].includes(entry.name))
      .flatMap((entry) =>
        entry.rows.map((row) => row.values[entry.columns.indexOf("artifact_relpath")])
      )
  );
  const at = (name: string) => table.columns.indexOf(name);
  for (const row of table.rows) {
    const phase = row.values[at("publication_state")],
      outcome = row.values[at("upload_state")];
    if (
      phase === "attached" &&
      outcome === "acknowledged" &&
      logReferences.has(row.values[at("relpath")])
    )
      continue;
    const scope = row.values[at("store_scope")];
    if (scope !== null && scope !== undefined && scope !== target.storageScope)
      throw new ApplicationImportError("artifact", "artifact_deletion_queue", "store_scope");
  }
}
/** Pending reruns only GET their original key. No rejected/unknown PUT is replayed. */
export async function prepareApplicationArtifacts(
  input: unknown,
  io: OfflineArtifactIO,
  existing: readonly (OfflineArtifactMapping | OfflineArtifactCheckpoint)[] = []
): Promise<OfflineArtifactMapping[]> {
  const snapshot = checkedApplicationSnapshot(input);
  assertApplicationArtifactPublicationScope(snapshot, io.target);
  const requirements = new Map<string, { size: string | null; checksum: string | null }[]>();
  for (const table of snapshot.tables.filter((entry) =>
    ["call_logs", "call_logs_v1_legacy"].includes(entry.name)
  )) {
    const at = table.columns.indexOf("artifact_relpath"),
      size = table.columns.indexOf("artifact_size_bytes"),
      check = table.columns.indexOf("artifact_sha256");
    if (at < 0) continue;
    for (const row of table.rows) {
      const reference = row.values[at];
      if (reference === null || reference === "") continue;
      if (typeof reference !== "string") throw new ApplicationImportError("artifact");
      const value = row.values[size],
        checksum = row.values[check];
      if (checksum !== null && checksum !== undefined && typeof checksum !== "string")
        throw new ApplicationImportError("artifact");
      const length =
        value && typeof value === "object" && value.kind === "integer"
          ? value.value
          : typeof value === "number"
            ? String(value)
            : null;
      const entries = requirements.get(reference) ?? [];
      entries.push({
        size: length,
        checksum: typeof checksum === "string" && checksum !== "" ? checksum : null,
      });
      requirements.set(reference, entries);
    }
  }
  const parsed = z.array(mappingSchema).safeParse(existing);
  if (!parsed.success) throw new ApplicationImportError("artifact");
  const journal: OfflineArtifactCheckpoint[] = [],
    seenSources = new Set<string>(),
    seenTargets = new Set<string>();
  for (const prior of parsed.data) {
    if (
      !requirements.has(prior.sourceReference) ||
      seenSources.has(prior.sourceReference) ||
      seenTargets.has(prior.targetReference) ||
      !prior.targetReference.endsWith(`_${prior.checksum}.json`) ||
      (prior.storageScope && prior.storageScope !== io.target.storageScope) ||
      (prior.state === "pending" &&
        (!prior.storageScope || !prior.singleAttempt || !prior.preparedAt))
    )
      throw new ApplicationImportError("artifact");
    const storageScope = prior.storageScope ?? io.target.storageScope;
    if (!storageScope) throw new ApplicationImportError("artifact");
    journal.push({
      ...prior,
      state: prior.state ?? "ready",
      uploadState: prior.uploadState ?? "acknowledged",
      storageScope,
      preparedAt: prior.preparedAt ?? new Date().toISOString(),
    });
    seenSources.add(prior.sourceReference);
    seenTargets.add(prior.targetReference);
  }
  const checkpoint = async () => {
    if (!io.checkpoint) throw new ApplicationImportError("artifact");
    await io.checkpoint(journal);
  };
  const mappings: OfflineArtifactMapping[] = [];
  for (const [reference, expected] of requirements) {
    const bytes = await io.readSource(reference);
    if (!(bytes instanceof Uint8Array) || bytes.byteLength > APPLICATION_SNAPSHOT_LIMITS.blobBytes)
      throw new ApplicationImportError("artifact");
    const checksum = sha(bytes);
    for (const entry of expected)
      if (
        (entry.size !== null && BigInt(entry.size) !== BigInt(bytes.byteLength)) ||
        (entry.checksum !== null &&
          entry.checksum !== checksum &&
          entry.checksum !== legacyChecksum(bytes))
      )
        throw new ApplicationImportError("artifact");
    let entry = journal.find((item) => item.sourceReference === reference);
    if (entry && (entry.sizeBytes !== bytes.byteLength || entry.checksum !== checksum))
      throw new ApplicationImportError("artifact");
    if (!entry) {
      if (!io.checkpoint || !io.target.storageScope) throw new ApplicationImportError("artifact");
      const publication: { prepared?: PreparedArtifact; entry?: OfflineArtifactCheckpoint } = {};
      try {
        const stored = await io.target.put(bytes, {
          async onPrepared(artifact) {
            if (
              publication.prepared ||
              artifact.storageScope !== io.target.storageScope ||
              artifact.sizeBytes !== bytes.byteLength ||
              artifact.checksum !== checksum ||
              seenTargets.has(artifact.reference)
            )
              throw new ApplicationImportError("artifact");
            publication.prepared = artifact;
            publication.entry = {
              sourceReference: reference,
              targetReference: artifact.reference,
              sizeBytes: artifact.sizeBytes,
              checksum: artifact.checksum,
              storageScope: artifact.storageScope,
              singleAttempt: true,
              preparedAt: new Date().toISOString(),
              state: "pending",
              uploadState: "uncertain",
            };
            journal.push(publication.entry);
            seenTargets.add(artifact.reference);
            await checkpoint();
          },
        });
        const uploaded = publication.entry;
        if (
          !publication.prepared ||
          !uploaded ||
          stored.reference !== uploaded.targetReference ||
          stored.sizeBytes !== uploaded.sizeBytes ||
          stored.checksum !== uploaded.checksum
        )
          throw new ApplicationImportError("artifact");
        uploaded.uploadState = "acknowledged";
        // A failed readback must leave the already acknowledged PUT identity durable.
        await checkpoint();
      } catch (error) {
        const pending = publication.entry;
        if (
          !pending ||
          !isArtifactStorageErrorKind(error, "put_unknown") ||
          error.reference !== pending.targetReference
        )
          throw new ApplicationImportError("artifact");
        // Retain pending identity. The following positive GET may reconcile this sole PUT.
      }
      entry = publication.entry;
      if (!entry) throw new ApplicationImportError("artifact");
    }
    const readback = await io.target.get(entry.targetReference);
    if (!readback || readback.byteLength !== bytes.byteLength || sha(readback) !== checksum)
      throw new ApplicationImportError("artifact");
    entry.state = "ready";
    entry.uploadState = "acknowledged";
    if (io.checkpoint) await checkpoint();
    mappings.push({
      sourceReference: reference,
      targetReference: entry.targetReference,
      sizeBytes: entry.sizeBytes,
      checksum: entry.checksum,
      storageScope: entry.storageScope,
      preparedAt: entry.preparedAt,
      ...(entry.singleAttempt ? { singleAttempt: true as const } : {}),
    });
  }
  return mappings;
}
/** Verify every object; missing/mutated bytes block restore qualification. */
export async function verifyApplicationArtifacts(
  mappings: readonly OfflineArtifactMapping[],
  store: Pick<NativeArtifactStore, "get" | "storageScope">
): Promise<void> {
  for (const mapping of mappings) {
    if (mapping.storageScope && mapping.storageScope !== store.storageScope)
      throw new ApplicationImportError("artifact");
    const bytes = await store.get(mapping.targetReference);
    if (!bytes || bytes.byteLength !== mapping.sizeBytes || sha(bytes) !== mapping.checksum)
      throw new ApplicationImportError("artifact");
  }
}
