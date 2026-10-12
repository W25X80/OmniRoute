import { createHash, randomUUID } from "node:crypto";
import {
  readFile,
  writeFile,
  mkdir,
  stat,
  realpath,
  rename,
  link,
  unlink,
  open,
} from "node:fs/promises";
import path from "node:path";
import { parseArgs } from "node:util";
import { mkdirSync, writeFileSync } from "node:fs";
import {
  NATIVE_BLOB_LIMITS,
  NativeBlobCarrierError,
  verifyNativeBlob,
  type NativeBlobSink,
  type NativeBlobSource,
} from "../../src/lib/db/postgres/nativeBlobCarrier.ts";
import { z } from "zod";
import type { NativeArtifactStore } from "../../src/domain/persistence/artifactStore.ts";
import { createS3ArtifactStore } from "../../src/lib/artifacts/s3ArtifactStore.ts";
import { readS3ArtifactConfiguration } from "../../src/lib/artifacts/s3ArtifactConfiguration.ts";
import { requireExplicitStorageEncryptionKey } from "../../src/lib/db/encryption.ts";
import { createPostgresDatabase } from "../../src/lib/db/postgres/connect.ts";
import { readPostgresEnvironment } from "../../src/lib/db/postgres/environment.ts";
import { isPostgresErrorKind } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  applicationSnapshotDigest,
  checkedApplicationSnapshot,
  captureSqliteApplicationSnapshot,
  openOfflineSqliteSource,
  ApplicationImportError,
  APPLICATION_SNAPSHOT_LIMITS,
} from "../../src/lib/db/postgres/applicationSnapshot.ts";
import {
  capturePostgresApplicationSnapshot,
  importApplicationSnapshot,
  readApplicationImportReceipt,
  validateApplicationSnapshotCredentials,
  verifyApplicationImport,
  ApplicationImportEffectsError,
} from "../../src/lib/db/postgres/applicationImport.ts";
import {
  prepareApplicationArtifacts,
  verifyApplicationArtifacts,
  type OfflineArtifactMapping,
  type OfflineArtifactCheckpoint,
} from "../../src/lib/db/postgres/applicationImportArtifacts.ts";

const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const objectSchema = z
  .object({
    reference: z.string(),
    file: z.string().regex(/^objects\/[0-9a-f]{64}\.bin$/),
    checksum: z.string().regex(/^[0-9a-f]{64}$/),
    sizeBytes: z.number().int().min(0).max(APPLICATION_SNAPSHOT_LIMITS.blobBytes),
  })
  .strict();
const bundleSchema = z
  .object({
    format: z.literal("omniroute-offline-storage-bundle-v1"),
    snapshot: z.unknown(),
    snapshotDigest: z.string().regex(/^[0-9a-f]{64}$/),
    objects: z.array(objectSchema),
  })
  .strict();
type Bundle = z.infer<typeof bundleSchema>;

async function atomicJson(filename: string, value: unknown, replace = false): Promise<void> {
  const temporary = `${filename}.${randomUUID()}.tmp`;
  const file = await open(temporary, "wx", 0o600);
  try {
    try {
      await file.writeFile(JSON.stringify(value) + "\n");
      await file.sync();
    } finally {
      await file.close();
    }
    if (replace) await rename(temporary, filename);
    else await link(temporary, filename);
    const directory = await open(path.dirname(path.resolve(filename)), "r");
    try {
      await directory.sync();
    } finally {
      await directory.close();
    }
  } finally {
    await unlink(temporary).catch(() => {});
  }
}
async function localArtifact(directory: string, reference: string): Promise<Uint8Array> {
  if (
    !reference ||
    reference.includes("\\") ||
    reference.includes("\0") ||
    path.posix.isAbsolute(reference) ||
    reference.split("/").some((segment) => segment === ".." || segment === "." || segment === "")
  )
    throw new ApplicationImportError("artifact");
  const base = await realpath(directory),
    filename = await realpath(path.join(base, reference));
  if (!filename.startsWith(base + path.sep)) throw new ApplicationImportError("artifact");
  const size = (await stat(filename)).size;
  if (size > APPLICATION_SNAPSHOT_LIMITS.blobBytes) throw new ApplicationImportError("limits");
  const bytes = await readFile(filename);
  if (bytes.byteLength !== size) throw new ApplicationImportError("artifact");
  return bytes;
}
function references(snapshot: ReturnType<typeof checkedApplicationSnapshot>): string[] {
  const result = new Set<string>();
  for (const table of snapshot.tables.filter((table) =>
    ["call_logs", "call_logs_v1_legacy"].includes(table.name)
  )) {
    const at = table.columns.indexOf("artifact_relpath");
    if (at < 0) continue;
    for (const row of table.rows) {
      const value = row.values[at];
      if (typeof value === "string" && value !== "") result.add(value);
    }
  }
  return [...result];
}
async function saveBundle(
  filename: string,
  snapshot: ReturnType<typeof checkedApplicationSnapshot>,
  read: (reference: string) => Promise<Uint8Array>
): Promise<Bundle> {
  const directory = path.dirname(path.resolve(filename));
  await mkdir(path.join(directory, "objects"), { recursive: true, mode: 0o700 });
  const objects: Bundle["objects"] = [];
  let bytesTotal = 0;
  for (const reference of references(snapshot)) {
    const bytes = await read(reference);
    if (bytes.byteLength > APPLICATION_SNAPSHOT_LIMITS.blobBytes)
      throw new ApplicationImportError("limits");
    bytesTotal += bytes.byteLength;
    if (bytesTotal > APPLICATION_SNAPSHOT_LIMITS.bytes) throw new ApplicationImportError("limits");
    const checksum = digest(bytes),
      file = `objects/${checksum}.bin`,
      target = path.join(directory, file);
    try {
      await writeFile(target, bytes, { mode: 0o600, flag: "wx" });
    } catch (error) {
      if (
        (error as { code?: unknown })?.code !== "EEXIST" ||
        digest(await readFile(target)) !== checksum
      )
        throw new ApplicationImportError("artifact");
    }
    objects.push({ reference, file, checksum, sizeBytes: bytes.byteLength });
  }
  const bundle: Bundle = {
    format: "omniroute-offline-storage-bundle-v1",
    snapshot,
    snapshotDigest: applicationSnapshotDigest(snapshot),
    objects,
  };
  await atomicJson(filename, bundle);
  return bundle;
}
async function loadBundle(filename: string): Promise<Bundle> {
  if ((await stat(filename)).size > APPLICATION_SNAPSHOT_LIMITS.bytes)
    throw new ApplicationImportError("limits");
  const result = bundleSchema.safeParse(JSON.parse(await readFile(filename, "utf8")));
  if (!result.success) throw new ApplicationImportError("schema");
  const bundle = result.data,
    snapshot = checkedApplicationSnapshot(bundle.snapshot);
  if (
    applicationSnapshotDigest(snapshot) !== bundle.snapshotDigest ||
    new Set(bundle.objects.map((object) => object.reference)).size !== bundle.objects.length
  )
    throw new ApplicationImportError("verification");
  for (const reference of references(snapshot))
    if (!bundle.objects.some((object) => object.reference === reference))
      throw new ApplicationImportError("artifact");
  return bundle;
}
function carrierSink(filename: string): NativeBlobSink {
  const directory = path.join(path.dirname(path.resolve(filename)), "blobs");
  // An existing carrier is never overwritten; a failed capture leaves an incomplete private carrier.
  mkdirSync(directory, { mode: 0o700 });
  const directories = new Set<string>();
  return {
    writeChunk(id, index, bytes) {
      if (
        !/^[0-9a-f-]{36}$/.test(id) ||
        !Number.isSafeInteger(index) ||
        index < 0 ||
        index >= NATIVE_BLOB_LIMITS.fileBytes / NATIVE_BLOB_LIMITS.chunkBytes ||
        bytes.byteLength > NATIVE_BLOB_LIMITS.chunkBytes
      )
        throw new NativeBlobCarrierError("limits");
      const folder = path.join(directory, id);
      if (!directories.has(id)) {
        mkdirSync(folder, { mode: 0o700 });
        directories.add(id);
      }
      writeFileSync(path.join(folder, `${index}.bin`), bytes, { mode: 0o600, flag: "wx" });
    },
  };
}
function carrierSource(filename: string): NativeBlobSource {
  const directory = path.dirname(path.resolve(filename));
  return {
    async readChunk(id, index) {
      if (
        !/^[0-9a-f-]{36}$/.test(id) ||
        !Number.isSafeInteger(index) ||
        index < 0 ||
        index >= NATIVE_BLOB_LIMITS.fileBytes / NATIVE_BLOB_LIMITS.chunkBytes
      )
        throw new NativeBlobCarrierError("verification");
      try {
        return await localArtifact(directory, `blobs/${id}/${index}.bin`);
      } catch {
        throw new NativeBlobCarrierError("verification");
      }
    },
  };
}
async function main(): Promise<void> {
  const { values } = parseArgs({
    allowPositionals: false,
    options: {
      action: { type: "string" },
      source: { type: "string" },
      bundle: { type: "string" },
      "artifact-dir": { type: "string" },
      offline: { type: "boolean", default: false },
      "large-files": { type: "boolean", default: false },
      "memory-budget-mib": { type: "string" },
    },
  });
  const action = values.action;
  if (
    !values.offline ||
    !values.bundle ||
    !["capture", "import", "verify", "export", "restore"].includes(action ?? "")
  )
    throw new ApplicationImportError("schema");
  requireExplicitStorageEncryptionKey();
  const sink = () => (values["large-files"] ? carrierSink(values.bundle!) : undefined);
  if (action === "capture") {
    if (!values.source) throw new ApplicationImportError("schema");
    const source = openOfflineSqliteSource(values.source, { largeFiles: values["large-files"] });
    let snapshot: ReturnType<typeof captureSqliteApplicationSnapshot>;
    try {
      snapshot = captureSqliteApplicationSnapshot(source, { blobSink: sink() });
    } finally {
      source.close();
    }
    const bundle = await saveBundle(values.bundle, snapshot, async (reference) => {
      if (!values["artifact-dir"]) throw new ApplicationImportError("artifact");
      return localArtifact(values["artifact-dir"], reference);
    });
    process.stdout.write(
      JSON.stringify({
        format: bundle.format,
        snapshotDigest: bundle.snapshotDigest,
        objects: bundle.objects.length,
      }) + "\n"
    );
    return;
  }
  const database = await createPostgresDatabase(
    { ...(await readPostgresEnvironment()), transactionTimeoutMs: 300000 },
    {
      warn() {
        process.stderr.write("PostgreSQL connection warning\n");
      },
    }
  );
  let store: NativeArtifactStore | undefined;
  const artifacts = () => (store ??= createS3ArtifactStore(readS3ArtifactConfiguration()));
  try {
    if (action === "import" || action === "restore") await migratePostgresSchema(database);
    if (action === "export") {
      const snapshot = await capturePostgresApplicationSnapshot(database, { blobSink: sink() }),
        bundle = await saveBundle(values.bundle, snapshot, async (reference) => {
          const bytes = await artifacts().get(reference);
          if (!bytes) throw new ApplicationImportError("artifact");
          return bytes;
        });
      process.stdout.write(
        JSON.stringify({
          format: bundle.format,
          snapshotDigest: bundle.snapshotDigest,
          objects: bundle.objects.length,
        }) + "\n"
      );
      return;
    }
    const bundle = await loadBundle(values.bundle),
      snapshot = checkedApplicationSnapshot(bundle.snapshot);
    if (
      (action === "import" && snapshot.source !== "sqlite") ||
      (action === "restore" && snapshot.source !== "postgres")
    )
      throw new ApplicationImportError("schema");
    validateApplicationSnapshotCredentials(snapshot);
    const carrierReferences = new Map<string, Parameters<typeof verifyNativeBlob>[0]>();
    for (const table of snapshot.tables)
      for (const row of table.rows)
        for (const cell of row.values)
          if (cell && typeof cell === "object" && cell.kind === "blob-reference")
            carrierReferences.set(cell.id, cell);
    const memoryBudgetBytes =
      values["memory-budget-mib"] && /^[1-9][0-9]*$/.test(values["memory-budget-mib"])
        ? Number(values["memory-budget-mib"]) * 1024 * 1024
        : 0;
    if (carrierReferences.size && !values["large-files"])
      throw new NativeBlobCarrierError("limits");
    if (
      carrierReferences.size &&
      (action === "import" || action === "restore") &&
      (!Number.isSafeInteger(memoryBudgetBytes) ||
        memoryBudgetBytes < NATIVE_BLOB_LIMITS.importMemoryBudgetBytes)
    )
      throw new NativeBlobCarrierError("limits");
    const blobs = { source: carrierSource(values.bundle!), memoryBudgetBytes };
    for (const reference of carrierReferences.values())
      await verifyNativeBlob(reference, blobs.source);
    let receipt = await readApplicationImportReceipt(database, snapshot);
    if (action === "verify" || receipt) receipt = await verifyApplicationImport(database, snapshot);
    else {
      const filename = `${values.bundle}.uploads.json`;
      let prior: (OfflineArtifactMapping | OfflineArtifactCheckpoint)[] = [];
      try {
        prior = JSON.parse(await readFile(filename, "utf8")) as (
          OfflineArtifactMapping | OfflineArtifactCheckpoint
        )[];
      } catch (error) {
        if ((error as { code?: unknown })?.code !== "ENOENT")
          throw new ApplicationImportError("artifact");
      }
      const mappings = await prepareApplicationArtifacts(
        snapshot,
        {
          readSource: async (reference) => {
            const object = bundle.objects.find((value) => value.reference === reference);
            if (!object) throw new ApplicationImportError("artifact");
            const bytes = await localArtifact(
              path.dirname(path.resolve(values.bundle!)),
              object.file
            );
            if (bytes.byteLength !== object.sizeBytes || digest(bytes) !== object.checksum)
              throw new ApplicationImportError("artifact");
            return bytes;
          },
          target: references(snapshot).length
            ? artifacts()
            : {
                async put() {
                  throw new ApplicationImportError("artifact");
                },
                async get() {
                  return null;
                },
              },
          checkpoint: async (mappings) => atomicJson(filename, mappings, true),
        },
        prior
      );
      receipt = await importApplicationSnapshot(database, snapshot, mappings, store, blobs);
    }
    if (receipt.artifacts.length) {
      try {
        await verifyApplicationArtifacts(receipt.artifacts, artifacts());
      } catch {
        throw new ApplicationImportEffectsError();
      }
    }
    process.stdout.write(
      JSON.stringify({
        format: receipt.format,
        sourceDigest: receipt.sourceDigest,
        planDigest: receipt.planDigest,
        schemaDigest: receipt.schemaDigest,
        rowCount: receipt.rowCount,
        tableCounts: receipt.tableCounts,
        artifacts: receipt.artifacts.length,
        transformations: receipt.transformations.length,
        alreadyImported: receipt.alreadyImported,
      }) + "\n"
    );
  } finally {
    await store?.close();
    await database.close();
  }
}
main().catch((error: unknown) => {
  process.stderr.write(
    isPostgresErrorKind(error, "commit_unknown")
      ? "Import COMMIT outcome is unknown. Rerun with the same bundle to reconcile the durable receipt; uploaded objects were retained.\n"
      : error instanceof NativeBlobCarrierError
        ? `Offline binary carrier action failed (${error.kind}); no qualified import receipt was produced by this action.\n`
        : error instanceof ApplicationImportEffectsError
          ? "Import committed; artifact_readback failed. Rerun with the same bundle to verify the durable receipt and retained objects.\n"
          : error instanceof ApplicationImportError
            ? `Offline storage action failed (${error.kind}); source and uploaded objects were retained.\n`
            : "Offline storage action failed; source and uploaded objects were retained.\n"
  );
  process.exitCode = 1;
});
