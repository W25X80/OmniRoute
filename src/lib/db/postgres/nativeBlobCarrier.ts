import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";

/** Offline file migration only; these bounds do not change runtime file or artifact APIs. */
export const NATIVE_BLOB_LIMITS = {
  fileBytes: 512 * 1024 * 1024,
  chunkBytes: 8 * 1024 * 1024,
  carrierBytes: 10 * 1024 * 1024 * 1024,
  importMemoryBudgetBytes: 2 * 1024 * 1024 * 1024,
} as const;
const checksum = z.string().regex(/^[0-9a-f]{64}$/);
export const nativeBlobReferenceSchema = z
  .object({
    kind: z.literal("blob-reference"),
    id: z.string().uuid(),
    sizeBytes: z.number().int().min(0).max(NATIVE_BLOB_LIMITS.fileBytes),
    checksum,
    chunks: z
      .array(
        z
          .object({
            sizeBytes: z.number().int().min(1).max(NATIVE_BLOB_LIMITS.chunkBytes),
            checksum,
          })
          .strict()
      )
      .max(NATIVE_BLOB_LIMITS.fileBytes / NATIVE_BLOB_LIMITS.chunkBytes),
  })
  .strict();
export type NativeBlobReference = z.infer<typeof nativeBlobReferenceSchema>;
export interface NativeBlobSource {
  readChunk(id: string, index: number): Promise<Uint8Array | null>;
}
/** Synchronous writes keep SQLite capture inside one consistent read transaction. */
export interface NativeBlobSink {
  writeChunk(id: string, index: number, bytes: Uint8Array): void;
}
export class NativeBlobCarrierError extends Error {
  constructor(readonly kind: "limits" | "verification") {
    super(`Offline binary carrier qualification failed (${kind})`);
    this.name = "NativeBlobCarrierError";
  }
}
const hash = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
export function checkedNativeBlobReference(input: unknown): NativeBlobReference {
  const parsed = nativeBlobReferenceSchema.safeParse(input);
  if (!parsed.success) throw new NativeBlobCarrierError("limits");
  const value = parsed.data;
  if (
    value.chunks.length !== Math.ceil(value.sizeBytes / NATIVE_BLOB_LIMITS.chunkBytes) ||
    value.chunks.some(
      (chunk, index) =>
        chunk.sizeBytes !==
        Math.min(
          NATIVE_BLOB_LIMITS.chunkBytes,
          value.sizeBytes - index * NATIVE_BLOB_LIMITS.chunkBytes
        )
    )
  )
    throw new NativeBlobCarrierError("verification");
  if (value.sizeBytes === 0 && value.checksum !== hash(new Uint8Array()))
    throw new NativeBlobCarrierError("verification");
  return value;
}
export function captureNativeBlob(
  sizeBytes: number,
  readChunk: (offset: number, length: number) => Uint8Array | null,
  sink: NativeBlobSink
): NativeBlobReference {
  if (!Number.isSafeInteger(sizeBytes) || sizeBytes < 0 || sizeBytes > NATIVE_BLOB_LIMITS.fileBytes)
    throw new NativeBlobCarrierError("limits");
  const id = randomUUID(),
    whole = createHash("sha256");
  const chunks: NativeBlobReference["chunks"] = [];
  for (let offset = 0; offset < sizeBytes; offset += NATIVE_BLOB_LIMITS.chunkBytes) {
    const length = Math.min(NATIVE_BLOB_LIMITS.chunkBytes, sizeBytes - offset);
    const bytes = readChunk(offset, length);
    if (!(bytes instanceof Uint8Array) || bytes.byteLength !== length)
      throw new NativeBlobCarrierError("verification");
    whole.update(bytes);
    chunks.push({ sizeBytes: length, checksum: hash(bytes) });
    const written = sink.writeChunk(id, chunks.length - 1, bytes);
    if (written !== undefined) throw new NativeBlobCarrierError("verification");
  }
  return checkedNativeBlobReference({
    kind: "blob-reference",
    id,
    sizeBytes,
    checksum: whole.digest("hex"),
    chunks,
  });
}
export async function captureNativeBlobAsync(
  sizeBytes: number,
  readChunk: (offset: number, length: number) => Promise<Uint8Array | null>,
  sink: NativeBlobSink
): Promise<NativeBlobReference> {
  if (!Number.isSafeInteger(sizeBytes) || sizeBytes < 0 || sizeBytes > NATIVE_BLOB_LIMITS.fileBytes)
    throw new NativeBlobCarrierError("limits");
  const id = randomUUID(),
    whole = createHash("sha256");
  const chunks: NativeBlobReference["chunks"] = [];
  for (let offset = 0; offset < sizeBytes; offset += NATIVE_BLOB_LIMITS.chunkBytes) {
    const length = Math.min(NATIVE_BLOB_LIMITS.chunkBytes, sizeBytes - offset);
    const bytes = await readChunk(offset, length);
    if (!(bytes instanceof Uint8Array) || bytes.byteLength !== length)
      throw new NativeBlobCarrierError("verification");
    whole.update(bytes);
    chunks.push({ sizeBytes: length, checksum: hash(bytes) });
    const written = sink.writeChunk(id, chunks.length - 1, bytes);
    if (written !== undefined) throw new NativeBlobCarrierError("verification");
  }
  return checkedNativeBlobReference({
    kind: "blob-reference",
    id,
    sizeBytes,
    checksum: whole.digest("hex"),
    chunks,
  });
}
/** Verify before SQL, then recheck the same chunks when assembling each native parameter. */
async function consume(
  reference: NativeBlobReference,
  source: NativeBlobSource,
  target?: Uint8Array
): Promise<void> {
  const checked = checkedNativeBlobReference(reference),
    whole = createHash("sha256");
  let offset = 0;
  for (let index = 0; index < checked.chunks.length; index++) {
    const expected = checked.chunks[index],
      bytes = await source.readChunk(checked.id, index);
    if (
      !(bytes instanceof Uint8Array) ||
      bytes.byteLength !== expected.sizeBytes ||
      hash(bytes) !== expected.checksum
    )
      throw new NativeBlobCarrierError("verification");
    whole.update(bytes);
    target?.set(bytes, offset);
    offset += bytes.byteLength;
  }
  if (offset !== checked.sizeBytes || whole.digest("hex") !== checked.checksum)
    throw new NativeBlobCarrierError("verification");
}
export async function verifyNativeBlob(
  reference: NativeBlobReference,
  source: NativeBlobSource
): Promise<void> {
  await consume(reference, source);
}
export async function assembleNativeBlob(
  reference: NativeBlobReference,
  source: NativeBlobSource,
  memoryBudgetBytes: number
): Promise<Buffer> {
  const checked = checkedNativeBlobReference(reference);
  if (
    !Number.isSafeInteger(memoryBudgetBytes) ||
    memoryBudgetBytes < NATIVE_BLOB_LIMITS.importMemoryBudgetBytes
  )
    throw new NativeBlobCarrierError("limits");
  const result = Buffer.allocUnsafe(checked.sizeBytes);
  await consume(checked, source, result);
  return result;
}
