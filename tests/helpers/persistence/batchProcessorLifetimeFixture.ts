import assert from "node:assert/strict";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import type { BatchItemCheckpoint, BatchRecord } from "../../../src/lib/db/batches.ts";
import type { NativeFileWork } from "../../../src/lib/db/nativeFileWork.ts";
import type { FileRecord } from "../../../src/lib/db/files.ts";
import { fixture } from "./runtimeOwnedWorkFixture.ts";

export const batchFixture = {
  events: [] as Array<{ operation: string; generation: number }>,
  batch: undefined as BatchRecord | undefined,
  checkpoints: new Map<number, BatchItemCheckpoint>(),
  files: new Map<string, FileRecord>(),
  pollGate: undefined as Promise<void> | undefined,
  pollEntered: undefined as (() => void) | undefined,
  dispatchGate: undefined as Promise<Response> | undefined,
  workerGate: undefined as Promise<void> | undefined,
  workerEntered: undefined as (() => void) | undefined,
  workerFailure: undefined as Error | undefined,
  dispatchEntered: undefined as (() => void) | undefined,
  reset() {
    this.events = [];
    this.checkpoints.clear();
    this.files.clear();
    this.pollGate = undefined;
    this.pollEntered = undefined;
    this.dispatchGate = undefined;
    this.workerGate = undefined;
    this.workerEntered = undefined;
    this.workerFailure = undefined;
    this.dispatchEntered = undefined;
    this.batch = {
      id: "native-lifetime-batch",
      endpoint: "/v1/embeddings",
      completionWindow: "24h",
      status: "validating",
      inputFileId: "native-input",
      apiKeyId: "owner-key",
      createdAt: Math.floor(Date.now() / 1000),
      requestCountsTotal: 0,
      requestCountsCompleted: 0,
      requestCountsFailed: 0,
    };
    const content = Buffer.from(
      JSON.stringify({
        method: "POST",
        url: "/v1/embeddings",
        custom_id: "owned-item",
        body: { model: "fixture-model", input: "hello" },
      })
    );
    this.files.set("native-input", {
      id: "native-input",
      bytes: content.byteLength,
      createdAt: this.batch.createdAt,
      purpose: "batch",
      filename: "input.jsonl",
      content,
    });
  },
};

function scoped(operation: string) {
  getSelectedControlPlane()!.assertReady();
  const generation = fixture.selectedGeneration();
  assert.equal(typeof generation, "number", `${operation} requires a fresh catalog scope`);
  batchFixture.events.push({ operation, generation: generation! });
}
export async function getPendingBatchesAsync() {
  scoped("pending");
  if (batchFixture.pollGate) {
    const gate = batchFixture.pollGate;
    batchFixture.pollGate = undefined;
    batchFixture.pollEntered?.();
    await gate;
    scoped("pending-resumed");
  }
  const batch = batchFixture.batch;
  return batch && ["validating", "in_progress", "finalizing", "cancelling"].includes(batch.status)
    ? [{ ...batch }]
    : [];
}
export async function getTerminalBatchesAsync() {
  scoped("terminal");
  return [];
}
export async function getBatchAsync() {
  scoped("batch-read");
  return batchFixture.batch ? { ...batchFixture.batch } : null;
}
export async function updateBatchAsync(_id: string, changes: Partial<BatchRecord>) {
  scoped(`batch-write:${changes.status ?? "progress"}`);
  Object.assign(batchFixture.batch!, changes);
  return batchFixture.batch;
}
export async function countBatchItemCheckpointsAsync() {
  scoped("checkpoint-count");
  return batchFixture.checkpoints.size;
}
export async function ensureBatchItemCheckpointsAsync(
  batchId: string,
  items: Array<{ lineNumber: number; customId: string | null }>
) {
  scoped("checkpoint-ensure");
  const now = Math.floor(Date.now() / 1000);
  for (const item of items)
    batchFixture.checkpoints.set(item.lineNumber, {
      batchId,
      ...item,
      status: "pending",
      result: null,
      error: null,
      createdAt: now,
      updatedAt: now,
    });
}
export async function listBatchItemCheckpointsAsync() {
  scoped("checkpoint-list");
  if (batchFixture.workerGate) {
    batchFixture.workerEntered?.();
    await batchFixture.workerGate;
    scoped("checkpoint-list-resumed");
  }
  if (batchFixture.workerFailure) throw batchFixture.workerFailure;
  return [...batchFixture.checkpoints.values()];
}
export async function markBatchItemProcessingAsync(_id: string, item: { lineNumber: number }) {
  scoped("checkpoint-processing");
  batchFixture.checkpoints.get(item.lineNumber)!.status = "processing";
}
export async function markBatchItemResultAsync(
  _id: string,
  item: { lineNumber: number },
  result: unknown
) {
  scoped("checkpoint-result");
  Object.assign(batchFixture.checkpoints.get(item.lineNumber)!, { status: "completed", result });
}
export async function markBatchItemErrorAsync(
  _id: string,
  item: { lineNumber: number },
  error: unknown
) {
  scoped("checkpoint-error");
  Object.assign(batchFixture.checkpoints.get(item.lineNumber)!, { status: "errored", error });
}
export async function isFileReferencedByOtherBatchAsync() {
  scoped("file-reference");
  return false;
}
export async function getFileContentAsync(id: string) {
  scoped("file-read");
  return batchFixture.files.get(id)?.content ?? null;
}
export async function getFileContentForConsumerAsync(id: string, consumer: NativeFileWork) {
  scoped("file-consumer-read");
  consumer.assertActive();
  const content = batchFixture.files.get(id)?.content ?? null;
  if (content) consumer.reserveDownload(content.byteLength);
  scoped("file-consumer-reserved");
  return content;
}
export async function deleteExpiredBatchFilesAsync(_id: string, _now?: number): Promise<void> {
  scoped("file-expiry");
  // This fixture exposes no terminal batches; no expiry candidate can reach a deletion.
  assert.equal((await getTerminalBatchesAsync()).length, 0);
}
export async function listFilesAsync() {
  scoped("file-list");
  return [...batchFixture.files.values()];
}
export async function deleteFileAsync(id: string) {
  scoped("file-delete");
  batchFixture.files.delete(id);
}
export async function createFileAsync(file: Omit<FileRecord, "id" | "createdAt">) {
  scoped("file-create");
  const record = {
    ...file,
    id: `native-output-${batchFixture.files.size}`,
    createdAt: Math.floor(Date.now() / 1000),
  };
  batchFixture.files.set(record.id, record);
  return record;
}
export async function getApiKeyById() {
  scoped("key-read");
  return { key: "fixture-owner-key" };
}
export const dispatch = {
  async dispatchBatchApiRequest() {
    scoped("dispatch");
    batchFixture.dispatchEntered?.();
    return batchFixture.dispatchGate ?? new Response(JSON.stringify({ data: [] }), { status: 200 });
  },
};

export async function transitionBatchAsync(
  id: string,
  expected: readonly BatchRecord["status"][],
  changes: Partial<BatchRecord> & { status: BatchRecord["status"] },
  discardOutputs = false
) {
  const current = await getBatchAsync();
  if (!current || !expected.includes(current.status)) return { changed: false, batch: current };
  await updateBatchAsync(id, changes);
  if (discardOutputs) {
    for (const fileId of [current.outputFileId, current.errorFileId])
      if (fileId) await deleteFileAsync(fileId);
  }
  return { changed: true, batch: await getBatchAsync() };
}
export async function completeBatchWithFilesAsync(
  id: string,
  completion: import("../../../src/lib/db/batches.ts").BatchCompletion,
  output: import("../../../src/lib/db/batches.ts").BatchOutputFile | null,
  error: import("../../../src/lib/db/batches.ts").BatchOutputFile | null
) {
  const current = await getBatchAsync();
  if (current?.status === "cancelling")
    return transitionBatchAsync(id, ["cancelling"], { status: "cancelled" });
  if (!current || current.status !== "finalizing") return { changed: false, batch: current };
  const save = async (file: typeof output) =>
    file
      ? (
          await createFileAsync({
            ...file,
            bytes: file.content.length,
            purpose: "batch_output",
            apiKeyId: current.apiKeyId,
          })
        ).id
      : null;
  return transitionBatchAsync(id, ["finalizing"], {
    ...completion,
    status: "completed",
    outputFileId: await save(output),
    errorFileId: await save(error),
  });
}
