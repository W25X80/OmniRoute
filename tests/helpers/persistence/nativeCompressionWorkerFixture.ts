import type { CompressionWorkerOptions } from "../../../open-sse/services/compression/compressionWorkerProtocol";
export const workerFixture = {
  mode: "fast" as "fast" | "timeout" | "success",
  calls: 0,
  options: undefined as CompressionWorkerOptions | undefined,
};
export async function runCompressionInWorker(
  body: Record<string, unknown>,
  _mode: unknown,
  options?: CompressionWorkerOptions
) {
  workerFixture.calls++;
  workerFixture.options = options;
  if (workerFixture.mode === "success")
    return { body: { ...body, workerReceipt: true }, compressed: false, stats: null };
  throw Object.assign(new Error("Controlled worker fault"), {
    retryInProcess: workerFixture.mode !== "timeout",
  });
}
