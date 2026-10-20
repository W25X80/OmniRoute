import { parentPort } from "node:worker_threads";
import type { CallLogArtifactWorkerRequest } from "../../../src/lib/usage/callLogArtifactProtocol.ts";
import { decodeStagedCallLog } from "../../../src/lib/usage/stagedCallLogDecode.ts";

let pending: Extract<CallLogArtifactWorkerRequest, { kind: "deserialize-staged" }> | undefined;
parentPort?.on("message", (request: CallLogArtifactWorkerRequest | { release: true }) => {
  if ("release" in request) {
    if (!pending) throw new Error("No staged job to release");
    parentPort?.postMessage({
      id: pending.id,
      kind: pending.kind,
      result: decodeStagedCallLog(pending.staging),
    });
    pending = undefined;
  } else {
    if (request.kind !== "deserialize-staged") throw new Error("Unexpected fixture job");
    pending = request;
    parentPort?.postMessage({ id: -1, marker: "entered" });
  }
});
