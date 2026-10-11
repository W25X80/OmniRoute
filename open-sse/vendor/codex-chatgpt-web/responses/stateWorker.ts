import { parentPort, workerData } from "node:worker_threads";
import {
  expandPreviousResponseInput,
  flushResponseState,
  previousResponseReplayPrefixLength,
  rememberResponseState,
} from "./state.ts";
import type { ResponseStateWorkerRequest, ResponseStateWorkerReply } from "./stateWorkerProtocol";

if (typeof workerData?.home !== "string" || !workerData.home) {
  throw new Error("Continuation cache worker home is missing");
}
process.env.CODEX_CHATGPT_WEB_HOME = workerData.home;

parentPort?.on("message", (request: ResponseStateWorkerRequest) => {
  if (!Number.isSafeInteger(request?.id) || request.id < 1) return;
  let reply: ResponseStateWorkerReply = { id: request.id, kind: "error" };
  try {
    if (request.kind === "lookup") {
      const body = { previous_response_id: request.responseId, input: [] };
      const expanded = expandPreviousResponseInput(body, request.namespace);
      reply = {
        id: request.id,
        kind: "lookup",
        items: expanded === body ? null : (expanded as { input: unknown[] }).input,
        prefixLength: previousResponseReplayPrefixLength(expanded),
      };
    } else if (request.kind === "remember") {
      rememberResponseState(request.request, request.response, request.options);
      reply = { id: request.id, kind: "ack" };
    } else if (request.kind === "flush") {
      flushResponseState();
      reply = { id: request.id, kind: "ack" };
    }
  } catch {
    // A cache failure never transfers paths, payloads, credentials or raw stacks.
  }
  try {
    parentPort?.postMessage(reply);
  } catch {
    parentPort?.postMessage({ id: request.id, kind: "error" });
  }
});
