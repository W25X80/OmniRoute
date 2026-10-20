import { parentPort } from "node:worker_threads";
import { serializeArtifactForStorage } from "./callLogArtifactSerialization.ts";
import { MAX_CALL_LOG_DECODE_BYTES } from "./callLogArtifactProtocol.ts";
import { decodeStagedCallLog } from "./stagedCallLogDecode.ts";
import type { CallLogArtifact } from "./callLogArtifacts.ts";
import type {
  CallLogArtifactEnvironment,
  CallLogArtifactWorkerRequest,
  CallLogArtifactWorkerReply,
} from "./callLogArtifactProtocol.ts";

function applyWriteEnvironment(environment: CallLogArtifactEnvironment): void {
  const values = {
    CALL_LOG_PIPELINE_MAX_SIZE_KB: environment.pipelineMaxSizeKb,
    CHAT_DEBUG_FILE: environment.chatDebugFile,
    APP_LOG_LEVEL: environment.appLogLevel,
  };
  for (const [name, value] of Object.entries(values)) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
}

parentPort?.on("message", async (request: CallLogArtifactWorkerRequest) => {
  let reply: CallLogArtifactWorkerReply =
    request.kind === "deserialize" || request.kind === "deserialize-staged"
      ? { id: request.id, kind: request.kind, result: null }
      : request.kind === "serialize"
        ? { id: request.id, kind: "serialize", bytes: null }
        : { id: request.id, kind: "write", result: null };
  try {
    if (request.kind === "deserialize-staged") {
      reply = { id: request.id, kind: request.kind, result: decodeStagedCallLog(request.staging) };
    } else if (request.kind === "deserialize") {
      if (
        request.bytes instanceof Uint8Array &&
        request.bytes.byteLength <= MAX_CALL_LOG_DECODE_BYTES
      ) {
        try {
          const artifact = JSON.parse(
            Buffer.from(request.bytes).toString("utf8")
          ) as CallLogArtifact;
          reply = { id: request.id, kind: "deserialize", result: { artifact, state: "ready" } };
        } catch {
          reply = {
            id: request.id,
            kind: "deserialize",
            result: { artifact: null, state: "corrupt" },
          };
        }
      }
    } else {
      applyWriteEnvironment(request.environment);
      if (request.kind === "serialize") {
        reply = {
          id: request.id,
          kind: "serialize",
          bytes: new TextEncoder().encode(serializeArtifactForStorage(request.artifact)),
        };
      } else {
        // SQLite alone loads the filesystem owner. PostgreSQL jobs never resolve a data path.
        const { writeCallArtifact } = await import("./callLogArtifactFiles.ts");
        const originalConsoleError = console.error;
        try {
          console.error = () => {};
          reply = { id: request.id, kind: "write", result: writeCallArtifact(request.artifact) };
        } finally {
          console.error = originalConsoleError;
        }
      }
    }
  } catch {
    // Only a typed generic failure crosses the worker boundary, never paths or payloads.
  }
  try {
    const transfer =
      reply.kind === "serialize" && reply.bytes?.buffer instanceof ArrayBuffer
        ? [reply.bytes.buffer]
        : [];
    parentPort?.postMessage(reply, transfer);
  } catch {
    parentPort?.postMessage(
      request.kind === "deserialize" || request.kind === "deserialize-staged"
        ? { id: request.id, kind: request.kind, result: null }
        : request.kind === "serialize"
          ? { id: request.id, kind: "serialize", bytes: null }
          : { id: request.id, kind: "write", result: null }
    );
  }
});
