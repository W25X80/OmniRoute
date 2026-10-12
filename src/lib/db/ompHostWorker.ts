import path from "node:path";
import { getOmpCredentials, saveOmpCredentials, deleteOmpCredentials } from "./omp";
import {
  OMP_HOST_MAX_FRAME_BYTES,
  isOmpHostOperation,
  ompHostFrameBytes,
  type OmpHostReply,
} from "./ompHostProtocol";

const databasePath = process.argv[2];
if (!databasePath || !path.isAbsolute(databasePath))
  throw new Error("OMP host storage is unavailable");
const capability = { kind: "native-host-sqlite" as const, databasePath, driverAvailable: true };
let closing = false;
function close(): void {
  if (closing) return;
  closing = true;
  if (process.connected) process.disconnect();
}
process.on("disconnect", close);
process.on("message", (message: unknown) => {
  if (!message || typeof message !== "object" || closing) return;
  const request = message as Record<string, unknown>;
  if (request.kind === "close") {
    close();
    return;
  }
  if (request.kind !== "operation" || !Number.isSafeInteger(request.id) || Number(request.id) < 1)
    return;
  let reply: OmpHostReply = { kind: "result", id: Number(request.id), ok: false };
  try {
    if (
      ompHostFrameBytes(request) > OMP_HOST_MAX_FRAME_BYTES ||
      !isOmpHostOperation(request.operation)
    )
      throw new Error("Invalid OMP host operation");
    const operation = request.operation;
    if (operation.kind === "get") {
      reply = {
        kind: "result",
        id: Number(request.id),
        ok: true,
        credentials: getOmpCredentials(operation.providerId, capability),
      };
    } else {
      if (operation.kind === "save")
        saveOmpCredentials(operation.providerId, operation.apiKey, operation.baseUrl, capability);
      else deleteOmpCredentials(operation.providerId, capability);
      reply = { kind: "result", id: Number(request.id), ok: true };
    }
    if (ompHostFrameBytes(reply) > OMP_HOST_MAX_FRAME_BYTES)
      reply = { kind: "result", id: Number(request.id), ok: false };
  } catch {
    // Preserve source read-empty/missing-driver behavior inside its owner. Transport/operation
    // failure is bounded and credential-free; no mutation is replayed after a missing ACK.
  }
  process.send?.(reply, (error) => {
    if (error) close();
  });
});
