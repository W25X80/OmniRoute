import { basename } from "node:path";
import { parentPort } from "node:worker_threads";
import { unzipSync } from "fflate";

parentPort?.once("message", (request: { archive: Uint8Array; executableName: string }) => {
  try {
    if (
      !(request?.archive instanceof Uint8Array) ||
      request.archive.byteLength > 100 * 1024 * 1024 ||
      (request.executableName !== "tunnel-client" && request.executableName !== "tunnel-client.exe")
    ) {
      throw new Error("Invalid tunnel archive request");
    }
    const files = unzipSync(request.archive);
    const entry = Object.entries(files).find(([name]) => basename(name) === request.executableName);
    if (!entry) {
      parentPort?.postMessage({ kind: "missing" });
    } else {
      const binary = entry[1];
      const transferable =
        binary.buffer instanceof ArrayBuffer &&
        binary.byteOffset === 0 &&
        binary.byteLength === binary.buffer.byteLength
          ? binary
          : binary.slice();
      parentPort?.postMessage({ kind: "binary", binary: transferable }, [transferable.buffer]);
    }
  } catch {
    parentPort?.postMessage({ kind: "error" });
  } finally {
    parentPort?.close();
  }
});
