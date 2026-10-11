import { createRequire } from "node:module";
import {
  configureCodeGraphWorker,
  closeCodeGraphWorkerDatabase,
  searchSymbols,
  findCallers,
  findCallees,
  getFileContext,
  listFiles,
  getCodeGraphStats,
} from "./codegraphKnowledge";
import { formatCodeGraphResult } from "./codegraphResult";
import type { CodeGraphWorkerRequest, CodeGraphWorkerReply } from "./codegraphQueryProtocol";

const load = createRequire(typeof __filename === "string" ? __filename : import.meta.url);
const operations = {
  searchSymbols,
  findCallers,
  findCallees,
  getFileContext,
  listFiles,
  getCodeGraphStats,
};
let closing = false;
function close(): void {
  if (closing) return;
  closing = true;
  closeCodeGraphWorkerDatabase();
  process.disconnect?.();
}
process.on("disconnect", close);
process.on("message", (message: CodeGraphWorkerRequest) => {
  if (message.kind === "close") {
    close();
    return;
  }
  if (closing || message.kind !== "query") return;
  let reply: CodeGraphWorkerReply;
  try {
    if (!Object.hasOwn(operations, message.operation)) throw new Error("Unknown operation");
    process.chdir(message.context.cwd);
    configureCodeGraphWorker(message.context, load);
    const operation = operations[message.operation] as (
      ...args: unknown[]
    ) => ReturnType<typeof getCodeGraphStats>;
    // Formatting here preserves Buffer JSON and keeps the source's 30-row projection off-thread.
    const output = formatCodeGraphResult(operation(...message.args));
    reply = { kind: "result", id: message.id, output };
  } catch {
    reply = { kind: "result", id: message.id, failed: true };
  }
  process.send?.(reply);
});
