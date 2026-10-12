import type { ChildProcess } from "node:child_process";
import { parseDatabaseBackend } from "../lib/db/backendSelection";
import { getMitmAlias } from "../lib/db/models/mitmAlias";
import { runPersistenceBackgroundWork } from "../lib/db/persistenceRuntime";
import { normalizeAliasMappings } from "./aliasConfig";

const CHANNEL = "omniroute-mitm-alias.v1";
const LIMIT = 32;
interface AliasRequest {
  channel: typeof CHANNEL;
  kind: "lookup";
  id: number;
  model: string;
  aliasKey: string;
}

function isAliasRequest(value: unknown): value is AliasRequest {
  if (!value || typeof value !== "object") return false;
  const request = value as Partial<AliasRequest>;
  return (
    request.channel === CHANNEL &&
    request.kind === "lookup" &&
    Number.isSafeInteger(request.id) &&
    request.id! > 0 &&
    typeof request.model === "string" &&
    request.model.length > 0 &&
    request.model.length <= 2048 &&
    typeof request.aliasKey === "string" &&
    request.aliasKey.length > 0 &&
    request.aliasKey.length <= 64
  );
}

/** Each lookup re-enters the existing application owner; the child never opens a SQL pool. */
export function attachNativeAliasBridge(child: ChildProcess): { close(): Promise<void> } {
  if (parseDatabaseBackend() !== "postgres") {
    throw new Error("Native MITM aliases require PostgreSQL");
  }
  let closed = false;
  let closing: Promise<void> | undefined;
  const active = new Map<number, Promise<void>>();
  function send(id: number, result: object): Promise<void> {
    if (!child.connected) return Promise.resolve();
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, 5000);
      const finish = () => {
        clearTimeout(timer);
        resolve();
      };
      try {
        child.send({ channel: CHANNEL, kind: "result", id, ...result }, finish);
      } catch {
        finish();
      }
    });
  }
  function onMessage(message: unknown): void {
    if (closed || !isAliasRequest(message)) return;
    const { id, aliasKey, model } = message;
    if (active.size >= LIMIT || active.has(id)) {
      void send(id, { ok: false });
      return;
    }
    const work = runPersistenceBackgroundWork(async () => {
      const mappings = normalizeAliasMappings(await getMitmAlias(aliasKey));
      const override =
        (Object.hasOwn(mappings, model) ? mappings[model] : undefined) || mappings["*"] || null;
      if (JSON.stringify(override).length > 8192) {
        throw new Error("MITM alias lookup is unavailable");
      }
      return override;
    })
      .then(
        (override) => send(id, { ok: true, override }),
        () => send(id, { ok: false })
      )
      .finally(() => active.delete(id));
    active.set(id, work);
  }
  function stop(): void {
    void close();
  }
  function close(): Promise<void> {
    if (closing) return closing;
    closed = true;
    child.removeListener("message", onMessage);
    child.removeListener("disconnect", stop);
    child.removeListener("exit", stop);
    closing = Promise.allSettled([...active.values()]).then(() => {});
    return closing;
  }
  child.on("message", onMessage);
  child.once("disconnect", stop);
  child.once("exit", stop);
  return { close };
}
