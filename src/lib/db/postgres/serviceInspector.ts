import { randomUUID } from "node:crypto";
import type { InspectorCustomHostRow, InspectorSessionRow } from "../_rowTypes";
import { InterceptedRequestSchema, type InterceptedRequest } from "../../../mitm/inspector/types";
import type { PostgresTransaction } from "./database";
import { first, rows, safeInteger } from "./serviceQuery";

export async function listCustomHosts(
  transaction: PostgresTransaction,
  opts?: { enabledOnly?: boolean }
): Promise<InspectorCustomHostRow[]> {
  return (
    await rows<Omit<InspectorCustomHostRow, "enabled"> & { enabled: number }>(
      transaction,
      `SELECT * FROM omniroute.inspector_custom_hosts ${opts?.enabledOnly === true ? "WHERE enabled = 1" : ""} ORDER BY host COLLATE "C" ASC`
    )
  ).map((row) => ({ ...row, enabled: row.enabled === 1 }));
}
export async function addCustomHost(
  transaction: PostgresTransaction,
  host: string,
  kind: "llm" | "app" | "custom" = "custom",
  label?: string
): Promise<void> {
  await transaction.query(
    "INSERT INTO omniroute.inspector_custom_hosts (host, enabled, label, kind, added_at) VALUES ($1, 1, $2, $3, $4) ON CONFLICT (host) DO NOTHING",
    [host, label ?? null, kind, new Date().toISOString()]
  );
}
export async function removeCustomHost(
  transaction: PostgresTransaction,
  host: string
): Promise<void> {
  await transaction.query("DELETE FROM omniroute.inspector_custom_hosts WHERE host = $1", [host]);
}
export async function toggleCustomHost(
  transaction: PostgresTransaction,
  host: string,
  enabled: boolean
): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.inspector_custom_hosts SET enabled = $1 WHERE host = $2",
    [enabled ? 1 : 0, host]
  );
}
export async function touchLastSeen(transaction: PostgresTransaction, host: string): Promise<void> {
  await transaction.query(
    "UPDATE omniroute.inspector_custom_hosts SET last_seen_at = $1 WHERE host = $2",
    [new Date().toISOString(), host]
  );
}
export async function isCustomHost(
  transaction: PostgresTransaction,
  host: string
): Promise<boolean> {
  return !!(await first(
    transaction,
    "SELECT 1 FROM omniroute.inspector_custom_hosts WHERE host = $1 AND enabled = 1",
    [host]
  ));
}
export async function createSession(
  transaction: PostgresTransaction,
  opts?: { name?: string; profile?: "llm" | "custom" | "all" }
): Promise<{ id: string; started_at: string }> {
  const id = randomUUID();
  const started_at = new Date().toISOString();
  await transaction.query(
    "INSERT INTO omniroute.inspector_sessions (id, name, started_at, profile) VALUES ($1, $2, $3, $4)",
    [id, opts?.name ?? null, started_at, opts?.profile ?? null]
  );
  return { id, started_at };
}
export async function stopSession(transaction: PostgresTransaction, id: string): Promise<void> {
  await transaction.query("UPDATE omniroute.inspector_sessions SET ended_at = $1 WHERE id = $2", [
    new Date().toISOString(),
    id,
  ]);
}
export async function renameSession(
  transaction: PostgresTransaction,
  id: string,
  name: string
): Promise<void> {
  await transaction.query("UPDATE omniroute.inspector_sessions SET name = $1 WHERE id = $2", [
    name,
    id,
  ]);
}
export async function listSessions(
  transaction: PostgresTransaction
): Promise<InspectorSessionRow[]> {
  return rows(
    transaction,
    'SELECT * FROM omniroute.inspector_sessions ORDER BY started_at COLLATE "C" DESC, id COLLATE "C" DESC'
  );
}
export async function getSession(
  transaction: PostgresTransaction,
  id: string
): Promise<InspectorSessionRow | null> {
  return (
    (await first<InspectorSessionRow>(
      transaction,
      "SELECT * FROM omniroute.inspector_sessions WHERE id = $1",
      [id]
    )) ?? null
  );
}
export async function appendSessionRequest(
  transaction: PostgresTransaction,
  sessionId: string,
  payload: string
): Promise<number> {
  const parent = await first(
    transaction,
    "SELECT id FROM omniroute.inspector_sessions WHERE id = $1 FOR UPDATE",
    [sessionId]
  );
  if (!parent) throw new Error("Inspector session does not exist");
  const next = safeInteger(
    (
      await first<{ next: string }>(
        transaction,
        "SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM omniroute.inspector_session_requests WHERE session_id = $1",
        [sessionId]
      )
    )?.next
  );
  await transaction.query(
    "INSERT INTO omniroute.inspector_session_requests (session_id, seq, payload) VALUES ($1, $2, $3)",
    [sessionId, next, payload]
  );
  await transaction.query(
    "UPDATE omniroute.inspector_sessions SET request_count = request_count + 1 WHERE id = $1",
    [sessionId]
  );
  return next;
}
export async function getSessionRequests(
  transaction: PostgresTransaction,
  sessionId: string
): Promise<Array<{ seq: number; payload: string }>> {
  return (
    await rows<{ seq: number; payload: string }>(
      transaction,
      "SELECT seq, payload FROM omniroute.inspector_session_requests WHERE session_id = $1 ORDER BY seq ASC",
      [sessionId]
    )
  ).map((row) => ({ ...row, seq: safeInteger(row.seq) }));
}
export async function deleteSession(transaction: PostgresTransaction, id: string): Promise<void> {
  await transaction.query("DELETE FROM omniroute.inspector_sessions WHERE id = $1", [id]);
}
export async function snapshotSession(
  transaction: PostgresTransaction,
  id: string
): Promise<InterceptedRequest[] | null> {
  if (!(await getSession(transaction, id))) return null;
  const result: InterceptedRequest[] = [];
  for (const row of await getSessionRequests(transaction, id)) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(row.payload);
    } catch {
      continue;
    }
    const item = InterceptedRequestSchema.safeParse(parsed);
    if (item.success) result.push(item.data as InterceptedRequest);
  }
  return result;
}
