import type { ConversationTurnIndex } from "../agenticConversations";
import type { PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";
export { acquireConversationCreationLock as lockConversationCreation } from "./locks";

/** Ordinary writers coexist; only an explicit whole-history reset takes this gate exclusively. */
export function lockConversationWrite(tx: PostgresTransaction): Promise<void> {
  return acquirePostgresLock(tx, "conversations", "shared");
}

export const CONVERSATION_INSERT_CHUNK_ROWS = 256;
export const CONVERSATION_INSERT_CHUNK_BYTES = 256 * 1024;

export interface ConversationHistoryVersion {
  incarnation: string;
  revision: string;
}

export interface NewConversationTurn {
  id: string;
  parentId: string | null;
  role: string;
  contentHash: string;
}

export interface ConversationIndexRow {
  id: string;
  parent_id: string | null;
  content_hash: string;
}

export function prepareConversationChunks(nodes: readonly NewConversationTurn[]): string[] {
  const chunks: string[] = [];
  let rows: string[] = [],
    bytes = 2;
  for (const node of nodes) {
    const row = JSON.stringify([node.id, node.parentId, node.role, node.contentHash]);
    const size = Buffer.byteLength(row, "utf8");
    if (size + 2 > CONVERSATION_INSERT_CHUNK_BYTES)
      throw new RangeError("Conversation turn node exceeds the insert payload limit");
    if (
      rows.length &&
      (rows.length === CONVERSATION_INSERT_CHUNK_ROWS ||
        bytes + size + 1 > CONVERSATION_INSERT_CHUNK_BYTES)
    ) {
      chunks.push(`[${rows.join(",")}]`);
      rows = [];
      bytes = 2;
    }
    rows.push(row);
    bytes += size + 1;
  }
  if (rows.length) chunks.push(`[${rows.join(",")}]`);
  return chunks;
}

export function buildConversationIndex(
  rows: readonly ConversationIndexRow[]
): ConversationTurnIndex {
  const nodeIds = new Set<string>(),
    byContentHash = new Map<string, string[]>(),
    parentsWithChildren = new Set<string>();
  for (const row of rows) {
    nodeIds.add(row.id);
    if (row.parent_id) parentsWithChildren.add(row.parent_id);
    if (row.content_hash) {
      const bucket = byContentHash.get(row.content_hash) || [];
      bucket.push(row.id);
      byContentHash.set(row.content_hash, bucket);
    }
  }
  return { nodeIds, byContentHash, parentsWithChildren };
}

export async function readConversationIndex(tx: PostgresTransaction, id: string) {
  return (
    await tx.query<ConversationIndexRow>(
      "SELECT id,parent_id,content_hash FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 ORDER BY insertion_order",
      [id]
    )
  ).rows;
}

/** Mutators acquire parent rows before any node rows, in stable root order. */
export async function lockConversationRoots(
  tx: PostgresTransaction,
  ids: readonly string[],
  skipLocked = false
): Promise<Array<ConversationHistoryVersion & { id: string }>> {
  if (!ids.length) return [];
  return (
    await tx.query<ConversationHistoryVersion & { id: string }>(
      `SELECT id,insertion_order::text AS incarnation,history_revision::text AS revision
     FROM omniroute.agentic_conversations WHERE id=ANY($1::text[])
     ORDER BY id COLLATE "C" FOR UPDATE${skipLocked ? " SKIP LOCKED" : ""}`,
      [[...new Set(ids)]]
    )
  ).rows;
}

export async function advanceConversationHistory(tx: PostgresTransaction, ids: readonly string[]) {
  if (!ids.length) return;
  await tx.query(
    "UPDATE omniroute.agentic_conversations SET history_revision=history_revision+1 WHERE id=ANY($1::text[])",
    [[...new Set(ids)]]
  );
}

/** Chunks retain one transaction and caller order, including identity gaps on conflicts. */
export async function insertConversationChunks(
  tx: PostgresTransaction,
  id: string,
  correlationId: string | null,
  chunks: readonly string[]
): Promise<number> {
  let inserted = 0;
  const now = new Date().toISOString();
  for (const payload of chunks) {
    const result = await tx.query(
      `INSERT INTO omniroute.conversation_turn_nodes
       (id,conversation_id,parent_id,role,content_hash,last_correlation_id,first_seen_at,last_seen_at)
       SELECT node->>0,$1,node->>1,node->>2,node->>3,$2,$3,$3
       FROM jsonb_array_elements($4::jsonb) WITH ORDINALITY AS input(node,ordinal)
       ORDER BY ordinal ON CONFLICT(id) DO NOTHING`,
      [id, correlationId, now, payload]
    );
    inserted += result.rowCount ?? 0;
  }
  return inserted;
}
