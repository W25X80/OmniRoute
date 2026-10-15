import type { PostgresTransaction } from "./database";
import {
  advanceConversationHistory,
  lockConversationCreation,
  lockConversationWrite,
} from "./conversationHistory";

/** Parent locks are acquired first; a busy early root cannot hide later eligible roots. */
export async function deleteExpiredConversationNodes(tx: PostgresTransaction, cutoff: string) {
  await lockConversationWrite(tx);
  const roots = (
    await tx.query<{ id: string }>(
      `SELECT a.id FROM omniroute.agentic_conversations a WHERE EXISTS(
       SELECT 1 FROM omniroute.conversation_turn_nodes n
       WHERE n.conversation_id=a.id AND n.last_seen_at_retention<$1::timestamptz)
     ORDER BY a.id COLLATE "C" LIMIT 16 FOR UPDATE OF a SKIP LOCKED`,
      [cutoff]
    )
  ).rows.map((row) => row.id);
  let deleted = 0;
  if (roots.length) {
    const result = await tx.query<{ conversation_id: string }>(
      `WITH expired AS MATERIALIZED (
         SELECT ctid FROM omniroute.conversation_turn_nodes
         WHERE conversation_id=ANY($1::text[]) AND last_seen_at_retention<$2::timestamptz
         ORDER BY last_seen_at_retention,id LIMIT 1000 FOR UPDATE SKIP LOCKED)
       DELETE FROM omniroute.conversation_turn_nodes WHERE ctid=ANY(ARRAY(SELECT ctid FROM expired))
       RETURNING conversation_id`,
      [roots, cutoff]
    );
    deleted = result.rowCount ?? 0;
    await advanceConversationHistory(
      tx,
      result.rows.map((row) => row.conversation_id)
    );
  }
  // Historical databases can contain orphan nodes. Coordinate with absent-root creation
  // before checking absence, so their deletion cannot silently change a new incarnation.
  const orphans = (
    await tx.query<{ conversation_id: string }>(
      `SELECT DISTINCT conversation_id COLLATE "C" AS conversation_id FROM (
       SELECT n.conversation_id FROM omniroute.conversation_turn_nodes n
       WHERE n.last_seen_at_retention<$1::timestamptz
       ORDER BY n.last_seen_at_retention,n.id LIMIT 1000) candidates
     WHERE NOT EXISTS(SELECT 1 FROM omniroute.agentic_conversations a
       WHERE a.id=candidates.conversation_id)
     ORDER BY conversation_id LIMIT 16`,
      [cutoff]
    )
  ).rows;
  for (const { conversation_id: id } of orphans) {
    if (!(await lockConversationCreation(tx, id, true))) continue;
    const result = await tx.query(
      `WITH expired AS MATERIALIZED (
         SELECT ctid FROM omniroute.conversation_turn_nodes n
         WHERE conversation_id=$1 AND last_seen_at_retention<$2::timestamptz
           AND NOT EXISTS(SELECT 1 FROM omniroute.agentic_conversations a WHERE a.id=$1)
         ORDER BY last_seen_at_retention,id LIMIT 1000 FOR UPDATE SKIP LOCKED)
       DELETE FROM omniroute.conversation_turn_nodes WHERE ctid=ANY(ARRAY(SELECT ctid FROM expired))`,
      [id, cutoff]
    );
    deleted += result.rowCount ?? 0;
  }
  return deleted;
}

export async function deleteExpiredConversationRoots(
  tx: PostgresTransaction,
  cutoff: string,
  orphansOnly: boolean
) {
  await lockConversationWrite(tx);
  const orphan = orphansOnly
    ? "AND NOT EXISTS(SELECT 1 FROM omniroute.conversation_turn_nodes n WHERE n.conversation_id=a.id)"
    : "";
  const roots = (
    await tx.query<{ id: string }>(
      `SELECT a.id FROM omniroute.agentic_conversations a
       WHERE last_seen_at::timestamptz<$1::timestamptz ${orphan}
       ORDER BY a.id COLLATE "C" LIMIT 1000 FOR UPDATE OF a SKIP LOCKED`,
      [cutoff]
    )
  ).rows.map((row) => row.id);
  if (!roots.length) return 0;
  // Recheck on a fresh READ COMMITTED snapshot after acquiring parent locks.
  // A writer can commit new nodes between the selection snapshot and lock acquisition.
  const result = await tx.query(
    `DELETE FROM omniroute.agentic_conversations a WHERE id=ANY($1::text[])
       AND last_seen_at::timestamptz<$2::timestamptz ${orphan}`,
    [roots, cutoff]
  );
  return result.rowCount ?? 0;
}

/** Caller owns the exclusive reset gate; advancing tokens invalidates every older snapshot. */
export async function invalidatePurgedConversationHistory(tx: PostgresTransaction) {
  await tx.query(
    `UPDATE omniroute.agentic_conversations a SET history_revision=history_revision+1
     WHERE EXISTS(SELECT 1 FROM omniroute.conversation_turn_nodes n WHERE n.conversation_id=a.id)`
  );
}
