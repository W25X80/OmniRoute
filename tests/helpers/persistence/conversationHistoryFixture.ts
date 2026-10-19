import type { PostgresTransaction } from "../../../src/lib/db/postgres/database.ts";
import { subtractConversationNodeCounts } from "../../../src/lib/db/postgres/conversationNodeCounts.ts";

/** Offline fixture DML bypasses runtime append owners but retains their derived invariant. */
export async function insertConversationFixture(
  tx: PostgresTransaction,
  insertSql: string,
  values: unknown[]
): Promise<void> {
  await tx.query(
    `WITH inserted AS (${insertSql} RETURNING conversation_id)
     INSERT INTO omniroute.conversation_node_counts(conversation_id,node_count)
     SELECT conversation_id,count(*) FROM inserted GROUP BY conversation_id
     ON CONFLICT(conversation_id) DO UPDATE
       SET node_count=conversation_node_counts.node_count+excluded.node_count`,
    values
  );
}

/** Only bounded fixture cleanup uses this; production retention must prove its own deltas. */
export async function deleteConversationFixture(
  tx: PostgresTransaction,
  deleteSql: string,
  values: unknown[] = []
): Promise<void> {
  const removed = await tx.query<{ conversation_id: string }>(
    `${deleteSql} RETURNING conversation_id`,
    values
  );
  await subtractConversationNodeCounts(tx, removed.rows);
}
