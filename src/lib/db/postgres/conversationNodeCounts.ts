import type { PostgresTransaction } from "./database";

/** Derived cardinality has no root FK: root-only deletion preserves historical nodes. */
export const POSTGRES_CONVERSATION_COUNT_STATEMENTS = [
  `CREATE TABLE omniroute.conversation_node_counts (
    conversation_id text PRIMARY KEY,
    node_count bigint NOT NULL CONSTRAINT conversation_node_counts_nonnegative CHECK(node_count>=0)
  )`,
  "LOCK TABLE omniroute.conversation_turn_nodes IN SHARE ROW EXCLUSIVE MODE",
  `INSERT INTO omniroute.conversation_node_counts(conversation_id,node_count)
   SELECT conversation_id,count(*) FROM omniroute.conversation_turn_nodes GROUP BY conversation_id`,
] as const;

export async function addConversationNodeCount(
  tx: PostgresTransaction,
  id: string,
  inserted: number
): Promise<void> {
  if (!inserted) return;
  if (!Number.isSafeInteger(inserted) || inserted < 0)
    throw new RangeError("Invalid conversation node count delta");
  await tx.query(
    `INSERT INTO omniroute.conversation_node_counts(conversation_id,node_count) VALUES($1,$2)
     ON CONFLICT(conversation_id) DO UPDATE
       SET node_count=conversation_node_counts.node_count+excluded.node_count`,
    [id, inserted]
  );
}

/** Only bounded retention batches reach JS; reset groups its actual deletions in SQL. */
export async function subtractConversationNodeCounts(
  tx: PostgresTransaction,
  deleted: readonly { conversation_id: string }[]
): Promise<void> {
  const groups = new Map<string, number>();
  for (const row of deleted)
    groups.set(row.conversation_id, (groups.get(row.conversation_id) ?? 0) + 1);
  if (!groups.size) return;
  const ids = [...groups.keys()];
  const result = await tx.query(
    `WITH locked AS MATERIALIZED (
       SELECT conversation_id FROM omniroute.conversation_node_counts
       WHERE conversation_id=ANY($1::text[]) ORDER BY conversation_id COLLATE "C" FOR UPDATE),
     deltas AS (SELECT * FROM unnest($1::text[],$2::bigint[]) AS d(conversation_id,removed))
     UPDATE omniroute.conversation_node_counts c SET node_count=c.node_count-d.removed
     FROM deltas d,locked l WHERE c.conversation_id=d.conversation_id
       AND l.conversation_id=c.conversation_id AND c.node_count>=d.removed`,
    [ids, [...groups.values()]]
  );
  if (result.rowCount !== groups.size) throw new Error("Conversation node count integrity failure");
  await tx.query(
    "DELETE FROM omniroute.conversation_node_counts WHERE conversation_id=ANY($1::text[]) AND node_count=0",
    [ids]
  );
}

/** Offline only, under the import/schema owner's exclusive source-table locks. */
export async function rebuildConversationNodeCounts(tx: PostgresTransaction): Promise<void> {
  await tx.query("DELETE FROM omniroute.conversation_node_counts");
  await tx.query(
    `INSERT INTO omniroute.conversation_node_counts(conversation_id,node_count)
     SELECT conversation_id,count(*) FROM omniroute.conversation_turn_nodes GROUP BY conversation_id`
  );
}

/** Independent offline reconciliation; never repair drift as a side effect of verification. */
export async function verifyConversationNodeCounts(tx: PostgresTransaction): Promise<void> {
  const invalid = await tx.query(
    `WITH actual AS (SELECT conversation_id,count(*) AS node_count
       FROM omniroute.conversation_turn_nodes GROUP BY conversation_id)
     SELECT 1 FROM actual a FULL JOIN omniroute.conversation_node_counts c USING(conversation_id)
     WHERE a.node_count IS DISTINCT FROM c.node_count OR c.node_count<=0 LIMIT 1`
  );
  if (invalid.rows.length) throw new Error("Conversation node count integrity failure");
}

/** Narrow physical check for the derived table; the import owner binds its other columns/keys. */
export async function verifyConversationNodeCountSchema(tx: PostgresTransaction): Promise<void> {
  const { rows } = await tx.query<{ compatible: boolean }>(
    `SELECT (
       (SELECT count(*)=3 AND count(DISTINCT a.attcollation)=1
        FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
        JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='omniroute' AND NOT a.attisdropped AND
          ((c.relname IN ('conversation_node_counts','conversation_turn_nodes') AND a.attname='conversation_id')
           OR (c.relname='agentic_conversations' AND a.attname='id')))
       AND EXISTS(SELECT 1 FROM pg_catalog.pg_constraint
         WHERE conrelid='omniroute.conversation_node_counts'::regclass
           AND conname='conversation_node_counts_nonnegative' AND contype='c' AND convalidated
           AND pg_get_expr(conbin,conrelid)='(node_count >= 0)')
     ) AS compatible`
  );
  if (rows[0]?.compatible !== true) throw new Error("Conversation node count schema mismatch");
}
