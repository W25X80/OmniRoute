import { v4 as uuidv4 } from "uuid";
import type {
  AgenticConversationRow,
  ConversationTurnNode,
  ConversationTurnIndex,
  ConversationTurnNodeWithSeq,
  ConversationTurnPage,
  MultiTurnConversationRow,
} from "../agenticConversations";
import type { PostgresDatabase, PostgresTransaction } from "./database";
import { acquirePostgresLock } from "./locks";
type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown): JsonRecord {
  return value && typeof value === "object" ? (value as JsonRecord) : {};
}

function toRow(value: unknown): AgenticConversationRow {
  const r = asRecord(value);
  return {
    id: String(r.id ?? ""),
    apiKeyId: typeof r.api_key_id === "string" ? r.api_key_id : null,
    fingerprintHash: String(r.fingerprint_hash ?? ""),
    turnCount: Number(r.turn_count ?? 1),
    firstSeenAt: String(r.first_seen_at ?? ""),
    lastSeenAt: String(r.last_seen_at ?? ""),
  };
}

function toTurnNode(value: unknown): ConversationTurnNode {
  const r = asRecord(value);
  return {
    id: String(r.id ?? ""),
    conversationId: String(r.conversation_id ?? ""),
    parentId: typeof r.parent_id === "string" ? r.parent_id : null,
    role: String(r.role ?? ""),
    contentHash: String(r.content_hash ?? ""),
    lastCorrelationId: typeof r.last_correlation_id === "string" ? r.last_correlation_id : null,
    firstSeenAt: String(r.first_seen_at ?? ""),
    lastSeenAt: String(r.last_seen_at ?? ""),
  };
}

function toTurnNodeWithSeq(value: unknown): ConversationTurnNodeWithSeq {
  const rec = asRecord(value);
  return { ...toTurnNode(value), seq: Number(rec.seq ?? 0) };
}

function toMultiTurnConversationRow(value: unknown): MultiTurnConversationRow {
  const rec = asRecord(value);
  return {
    ...toRow(rec),
    lastCallLogId: typeof rec.last_call_log_id === "string" ? rec.last_call_log_id : null,
    lastModel: typeof rec.last_model === "string" ? rec.last_model : null,
    lastProvider: typeof rec.last_provider === "string" ? rec.last_provider : null,
    lastStatus: typeof rec.last_status === "number" ? rec.last_status : null,
    lastArtifactRelPath:
      typeof rec.last_artifact_relpath === "string" ? rec.last_artifact_relpath : null,
    lastApiKeyId: typeof rec.last_api_key_id === "string" ? rec.last_api_key_id : null,
  };
}

// Bound each SQL payload without splitting the logical append's transaction.
export const CONVERSATION_INSERT_CHUNK_ROWS = 256;
export const CONVERSATION_INSERT_CHUNK_BYTES = 256 * 1024;

function prepareNodeChunks(
  nodes: Array<{ id: string; parentId: string | null; role: string; contentHash: string }>
): string[] {
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

const MULTI_TURN_CONVERSATION_SELECT = `
  SELECT ac.*, latest.id as last_call_log_id, latest.model as last_model,
         latest.provider as last_provider, latest.status as last_status,
         latest.artifact_relpath as last_artifact_relpath,
         latest.api_key_id as last_api_key_id
  FROM omniroute.agentic_conversations ac
  LEFT JOIN (
    SELECT cl1.id, cl1.session_tag, cl1.model, cl1.provider, cl1.status,
           cl1.artifact_relpath, cl1.api_key_id
    FROM omniroute.call_logs cl1
    WHERE cl1.timestamp = (
      SELECT MAX(cl2.timestamp) FROM omniroute.call_logs cl2 WHERE cl2.session_tag = cl1.session_tag
    )
  ) latest ON latest.session_tag = ac.id
`;

export function createPostgresAgenticConversationsRepository(database: PostgresDatabase) {
  function operation<T>(
    write: boolean,
    body: (tx: PostgresTransaction) => Promise<T>,
    name: string
  ) {
    return database.transaction(
      async (tx) => {
        await acquirePostgresLock(tx, "conversations", write ? "exclusive" : "shared");
        return body(tx);
      },
      { operation: name }
    );
  }
  return {
    createAgenticConversation: (input: {
      id?: string;
      apiKeyId: string | null;
      fingerprintHash: string;
    }) =>
      operation(
        true,
        async (tx) => {
          const now = new Date().toISOString(),
            id = input.id || `conv_${uuidv4()}`;
          await tx.query(
            "INSERT INTO omniroute.agentic_conversations(id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,turn_count,first_seen_at,last_seen_at) VALUES($1,$2,$3,0,'',1,$4,$4)",
            [id, input.apiKeyId, input.fingerprintHash, now]
          );
          return {
            id,
            apiKeyId: input.apiKeyId,
            fingerprintHash: input.fingerprintHash,
            turnCount: 1,
            firstSeenAt: now,
            lastSeenAt: now,
          };
        },
        "conversations.createAgenticConversation"
      ),
    findAgenticConversationsByFingerprint: async (fingerprint: string) =>
      (
        await operation(
          false,
          async (tx) =>
            (
              await tx.query(
                "SELECT * FROM omniroute.agentic_conversations WHERE fingerprint_hash=$1 ORDER BY last_seen_at DESC,insertion_order LIMIT 20",
                [fingerprint]
              )
            ).rows,
          "conversations.findAgenticConversationsByFingerprint"
        )
      ).map(toRow),
    updateAgenticConversation: (id: string, patch: { turnCount: number }) =>
      operation(
        true,
        async (tx) => {
          await tx.query(
            "UPDATE omniroute.agentic_conversations SET turn_count=$1,last_seen_at=$2 WHERE id=$3",
            [patch.turnCount, new Date().toISOString(), id]
          );
        },
        "conversations.updateAgenticConversation"
      ),
    getConversationTurnIndex: async (id: string): Promise<ConversationTurnIndex> => {
      const rows = await operation(
        false,
        async (tx) =>
          (
            await tx.query<{ id: string; parent_id: string | null; content_hash: string }>(
              "SELECT id,parent_id,content_hash FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 ORDER BY insertion_order",
              [id]
            )
          ).rows,
        "conversations.getConversationTurnIndex"
      );
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
    },
    insertConversationTurnNodes: (
      id: string,
      correlationId: string | null,
      nodes: Array<{ id: string; parentId: string | null; role: string; contentHash: string }>
    ) => {
      const chunks = prepareNodeChunks(nodes);
      if (!chunks.length) return Promise.resolve();
      return operation(
        true,
        async (tx) => {
          const now = new Date().toISOString();
          for (const payload of chunks) {
            // ORDER BY feeds identity allocation in caller order, including conflict gaps.
            // Chunks share this transaction: a later failure rolls back the whole append.
            await tx.query(
              `INSERT INTO omniroute.conversation_turn_nodes
            (id,conversation_id,parent_id,role,content_hash,last_correlation_id,first_seen_at,last_seen_at)
            SELECT node->>0,$1,node->>1,node->>2,node->>3,$2,$3,$3
            FROM jsonb_array_elements($4::jsonb) WITH ORDINALITY AS input(node,ordinal)
            ORDER BY ordinal ON CONFLICT(id) DO NOTHING`,
              [id, correlationId, now, payload]
            );
          }
        },
        "conversations.insertConversationTurnNodes"
      );
    },
    getConversationTurnPage: async (
      id: string,
      opts: { limit?: number; beforeSeq?: number; afterSeq?: number } = {}
    ): Promise<ConversationTurnPage> => {
      const limit = Math.max(1, Math.min(opts.limit ?? 20, 500));
      const rows = await operation(
        false,
        async (tx) => {
          if (opts.afterSeq != null)
            return (
              await tx.query(
                "SELECT insertion_order AS seq,* FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 AND insertion_order>$2 ORDER BY insertion_order",
                [id, opts.afterSeq]
              )
            ).rows;
          return (
            await tx.query(
              `SELECT insertion_order AS seq,* FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 ${opts.beforeSeq != null ? "AND insertion_order<$2" : ""} ORDER BY insertion_order DESC LIMIT $${opts.beforeSeq != null ? 3 : 2}`,
              opts.beforeSeq != null ? [id, opts.beforeSeq, limit + 1] : [id, limit + 1]
            )
          ).rows;
        },
        "conversations.getConversationTurnPage"
      );
      if (opts.afterSeq != null) return { nodes: rows.map(toTurnNodeWithSeq), hasMore: false };
      const hasMore = rows.length > limit;
      return {
        nodes: (hasMore ? rows.slice(0, limit) : rows).reverse().map(toTurnNodeWithSeq),
        hasMore,
      };
    },
    resolveCallLogIdsByCorrelationIds: async (correlationIds: string[]) => {
      const unique = [...new Set(correlationIds.filter(Boolean))],
        result = new Map<string, string>();
      if (!unique.length) return result;
      const rows = await operation(
        false,
        async (tx) =>
          (
            await tx.query<{ id: string; correlation_id: string }>(
              "SELECT id,correlation_id FROM omniroute.call_logs WHERE correlation_id=ANY($1::text[])",
              [unique]
            )
          ).rows,
        "conversations.resolveCallLogIdsByCorrelationIds"
      );
      for (const row of rows)
        if (row.correlation_id && row.id && !result.has(row.correlation_id))
          result.set(row.correlation_id, row.id);
      return result;
    },
    touchOrCreateExternalConversation: (id: string, ctx: { apiKeyId: string | null }) =>
      operation(
        true,
        async (tx) => {
          const now = new Date().toISOString();
          await tx.query(
            `INSERT INTO omniroute.agentic_conversations(id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,turn_count,first_seen_at,last_seen_at)
        VALUES($1,$2,'',0,'',1,$3,$3) ON CONFLICT(id) DO UPDATE SET turn_count=agentic_conversations.turn_count+1,last_seen_at=excluded.last_seen_at`,
            [id, ctx.apiKeyId, now]
          );
        },
        "conversations.touchOrCreateExternalConversation"
      ),
    listMultiTurnConversations: async (filter: { limit?: number; offset?: number } = {}) => {
      const result = await operation(
        false,
        async (tx) => {
          const floor =
            "WHERE (SELECT count(*) FROM omniroute.conversation_turn_nodes n WHERE n.conversation_id=ac.id)>=2";
          const total = Number(
            (
              await tx.query<{ c: string }>(
                `SELECT count(*) AS c FROM omniroute.agentic_conversations ac ${floor}`
              )
            ).rows[0].c
          );
          const rows = (
            await tx.query(
              `${MULTI_TURN_CONVERSATION_SELECT} ${floor} ORDER BY ac.last_seen_at DESC,ac.insertion_order LIMIT $1 OFFSET $2`,
              [Math.max(1, Math.min(filter.limit ?? 50, 200)), Math.max(0, filter.offset ?? 0)]
            )
          ).rows;
          return { rows, total };
        },
        "conversations.listMultiTurnConversations"
      );
      return { rows: result.rows.map(toMultiTurnConversationRow), total: result.total };
    },
    getMultiTurnConversationById: async (id: string) => {
      const row = await operation(
        false,
        async (tx) =>
          (await tx.query(`${MULTI_TURN_CONVERSATION_SELECT} WHERE ac.id=$1`, [id])).rows[0],
        "conversations.getMultiTurnConversationById"
      );
      return row ? toMultiTurnConversationRow(row) : null;
    },
  };
}
