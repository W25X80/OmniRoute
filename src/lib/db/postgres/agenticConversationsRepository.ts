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
  function operation<T>(write: boolean, body: (tx: PostgresTransaction) => Promise<T>) {
    return database.transaction(async (tx) => {
      await tx.query(`SELECT pg_advisory_xact_lock${write ? "" : "_shared"}(18765,12)`);
      return body(tx);
    });
  }
  return {
    createAgenticConversation: (input: {
      id?: string;
      apiKeyId: string | null;
      fingerprintHash: string;
    }) =>
      operation(true, async (tx) => {
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
      }),
    findAgenticConversationsByFingerprint: (fingerprint: string) =>
      operation(false, async (tx) =>
        (
          await tx.query(
            "SELECT * FROM omniroute.agentic_conversations WHERE fingerprint_hash=$1 ORDER BY last_seen_at DESC,insertion_order LIMIT 20",
            [fingerprint]
          )
        ).rows.map(toRow)
      ),
    updateAgenticConversation: (id: string, patch: { turnCount: number }) =>
      operation(true, async (tx) => {
        await tx.query(
          "UPDATE omniroute.agentic_conversations SET turn_count=$1,last_seen_at=$2 WHERE id=$3",
          [patch.turnCount, new Date().toISOString(), id]
        );
      }),
    getConversationTurnIndex: (id: string): Promise<ConversationTurnIndex> =>
      operation(false, async (tx) => {
        const rows = (
          await tx.query<{ id: string; parent_id: string | null; content_hash: string }>(
            "SELECT id,parent_id,content_hash FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 ORDER BY insertion_order",
            [id]
          )
        ).rows;
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
      }),
    insertConversationTurnNodes: (
      id: string,
      correlationId: string | null,
      nodes: Array<{ id: string; parentId: string | null; role: string; contentHash: string }>
    ) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        for (const node of nodes)
          await tx.query(
            "INSERT INTO omniroute.conversation_turn_nodes(id,conversation_id,parent_id,role,content_hash,last_correlation_id,first_seen_at,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,$7) ON CONFLICT(id) DO NOTHING",
            [node.id, id, node.parentId, node.role, node.contentHash, correlationId, now]
          );
      }),
    getConversationTurnPage: (
      id: string,
      opts: { limit?: number; beforeSeq?: number; afterSeq?: number } = {}
    ): Promise<ConversationTurnPage> =>
      operation(false, async (tx) => {
        const limit = Math.max(1, Math.min(opts.limit ?? 20, 500));
        if (opts.afterSeq != null) {
          const rows = (
            await tx.query(
              "SELECT insertion_order AS seq,* FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 AND insertion_order>$2 ORDER BY insertion_order",
              [id, opts.afterSeq]
            )
          ).rows;
          return { nodes: rows.map(toTurnNodeWithSeq), hasMore: false };
        }
        const rows = (
          await tx.query(
            `SELECT insertion_order AS seq,* FROM omniroute.conversation_turn_nodes WHERE conversation_id=$1 ${opts.beforeSeq != null ? "AND insertion_order<$2" : ""} ORDER BY insertion_order DESC LIMIT $${opts.beforeSeq != null ? 3 : 2}`,
            opts.beforeSeq != null ? [id, opts.beforeSeq, limit + 1] : [id, limit + 1]
          )
        ).rows;
        const hasMore = rows.length > limit;
        return {
          nodes: (hasMore ? rows.slice(0, limit) : rows).reverse().map(toTurnNodeWithSeq),
          hasMore,
        };
      }),
    resolveCallLogIdsByCorrelationIds: (correlationIds: string[]) =>
      operation(false, async (tx) => {
        const unique = [...new Set(correlationIds.filter(Boolean))],
          result = new Map<string, string>();
        if (!unique.length) return result;
        const rows = (
          await tx.query<{ id: string; correlation_id: string }>(
            "SELECT id,correlation_id FROM omniroute.call_logs WHERE correlation_id=ANY($1::text[])",
            [unique]
          )
        ).rows;
        for (const row of rows)
          if (row.correlation_id && row.id && !result.has(row.correlation_id))
            result.set(row.correlation_id, row.id);
        return result;
      }),
    touchOrCreateExternalConversation: (id: string, ctx: { apiKeyId: string | null }) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        await tx.query(
          `INSERT INTO omniroute.agentic_conversations(id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,turn_count,first_seen_at,last_seen_at)
        VALUES($1,$2,'',0,'',1,$3,$3) ON CONFLICT(id) DO UPDATE SET turn_count=agentic_conversations.turn_count+1,last_seen_at=excluded.last_seen_at`,
          [id, ctx.apiKeyId, now]
        );
      }),
    listMultiTurnConversations: (filter: { limit?: number; offset?: number } = {}) =>
      operation(false, async (tx) => {
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
        return { rows: rows.map(toMultiTurnConversationRow), total };
      }),
    getMultiTurnConversationById: (id: string) =>
      operation(false, async (tx) => {
        const row = (await tx.query(`${MULTI_TURN_CONVERSATION_SELECT} WHERE ac.id=$1`, [id]))
          .rows[0];
        return row ? toMultiTurnConversationRow(row) : null;
      }),
  };
}
