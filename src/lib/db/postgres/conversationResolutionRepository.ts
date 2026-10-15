import type {
  ConversationResolutionCommand,
  ConversationHistorySnapshot,
} from "../conversationResolution";
import type { PostgresDatabase } from "./database";
import {
  advanceConversationHistory,
  buildConversationIndex,
  insertConversationChunks,
  lockConversationRoots,
  lockConversationCreation,
  lockConversationWrite,
  prepareConversationChunks,
  readConversationIndex,
  type ConversationHistoryVersion,
} from "./conversationHistory";

export function createPostgresConversationResolutionRepository(database: PostgresDatabase) {
  return {
    async snapshot(id: string): Promise<ConversationHistorySnapshot | null> {
      const result = await database.transaction(
        async (tx) => {
          const root = (
            await tx.query<ConversationHistoryVersion>(
              `SELECT insertion_order::text AS incarnation,history_revision::text AS revision
           FROM omniroute.agentic_conversations WHERE id=$1`,
              [id]
            )
          ).rows[0];
          if (!root) return null;
          return { version: root, rows: await readConversationIndex(tx, id) };
        },
        { operation: "conversations.snapshot", readOnlySnapshot: true }
      );
      return result
        ? { version: result.version, index: buildConversationIndex(result.rows) }
        : null;
    },
    async commit(command: ConversationResolutionCommand): Promise<boolean> {
      const chunks = prepareConversationChunks(command.nodes);
      return database.transaction(
        async (tx) => {
          await lockConversationWrite(tx);
          if (command.kind === "existing") {
            const [root] = await lockConversationRoots(tx, [command.id]);
            if (
              !root ||
              !command.expected ||
              root.incarnation !== command.expected.incarnation ||
              root.revision !== command.expected.revision
            )
              return false;
          } else {
            await lockConversationCreation(tx, command.id);
            const now = new Date().toISOString();
            await tx.query(
              `INSERT INTO omniroute.agentic_conversations
             (id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,turn_count,first_seen_at,last_seen_at)
             VALUES($1,$2,$3,0,'',1,$4,$4)`,
              [command.id, command.apiKeyId, command.fingerprintHash, now]
            );
          }
          const inserted = await insertConversationChunks(
            tx,
            command.id,
            command.correlationId,
            chunks
          );
          if (inserted) await advanceConversationHistory(tx, [command.id]);
          if (command.kind === "existing")
            await tx.query(
              "UPDATE omniroute.agentic_conversations SET turn_count=turn_count+1,last_seen_at=$2 WHERE id=$1",
              [command.id, new Date().toISOString()]
            );
          return true;
        },
        { operation: "conversations.commitResolution" }
      );
    },
  };
}
