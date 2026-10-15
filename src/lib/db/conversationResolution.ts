import {
  createAgenticConversation,
  getConversationTurnIndex,
  insertConversationTurnNodes,
  updateAgenticConversation,
  type ConversationTurnIndex,
} from "./agenticConversations";
import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresConversationResolutionRepository } from "./postgres/conversationResolutionRepository";
import type {
  ConversationHistoryVersion,
  NewConversationTurn,
} from "./postgres/conversationHistory";

export interface ConversationHistorySnapshot {
  index: ConversationTurnIndex;
  version: ConversationHistoryVersion | null;
}

export type ConversationResolutionCommand = {
  id: string;
  correlationId: string | null;
  nodes: readonly NewConversationTurn[];
} & (
  | {
      kind: "create";
      apiKeyId: string | null;
      fingerprintHash: string;
    }
  | {
      kind: "existing";
      expected: ConversationHistoryVersion | null;
      /** SQLite's existing counter behavior; native commit increments its locked row. */
      turnCount: number;
    }
);

export async function getConversationHistorySnapshotAsync(
  id: string
): Promise<ConversationHistorySnapshot | null> {
  const database = getSelectedPostgresDatabase();
  if (database) return createPostgresConversationResolutionRepository(database).snapshot(id);
  return { index: getConversationTurnIndex(id), version: null };
}

/** False means an acknowledged pre-mutation conflict; failures never authorize replay. */
export async function commitConversationResolutionAsync(
  command: ConversationResolutionCommand
): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (database) return createPostgresConversationResolutionRepository(database).commit(command);
  const db = getDbInstance();
  db.transaction(() => {
    if (command.kind === "create") createAgenticConversation(command);
    insertConversationTurnNodes(command.id, command.correlationId, [...command.nodes]);
    if (command.kind === "existing")
      updateAgenticConversation(command.id, { turnCount: command.turnCount });
  })();
  return true;
}
