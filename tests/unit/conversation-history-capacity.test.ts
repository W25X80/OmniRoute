import assert from "node:assert/strict";
import test from "node:test";
import { readConversationIndex } from "../../src/lib/db/postgres/conversationHistory.ts";
import type { PostgresTransaction } from "../../src/lib/db/postgres/database.ts";

test("history overflow is rejected instead of exposing a false leaf in a truncated index", async () => {
  let requestedLimit: unknown;
  const tx = {
    async query<Row>(_text: string, values: unknown[]) {
      requestedLimit = values[1];
      return {
        rows: Array.from({ length: 32769 }, (_, i) => ({
          id: String(i),
          parent_id: i ? String(i - 1) : null,
          content_hash: String(i),
        })) as Row[],
        rowCount: 32769,
        command: "SELECT",
      };
    },
  } as PostgresTransaction;
  await assert.rejects(readConversationIndex(tx, "oversized-history"), /history.*capacity/);
  assert.equal(
    requestedLimit,
    32769,
    "the database must bound materialization before JavaScript indexing"
  );
});
