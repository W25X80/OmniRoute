import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type { createPostgresA2ATasksRepository } from "../../../src/lib/db/postgres/a2aTasksRepository.ts";
import type { createPostgresAgentMetadataRepository } from "../../../src/lib/db/postgres/agentMetadataRepository.ts";

type Tasks = Pick<
  ReturnType<typeof createPostgresA2ATasksRepository>,
  "upsertA2ATask" | "getA2ATaskHistoryById"
>;
type Bridge = Pick<
  ReturnType<typeof createPostgresAgentMetadataRepository>,
  "setMappings" | "getMappingsForAgent"
>;
export function registerAgentRepositoriesConformance(
  register: (name: string, body: () => Promise<void>) => unknown,
  repositories: () => { tasks: Tasks; bridge: Bridge }
): void {
  register("shared: task updates preserve the original principal and input", async () => {
    const { tasks } = repositories(),
      id = randomUUID(),
      owner = randomUUID(),
      other = randomUUID();
    const row = {
      id,
      state: "submitted",
      skillId: "original",
      inputJson: '{"original":true}',
      outputJson: null,
      apiKeyId: owner,
      createdAt: "2026-01-01T00:00:00.000Z",
      updatedAt: "2026-01-01T00:00:00.000Z",
      completedAt: null,
    };
    await tasks.upsertA2ATask(row);
    await tasks.upsertA2ATask({
      ...row,
      state: "completed",
      apiKeyId: other,
      inputJson: "changed",
      skillId: "changed",
      createdAt: "changed",
    });
    const result = await tasks.getA2ATaskHistoryById(id, owner);
    assert.equal(result?.api_key_id, owner);
    assert.equal(result?.input_json, row.inputJson);
    assert.equal(result?.skill_id, row.skillId);
    assert.equal(result?.created_at, row.createdAt);
    assert.equal(result?.state, "completed");
    assert.equal(await tasks.getA2ATaskHistoryById(id, other), null);
  });
  register(
    "shared: duplicate bridge replacements preserve the previous complete mapping",
    async () => {
      const { bridge } = repositories(),
        id = randomUUID();
      await bridge.setMappings(id, [{ source: "old", target: "preserved" }]);
      await assert.rejects(
        bridge.setMappings(id, [
          { source: "new", target: "a" },
          { source: "new", target: "b" },
        ])
      );
      const rows = await bridge.getMappingsForAgent(id);
      assert.deepEqual(
        rows.map((row) => ({ source: row.source_model, target: row.target_model })),
        [{ source: "old", target: "preserved" }]
      );
    }
  );
}
