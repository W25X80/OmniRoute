import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { before, after, describe, it } from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { POSTGRES_AGENT_SCHEMA_STATEMENTS } from "../../src/lib/db/postgres/agentSchema.ts";
import { createPostgresA2ATasksRepository } from "../../src/lib/db/postgres/a2aTasksRepository.ts";
import { createPostgresAgentMetadataRepository } from "../../src/lib/db/postgres/agentMetadataRepository.ts";
import { createPostgresAgenticConversationsRepository } from "../../src/lib/db/postgres/agenticConversationsRepository.ts";
import { createPostgresServerToolExecutionRepository } from "../../src/lib/db/postgres/serverToolExecutionRepository.ts";
import { registerAgentRepositoriesConformance } from "../helpers/persistence/agentRepositoriesConformance.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import type { UpsertA2ATaskInput } from "../../src/lib/db/a2aTasks.ts";

process.env.STORAGE_ENCRYPTION_KEY ||= "isolated-agent-persistence-encryption-key";

describe("native agent persistence", { skip: !postgresTestsEnabled, concurrency: false }, () => {
  let database: PostgresDatabase;
  const owner = randomUUID(),
    other = randomUUID(),
    prefix = randomUUID();
  before(async () => {
    database = await createTestPostgresDatabase({ maxConnections: 16 });
    await migratePostgresSchema(database);
    await database.transaction(async (tx) => {
      if (
        !(await tx.query("SELECT to_regclass('omniroute.a2a_tasks') AS table_name")).rows[0]
          .table_name
      )
        for (const sql of POSTGRES_AGENT_SCHEMA_STATEMENTS) await tx.query(sql);
      for (const id of [owner, other])
        await tx.query(
          "INSERT INTO omniroute.api_keys(id,name,key,created_at) VALUES($1,$1,$2,$3)",
          [id, randomUUID(), new Date().toISOString()]
        );
    });
  });
  after(async () => {
    await database?.close();
  });
  function task(id: string, apiKeyId: string | null): UpsertA2ATaskInput {
    const now = new Date().toISOString();
    return {
      id,
      state: "submitted",
      skillId: "skill",
      inputJson: '{"value":1}',
      outputJson: null,
      apiKeyId,
      createdAt: now,
      updatedAt: now,
      completedAt: null,
    };
  }

  registerAgentRepositoriesConformance(
    (name, body) => it(name, body),
    () => ({
      tasks: createPostgresA2ATasksRepository(database),
      bridge: createPostgresAgentMetadataRepository(database),
    })
  );

  it("preserves initial task identity and owner visibility", async () => {
    const repo = createPostgresA2ATasksRepository(database),
      a = task(prefix + "-a", owner),
      unowned = task(prefix + "-unowned", null);
    await repo.upsertA2ATask(a);
    await repo.upsertA2ATask(unowned);
    await repo.upsertA2ATask({
      ...a,
      state: "working",
      apiKeyId: other,
      inputJson: '{"value":2}',
      skillId: "changed",
      createdAt: "changed",
    });
    const row = await repo.getA2ATaskHistoryById(a.id, owner);
    assert.equal(row?.api_key_id, owner);
    assert.equal(row?.input_json, a.inputJson);
    assert.equal(row?.skill_id, a.skillId);
    assert.equal(row?.created_at, a.createdAt);
    assert.equal(row?.state, "working");
    assert.equal(await repo.getA2ATaskHistoryById(a.id, other), null);
    assert.ok(await repo.getA2ATaskHistoryById(unowned.id, other));
  });

  it("task state and event commit together, or both roll back", async () => {
    const repo = createPostgresA2ATasksRepository(database),
      row = task(prefix + "-atomic", owner);
    await repo.persistA2ATaskHistory(row, "state:submitted");
    await database.transaction(async (tx) => {
      await tx.query(
        "ALTER TABLE omniroute.a2a_task_events ADD CONSTRAINT agent_test_event_rejection CHECK(event_type <> 'native-test-reject')"
      );
    });
    try {
      await assert.rejects(
        repo.persistA2ATaskHistory({ ...row, state: "working" }, "native-test-reject")
      );
      assert.equal((await repo.getA2ATaskHistoryById(row.id))?.state, "submitted");
      assert.deepEqual(
        (await repo.listA2ATaskEvents(row.id)).map((e) => e.event_type),
        ["state:submitted"]
      );
    } finally {
      await database.transaction(async (tx) => {
        await tx.query(
          "ALTER TABLE omniroute.a2a_task_events DROP CONSTRAINT agent_test_event_rejection"
        );
      });
    }
  });

  it("one concurrent tool claimant executes; replay/conflict/expired claims never reclaim", async () => {
    const repo = createPostgresServerToolExecutionRepository(database),
      input = {
        apiKeyId: owner,
        requestIdentity: prefix,
        toolCallId: "call",
        toolName: "tool",
        inputDigest: "digest",
        leaseExpiresAt: new Date(Date.now() + 60000).toISOString(),
      };
    const claims = await Promise.all(
      Array.from({ length: 12 }, () => repo.claimServerToolExecution(input))
    );
    assert.equal(claims.filter((c) => c.kind === "claimed").length, 1);
    const winner = claims.find((c) => c.kind === "claimed")!;
    assert.equal(
      await repo.finalizeServerToolExecution({
        executionId: winner.executionId,
        status: "success",
        output: { value: 7 },
        errorMessage: null,
        durationMs: 3,
      }),
      true
    );
    assert.equal(
      await repo.finalizeServerToolExecution({
        executionId: winner.executionId,
        status: "error",
        output: null,
        errorMessage: "late",
        durationMs: 9,
      }),
      false
    );
    const replay = await repo.claimServerToolExecution(input);
    assert.equal(replay.kind, "replay");
    if (replay.kind === "replay") assert.deepEqual(replay.output, { value: 7 });
    assert.equal(
      (await repo.claimServerToolExecution({ ...input, inputDigest: "different" })).kind,
      "identity_conflict"
    );
    const expired = { ...input, toolCallId: "expired", leaseExpiresAt: new Date(0).toISOString() };
    assert.equal((await repo.claimServerToolExecution(expired)).kind, "claimed");
    assert.equal((await repo.claimServerToolExecution(expired)).kind, "unknown");
  });

  it("context tokens encrypt at rest, coalesce fields, gate enabled state, and isolate owners", async () => {
    const repo = createPostgresAgentMetadataRepository(database);
    await repo.setApiKeyContextSource(owner, "obsidian", {
      token: "secret-token",
      baseUrl: "http://owner",
      vaultPath: "vault",
      enabled: true,
    });
    await repo.setApiKeyContextSource(owner, "obsidian", { baseUrl: "" });
    assert.deepEqual(await repo.getApiKeyContextSource(owner, "obsidian"), {
      apiKeyId: owner,
      sourceType: "obsidian",
      token: "secret-token",
      baseUrl: "",
      vaultPath: "vault",
      enabled: true,
    });
    assert.equal(await repo.getApiKeyContextSource(other, "obsidian"), null);
    const stored = await database.transaction(
      async (tx) =>
        (
          await tx.query<{ token: string }>(
            "SELECT token FROM omniroute.api_key_context_sources WHERE api_key_id=$1",
            [owner]
          )
        ).rows[0].token
    );
    assert.ok(stored.startsWith("enc:v1:"));
    assert.notEqual(stored, "secret-token");
    await repo.setApiKeyContextSource(owner, "obsidian", { enabled: false });
    assert.equal(await repo.getApiKeyContextSource(owner, "obsidian"), null);
    await database.transaction(async (tx) => {
      await tx.query(
        "UPDATE omniroute.api_key_context_sources SET token='legacy-plaintext',enabled=1 WHERE api_key_id=$1",
        [owner]
      );
    });
    assert.equal((await repo.getApiKeyContextSource(owner, "obsidian"))?.token, "legacy-plaintext");
    await database.transaction(async (tx) => {
      await tx.query(
        "UPDATE omniroute.api_key_context_sources SET token='enc:v1:malformed' WHERE api_key_id=$1",
        [owner]
      );
    });
    await assert.rejects(repo.getApiKeyContextSource(owner, "obsidian"), /could not be decrypted/);
  });

  it("bridge replaces roll back on duplicates and preserve independent state fields", async () => {
    const repo = createPostgresAgentMetadataRepository(database),
      id = prefix + "-bridge";
    await repo.setMappings(id, [{ source: "old", target: "model" }]);
    await assert.rejects(
      repo.setMappings(id, [
        { source: "duplicate", target: "a" },
        { source: "duplicate", target: "b" },
      ])
    );
    assert.deepEqual(
      (await repo.getMappingsForAgent(id)).map((r) => r.source_model),
      ["old"]
    );
    await repo.upsertAgentBridgeState({ agent_id: id, dns_enabled: true, cert_trusted: true });
    await repo.upsertAgentBridgeState({ agent_id: id, last_error: "error" });
    assert.deepEqual(await repo.getAgentBridgeState(id), {
      agent_id: id,
      dns_enabled: true,
      cert_trusted: true,
      setup_completed: false,
      last_started_at: null,
      last_error: "error",
    });
  });

  it("conversation concurrent touches preserve owner/count and node cursors preserve insertion", async () => {
    const repo = createPostgresAgenticConversationsRepository(database),
      id = prefix + "-conversation";
    await repo.touchOrCreateExternalConversation(id, { apiKeyId: owner });
    await Promise.all(
      Array.from({ length: 10 }, () =>
        repo.touchOrCreateExternalConversation(id, { apiKeyId: other })
      )
    );
    const row = await database.transaction(
      async (tx) =>
        (
          await tx.query<{ api_key_id: string; turn_count: number }>(
            "SELECT api_key_id,turn_count FROM omniroute.agentic_conversations WHERE id=$1",
            [id]
          )
        ).rows[0]
    );
    assert.equal(row.api_key_id, owner);
    assert.equal(row.turn_count, 11);
    const nodes = Array.from({ length: 4 }, (_, i) => ({
      id: prefix + "-node" + i,
      parentId: i ? prefix + "-node" + (i - 1) : null,
      role: "user",
      contentHash: "hash" + i,
    }));
    await repo.insertConversationTurnNodes(id, null, nodes);
    await repo.insertConversationTurnNodes(id, "later", nodes);
    const last = await repo.getConversationTurnPage(id, { limit: 2 });
    assert.deepEqual(
      last.nodes.map((n) => n.id),
      nodes.slice(2).map((n) => n.id)
    );
    assert.equal(last.hasMore, true);
    const first = await repo.getConversationTurnPage(id, {
      beforeSeq: last.nodes[0].seq,
      limit: 2,
    });
    assert.deepEqual(
      first.nodes.map((n) => n.id),
      nodes.slice(0, 2).map((n) => n.id)
    );
    const tail = await repo.getConversationTurnPage(id, { afterSeq: first.nodes[1].seq, limit: 1 });
    assert.equal(tail.nodes.length, 2);
  });

  it("plugin activation history, fractional analytics and concurrent aggregate increments survive", async () => {
    const repo = createPostgresAgentMetadataRepository(database),
      name = prefix + "-plugin";
    await repo.insertPlugin({
      id: randomUUID(),
      name,
      version: "1",
      main: "index.js",
      manifest: { name },
      pluginDir: "/isolated/plugin",
    });
    await repo.updatePluginStatus(name, "active");
    const activated = (await repo.getPluginByName(name))?.activatedAt;
    await repo.updatePluginStatus(name, "inactive");
    assert.equal((await repo.getPluginByName(name))?.activatedAt, activated);
    await repo.recordPluginExecution(name, "onRequest", 0.25, true);
    await repo.recordPluginExecution(name, "onRequest", 0.75, false);
    assert.deepEqual(await repo.getPluginAnalyticsSummary(name), {
      totalCalls: 2,
      successCount: 1,
      failureCount: 1,
      avgDurationMs: 0.5,
    });
    await Promise.all(
      Array.from({ length: 10 }, () => repo.recordPluginMetric(name, "onRequest", 0.5, false))
    );
    const metric = (await repo.getPluginMetrics(name))[0];
    assert.equal(metric.calls, 10);
    assert.equal(metric.totalDurationMs, 5);
    assert.equal(metric.errors, 0);
  });
});
