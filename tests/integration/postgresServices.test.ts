import assert from "node:assert/strict";
import test from "node:test";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";
import { POSTGRES_SERVICE_SCHEMA_STATEMENTS } from "../../src/lib/db/postgres/serviceSchema.ts";
import * as files from "../../src/lib/db/postgres/serviceFiles.ts";
import * as batches from "../../src/lib/db/postgres/serviceBatches.ts";
import * as inspector from "../../src/lib/db/postgres/serviceInspector.ts";
import * as keys from "../../src/lib/db/postgres/serviceRegisteredKeys.ts";
import * as radar from "../../src/lib/db/postgres/serviceRadar.ts";
import * as webhooks from "../../src/lib/db/postgres/serviceWebhooks.ts";
import * as operations from "../../src/lib/db/postgres/serviceOperations.ts";
import * as versions from "../../src/lib/db/postgres/serviceVersionManager.ts";
import * as evals from "../../src/lib/db/postgres/serviceEvals.ts";
import { RegisteredKeyQuotaError } from "../../src/lib/db/registeredKeys.ts";
import { looksEncrypted } from "../../src/lib/db/encryption.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";

let database: PostgresDatabase;
const originalKey = process.env.STORAGE_ENCRYPTION_KEY;
test.before(async () => {
  if (!postgresTestsEnabled) return;
  process.env.STORAGE_ENCRYPTION_KEY = "postgres-services-test-key-stable";
  database = await createTestPostgresDatabase({
    maxConnections: 6,
    lockTimeoutMs: 10000,
    statementTimeoutMs: 12000,
    queryTimeoutMs: 13000,
    transactionTimeoutMs: 30000,
  });
  await migratePostgresSchema(database);
  // This fixture also runs against the foundation alone before root assembles migration3.
  await database.transaction(async (transaction) => {
    for (const statement of POSTGRES_SERVICE_SCHEMA_STATEMENTS) await transaction.query(statement);
  });
});
test.after(async () => {
  if (database) await database.close();
  if (originalKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
  else process.env.STORAGE_ENCRYPTION_KEY = originalKey;
});
const options = { skip: !postgresTestsEnabled };

test(
  "files retain byte-identical content, owner cursors and owner-scoped soft deletion",
  options,
  async () => {
    const body = Buffer.from([0, 1, 255, 9]);
    const owned = await database.transaction((tx) =>
      files.createFile(tx, {
        bytes: body.length,
        filename: "owned.jsonl",
        purpose: "batch",
        content: body,
        apiKeyId: "services-owner",
      })
    );
    const foreign = await database.transaction((tx) =>
      files.createFile(tx, {
        bytes: 1,
        filename: "foreign",
        purpose: "batch",
        content: Buffer.from("x"),
        apiKeyId: "services-foreign",
      })
    );
    assert.deepEqual(await database.transaction((tx) => files.getFileContent(tx, owned.id)), body);
    const meta = await database.transaction((tx) => files.getFile(tx, owned.id));
    assert.equal(Object.hasOwn(meta!, "content"), false);
    assert.equal(typeof meta!.bytes, "number");
    const page = await database.transaction((tx) =>
      files.listFiles(tx, { apiKeyId: "services-owner", after: foreign.id, limit: 20 })
    );
    assert.deepEqual(
      page.map((item) => item.id),
      [owned.id]
    );
    assert.equal(
      await database.transaction((tx) => files.deleteFileOwnedBy(tx, foreign.id, "services-owner")),
      false
    );
    assert.deepEqual(
      await database.transaction((tx) => files.getFileContent(tx, foreign.id)),
      Buffer.from("x")
    );
    await assert.rejects(database.transaction((tx) => files.deleteFileOwnedBy(tx, owned.id, "")));
  }
);

test(
  "batch cleanup preserves a shared file until every referencing batch is removed",
  options,
  async () => {
    const file = await database.transaction((tx) =>
      files.createFile(tx, {
        bytes: 2,
        filename: "shared",
        purpose: "batch",
        content: Buffer.from("ok"),
        apiKeyId: "batch-owner",
      })
    );
    const complete = await database.transaction((tx) =>
      batches.createBatch(tx, {
        endpoint: "/v1/chat/completions",
        completionWindow: "24h",
        status: "completed",
        inputFileId: file.id,
        apiKeyId: "batch-owner",
      })
    );
    const pending = await database.transaction((tx) =>
      batches.createBatch(tx, {
        endpoint: "/v1/chat/completions",
        completionWindow: "24h",
        inputFileId: file.id,
        apiKeyId: "batch-owner",
      })
    );
    await database.transaction((tx) =>
      batches.ensureBatchItemCheckpoints(tx, complete.id, [{ lineNumber: 1, customId: "one" }])
    );
    await database.transaction((tx) =>
      batches.ensureBatchItemCheckpoints(tx, complete.id, [
        { lineNumber: 1, customId: "different" },
      ])
    );
    assert.equal(
      (await database.transaction((tx) => batches.listBatchItemCheckpoints(tx, complete.id)))[0]
        .customId,
      "one"
    );
    const removed = await batches.deleteCompletedBatches(database, { apiKeyId: "batch-owner" });
    assert.deepEqual(removed, { deletedBatches: 1, deletedFiles: 0, hasMore: false });
    assert.deepEqual(
      await database.transaction((tx) => files.getFileContent(tx, file.id)),
      Buffer.from("ok")
    );
    await database.transaction((tx) => batches.deleteBatch(tx, pending.id));
    assert.equal(await database.transaction((tx) => files.getFileContent(tx, file.id)), null);
    await assert.rejects(
      batches.deleteCompletedBatches(database, { allTenants: true, apiKeyId: "" } as never)
    );
  }
);

test(
  "session appends from concurrent connections allocate unique sequences and exact count",
  options,
  async () => {
    const session = await database.transaction((tx) =>
      inspector.createSession(tx, { name: "concurrent" })
    );
    const sequences = await Promise.all(
      Array.from({ length: 6 }, (_, index) =>
        database.transaction((tx) =>
          inspector.appendSessionRequest(tx, session.id, JSON.stringify({ index }))
        )
      )
    );
    assert.deepEqual(
      sequences.slice().sort((a, b) => a - b),
      [1, 2, 3, 4, 5, 6]
    );
    assert.equal(
      (await database.transaction((tx) => inspector.getSession(tx, session.id)))!.request_count,
      6
    );
    await database.transaction((tx) => inspector.deleteSession(tx, session.id));
    assert.deepEqual(
      await database.transaction((tx) => inspector.getSessionRequests(tx, session.id)),
      []
    );
  }
);

test(
  "registered key issuance rechecks quota atomically and concurrent idempotency never reissues a secret",
  options,
  async () => {
    await database.transaction((tx) =>
      keys.setProviderKeyLimit(tx, "services-provider", { maxActiveKeys: 1 })
    );
    const contenders = await Promise.allSettled(
      ["one", "two"].map((name) =>
        database.transaction((tx) =>
          keys.issueRegisteredKey(tx, { name, provider: "services-provider" })
        )
      )
    );
    assert.equal(contenders.filter((item) => item.status === "fulfilled").length, 1);
    const rejected = contenders.find((item) => item.status === "rejected");
    assert.ok(
      rejected?.status === "rejected" && rejected.reason instanceof RegisteredKeyQuotaError
    );
    const duplicate = await Promise.all(
      Array.from({ length: 2 }, () =>
        database.transaction((tx) =>
          keys.issueRegisteredKey(tx, { name: "idempotent", idempotencyKey: "services-idempotent" })
        )
      )
    );
    assert.equal(duplicate.filter((item) => "rawKey" in item).length, 1);
    assert.equal(duplicate.filter((item) => "idempotencyConflict" in item).length, 1);
  }
);

test(
  "Radar key changes clear every entitlement-sensitive feed and overrides preserve tombstones",
  options,
  async () => {
    await database.transaction(async (tx) => {
      await radar.setRadarCache(tx, {
        version: "v1",
        tier: "community",
        payload: "{}",
        signature: "s",
      });
      await radar.setRadarReferralsCache(tx, {
        generatedAt: "now",
        tier: "community",
        payload: "{}",
        signature: "s",
      });
      await radar.setRadarOffersCache(tx, {
        version: "v1",
        tier: "live",
        payload: "{}",
        signature: "s",
      });
      await radar.setRadarIntelCache(tx, {
        version: "v1",
        tier: "live",
        payload: "{}",
        signature: "s",
        supporterIdentity: "radar:" + "a".repeat(64),
      });
      await radar.setRadarKey(tx, "supporter-secret");
    });
    const caches = await database.transaction(async (tx) => [
      await radar.getRadarCache(tx),
      await radar.getRadarReferralsCache(tx),
      await radar.getRadarOffersCache(tx),
      await radar.getRadarIntelCache(tx),
    ]);
    assert.deepEqual(caches, [null, null, null, null]);
    await database.transaction(async (tx) => {
      await radar.setRadarModelTombstone(tx, "provider", "model", true);
      await radar.setRadarLocalModelOverride(tx, "provider", "model", { displayName: "Display" });
      await radar.clearRadarLocalModelOverride(tx, "provider", "model");
    });
    const local = await database.transaction((tx) => radar.getRadarLocalMergeState(tx));
    assert.ok(local.tombstones.has("provider:model"));
    assert.equal(local.localOverrides.size, 0);
  }
);

test(
  "webhook history is bounded and its public projection omits captured payloads",
  options,
  async () => {
    const webhook = await database.transaction((tx) =>
      webhooks.createWebhook(tx, { url: "https://example.test/webhook" })
    );
    await database.transaction(async (tx) => {
      for (let index = 0; index < 105; index++)
        await webhooks.insertDelivery(tx, {
          webhookId: webhook.id,
          eventType: "test",
          status: "success",
          payloadSnapshot: "private",
          error: "at /private/trace.ts:1",
        });
    });
    const deliveries = await database.transaction((tx) =>
      webhooks.getDeliveries(tx, webhook.id, 200)
    );
    assert.equal(deliveries.length, 100);
    assert.equal(
      deliveries.some((row) => Object.hasOwn(row, "payload_snapshot")),
      false
    );
    assert.equal(
      deliveries.some((row) => row.error?.includes("/private/trace")),
      false
    );
  }
);

test(
  "job re-registration preserves operator disable and log acknowledgements cannot regress cursor",
  options,
  async () => {
    const now = new Date().toISOString();
    const job = {
      id: "services-job",
      type: "interval" as const,
      cron: null,
      intervalMs: 10000,
      enabled: true,
      envFlag: null,
      config: {},
      createdAt: now,
      updatedAt: now,
    };
    await database.transaction(async (tx) => {
      await operations.upsertJob(tx, job);
      await operations.updateJobEnabled(tx, job.id, false);
      await operations.upsertJob(tx, { ...job, intervalMs: 20000 });
    });
    assert.equal(
      (await database.transaction((tx) => operations.getJob(tx, job.id)))!.enabled,
      false
    );
    const destination = await database.transaction((tx) =>
      operations.createLogExportDestination(tx, { name: "test", type: "test", config: {} })
    );
    await database.transaction(async (tx) => {
      await operations.advanceLogExportCursor(tx, destination.id, 10, 4);
      await operations.advanceLogExportCursor(tx, destination.id, 10, 4);
      await operations.advanceLogExportCursor(tx, destination.id, 9, 4);
    });
    const stored = await database.transaction((tx) =>
      operations.getLogExportDestination(tx, destination.id)
    );
    assert.equal(stored!.cursorRowId, 10);
    assert.equal(stored!.exportedTotal, 4);
  }
);

test(
  "new service credentials are ciphertext and partial metadata writes preserve the stored bytes",
  options,
  async () => {
    const tool = await database.transaction((tx) =>
      versions.upsertVersionManagerTool(tx, {
        tool: "services-credentials",
        apiKey: "private-api",
        managementKey: "private-management",
      })
    );
    assert.equal(tool.apiKey, "private-api");
    const before = await database.transaction((tx) =>
      tx.query<{ api_key: string; management_key: string }>(
        "SELECT api_key, management_key FROM omniroute.version_manager WHERE tool = $1",
        [tool.tool]
      )
    );
    assert.ok(looksEncrypted(before.rows[0].api_key));
    assert.ok(looksEncrypted(before.rows[0].management_key));
    await database.transaction((tx) =>
      versions.updateVersionManagerTool(tx, tool.tool, { healthStatus: "healthy" })
    );
    const after = await database.transaction((tx) =>
      tx.query<{ api_key: string; management_key: string }>(
        "SELECT api_key, management_key FROM omniroute.version_manager WHERE tool = $1",
        [tool.tool]
      )
    );
    assert.deepEqual(after.rows, before.rows);
  }
);

test("eval suite replacement rolls back if a later case is invalid", options, async () => {
  const suite = await database.transaction((tx) =>
    evals.saveCustomEvalSuite(tx, {
      name: "transactional",
      cases: [
        {
          name: "valid",
          input: { messages: [{ role: "user", content: "hello" }] },
          expected: { strategy: "contains", value: "yes" },
        },
      ],
    })
  );
  await assert.rejects(
    database.transaction((tx) =>
      evals.saveCustomEvalSuite(tx, {
        id: suite.id,
        name: "changed",
        cases: [
          {
            name: "invalid",
            input: { messages: [] },
            expected: { strategy: "contains", value: "yes" },
          },
        ],
      })
    )
  );
  const stored = await database.transaction((tx) => evals.getCustomEvalSuite(tx, suite.id));
  assert.equal(stored!.name, "transactional");
  assert.equal(stored!.cases[0].name, "valid");
});
