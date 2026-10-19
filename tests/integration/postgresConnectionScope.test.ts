import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { closeNativeStreamingFixture } from "../helpers/persistence/nativeStreamingFixtureShutdown.ts";
import { streamingNativeFixture } from "../helpers/persistence/streamingNativeIntegrationFixture.ts";
import type { PostgresDatabase } from "../../src/lib/db/postgres/database.ts";

// Real Request route, authentication, generated quota combo and PostgreSQL repositories.
// The provider HTTP and S3 transport are controlled; no live-provider/AWS claim is made.
test(
  "native quota combos cannot widen a disjoint key connection scope",
  { skip: !postgresTestsEnabled, timeout: 45000 },
  async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "omni-native-key-scope-"));
    const environment = [
      "OMNIROUTE_DATABASE_BACKEND",
      "DATA_DIR",
      "STORAGE_ENCRYPTION_KEY",
      "APP_LOG_TO_FILE",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "STREAM_RECOVERY_ENABLED",
      "OMNIROUTE_BUILDING",
      "NEXT_PHASE",
    ];
    const previous = new Map(environment.map((key) => [key, process.env[key]]));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.DATA_DIR = directory;
    process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
    process.env.APP_LOG_TO_FILE = "false";
    process.env.STREAM_RECOVERY_ENABLED = "false";
    delete process.env.OMNIROUTE_BUILDING;
    delete process.env.NEXT_PHASE;
    const runtimeUrl = new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url).href;
    const contextUrl = new URL("../../src/lib/db/controlPlaneContext.ts", import.meta.url).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/streamingNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const probeUrl = new URL("../helpers/persistence/streamingSqliteGuardProbe.ts", import.meta.url)
      .href;
    const hooks = registerHooks({
      resolve(specifier, context, next) {
        if (
          context.parentURL === runtimeUrl &&
          [
            "./postgres/environment",
            "./postgres/controlPlane",
            "../artifacts/s3ArtifactConfiguration",
            "../artifacts/s3ArtifactStore",
          ].includes(specifier)
        )
          return { url: fixtureUrl, shortCircuit: true };
        const resolved = next(specifier, context);
        if (resolved.url === contextUrl && context.parentURL !== probeUrl)
          return { url: probeUrl, shortCircuit: true };
        return resolved;
      },
    });
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    let verification: PostgresDatabase | undefined;
    const keys: Array<{ id: string; key: string }> = [];
    const connections: string[] = [];
    let poolId = "",
      groupId = "";
    const originalFetch = globalThis.fetch;
    const dispatched: string[] = [];
    try {
      runtime = await import(runtimeUrl);
      await runtime.ensurePersistenceReady();
      verification = await createTestPostgresDatabase();
      const { createApiKey, updateApiKeyPermissions } = await import("../../src/lib/db/apiKeys.ts");
      const { createProviderConnection } = await import("../../src/lib/db/providers.ts");
      const pools = await import("../../src/lib/db/quotaPools.ts");
      const { PostgresQuotaRepository } =
        await import("../../src/lib/db/postgres/quotaRepository.ts");
      const { quotaModelName } = await import("../../src/lib/quota/quotaModelNaming.ts");
      const { getComboByName } = await import("../../src/lib/db/combos.ts");
      const { GlmExecutor } = await import("../../open-sse/executors/glm.ts");
      const { POST } = await import("../../src/app/api/v1/chat/completions/route.ts");
      const tag = randomUUID();
      const name = `private-scope-${tag}`;
      const target = quotaModelName(name, "glm", "glm-4.7");
      const repository = new PostgresQuotaRepository(verification);
      await runtime.runPersistenceRequest(async () => {
        for (const label of ["A", "B"]) {
          const connection = await createProviderConnection({
            provider: "glm",
            authType: "apikey",
            apiKey: `fixture-${label}-${tag}`,
            isActive: true,
            name: `${name}-${label}`,
            providerSpecificData: { primaryTransport: "openai" },
          });
          connections.push(String(connection.id));
          const key = await createApiKey(`${name}-${label}`, `scope-${label}-${tag}`, [], {
            allowedConnections: [String(connection.id)],
          });
          keys.push({ id: String(key.id), key: String(key.key) });
        }
        groupId = (await repository.createGroup(name)).id;
        const pool = await pools.createPoolAsync({
          groupId,
          name,
          connectionId: connections[1],
          allocations: keys.map(({ id }) => ({ apiKeyId: id, weight: 100, policy: "hard" })),
        });
        poolId = pool.id;
        for (const key of keys) await updateApiKeyPermissions(key.id, { allowedQuotas: [poolId] });
        const generated = await getComboByName(target);
        assert.ok(generated, "production pool maintenance must create the requested quota combo");
      });
      const execute = GlmExecutor.prototype.execute;
      t.mock.method(
        GlmExecutor.prototype,
        "execute",
        function (this: InstanceType<typeof GlmExecutor>, input: Parameters<typeof execute>[0]) {
          dispatched.push(String(input.credentials.connectionId));
          return Reflect.apply(execute, this, [input]);
        }
      );
      globalThis.fetch = async () =>
        Response.json({
          id: "private-scope-answer",
          object: "chat.completion",
          model: "glm-4.7",
          choices: [
            { index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" },
          ],
          usage: { prompt_tokens: 4, completion_tokens: 2, total_tokens: 6 },
        });
      const serve = (key: string) =>
        runtime!.runPersistenceRequest(() =>
          POST(
            new Request("http://localhost/v1/chat/completions", {
              method: "POST",
              headers: { "content-type": "application/json", authorization: `Bearer ${key}` },
              body: JSON.stringify({
                model: target,
                stream: false,
                max_tokens: 8,
                messages: [{ role: "user", content: "Scope oracle" }],
              }),
            })
          )
        );
      const negative = await serve(keys[0].key);
      assert.equal(negative.status, 503, await negative.clone().text());
      await negative.text();
      assert.deepEqual(dispatched, [], "key A cannot dispatch the sole group member B");
      const positive = await serve(keys[1].key);
      assert.equal(positive.status, 200, await positive.clone().text());
      assert.equal((await positive.json()).choices[0].message.content, "ok");
      assert.deepEqual(dispatched, [connections[1]], "key B must retain access to group member B");
      assert.equal(
        streamingNativeFixture.sqliteAttempts,
        0,
        streamingNativeFixture.sqliteAttemptStacks.join("\n")
      );
    } finally {
      globalThis.fetch = originalFetch;
      t.mock.restoreAll();
      try {
        if (runtime && poolId)
          await runtime.runPersistenceRequest(async () => {
            const { deletePool } = await import("../../src/lib/db/quotaPools.ts");
            await deletePool(poolId);
          });
      } finally {
        try {
          await closeNativeStreamingFixture(runtime);
        } finally {
          try {
            if (verification) {
              await verification.transaction(async (tx) => {
                for (const { id } of keys) {
                  for (const table of [
                    "call_logs",
                    "usage_history",
                    "request_cost_ledger",
                    "domain_cost_receipts",
                    "domain_cost_history",
                    "api_key_quota_counters",
                  ])
                    await tx.query(`DELETE FROM omniroute.${table} WHERE api_key_id=$1`, [id]);
                  await tx.query("DELETE FROM omniroute.api_keys WHERE id=$1", [id]);
                }
                for (const id of connections)
                  await tx.query("DELETE FROM omniroute.provider_connections WHERE id=$1", [id]);
                if (groupId)
                  await tx.query("DELETE FROM omniroute.quota_groups WHERE id=$1", [groupId]);
              });
            }
          } finally {
            try {
              await verification?.close();
            } finally {
              hooks.deregister();
              delete globalThis.__omniroutePersistenceRuntime;
              for (const [key, value] of previous) {
                if (value === undefined) delete process.env[key];
                else process.env[key] = value;
              }
              await rm(directory, { recursive: true, force: true });
            }
          }
        }
      }
    }
  }
);
