import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { getSelectedPostgresDatabase } from "../../src/lib/db/controlPlaneContext.ts";
import { postgresTestsEnabled } from "../helpers/persistence/postgresTestDatabase.ts";

test(
  "native optional usage rejection preserves an already successful paid response",
  {
    skip: !postgresTestsEnabled,
    timeout: 30_000,
  },
  async (t) => {
    const runtimeUrl = pathToFileURL(
      realpathSync(new URL("../../src/lib/db/persistenceRuntime.ts", import.meta.url))
    ).href;
    const fixtureUrl = new URL(
      "../helpers/persistence/runtimeNativeIntegrationFixture.ts",
      import.meta.url
    ).href;
    const hooks = registerHooks({
      resolve(specifier, context, nextResolve) {
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
        return nextResolve(specifier, context);
      },
    });
    const directory = mkdtempSync(join(tmpdir(), "pg-completed-usage-observer-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
    };
    let runtime: typeof import("../../src/lib/db/persistenceRuntime.ts") | undefined;
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown) => {
      unhandled.push(reason);
    };
    const model = `optional-completed-${randomUUID().replaceAll("-", "")}`;
    let triggerInstalled = false;
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "3d".repeat(32);
      process.env.JWT_SECRET = "isolated-failure-usage-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-failure-usage-api-secret";
      assert.equal(globalThis.__omniroutePersistenceRuntime, undefined);
      runtime = await import(runtimeUrl);
      await runtime!.ensurePersistenceReady();
      await runtime!.runPersistenceRequest(async () => {
        await getSelectedPostgresDatabase()!.transaction(async (tx) => {
          await tx.query(`CREATE FUNCTION omniroute.reject_completed_usage() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'completed-private-fault-no-leak'; END $$`);
          await tx.query(`CREATE TRIGGER reject_completed_usage BEFORE INSERT ON omniroute.usage_history
          FOR EACH ROW WHEN (NEW.model='${model}') EXECUTE FUNCTION omniroute.reject_completed_usage()`);
        });
      });
      triggerInstalled = true;
      const warnings: string[] = [];
      const reported: Array<() => void> = [];
      t.mock.method(console, "warn", (...args: unknown[]) => {
        const message = args.map(String).join(" ");
        if (message.startsWith("[USAGE] Failed to persist completed-request usage:")) {
          warnings.push(message);
          reported.shift()?.();
        }
      });
      let providerSends = 0;
      t.mock.method(globalThis, "fetch", async () => {
        providerSends++;
        return new Response(
          JSON.stringify({
            id: "chatcmpl_completed_usage",
            object: "chat.completion",
            model,
            choices: [
              {
                index: 0,
                message: { role: "assistant", content: "paid answer" },
                finish_reason: "stop",
              },
            ],
            usage: { prompt_tokens: 7, completion_tokens: 3, total_tokens: 10 },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      });
      process.on("unhandledRejection", onUnhandled);
      for (const stream of [false]) {
        const warning = new Promise<void>((resolve) => {
          reported.push(resolve);
        });
        const result = await runtime!.runPersistenceRequest(async () => {
          const { handleChatCore } = await import("../../open-sse/handlers/chatCore.ts");
          const body = {
            model: `openai/${model}`,
            messages: [{ role: "user", content: "hi" }],
            max_tokens: 8,
            stream,
          };
          return handleChatCore({
            body,
            modelInfo: { provider: "openai", model, extendedContext: false },
            credentials: { apiKey: "sk-test-not-real", providerSpecificData: {} },
            log: { debug() {}, info() {}, warn() {}, error() {} },
            clientRawRequest: {
              endpoint: "/v1/chat/completions",
              body,
              headers: new Headers({ accept: "application/json" }),
            },
            userAgent: "native-optional-completed-test",
            connectionId: undefined,
            onCredentialsRefreshed: undefined,
            onRequestSuccess: undefined,
            onStreamFailure: undefined,
            onDisconnect: undefined,
            comboName: undefined,
          });
        });
        assert.equal(result.success, true);
        assert.equal(result.response.status, 200);
        const payload = await result.response.json();
        assert.equal(payload.choices[0].message.content, "paid answer");
        assert.equal(payload.usage.prompt_tokens, 7);
        assert.equal(payload.usage.completion_tokens, 3);
        assert.equal(providerSends, 1, "an optional write failure must not resend upstream");
        await warning;
      }
      await new Promise<void>((resolve) => setImmediate(resolve));
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(warnings.length, 1);
      assert.equal(unhandled.length, 0);
      assert.ok(
        warnings.every(
          (message) =>
            !message.includes("completed-private-fault-no-leak") && !message.includes("at /")
        )
      );
      await runtime!.runPersistenceRequest(async () => {
        const result = await getSelectedPostgresDatabase()!.transaction((tx) =>
          tx.query<{ count: string }>(
            "SELECT COUNT(*)::text AS count FROM omniroute.usage_history WHERE model=$1",
            [model]
          )
        );
        assert.equal(result.rows[0].count, "0");
      });
      assert.equal(existsSync(join(directory, "storage.sqlite")), false);
    } finally {
      process.off("unhandledRejection", onUnhandled);
      if (triggerInstalled)
        await runtime?.runPersistenceRequest(async () => {
          await getSelectedPostgresDatabase()!.transaction(async (tx) => {
            await tx.query("DROP TRIGGER reject_completed_usage ON omniroute.usage_history");
            await tx.query("DROP FUNCTION omniroute.reject_completed_usage()");
          });
        });
      await runtime?.closePersistenceRuntime().catch(() => {});
      t.mock.restoreAll();
      hooks.deregister();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
