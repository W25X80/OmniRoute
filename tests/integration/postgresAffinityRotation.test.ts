import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type {
  ChatCoreExecutorResult,
  PipelineConnectionContext,
  PipelineStateHooks,
  PipelineTargetContext,
  PipelineWireState,
  ProviderExecutionPipelineInput,
  ProviderExecutionPolicy,
} from "../../open-sse/handlers/chatCore/providerExecutionPipeline.ts";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresControlPlane } from "../../src/lib/db/controlPlaneContext.ts";
import type { PostgresDatabase, PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { createPostgresSessionRepository } from "../../src/lib/db/postgres/sessionRepository.ts";
import { deleteSessionAccountAffinityAsync } from "../../src/lib/db/sessionAccountAffinity.ts";
import {
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function jsonResponse(body: unknown, status: number, extraHeaders: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...extraHeaders },
  });
}

function makeAttempt(
  body: unknown,
  status: number,
  extra: Partial<ChatCoreExecutorResult> = {}
): ChatCoreExecutorResult {
  const response = jsonResponse(body, status, extra.headers as Record<string, string> | undefined);
  return {
    response,
    url: extra.url ?? "https://upstream.test/v1/chat/completions",
    headers: extra.headers ?? { "content-type": "application/json" },
    transformedBody: extra.transformedBody ?? { model: "gpt-5" },
    ...extra,
  };
}

function noopState(): PipelineStateHooks {
  return {
    updatePendingStage: () => {},
    recordRateLimitHeaders: () => {},
    recordRateLimitBody: () => {},
    writeTerminalStatus: async () => {},
    persistConnectionPatch: () => {},
    setConnectionRateLimitedUntil: () => {},
    lockModel: () => {},
    recordAntigravityQuotaState: async () => {},
    markAccountSemaphoreBlocked: () => {},
    isolateProbeFailures: () => false,
  };
}

function makeInput(opts: {
  policy: ProviderExecutionPolicy;
  provider: string;
  model?: string;
  stream?: boolean;
  connectionId?: string;
  send: (model: string, allowDedup: boolean) => Promise<ChatCoreExecutorResult>;
  getProviderCredentials?: PipelineConnectionContext["getProviderCredentials"];
  replaceCredentials?: PipelineConnectionContext["replaceCredentials"];
  getCurrentConnectionId?: () => string | undefined;
  refreshCredentials?: PipelineConnectionContext["refreshCredentials"];
  onCredentialsRefreshed?: PipelineConnectionContext["onCredentialsRefreshed"];
  getNextFamilyFallback?: ProviderExecutionPipelineInput["getNextFamilyFallback"];
  state?: Partial<PipelineStateHooks>;
}): ProviderExecutionPipelineInput {
  const model = opts.model ?? "gpt-5";
  const connectionId = opts.connectionId ?? "conn-a";
  let currentId: string | undefined = connectionId;
  let credentials: Record<string, unknown> = { connectionId };
  const target: PipelineTargetContext = {
    provider: opts.provider,
    requestedModel: model,
    sourceFormat: "openai",
    targetFormat: "openai",
    stream: opts.stream ?? false,
  };
  const wire: PipelineWireState = {
    body: { model, messages: [{ role: "user", content: "hi" }] },
    currentModel: model,
    triedModels: new Set([model]),
    setBodyAndModel: (body, nextModel) => {
      wire.body = body;
      wire.currentModel = nextModel;
      wire.triedModels.add(nextModel);
    },
  };
  const connection: PipelineConnectionContext = {
    initialConnectionId: connectionId,
    getCurrentConnectionId: opts.getCurrentConnectionId ?? (() => currentId),
    getCredentials: () => credentials,
    replaceCredentials:
      opts.replaceCredentials ??
      ((next) => {
        credentials = next;
        currentId = typeof next.connectionId === "string" ? next.connectionId : currentId;
      }),
    onCredentialsRefreshed: opts.onCredentialsRefreshed ?? (() => {}),
    assertManagedLeaseFence: () => {},
    refreshCredentials: opts.refreshCredentials,
    getProviderCredentials:
      opts.getProviderCredentials ??
      (async () => {
        throw new Error("getProviderCredentials must not be called in this fixture");
      }),
  };
  return {
    policy: opts.policy,
    target,
    connection,
    wire,
    state: { ...noopState(), ...(opts.state || {}) },
    sendProviderAttempt: opts.send,
    getNextFamilyFallback: opts.getNextFamilyFallback,
  };
}

test(
  "native Codex rotation waits for durable affinity deletion without opening SQLite",
  {
    skip: !postgresTestsEnabled,
  },
  async () => {
    const directory = mkdtempSync(join(tmpdir(), "pg-affinity-rotation-"));
    const saved = {
      DATA_DIR: process.env.DATA_DIR,
      OMNIROUTE_DATABASE_BACKEND: process.env.OMNIROUTE_DATABASE_BACKEND,
      STORAGE_ENCRYPTION_KEY: process.env.STORAGE_ENCRYPTION_KEY,
      JWT_SECRET: process.env.JWT_SECRET,
      API_KEY_SECRET: process.env.API_KEY_SECRET,
    };
    let plane: Awaited<ReturnType<typeof initializePostgresControlPlane>> | undefined;
    let release!: () => void;
    try {
      process.env.DATA_DIR = directory;
      process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
      process.env.STORAGE_ENCRYPTION_KEY = "isolated-affinity-rotation-storage-secret";
      process.env.JWT_SECRET = "isolated-affinity-rotation-jwt-secret";
      process.env.API_KEY_SECRET = "isolated-affinity-rotation-api-secret";
      plane = await initializePostgresControlPlane({
        connection: await testPostgresOptions({ maxConnections: 4 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      });
      await plane.run(async (binding) => {
        assert.ok(binding.database);
        const sessions = createPostgresSessionRepository(binding.database);
        const session = randomUUID();
        await sessions.upsertSessionAccountAffinity(session, "codex", "conn-a", Date.now(), 60_000);
        let deleteStarted!: () => void;
        const started = new Promise<void>((resolve) => {
          deleteStarted = resolve;
        });
        const gate = new Promise<void>((resolve) => {
          release = resolve;
        });
        const intercepted = new Proxy(binding.database, {
          get(target, property, receiver) {
            if (property !== "transaction") return Reflect.get(target, property, receiver);
            return <T>(body: (tx: PostgresTransaction) => Promise<T>) =>
              target.transaction((tx) =>
                body({
                  query: async <Row = Record<string, unknown>>(sql: string, values?: unknown[]) => {
                    if (
                      sql.includes("DELETE FROM omniroute.key_value") &&
                      sql.includes("session_account_affinity")
                    ) {
                      deleteStarted();
                      await gate;
                    }
                    return tx.query<Row>(sql, values);
                  },
                })
              );
          },
        }) as PostgresDatabase;
        const { runProviderExecutionPipeline } =
          await import("../../open-sse/handlers/chatCore/providerExecutionPipeline.ts");
        let sends = 0,
          selections = 0;
        const input = makeInput({
          policy: { allowAccountRotation: true, allowModelFallback: true },
          provider: "codex",
          send: async () => {
            sends++;
            return sends === 1
              ? makeAttempt({ error: { message: "rate limited", type: "rate_limit_error" } }, 429)
              : makeAttempt({ choices: [{ message: { role: "assistant", content: "ok" } }] }, 200);
          },
          getProviderCredentials: (async () => {
            selections++;
            assert.equal(await sessions.getSessionAccountAffinity(session, "codex", 60_000), null);
            return { connectionId: "conn-b", allRateLimited: false };
          }) as PipelineConnectionContext["getProviderCredentials"],
          state: {
            onClearSessionAffinity: () => deleteSessionAccountAffinityAsync(session, "codex"),
          },
        });
        const execution = withPostgresControlPlane({ ...binding, database: intercepted }, () =>
          runProviderExecutionPipeline(input)
        );
        try {
          await started;
          assert.equal(sends, 1);
          assert.equal(selections, 0);
          assert.equal(existsSync(join(directory, "storage.sqlite")), false);
        } finally {
          release();
        }
        const outcome = await execution;
        assert.equal(outcome.kind, "response");
        assert.equal(sends, 2);
        assert.equal(selections, 1);
        assert.equal(await sessions.getSessionAccountAffinity(session, "codex", 60_000), null);
        assert.equal(existsSync(join(directory, "storage.sqlite")), false);
      });
    } finally {
      release?.();
      await plane?.close();
      for (const [name, value] of Object.entries(saved)) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
