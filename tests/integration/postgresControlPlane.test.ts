import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { existsSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import {
  PostgresSecretError,
  createPostgresSecretRepository,
} from "../../src/lib/db/postgres/secretRepository.ts";
import type { PostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";

test(
  "PostgreSQL control-plane initialization and existing facade composition",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "omniroute-pg-control-plane-"));
    const saved = {
      data: process.env.DATA_DIR,
      encryption: process.env.STORAGE_ENCRYPTION_KEY,
      jwt: process.env.JWT_SECRET,
      api: process.env.API_KEY_SECRET,
      password: process.env.INITIAL_PASSWORD,
      backend: process.env.OMNIROUTE_DATABASE_BACKEND,
    };
    process.env.DATA_DIR = directory;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-pg-control-plane-encryption-key";
    delete process.env.JWT_SECRET;
    delete process.env.API_KEY_SECRET;
    delete process.env.INITIAL_PASSWORD;
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const settings = await import("../../src/lib/db/settings.ts");
    const providers = await import("../../src/lib/db/providers.ts");
    const keys = await import("../../src/lib/db/apiKeys.ts");
    const caches = await import("../../src/lib/db/readCache.ts");
    const sqlite = await import("../../src/lib/db/core.ts");
    const legacySecrets = await import("../../src/lib/db/secrets.ts");
    const { UnsupportedPersistenceDomainError, getSelectedControlPlane } =
      await import("../../src/lib/db/controlPlaneContext.ts");
    const database = await createTestPostgresDatabase();
    const planes: PostgresControlPlane[] = [];
    let reloads = 0;
    const options = {
      connection: await testPostgresOptions(),
      logger: { warn() {} },
      runtime: {
        applyRuntimeSettings() {
          reloads++;
        },
        scheduleCatalogRevalidationAfterInit() {},
      },
    };
    try {
      await database.transaction((tx) => tx.query("DROP SCHEMA IF EXISTS omniroute CASCADE"));
      // Migration concurrency is qualified independently. This witness isolates bootstrap
      // secret contenders from first-install DDL and its deliberately finite lock budget.
      await migratePostgresSchema(database);
      const initialized = await Promise.allSettled([
        initializePostgresControlPlane(options),
        initializePostgresControlPlane(options),
      ]);
      for (const result of initialized) {
        if (result.status === "fulfilled") planes.push(result.value);
      }
      for (const result of initialized) {
        if (result.status === "rejected") throw result.reason;
      }
      const [first, second] = planes;

      await t.test(
        "startup lock failure is bounded and exposes no ready control plane",
        async () => {
          let release!: () => void;
          let entered!: () => void;
          const gate = new Promise<void>((resolve) => {
            release = resolve;
          });
          const ready = new Promise<void>((resolve) => {
            entered = resolve;
          });
          const holder = database.transaction(async (tx) => {
            await tx.query("SELECT pg_advisory_xact_lock(18765, 1)");
            entered();
            await gate;
          });
          try {
            await ready;
            await assert.rejects(
              initializePostgresControlPlane({
                ...options,
                connection: { ...options.connection, lockTimeoutMs: 50 },
              }),
              (error: PostgresError) => error instanceof PostgresError && error.sqlState === "55P03"
            );
            assert.equal(process.env.JWT_SECRET, undefined);
            assert.equal(process.env.API_KEY_SECRET, undefined);
          } finally {
            release();
            await holder;
          }
        }
      );

      await t.test(
        "concurrent bootstrap uses one encrypted durable winner and preserves global env",
        async () => {
          assert.deepEqual(first.bootstrapSecrets, second.bootstrapSecrets);
          assert.equal(first.state, "ready");
          assert.equal(process.env.JWT_SECRET, undefined);
          assert.equal(process.env.API_KEY_SECRET, undefined);
          const { rows } = await database.transaction((tx) =>
            tx.query<{ key: string; value: string }>(
              "SELECT key,value FROM omniroute.key_value WHERE namespace='secrets' ORDER BY key"
            )
          );
          assert.equal(rows.length, 2);
          assert.ok(rows.every((row) => JSON.parse(row.value).startsWith("enc:v1:")));
          assert.equal(existsSync(join(directory, "storage.sqlite")), false);
        }
      );

      let key = "";
      let providerId = "";
      await t.test(
        "existing async facades perform settings/provider/key CRUD without SQLite",
        async () => {
          await first.run(async () => {
            await settings.updateSettings(
              { cloudEnabled: true, oidcClientSecret: "oidc-control-secret" },
              { expectedRevision: 0 }
            );
            assert.equal((await caches.getCachedSettings()).cloudEnabled, true);
            assert.equal((await settings.getSettings()).oidcClientSecret, "oidc-control-secret");
            assert.equal(await settings.getSettingsRevision(), 1);
            const connection = await providers.createProviderConnection({
              provider: "openai",
              authType: "apikey",
              apiKey: "provider-control-secret",
            });
            assert.ok(connection);
            providerId = String(connection.id);
            const raw = await providers.getRawProviderConnections({ provider: "openai" });
            assert.equal(raw.length, 1);
            assert.match(String(raw[0].apiKey), /^enc:v1:/);
            assert.equal(
              (await providers.getProviderConnectionById(providerId))?.apiKey,
              "provider-control-secret"
            );
            assert.equal(await providers.getProviderConnectionsCountAsync(), 1);
            const created = await keys.createApiKey("control-plane", "test-machine");
            key = created.key;
            assert.equal(await keys.validateApiKey(key), true);
            assert.equal((await keys.getApiKeyMetadata(key))?.id, created.id);
            assert.equal(await keys.getApiKeysCountAsync(), 1);
            const group = await getSelectedControlPlane()!.keyGroups.createKeyGroup("engineering");
            await getSelectedControlPlane()!.keyGroups.addGroupPermission(
              group.id,
              "*",
              "allow",
              "openai"
            );
            await getSelectedControlPlane()!.keyGroups.addKeyToGroup(created.id, group.id);
            assert.equal(await keys.isModelAllowedForKey(key, "openai/gpt-4.1"), true);
            assert.equal(await keys.isModelAllowedForKey(key, "anthropic/claude"), false);
            assert.equal(
              await legacySecrets.getPersistedSecretAsync("jwtSecret"),
              first.bootstrapSecrets.jwtSecret
            );
            assert.throws(() => sqlite.getDbInstance(), UnsupportedPersistenceDomainError);
            assert.throws(() => keys.getApiKeysCount(), UnsupportedPersistenceDomainError);
            assert.throws(
              () => legacySecrets.getPersistedSecret("jwtSecret"),
              UnsupportedPersistenceDomainError
            );
            assert.equal(typeof (await caches.getCachedPricing()), "object");
          });
          assert.equal(reloads, 1);
          assert.throws(getSelectedControlPlane, /PostgreSQL application persistence is not ready/);
          assert.equal(existsSync(join(directory, "storage.sqlite")), false);
        }
      );

      await t.test("a second composition sees current settings, keys and revocation", async () => {
        await second.run(async () => {
          assert.equal((await caches.getCachedSettings()).cloudEnabled, true);
          assert.equal(await keys.validateApiKey(key), true);
          const metadata = await keys.getApiKeyMetadata(key);
          assert.ok(metadata);
          assert.equal(await keys.revokeApiKey(metadata.id), true);
        });
        await first.run(async () => assert.equal(await keys.validateApiKey(key), false));
      });

      await t.test(
        "operator-only CLI actions preserve native asynchronous persistence",
        async () => {
          const tokenRoute = await import("../../src/app/api/cli/tokens/route.ts");
          const keyRoute = await import("../../src/app/api/cli-tools/keys/route.ts");
          const accessTokens = await import("../../src/lib/db/accessTokens.ts");
          const { SignJWT } = await import("jose");
          const jwtEnvironment = process.env.JWT_SECRET;
          const session = await new SignJWT({ authenticated: true })
            .setProtectedHeader({ alg: "HS256" })
            .setIssuedAt()
            .setExpirationTime("1h")
            .sign(new TextEncoder().encode(first.bootstrapSecrets.jwtSecret));
          process.env.JWT_SECRET = first.bootstrapSecrets.jwtSecret;
          try {
            await first.run(async () => {
              const previous = await settings.getSettings();
              await settings.updateSettings({ requireLogin: true });
              try {
                const machine = await keys.createApiKey("native-cli-machine", "native-cli", [
                  "manage",
                ]);
                const victim = await keys.createApiKey("native-cli-operator", "native-cli", [
                  "admin",
                ]);
                const before = await accessTokens.listAccessTokensAsync();
                const denied = await tokenRoute.POST(
                  new Request("http://localhost/api/cli/tokens", {
                    method: "POST",
                    headers: {
                      authorization: `Bearer ${machine.key}`,
                      "content-type": "application/json",
                    },
                    body: JSON.stringify({ name: "native-denied-admin", scope: "admin" }),
                  })
                );
                assert.equal(denied.status, 403);
                assert.deepEqual(await accessTokens.listAccessTokensAsync(), before);
                const masked = await keyRoute.GET(
                  new Request("http://localhost/api/cli-tools/keys", {
                    headers: { authorization: `Bearer ${machine.key}` },
                  })
                );
                assert.equal(masked.status, 200);
                const text = await masked.text();
                assert.ok(!text.includes(machine.key) && !text.includes(victim.key));
                assert.ok(!text.includes('"rawKey"'));
                const granted = await tokenRoute.POST(
                  new Request("http://localhost/api/cli/tokens", {
                    method: "POST",
                    headers: {
                      cookie: `auth_token=${session}`,
                      "content-type": "application/json",
                    },
                    body: JSON.stringify({ name: "native-operator-admin", scope: "admin" }),
                  })
                );
                assert.equal(granted.status, 200);
                assert.equal((await granted.json()).scope, "admin");
                const plaintext = await keyRoute.GET(
                  new Request("http://localhost/api/cli-tools/keys", {
                    headers: { cookie: `auth_token=${session}` },
                  })
                );
                assert.equal(plaintext.status, 200);
                assert.equal(
                  (await plaintext.json()).keys.find((entry) => entry.id === victim.id).rawKey,
                  victim.key
                );
                assert.throws(() => sqlite.getDbInstance(), UnsupportedPersistenceDomainError);
                assert.equal(process.env.JWT_SECRET, first.bootstrapSecrets.jwtSecret);
              } finally {
                await settings.updateSettings({ requireLogin: previous.requireLogin });
              }
            });
          } finally {
            if (jwtEnvironment === undefined) delete process.env.JWT_SECRET;
            else process.env.JWT_SECRET = jwtEnvironment;
          }
          assert.equal(process.env.JWT_SECRET, jwtEnvironment);
        }
      );

      await t.test(
        "unported synchronous access rejects even with an existing SQLite singleton",
        async () => {
          mkdirSync(directory, { recursive: true });
          const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
          process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
          try {
            const legacy = sqlite.getDbInstance();
            assert.ok(legacy);
            await settings.updateSettings({ cloudEnabled: false });
            assert.equal((await caches.getCachedSettings()).cloudEnabled, false);
            process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
            await first.run(async () => {
              assert.throws(() => sqlite.getDbInstance(), UnsupportedPersistenceDomainError);
              assert.equal((await caches.getCachedSettings()).cloudEnabled, true);
              assert.equal(
                (await caches.getCachedRawProviderConnections())[0] &&
                  (await providers.getProviderConnectionById(providerId))?.id,
                providerId
              );
            });
            process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
            assert.equal((await caches.getCachedSettings()).cloudEnabled, false);
          } finally {
            sqlite.closeDbInstance({ checkpointMode: null });
            if (previousBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
            else process.env.OMNIROUTE_DATABASE_BACKEND = previousBackend;
          }
        }
      );

      await t.test(
        "escaped repositories and extracted methods cannot start work after a run settles",
        async () => {
          const escaped = await second.run((repositories) => ({
            apiKeys: repositories.apiKeys,
            groups: repositories.keyGroups,
            secrets: repositories.secrets,
            createGroup: repositories.keyGroups.createKeyGroup,
          }));
          assert.throws(() => escaped.apiKeys.getApiKeys(), /scope has ended/);
          assert.throws(() => escaped.groups.createKeyGroup("escaped"), /scope has ended/);
          assert.throws(() => escaped.createGroup("escaped-method"), /scope has ended/);
          assert.throws(() => escaped.secrets.get("jwtSecret"), /scope has ended/);
          await second.run(async (repositories) => {
            const groups = await repositories.keyGroups.getAllKeyGroups();
            assert.equal(
              groups.some((group) => group.name.startsWith("escaped")),
              false
            );
          });
        }
      );

      await t.test(
        "close stops new scopes, drains an admitted scope and revokes its late continuations",
        async () => {
          let release!: () => void;
          const blocked = new Promise<void>((resolve) => {
            release = resolve;
          });
          let entered!: () => void;
          const ready = new Promise<void>((resolve) => {
            entered = resolve;
          });
          let late!: () => Promise<unknown>;
          const running = first.run(async () => {
            late = () => settings.getSettings();
            // Capture an actual async resource in this scope, rather than only a closure.
            const { AsyncResource } = await import("node:async_hooks");
            const resource = new AsyncResource("pg-control-plane-late-test");
            const continuation = late;
            late = () => resource.runInAsyncScope(continuation);
            entered();
            await blocked;
            assert.equal((await settings.getSettings()).cloudEnabled, true);
          });
          await ready;
          const closing = first.close();
          assert.equal(first.state, "closing");
          await assert.rejects(
            first.run(() => settings.getSettings()),
            /not ready/
          );
          release();
          await running;
          await closing;
          assert.equal(first.state, "closed");
          await assert.rejects(late(), /scope has ended/);
          await first.close();
        }
      );

      await t.test(
        "restart restores secrets; changed encryption key fails before new work",
        async () => {
          const restarted = await initializePostgresControlPlane(options);
          planes.push(restarted);
          assert.deepEqual(restarted.bootstrapSecrets, first.bootstrapSecrets);
          const original = process.env.STORAGE_ENCRYPTION_KEY;
          process.env.STORAGE_ENCRYPTION_KEY = "different-key-must-not-reuse-cached-derived-key";
          try {
            await assert.rejects(
              restarted.run(() => settings.getSettings()),
              /stable explicit/
            );
            await assert.rejects(initializePostgresControlPlane(options), /stable explicit/);
          } finally {
            process.env.STORAGE_ENCRYPTION_KEY = original;
          }
        }
      );

      await t.test(
        "corrupt persistent secrets fail bootstrap and cannot be replaced by random candidates",
        async () => {
          await database.transaction((tx) =>
            tx.query(
              "UPDATE omniroute.key_value SET value=$1 WHERE namespace='secrets' AND key='jwtSecret'",
              [JSON.stringify("enc:v1:invalid:invalid:invalid")]
            )
          );
          await assert.rejects(initializePostgresControlPlane(options), PostgresSecretError);
          let generated = false;
          await assert.rejects(
            createPostgresSecretRepository(database).getOrCreate("jwtSecret", () => {
              generated = true;
              return "replacement";
            }),
            PostgresSecretError
          );
          assert.equal(generated, false);
        }
      );

      await t.test(
        "lost bootstrap COMMIT is unknown, and a later startup reads the durable winner",
        async () => {
          await database.transaction((tx) =>
            tx.query("DELETE FROM omniroute.key_value WHERE namespace='secrets'")
          );
          const proxy = await createPostgresReplyLossProxy({
            upstreamPort: options.connection.port!,
            certificate: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_CA!, "utf8"),
            key: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_KEY!, "utf8"),
            // Target the secret owner, independent of migration/initialization transactions.
            dropCommitAfterQuery:
              "INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('secrets',",
          });
          try {
            await assert.rejects(
              initializePostgresControlPlane({
                ...options,
                connection: {
                  ...options.connection,
                  port: proxy.port,
                  queryTimeoutMs: 1_000,
                  statementTimeoutMs: 800,
                  lockTimeoutMs: 200,
                },
              }),
              (error: PostgresError) =>
                error instanceof PostgresError && error.kind === "commit_unknown"
            );
            assert.equal(proxy.dropped(), true);
            const durable = await createPostgresSecretRepository(database).get("jwtSecret");
            assert.ok(durable);
            const reconciled = await initializePostgresControlPlane(options);
            planes.push(reconciled);
            assert.equal(reconciled.bootstrapSecrets.jwtSecret, durable);
          } finally {
            await proxy.close();
          }
        }
      );
    } finally {
      for (const plane of planes) await plane.close();
      sqlite.closeDbInstance({ checkpointMode: null });
      await database.close();
      const restore = (name: string, value: string | undefined) => {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      };
      restore("DATA_DIR", saved.data);
      restore("STORAGE_ENCRYPTION_KEY", saved.encryption);
      restore("JWT_SECRET", saved.jwt);
      restore("API_KEY_SECRET", saved.api);
      restore("INITIAL_PASSWORD", saved.password);
      restore("OMNIROUTE_DATABASE_BACKEND", saved.backend);
      rmSync(directory, { recursive: true, force: true });
    }
  }
);
