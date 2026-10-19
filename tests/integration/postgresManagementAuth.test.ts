import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import test from "node:test";
import { SignJWT, decodeJwt } from "jose";
import { NextRequest } from "next/server";
import type { SettingsRepository } from "../../src/domain/persistence/settingsRepository.ts";
import type {
  PostgresDatabase,
  PostgresTransaction,
  PostgresTransactionOptions,
} from "../../src/lib/db/postgres/database.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { createPostgresReplyLossProxy } from "../helpers/persistence/postgresReplyLossProxy.ts";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("Native management barrier was not reached")),
          1500
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

type QueryObserver = (sql: string, values?: unknown[]) => void | Promise<void>;

/** Delegate every query and COMMIT to the actual database; observe only an awaited boundary. */
function observeQueries(
  database: PostgresDatabase,
  after: QueryObserver,
  before?: QueryObserver
): PostgresDatabase {
  return new Proxy(database, {
    get(target, property, receiver) {
      if (property === "transaction") {
        return <T>(
          body: (tx: PostgresTransaction) => Promise<T>,
          options?: PostgresTransactionOptions
        ) =>
          target.transaction(
            async (tx) =>
              body({
                noteAdvisoryLock: tx.noteAdvisoryLock?.bind(tx),
                async query<Row>(sql: string, values?: unknown[]) {
                  await before?.(sql, values);
                  const result = await tx.query<Row>(sql, values);
                  await after(sql, values);
                  return result;
                },
              }),
            options
          );
      }
      const value: unknown = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function observeSettingsUpdate(
  repository: SettingsRepository,
  before: () => Promise<void>
): SettingsRepository {
  return {
    get: () => repository.get(),
    getRevision: () => repository.getRevision(),
    async update(updates, options) {
      await before();
      return repository.update(updates, options);
    },
  };
}

function request(path: string, body?: unknown, headers: HeadersInit = {}, method = "POST") {
  return new NextRequest("http://management.test" + path, {
    method,
    headers: { "content-type": "application/json", ...Object.fromEntries(new Headers(headers)) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  });
}

/** Reproduce Next's override envelope, including deletion of stripped trusted headers. */
function forwardedHeaders(response: Response): Headers {
  const names = response.headers.get("x-middleware-override-headers");
  assert.ok(names, "the real pipeline must produce a forwarding envelope");
  const forwarded = new Headers();
  for (const name of names.split(",").map((name) => name.trim())) {
    const value = response.headers.get("x-middleware-request-" + name);
    if (value !== null) forwarded.set(name, value);
  }
  return forwarded;
}

test(
  "native management principals, epochs, atomic mutations and bootstrap outcome authority",
  {
    skip: !postgresTestsEnabled,
    timeout: 120000,
  },
  async (t) => {
    const environmentNames = [
      "OMNIROUTE_DATABASE_BACKEND",
      "STORAGE_ENCRYPTION_KEY",
      "JWT_SECRET",
      "API_KEY_SECRET",
      "INITIAL_PASSWORD",
      "REDIS_URL",
      "OMNIROUTE_PEER_STAMP_TOKEN",
      "OMNIROUTE_CLI_SALT",
      "OMNIROUTE_DISABLE_CLI_TOKEN",
      "OMNIROUTE_INTERNAL_SERVICE_TOKEN",
      "OMNIROUTE_INTERNAL_SERVICE_TOKEN_FILE",
    ];
    const previous = new Map(environmentNames.map((name) => [name, process.env[name]]));
    let closePlane: () => Promise<void> = async () => {};
    let closeVerifier: () => Promise<void> = async () => {};
    t.after(async () => {
      const outcomes = await Promise.allSettled([closePlane(), closeVerifier()]);
      for (const [name, value] of previous)
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      const failures = outcomes.filter((outcome) => outcome.status === "rejected");
      if (failures.length)
        throw new AggregateError(
          failures.map((outcome) => outcome.reason),
          "Native management fixture cleanup failed"
        );
    });
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    process.env.STORAGE_ENCRYPTION_KEY = "7a".repeat(32);
    process.env.JWT_SECRET = "native-management-test-jwt-secret";
    process.env.API_KEY_SECRET = "native-management-test-api-key-secret";
    process.env.OMNIROUTE_PEER_STAMP_TOKEN = "native-management-test-peer-stamp";
    process.env.OMNIROUTE_CLI_SALT = "native-management-test-cli-salt";
    for (const name of [
      "INITIAL_PASSWORD",
      "REDIS_URL",
      "OMNIROUTE_DISABLE_CLI_TOKEN",
      "OMNIROUTE_INTERNAL_SERVICE_TOKEN",
      "OMNIROUTE_INTERNAL_SERVICE_TOKEN_FILE",
    ])
      delete process.env[name];
    const prefix = "native-management-" + randomUUID();
    const keyIds: string[] = [];
    const { initializePostgresControlPlane } =
      await import("../../src/lib/db/postgres/controlPlane.ts");
    const { withPostgresControlPlane } = await import("../../src/lib/db/controlPlaneContext.ts");
    const { withPostgresCatalogSnapshot } =
      await import("../../src/lib/db/postgres/catalogSnapshot.ts");
    const { createPostgresSettingsRepository, PostgresSettingsEffectsError } =
      await import("../../src/lib/db/postgres/settingsRepository.ts");
    const { isPostgresErrorKind } = await import("../../src/lib/db/postgres/database.ts");
    const { createPostgresDatabase } = await import("../../src/lib/db/postgres/connect.ts");
    const options = await testPostgresOptions({
      maxConnections: 6,
      lockTimeoutMs: 2000,
      statementTimeoutMs: 3500,
      queryTimeoutMs: 4000,
      transactionTimeoutMs: 8000,
    });
    const plane = await initializePostgresControlPlane({
      connection: options,
      logger: { warn() {} },
      runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
    });
    closePlane = () => plane.close();
    const verifier = await createTestPostgresDatabase(options);
    closeVerifier = () => verifier.close();
    const originalSettings = (
      await verifier.transaction((tx) =>
        tx.query<{ key: string; value: string }>(
          "SELECT key,value FROM omniroute.key_value WHERE namespace='settings'"
        )
      )
    ).rows;
    const {
      auth,
      passwords,
      sessions,
      bootstrap,
      keys,
      syncTokens,
      syncRecords,
      accessTokens,
      headers,
      pipeline,
      login,
      mainSettings,
      loginSettings,
      relay,
      sync,
      bundle,
      cli,
      cliKeys,
      keyRoute,
      regenerate,
      exportJson,
      importJson,
      exportFile,
      exportAll,
      importFile,
      backups,
    } = await plane.run((binding) =>
      withPostgresCatalogSnapshot(binding.database!, async () => {
        const auth = await import("../../src/lib/db/authSettings.ts");
        const passwords = await import("../../src/lib/auth/managementPassword.ts");
        const sessions = await import("../../src/shared/utils/dashboardSessionToken.ts");
        const bootstrap = await import("../../src/lib/auth/bootstrapToken.ts");
        const keys = await import("../../src/lib/db/apiKeys.ts");
        const syncTokens = await import("../../src/lib/sync/tokens.ts");
        const syncRecords = await import("../../src/lib/db/syncTokens.ts");
        const accessTokens = await import("../../src/lib/db/accessTokens.ts");
        const headers = await import("../../src/server/authz/headers.ts");
        const pipeline = await import("../../src/server/authz/pipeline.ts");
        const login = await import("../../src/app/api/auth/login/route.ts");
        const mainSettings = await import("../../src/app/api/settings/route.ts");
        const loginSettings = await import("../../src/app/api/settings/require-login/route.ts");
        const relay = await import("../../src/app/api/relay/tokens/route.ts");
        const sync = await import("../../src/app/api/sync/tokens/route.ts");
        const bundle = await import("../../src/app/api/sync/bundle/route.ts");
        const cli = await import("../../src/app/api/cli/tokens/route.ts");
        const cliKeys = await import("../../src/app/api/cli-tools/keys/route.ts");
        const keyRoute = await import("../../src/app/api/keys/route.ts");
        const regenerate = await import("../../src/app/api/keys/[id]/regenerate/route.ts");
        const exportJson = await import("../../src/app/api/settings/export-json/route.ts");
        const importJson = await import("../../src/app/api/settings/import-json/route.ts");
        const exportFile = await import("../../src/app/api/db-backups/export/route.ts");
        const exportAll = await import("../../src/app/api/db-backups/exportAll/route.ts");
        const importFile = await import("../../src/app/api/db-backups/import/route.ts");
        const backups = await import("../../src/app/api/db-backups/route.ts");
        return {
          auth,
          passwords,
          sessions,
          bootstrap,
          keys,
          syncTokens,
          syncRecords,
          accessTokens,
          headers,
          pipeline,
          login,
          mainSettings,
          loginSettings,
          relay,
          sync,
          bundle,
          cli,
          cliKeys,
          keyRoute,
          regenerate,
          exportJson,
          importJson,
          exportFile,
          exportAll,
          importFile,
          backups,
        };
      })
    );
    const noEffects = {
      invalidateSettingsCache() {},
      bumpProxyConfigGeneration() {},
      applyRuntimeSettings() {},
      scheduleCatalogRevalidationAfterInit() {},
    };
    const originalHash = await passwords.hashManagementPassword("native-original-password");
    const authSelect = "SELECT key,value FROM omniroute.key_value WHERE namespace='settings'";
    try {
      await plane.run((binding) =>
        withPostgresCatalogSnapshot(binding.database!, async () => {
          async function seed(requireLogin = false, password = true) {
            await verifier.transaction(async (tx) => {
              await tx.query("SELECT pg_advisory_xact_lock(18765,3)");
              await tx.query(
                "DELETE FROM omniroute.key_value WHERE namespace='settings' AND key<>'managementAuthGeneration'"
              );
            });
            await binding.settings.update({
              setupComplete: true,
              requireLogin,
              password: password ? originalHash : "",
              oidcEnabled: false,
              bruteForceProtection: false,
              cloudEnabled: false,
            });
            bootstrap.__resetBootstrapTokenForTest();
            return auth.getAuthSettingsSnapshot();
          }
          async function cookie(expiresSoon = false, issuedAt?: number) {
            const state = await auth.getAuthSettingsSnapshot();
            assert.ok(state.generation);
            const token =
              expiresSoon || issuedAt !== undefined
                ? await new SignJWT({
                    authenticated: true,
                    managementAuthGeneration: state.generation,
                  })
                    .setProtectedHeader({ alg: "HS256" })
                    .setIssuedAt(issuedAt)
                    .setJti(randomUUID())
                    .setExpirationTime(expiresSoon ? "1d" : "30d")
                    .sign(sessions.getDashboardJwtSecret()!)
                : await sessions.mintDashboardSessionToken(
                    sessions.getDashboardJwtSecret()!,
                    state.generation
                  );
            return { cookie: "auth_token=" + token };
          }
          async function createKey(scopes: string[] = []) {
            const key = await keys.createApiKey(prefix, "native-management-fixture", scopes);
            keyIds.push(key.id);
            return key;
          }
          async function issue(link?: string) {
            const credential = await cookie();
            const response = await sync.POST(
              request(
                "/api/sync/tokens",
                { name: prefix, ttl: "1h" },
                { ...credential, ...(link ? { authorization: "Bearer " + link } : {}) }
              )
            );
            assert.equal(response.status, 201);
            const body = (await response.json()) as { token: string; syncToken: { id: string } };
            const record = await syncRecords.getSyncTokenById(body.syncToken.id);
            assert.ok(record);
            return { token: body.token, record };
          }
          async function durableCounts() {
            return (
              await verifier.transaction((tx) =>
                tx.query(
                  "SELECT (SELECT count(*) FROM omniroute.sync_tokens WHERE name=$1)::text AS sync_count, (SELECT count(*) FROM omniroute.relay_tokens WHERE name=$1)::text AS relay_count, (SELECT count(*) FROM omniroute.cli_access_tokens WHERE name=$1)::text AS cli_count",
                  [prefix]
                )
              )
            ).rows[0];
          }
          async function waitForKeyPolicyWaiters(count: number) {
            for (let attempt = 0; attempt < 50; attempt++) {
              const result = await verifier.transaction((tx) =>
                tx.query<{ count: string }>(
                  "SELECT count(*)::text AS count FROM pg_locks WHERE locktype='advisory' AND classid=18765 AND objid=5 AND NOT granted"
                )
              );
              if (Number(result.rows[0].count) >= count) return;
              await delay(10);
            }
            assert.fail("native key-policy contenders did not reach the actual lock queue");
          }

          await t.test(
            "actual native routes deny weaker principals without durable issuance; current operator and managed workflows survive",
            async () => {
              await seed();
              const manage = await createKey(["manage"]);
              const machine = await accessTokens.createAccessTokenAsync({
                name: prefix,
                scope: "admin",
              });
              for (const requireLogin of [false, true]) {
                await binding.settings.update({ requireLogin });
                const before = await auth.getAuthSettingsSnapshot(),
                  counts = await durableCounts();
                for (const credential of [
                  {},
                  { authorization: "Bearer " + manage.key },
                  { authorization: "Bearer " + machine.secret },
                ]) {
                  const expected = "authorization" in credential ? 403 : 401;
                  for (const [path, run] of [
                    ["/api/relay/tokens", relay.POST],
                    ["/api/sync/tokens", sync.POST],
                    ["/api/cli/tokens", cli.POST],
                    ["/api/settings/require-login", loginSettings.POST],
                    ["/api/settings/import-json", importJson.POST],
                    ["/api/db-backups/import", importFile.POST],
                    ["/api/db-backups", backups.POST],
                  ] as const) {
                    const response = await run(
                      request(
                        path,
                        path === "/api/settings/require-login"
                          ? {
                              password: "native-attacker-password",
                              requireLogin: false,
                            }
                          : {
                              name: prefix,
                              scope: "admin",
                            },
                        credential
                      )
                    );
                    assert.equal(response.status, expected, path);
                    assert.equal(response.headers.get("set-cookie"), null);
                    const text = await response.text();
                    assert.doesNotMatch(text, /oma_live_|osync_/);
                  }
                  assert.equal(
                    (
                      await mainSettings.PATCH(
                        request(
                          "/api/settings",
                          { requireLogin: false, currentPassword: "native-original-password" },
                          credential,
                          "PATCH"
                        )
                      )
                    ).status,
                    expected
                  );
                  for (const [path, run] of [
                    ["/api/settings/export-json", exportJson.GET],
                    ["/api/db-backups/export", exportFile.GET],
                    ["/api/db-backups/exportAll", exportAll.GET],
                  ] as const)
                    assert.equal(
                      (await run(request(path, undefined, credential, "GET"))).status,
                      expected,
                      path
                    );
                }
                assert.deepEqual(await durableCounts(), counts);
                assert.equal((await auth.getAuthSettingsSnapshot()).revision, before.revision);
              }
              await binding.settings.update({ requireLogin: false });
              const operator = await cookie();
              assert.equal(
                (await relay.POST(request("/api/relay/tokens", { name: prefix }, operator))).status,
                200
              );
              await issue();
              assert.equal(
                (
                  await cli.POST(
                    request("/api/cli/tokens", { name: prefix, scope: "admin" }, operator)
                  )
                ).status,
                200
              );
              assert.equal(
                (
                  await cli.POST(
                    request(
                      "/api/cli/tokens",
                      { name: prefix, scope: "write" },
                      { authorization: "Bearer " + machine.secret }
                    )
                  )
                ).status,
                200
              );
              const logicalExport = await exportJson.GET(
                request("/api/settings/export-json", undefined, operator, "GET")
              );
              assert.equal(logicalExport.status, 200);
              const exported = (await logicalExport.json()) as {
                apiKeys: Array<{ id: string; key: string }>;
              };
              assert.ok(
                exported.apiKeys.some((key) => key.id === manage.id && key.key === manage.key)
              );
              for (const [path, run] of [
                ["/api/db-backups/export", exportFile.GET],
                ["/api/db-backups/exportAll", exportAll.GET],
              ] as const)
                assert.equal(
                  (await run(request(path, undefined, operator, "GET"))).status,
                  501,
                  path
                );
              for (const [path, run] of [
                ["/api/settings/import-json", importJson.POST],
                ["/api/db-backups/import", importFile.POST],
                ["/api/db-backups", backups.POST],
              ] as const)
                assert.equal((await run(request(path, {}, operator))).status, 501, path);
              for (const [scopes, credential, expected] of [
                [[], {}, 201],
                [["manage"], {}, 401],
                [["manage"], { authorization: "Bearer " + manage.key }, 201],
              ] as const) {
                const response = await keyRoute.POST(
                  request("/api/keys", { name: prefix, scopes }, credential)
                );
                assert.equal(response.status, expected);
                if (expected === 201) keyIds.push(((await response.json()) as { id: string }).id);
              }
              const readToken = await accessTokens.createAccessTokenAsync({
                name: prefix,
                scope: "read",
              });
              for (const credential of [{}, { authorization: "Bearer " + readToken.secret }]) {
                const response = await cliKeys.GET(
                  request("/api/cli-tools/keys", undefined, credential, "GET")
                );
                assert.equal(response.status, 200);
                assert.equal((await response.text()).includes(manage.key), false);
              }
              const raw = await cliKeys.GET(
                request("/api/cli-tools/keys", undefined, operator, "GET")
              );
              assert.equal(raw.status, 200);
              assert.equal((await raw.text()).includes(manage.key), true);
              process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN = "native-management-internal-token";
              try {
                const internalHeaders = {
                  "x-omniroute-internal-service-token":
                    process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN,
                  [headers.PEER_IP_HEADER]: process.env.OMNIROUTE_PEER_STAMP_TOKEN + "|127.0.0.1",
                };
                const internalRequest = request(
                  "/api/sync/tokens",
                  { name: prefix },
                  internalHeaders
                );
                const verdict = await pipeline.runAuthzPipeline(internalRequest);
                assert.equal(verdict.headers.get("x-middleware-next"), "1");
                const forwarded = forwardedHeaders(verdict);
                assert.equal(forwarded.has(headers.PEER_IP_HEADER), false);
                const response = await sync.POST(
                  request("/api/sync/tokens", { name: prefix }, forwarded)
                );
                assert.equal(response.status, 201);
                const issued = (await response.json()) as { syncToken: { id: string } };
                assert.equal(
                  (await syncRecords.getSyncTokenById(issued.syncToken.id))!.issuedBy,
                  "internal"
                );
                assert.equal(
                  (await relay.POST(request("/api/relay/tokens", { name: prefix }, forwarded)))
                    .status,
                  403
                );
                assert.equal(
                  (
                    await exportJson.GET(
                      request("/api/settings/export-json", undefined, forwarded, "GET")
                    )
                  ).status,
                  403
                );
              } finally {
                delete process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN;
              }
            }
          );

          await t.test(
            "both actual password writers reject a stale repository CAS and renew only their acknowledged generation",
            async (sub) => {
              const instant = Date.now();
              sub.mock.method(Date, "now", () => instant);
              try {
                for (const main of [false, true]) {
                  await seed(true);
                  const before = await auth.getAuthSettingsSnapshot(),
                    credential = await cookie(false, Math.floor(instant / 1000));
                  const entered = deferred(),
                    release = deferred();
                  const observed = observeSettingsUpdate(binding.settings, async () => {
                    entered.resolve();
                    await release.promise;
                  });
                  const body = main
                    ? {
                        currentPassword: "native-original-password",
                        newPassword: "native-next-password",
                      }
                    : {
                        currentPassword: "native-original-password",
                        password: "native-next-password",
                      };
                  const run = () =>
                    main
                      ? mainSettings.PATCH(request("/api/settings", body, credential, "PATCH"))
                      : loginSettings.POST(
                          request("/api/settings/require-login", body, credential)
                        );
                  const pending = withPostgresControlPlane({ ...binding, settings: observed }, run);
                  try {
                    await bounded(entered.promise);
                    await binding.settings.update(
                      { theme: "native-concurrent" },
                      { expectedRevision: before.revision }
                    );
                  } finally {
                    release.resolve();
                    await Promise.allSettled([pending]);
                  }
                  assert.equal((await pending).status, 409);
                  const after = await auth.getAuthSettingsSnapshot();
                  assert.equal(after.generation, before.generation);
                  assert.equal(after.revision, before.revision + 1);
                  assert.equal(after.settings.password, before.settings.password);
                  const response = await run();
                  assert.equal(response.status, 200);
                  const next = await auth.getAuthSettingsSnapshot();
                  assert.notEqual(next.generation, before.generation);
                  assert.equal(next.settings.sessionsValidAfter, Math.floor(instant / 1000));
                  const oldToken = credential.cookie.slice("auth_token=".length);
                  assert.equal(decodeJwt(oldToken).iat, Math.floor(instant / 1000));
                  assert.equal(await sessions.verifyDashboardSessionToken(oldToken), null);
                  const renewed = /auth_token=([^;]+)/.exec(
                    response.headers.get("set-cookie") ?? ""
                  )?.[1];
                  assert.ok(renewed);
                  assert.equal(decodeJwt(renewed).managementAuthGeneration, next.generation);
                  assert.ok(await sessions.verifyDashboardSessionToken(renewed));
                }
              } finally {
                sub.mock.restoreAll();
              }
            }
          );

          await t.test(
            "actual old-password login and verified refresh crossing rotation never obtain the new epoch",
            async () => {
              await seed(true);
              const before = await auth.getAuthSettingsSnapshot();
              const entered = deferred(),
                release = deferred();
              let armed = true,
                issuedCookie: string | undefined;
              const observed = observeQueries(binding.database!, async (sql) => {
                if (armed && sql === authSelect) {
                  armed = false;
                  entered.resolve();
                  await release.promise;
                }
              });
              const oldStore = login.authRouteInternals.getCookieStore;
              login.authRouteInternals.getCookieStore = (async () => ({
                set(_name: string, value: string) {
                  issuedCookie = value;
                },
              })) as typeof oldStore;
              const pending = withPostgresControlPlane({ ...binding, database: observed }, () =>
                login.POST(request("/api/auth/login", { password: "native-original-password" }))
              );
              try {
                await bounded(entered.promise);
                await binding.settings.update({
                  password: await passwords.hashManagementPassword("native-rotated-password"),
                });
                release.resolve();
                assert.equal((await pending).status, 200);
                assert.ok(issuedCookie);
                assert.equal(decodeJwt(issuedCookie).managementAuthGeneration, before.generation);
                assert.equal(await sessions.verifyDashboardSessionToken(issuedCookie), null);
                issuedCookie = undefined;
                assert.equal(
                  (
                    await login.POST(
                      request("/api/auth/login", { password: "native-rotated-password" })
                    )
                  ).status,
                  200
                );
                assert.ok(issuedCookie);
                assert.ok(await sessions.verifyDashboardSessionToken(issuedCookie));
              } finally {
                release.resolve();
                await Promise.allSettled([pending]);
                login.authRouteInternals.getCookieStore = oldStore;
              }
              await seed(true);
              const credential = await cookie(true),
                oldEpoch = (await auth.getAuthSettingsSnapshot()).generation;
              const refreshEntered = deferred(),
                refreshRelease = deferred();
              let reads = 0;
              const refreshDatabase = observeQueries(binding.database!, async (sql) => {
                if (sql === authSelect && ++reads === 2) {
                  refreshEntered.resolve();
                  await refreshRelease.promise;
                }
              });
              const refreshing = withPostgresControlPlane(
                { ...binding, database: refreshDatabase },
                () =>
                  pipeline.runAuthzPipeline(request("/dashboard", undefined, credential, "GET"), {
                    enforce: true,
                  })
              );
              try {
                await bounded(refreshEntered.promise);
                await binding.settings.update({
                  password: await passwords.hashManagementPassword("native-next-password"),
                });
              } finally {
                refreshRelease.resolve();
                await Promise.allSettled([refreshing]);
              }
              const response = await refreshing;
              const refreshed = /auth_token=([^;]+)/.exec(
                response.headers.get("set-cookie") ?? ""
              )?.[1];
              assert.ok(refreshed, "the real refresh boundary must execute");
              assert.equal(decodeJwt(refreshed).managementAuthGeneration, oldEpoch);
              assert.equal(await sessions.verifyDashboardSessionToken(refreshed), null);
              const stable = await pipeline.runAuthzPipeline(
                request("/dashboard", undefined, await cookie(true), "GET"),
                { enforce: true }
              );
              const stableCookie = /auth_token=([^;]+)/.exec(
                stable.headers.get("set-cookie") ?? ""
              )?.[1];
              assert.ok(stableCookie);
              assert.ok(await sessions.verifyDashboardSessionToken(stableCookie));
            }
          );

          await t.test(
            "sync final native authorization rejects revoked and linked-key changes; valid200 and304 remain available",
            async () => {
              await seed();
              await syncRecords.createSyncTokenRecord({
                name: prefix,
                tokenHash: syncTokens.hashSyncToken("native-legacy-secret"),
              });
              assert.equal(await syncTokens.validateSyncToken("native-legacy-secret"), null);
              const linked = await createKey(["manage"]),
                issued = await issue(linked.key);
              assert.equal(issued.record.issuedBy, "session");
              assert.equal(
                issued.record.authGeneration,
                (await auth.getAuthSettingsSnapshot()).generation
              );
              assert.ok(Date.parse(issued.record.expiresAt!) > Date.now());
              const first = await bundle.GET(
                request("/api/sync/bundle", undefined, { "x-sync-token": issued.token }, "GET")
              );
              assert.equal(first.status, 200);
              assert.equal(
                (
                  await bundle.GET(
                    request(
                      "/api/sync/bundle",
                      undefined,
                      { "x-sync-token": issued.token, "if-none-match": first.headers.get("etag")! },
                      "GET"
                    )
                  )
                ).status,
                304
              );
              for (const [column, invalid] of [
                ["issued_by", null],
                ["issued_by", "open"],
                ["issued_by", "unknown"],
                ["expires_at", null],
                ["expires_at", "invalid"],
                ["expires_at", "2000-01-01 00:00:00"],
                ["created_at", "invalid"],
                ["auth_generation", randomUUID()],
                ["sync_api_key_id", "native-missing-" + randomUUID()],
              ] as const) {
                const previousValue = (
                  await verifier.transaction((tx) =>
                    tx.query<{ value: unknown }>(
                      `SELECT ${column} AS value FROM omniroute.sync_tokens WHERE id=$1`,
                      [issued.record.id]
                    )
                  )
                ).rows[0].value;
                try {
                  await verifier.transaction((tx) =>
                    tx.query(`UPDATE omniroute.sync_tokens SET ${column}=$1 WHERE id=$2`, [
                      invalid,
                      issued.record.id,
                    ])
                  );
                  assert.equal(
                    await syncTokens.validateSyncToken(issued.token),
                    null,
                    column + ":" + invalid
                  );
                } finally {
                  await verifier.transaction((tx) =>
                    tx.query(`UPDATE omniroute.sync_tokens SET ${column}=$1 WHERE id=$2`, [
                      previousValue,
                      issued.record.id,
                    ])
                  );
                }
              }
              const beforeFinal = deferred(),
                finish = deferred();
              let finalRead = false;
              const observed = observeQueries(
                binding.database!,
                () => {},
                async (sql) => {
                  if (
                    !finalRead &&
                    sql.startsWith("SELECT t.*") &&
                    sql.endsWith(" FOR UPDATE OF t")
                  ) {
                    finalRead = true;
                    beforeFinal.resolve();
                    await finish.promise;
                  }
                }
              );
              const pending = withPostgresControlPlane({ ...binding, database: observed }, () =>
                bundle.GET(
                  request(
                    "/api/sync/bundle",
                    undefined,
                    { "x-sync-token": issued.token, "if-none-match": first.headers.get("etag")! },
                    "GET"
                  )
                )
              );
              try {
                await bounded(beforeFinal.promise);
                await syncRecords.revokeSyncToken(issued.record.id);
              } finally {
                finish.resolve();
                await Promise.allSettled([pending]);
              }
              assert.equal((await pending).status, 401);
              const rowRace = await issue(),
                locked = deferred(),
                unlock = deferred();
              const lockedDatabase = observeQueries(binding.database!, async (sql) => {
                if (sql.startsWith("SELECT t.*") && sql.endsWith(" FOR UPDATE OF t")) {
                  locked.resolve();
                  await unlock.promise;
                }
              });
              const marking = withPostgresControlPlane(
                { ...binding, database: lockedDatabase },
                () => syncTokens.markSyncTokenUsed(rowRace.record)
              );
              let rowRevocation: ReturnType<typeof syncRecords.revokeSyncToken> | undefined;
              try {
                await bounded(locked.promise);
                rowRevocation = syncRecords.revokeSyncToken(rowRace.record.id);
                void rowRevocation.catch(() => {});
                let observedLock = false;
                for (let attempt = 0; attempt < 50; attempt++) {
                  const waiting = await verifier.transaction((tx) =>
                    tx.query<{ count: string }>(
                      "SELECT count(*)::text AS count FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'UPDATE omniroute.sync_tokens SET%'"
                    )
                  );
                  if (Number(waiting.rows[0].count) === 1) {
                    observedLock = true;
                    break;
                  }
                  await delay(10);
                }
                assert.equal(
                  observedLock,
                  true,
                  "the actual revoke must block on final authorization's row lock"
                );
              } finally {
                unlock.resolve();
                await Promise.allSettled([marking, rowRevocation]);
              }
              assert.equal(await marking, true);
              assert.ok((await rowRevocation)?.revokedAt);
              assert.equal(await syncTokens.validateSyncToken(rowRace.token), null);
              const keyForRace = await createKey(["manage"]),
                raced = await issue(keyForRace.key);
              assert.ok(await syncTokens.validateSyncToken(raced.token));
              const held = deferred(),
                unhold = deferred();
              const blocker = verifier.transaction(async (tx) => {
                await tx.query("SELECT pg_advisory_xact_lock(18765,5)");
                held.resolve();
                await unhold.promise;
              });
              void blocker.catch(() => {});
              let revoking: Promise<boolean> | undefined, touching: Promise<boolean> | undefined;
              try {
                await bounded(held.promise);
                revoking = keys.revokeApiKey(keyForRace.id);
                void revoking.catch(() => {});
                await waitForKeyPolicyWaiters(1);
                touching = syncTokens.markSyncTokenUsed(raced.record);
                void touching.catch(() => {});
                await waitForKeyPolicyWaiters(2);
              } finally {
                unhold.resolve();
                await blocker;
                await Promise.allSettled([revoking, touching]);
              }
              assert.equal(await revoking, true);
              assert.equal(await touching, false);
              assert.equal(await syncTokens.validateSyncToken(raced.token), null);
              for (const [column, invalid, valid] of [
                ["is_active", 0, 1],
                ["is_banned", 1, 0],
                ["revoked_at", "2026-10-19", null],
                ["expires_at", "invalid", null],
                ["expires_at", "2000-01-01 00:00:00", null],
              ] as const) {
                const key = await createKey(["manage"]),
                  current = await issue(key.key);
                try {
                  await verifier.transaction((tx) =>
                    tx.query(`UPDATE omniroute.api_keys SET ${column}=$1 WHERE id=$2`, [
                      invalid,
                      key.id,
                    ])
                  );
                  assert.equal(await syncTokens.markSyncTokenUsed(current.record), false, column);
                } finally {
                  await verifier.transaction((tx) =>
                    tx.query(`UPDATE omniroute.api_keys SET ${column}=$1 WHERE id=$2`, [
                      valid,
                      key.id,
                    ])
                  );
                }
                assert.equal(await syncTokens.markSyncTokenUsed(current.record), true);
              }
              const oldEpoch = await issue();
              await binding.settings.update({ password: originalHash });
              assert.equal(await syncTokens.validateSyncToken(oldEpoch.token), null);
              assert.ok(await syncTokens.validateSyncToken((await issue()).token));
            }
          );

          await t.test(
            "the actual regenerate route conflicts after a native scope grant and preserves the old secret",
            async () => {
              await seed();
              const key = await createKey();
              const before = (
                await verifier.transaction((tx) =>
                  tx.query("SELECT key,key_hash FROM omniroute.api_keys WHERE id=$1", [key.id])
                )
              ).rows[0];
              const entered = deferred(),
                release = deferred();
              const observedKeys = new Proxy(binding.apiKeys, {
                get(target, property, receiver) {
                  if (property === "regenerateApiKey")
                    return async (...args: Parameters<typeof target.regenerateApiKey>) => {
                      entered.resolve();
                      await release.promise;
                      return target.regenerateApiKey(...args);
                    };
                  const value: unknown = Reflect.get(target, property, receiver);
                  return typeof value === "function" ? value.bind(target) : value;
                },
              });
              const run = () =>
                regenerate.POST(request("/api/keys/" + key.id + "/regenerate"), {
                  params: Promise.resolve({ id: key.id }),
                });
              const pending = withPostgresControlPlane({ ...binding, apiKeys: observedKeys }, run);
              try {
                await bounded(entered.promise);
                await keys.updateApiKeyPermissions(key.id, { scopes: ["manage"] });
              } finally {
                release.resolve();
                await Promise.allSettled([pending]);
              }
              assert.equal((await pending).status, 409);
              assert.deepEqual(
                (
                  await verifier.transaction((tx) =>
                    tx.query("SELECT key,key_hash FROM omniroute.api_keys WHERE id=$1", [key.id])
                  )
                ).rows[0],
                before
              );
              assert.equal((await run()).status, 401);
              const operator = await cookie();
              const managed = await regenerate.POST(
                request("/api/keys/" + key.id + "/regenerate", undefined, operator),
                { params: Promise.resolve({ id: key.id }) }
              );
              assert.equal(managed.status, 200);
              const managedKey = ((await managed.json()) as { key: string }).key;
              assert.notEqual(managedKey, key.key);
              assert.equal((await keys.getApiKeyById(key.id))!.key, managedKey);
              const inference = await createKey();
              const unscoped = await regenerate.POST(
                request("/api/keys/" + inference.id + "/regenerate"),
                {
                  params: Promise.resolve({ id: inference.id }),
                }
              );
              assert.equal(unscoped.status, 200);
              const inferenceKey = ((await unscoped.json()) as { key: string }).key;
              assert.notEqual(inferenceKey, inference.key);
              assert.equal((await keys.getApiKeyById(inference.id))!.key, inferenceKey);
            }
          );

          await t.test(
            "bootstrap proof is reserved before native mutation; actual locality and enabled OIDC remain distinct",
            async () => {
              await seed(false, false);
              await binding.settings.update({ oidcEnabled: true });
              assert.equal(
                (
                  await loginSettings.POST(
                    request(
                      "/api/settings/require-login",
                      { password: "native-first-password" },
                      {
                        [headers.BOOTSTRAP_TOKEN_HEADER]: bootstrap.getOrCreateBootstrapToken({
                          log() {},
                        }),
                      }
                    )
                  )
                ).status,
                401
              );
              await seed(false, false);
              const forged = request(
                "/api/settings/require-login",
                { password: "native-first-password" },
                { host: "localhost", [headers.AUTHZ_HEADER_PEER_LOCALITY]: "loopback" }
              );
              const verdict = await pipeline.runAuthzPipeline(forged);
              const forwarded = forwardedHeaders(verdict);
              assert.equal(verdict.headers.get("x-middleware-next"), "1");
              assert.equal(forwarded.get(headers.AUTHZ_HEADER_PEER_LOCALITY), "remote");
              assert.equal(
                (
                  await loginSettings.POST(
                    request(
                      "/api/settings/require-login",
                      { password: "native-first-password" },
                      forwarded
                    )
                  )
                ).status,
                401
              );
              const token = bootstrap.getOrCreateBootstrapToken({ log() {} }),
                entered = deferred(),
                release = deferred();
              const settings = observeSettingsUpdate(binding.settings, async () => {
                entered.resolve();
                await release.promise;
              });
              const req = () =>
                request(
                  "/api/settings/require-login",
                  { password: "native-first-password" },
                  { [headers.BOOTSTRAP_TOKEN_HEADER]: token }
                );
              const pending = withPostgresControlPlane({ ...binding, settings }, () =>
                loginSettings.POST(req())
              );
              try {
                await bounded(entered.promise);
                assert.equal(bootstrap.peekBootstrapToken(token), false);
                assert.equal((await loginSettings.POST(req())).status, 401);
              } finally {
                release.resolve();
                await Promise.allSettled([pending]);
              }
              assert.equal((await pending).status, 200);
              assert.equal(bootstrap.peekBootstrapToken(token), false);
              assert.equal((await loginSettings.POST(req())).status, 401);
              await seed(false, false);
              const local = request("/api/settings/require-login", {
                password: "native-local-first-password",
              });
              Object.defineProperty(local, "socket", { value: { remoteAddress: "127.0.0.1" } });
              assert.equal((await loginSettings.POST(local)).status, 200);
              const { getMachineTokenAsync } = await import("../../src/lib/machineToken.ts");
              const localToken = await getMachineTokenAsync();
              assert.ok(
                localToken,
                "the actual host CLI identity must be available for the positive control"
              );
              await seed(true, false);
              await binding.settings.update({ oidcEnabled: true });
              const recover = request(
                "/api/settings/require-login",
                { password: "native-cli-recovered-password" },
                {
                  [headers.CLI_TOKEN_HEADER]: localToken,
                  [headers.PEER_IP_HEADER]: process.env.OMNIROUTE_PEER_STAMP_TOKEN + "|127.0.0.1",
                }
              );
              const admitted = await pipeline.runAuthzPipeline(recover, { enforce: true });
              assert.equal(admitted.headers.get("x-middleware-next"), "1");
              const cliForwarded = forwardedHeaders(admitted);
              assert.equal(cliForwarded.has(headers.PEER_IP_HEADER), false);
              assert.equal(
                (
                  await loginSettings.POST(
                    request(
                      "/api/settings/require-login",
                      { password: "native-cli-recovered-password" },
                      cliForwarded
                    )
                  )
                ).status,
                200
              );
            }
          );

          await t.test(
            "bootstrap CAS conflict releases proof; deferred COMMIT rejection remains a required release oracle",
            async () => {
              await seed(false, false);
              const token = bootstrap.getOrCreateBootstrapToken({ log() {} }),
                entered = deferred(),
                release = deferred();
              const observed = observeSettingsUpdate(binding.settings, async () => {
                entered.resolve();
                await release.promise;
              });
              const pending = withPostgresControlPlane({ ...binding, settings: observed }, () =>
                loginSettings.POST(
                  request(
                    "/api/settings/require-login",
                    { password: "native-first-password" },
                    { [headers.BOOTSTRAP_TOKEN_HEADER]: token }
                  )
                )
              );
              try {
                await bounded(entered.promise);
                await binding.settings.update({ oidcEnabled: true });
              } finally {
                release.resolve();
                await Promise.allSettled([pending]);
              }
              assert.equal((await pending).status, 409);
              assert.equal(bootstrap.peekBootstrapToken(token), true);
              await seed(false, false);
              const rejectedProof = bootstrap.getOrCreateBootstrapToken({ log() {} });
              const before = await auth.getAuthSettingsSnapshot();
              await verifier.transaction(async (tx) => {
                await tx.query(
                  "CREATE FUNCTION omniroute.reject_management_commit_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='isolated management deferred rejection'; END $$"
                );
                await tx.query(
                  "CREATE CONSTRAINT TRIGGER reject_management_commit_test AFTER INSERT OR UPDATE ON omniroute.key_value DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.namespace='settings' AND NEW.key='password') EXECUTE FUNCTION omniroute.reject_management_commit_test()"
                );
              });
              let error: unknown;
              const actual = createPostgresSettingsRepository(binding.database!, noEffects);
              const capturing: SettingsRepository = {
                ...actual,
                async update(updates, options) {
                  try {
                    return await actual.update(updates, options);
                  } catch (caught) {
                    error = caught;
                    throw caught;
                  }
                },
              };
              try {
                const response = await withPostgresControlPlane(
                  { ...binding, settings: capturing },
                  () =>
                    loginSettings.POST(
                      request(
                        "/api/settings/require-login",
                        { password: "native-rejected-password" },
                        { [headers.BOOTSTRAP_TOKEN_HEADER]: rejectedProof }
                      )
                    )
                );
                assert.equal(response.status, 500);
                assert.equal(response.headers.get("set-cookie"), null);
                assert.ok(isPostgresErrorKind(error, "transaction_failed"));
                assert.equal(error.sqlState, "23514");
                const after = await auth.getAuthSettingsSnapshot();
                assert.equal(after.revision, before.revision);
                assert.equal(after.generation, before.generation);
                assert.equal(after.settings.password, before.settings.password);
                assert.equal(
                  bootstrap.peekBootstrapToken(rejectedProof),
                  true,
                  "a proven COMMIT rollback must release the reserved proof; known frozen-source gap"
                );
              } finally {
                await verifier.transaction(async (tx) => {
                  await tx.query(
                    "DROP TRIGGER reject_management_commit_test ON omniroute.key_value"
                  );
                  await tx.query("DROP FUNCTION omniroute.reject_management_commit_test()");
                });
              }
            }
          );

          await t.test(
            "acknowledged effect failure and lost real COMMIT acknowledgement consume proof without replay or cookie",
            async () => {
              await seed(false, false);
              const token = bootstrap.getOrCreateBootstrapToken({ log() {} });
              const before = await auth.getAuthSettingsSnapshot();
              let committedError: unknown,
                updates = 0;
              const actual = createPostgresSettingsRepository(binding.database!, {
                ...noEffects,
                invalidateSettingsCache() {
                  throw new Error("isolated effect failure");
                },
              });
              const capturing: SettingsRepository = {
                ...actual,
                async update(values, options) {
                  updates++;
                  try {
                    return await actual.update(values, options);
                  } catch (error) {
                    committedError = error;
                    throw error;
                  }
                },
              };
              const response = await withPostgresControlPlane(
                { ...binding, settings: capturing },
                () =>
                  loginSettings.POST(
                    request(
                      "/api/settings/require-login",
                      { password: "native-effect-password" },
                      { [headers.BOOTSTRAP_TOKEN_HEADER]: token }
                    )
                  )
              );
              assert.equal(response.status, 500);
              assert.equal(response.headers.get("set-cookie"), null);
              assert.ok(committedError instanceof PostgresSettingsEffectsError);
              assert.equal(committedError.committed, true);
              assert.equal(updates, 1);
              assert.equal((await auth.getAuthSettingsSnapshot()).revision, before.revision + 1);
              assert.equal(bootstrap.peekBootstrapToken(token), false);
              await seed(false, false);
              const unknownProof = bootstrap.getOrCreateBootstrapToken({ log() {} });
              const unknownBefore = await auth.getAuthSettingsSnapshot();
              const proxy = await createPostgresReplyLossProxy({
                upstreamPort: options.port!,
                certificate: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_CA!, "utf8"),
                key: await readFile(process.env.OMNIROUTE_TEST_POSTGRES_KEY!, "utf8"),
                dropCommitAfterQuery:
                  "INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('settings',",
              });
              const uncertain = await createPostgresDatabase(
                { ...options, port: proxy.port },
                { warn() {} }
              );
              let unknownError: unknown,
                unknownUpdates = 0;
              const uncertainOwner = createPostgresSettingsRepository(uncertain, noEffects);
              const unknownSettings: SettingsRepository = {
                ...uncertainOwner,
                async update(values, options) {
                  unknownUpdates++;
                  try {
                    return await uncertainOwner.update(values, options);
                  } catch (error) {
                    unknownError = error;
                    throw error;
                  }
                },
              };
              try {
                const failed = await withPostgresControlPlane(
                  { ...binding, settings: unknownSettings },
                  () =>
                    loginSettings.POST(
                      request(
                        "/api/settings/require-login",
                        { password: "native-unknown-password" },
                        { [headers.BOOTSTRAP_TOKEN_HEADER]: unknownProof }
                      )
                    )
                );
                assert.equal(failed.status, 500);
                assert.equal(failed.headers.get("set-cookie"), null);
                assert.equal(proxy.dropped(), true);
                assert.ok(isPostgresErrorKind(unknownError, "commit_unknown"));
                const durable = await auth.getAuthSettingsSnapshot();
                assert.equal(durable.revision, unknownBefore.revision + 1);
                assert.notEqual(durable.generation, unknownBefore.generation);
                assert.equal(
                  await passwords.verifyManagementPassword(
                    "native-unknown-password",
                    String(durable.settings.password)
                  ),
                  true
                );
                assert.equal(bootstrap.peekBootstrapToken(unknownProof), false);
                assert.equal(unknownUpdates, 1);
                assert.equal(
                  (
                    await loginSettings.POST(
                      request(
                        "/api/settings/require-login",
                        { password: "native-replay-password" },
                        { [headers.BOOTSTRAP_TOKEN_HEADER]: unknownProof }
                      )
                    )
                  ).status,
                  401
                );
                assert.equal((await auth.getAuthSettingsSnapshot()).revision, durable.revision);
              } finally {
                await uncertain.close();
                await proxy.close();
              }
            }
          );
        })
      );
    } finally {
      bootstrap.__resetBootstrapTokenForTest();
      await plane.run(async () => {
        for (const id of keyIds) if (id) await keys.deleteApiKey(id);
      });
      await verifier.transaction(async (tx) => {
        await tx.query("DELETE FROM omniroute.sync_tokens WHERE name=$1", [prefix]);
        await tx.query("DELETE FROM omniroute.relay_tokens WHERE name=$1", [prefix]);
        await tx.query("DELETE FROM omniroute.cli_access_tokens WHERE name=$1", [prefix]);
        await tx.query("SELECT pg_advisory_xact_lock(18765,3)");
        await tx.query("DELETE FROM omniroute.key_value WHERE namespace='settings'");
        for (const row of originalSettings)
          await tx.query(
            "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('settings',$1,$2)",
            [row.key, row.value]
          );
      });
    }
  }
);
