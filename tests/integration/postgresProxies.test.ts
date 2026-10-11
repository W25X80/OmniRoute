import assert from "node:assert/strict";
import test from "node:test";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresProxyRepository } from "../../src/lib/db/postgres/proxyRepository.ts";
import { createPostgresFreeProxyRepository } from "../../src/lib/db/postgres/freeProxyRepository.ts";
import { createPostgresUpstreamProxyRepository } from "../../src/lib/db/postgres/upstreamProxyRepository.ts";
import { createPostgresRelayProxyRepository } from "../../src/lib/db/postgres/relayProxyRepository.ts";
import { createPostgresProxySubscriptionRepository } from "../../src/lib/db/postgres/proxySubscriptionRepository.ts";
import { createPostgresOperatorEgressRepository } from "../../src/lib/db/postgres/proxyOperatorEgressRepository.ts";
import { resolvePostgresProxyForConnection } from "../../src/lib/db/postgres/proxyResolution.ts";
// Real PostgreSQL assertions; the runner supplies only a disposable TLS endpoint.
test(
  "PostgreSQL proxy owners preserve required egress and durable state",
  { skip: !postgresTestsEnabled },
  async (t) => {
    const database = await createTestPostgresDatabase({
      maxConnections: 4,
      lockTimeoutMs: 10_000,
      statementTimeoutMs: 12_000,
      queryTimeoutMs: 13_000,
      transactionTimeoutMs: 30_000,
    });
    const other = await createTestPostgresDatabase({ maxConnections: 4 });
    const proxies = createPostgresProxyRepository(database),
      otherProxies = createPostgresProxyRepository(other);
    const free = createPostgresFreeProxyRepository(database),
      upstream = createPostgresUpstreamProxyRepository(database),
      relays = createPostgresRelayProxyRepository(database),
      subscriptions = createPostgresProxySubscriptionRepository(database),
      egress = createPostgresOperatorEgressRepository(database);
    const payload = {
      name: "alpha",
      type: "http",
      host: "proxy.example",
      port: 8080,
      username: "user",
      password: "secret",
    };
    // Cursor cases also enter the selected control plane and prepare its feature-flag snapshot.
    let plane: import("../../src/lib/db/postgres/controlPlane.ts").PostgresControlPlane | undefined;
    const savedKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-postgres-proxy-test-key";
    try {
      await migratePostgresSchema(database);
      await database.transaction((tx) =>
        tx.query(
          "TRUNCATE omniroute.proxy_assignments,omniroute.proxy_scope_rotation,omniroute.proxy_registry,omniroute.free_proxies,omniroute.free_proxy_sync_errors,omniroute.proxy_subscriptions,omniroute.relay_tokens,omniroute.relay_rate_limits,omniroute.relay_logs,omniroute.proxy_operator_egress,omniroute.key_value RESTART IDENTITY CASCADE"
        )
      );
      await t.test(
        "upsert identity ignores password and never claims another subscription",
        async () => {
          const first = await proxies.upsert(payload);
          assert.equal(first.action, "created");
          assert.ok(first.proxy);
          const changed = await proxies.upsert({ ...payload, password: "rotated" });
          assert.equal(changed.action, "updated");
          assert.equal(changed.proxy!.id, first.proxy.id);
          assert.equal(
            (await proxies.get(first.proxy.id, { includeSecrets: true }))!.password,
            "rotated"
          );
          await proxies.update(first.proxy.id, { status: "disabled" });
          const omitted = await proxies.upsert({ ...payload, password: "rotated" });
          assert.equal(omitted.proxy!.status, "disabled");
          const skipped = await proxies.upsert(
            { ...payload, subscriptionId: "other-subscription" },
            { claimOwnership: false }
          );
          assert.deepEqual(skipped, { proxy: null, action: "skipped" });
          const distinct = await proxies.upsert({ ...payload, username: "second-user" });
          assert.equal(distinct.action, "created");
          assert.notEqual(distinct.proxy!.id, first.proxy.id);
        }
      );
      await t.test(
        "concurrent pool additions are idempotent and positions remain unique",
        async () => {
          const a = (await proxies.create({ ...payload, name: "pool-a", host: "pool-a.example" }))!,
            b = (await proxies.create({ ...payload, name: "pool-b", host: "pool-b.example" }))!;
          await Promise.all([
            proxies.addToPool("provider", "openai", [a.id, b.id]),
            otherProxies.addToPool("provider", "openai", [a.id, b.id]),
          ]);
          const members = await proxies.pool("provider", "openai");
          assert.equal(members.length, 2);
          assert.equal(new Set(members.map((row) => row.position)).size, 2);
          await proxies.assign("provider", "openai", a.id);
          assert.deepEqual(
            (await proxies.pool("provider", "openai")).map((row) => row.proxyId),
            [a.id]
          );
          await proxies.addToPool("provider", "openai", [b.id]);
        }
      );
      await t.test(
        "pool cursors serialize across clients and preserve sticky transitions",
        async () => {
          const { initializePostgresControlPlane } =
            await import("../../src/lib/db/postgres/controlPlane.ts");
          const { withPostgresCatalogSnapshot } =
            await import("../../src/lib/db/postgres/catalogSnapshot.ts");
          plane = await initializePostgresControlPlane({
            connection: await testPostgresOptions(),
            logger: { warn() {} },
            runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
          });
          await plane.run(async (binding) => {
            const scopedDatabase = binding.database!;
            const scoped = createPostgresProxyRepository(scopedDatabase);
            const a = (await scoped.create({ ...payload, name: "rr-a", host: "rr-a.example" }))!;
            const b = (await scoped.create({ ...payload, name: "rr-b", host: "rr-b.example" }))!;
            await scoped.addToPool("combo", "rr-test", [a.id, b.id]);
            const picked: string[] = [];
            await withPostgresCatalogSnapshot(scopedDatabase, async () => {
              for (let wave = 0; wave < 4; wave++) {
                const results = await Promise.all([
                  scoped.resolveScope("combo", "rr-test"),
                  otherProxies.resolveScope("combo", "rr-test"),
                ]);
                picked.push(...results.map((result) => String(result!.proxy.host)));
              }
              assert.equal(picked.filter((id) => id === a.host).length, 4);
              assert.equal(picked.filter((id) => id === b.host).length, 4);
              const state = (
                await scopedDatabase.transaction((tx) =>
                  tx.query<{ cursor: string; rotated_at: string | null }>(
                    "SELECT cursor,rotated_at FROM omniroute.proxy_scope_rotation WHERE scope='combo' AND scope_id='rr-test'"
                  )
                )
              ).rows[0];
              assert.equal(Number(state.cursor), 8);
              assert.equal(state.rotated_at, null);
              await scoped.setStrategy("combo", "rr-test", "sticky", { stickyWindowMinutes: 30 });
              const first = await scoped.resolveScope("combo", "rr-test"),
                second = await otherProxies.resolveScope("combo", "rr-test");
              assert.equal(first!.proxy.host, b.host);
              assert.equal(second!.proxy.host, b.host);
              const keyProxy = (await scoped.create({
                ...payload,
                name: "key-only",
                host: "key-only.example",
              }))!;
              await scopedDatabase.transaction(async (tx) => {
                const now = new Date().toISOString();
                await tx.query(
                  "INSERT INTO omniroute.provider_connections(id,provider,per_key_proxy_enabled,created_at,updated_at) VALUES('proxy-test-connection','openai',1,$1,$1)",
                  [now]
                );
                await tx.query(
                  "INSERT INTO omniroute.api_keys(id,name,key,created_at,proxy_id) VALUES('proxy-test-key','proxy-test','not-an-auth-test-key',$1,$2)",
                  [now, keyProxy.id]
                );
                await tx.query(
                  "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('settings','perKeyProxyEnabled','malformed-json') ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value"
                );
              });
              const guarded = await resolvePostgresProxyForConnection(
                scopedDatabase,
                "proxy-test-connection",
                "proxy-test-key"
              );
              assert.notEqual((guarded.proxy as { host?: string } | null)?.host, keyProxy.host);
            });
          });
        }
      );
      await t.test(
        "all-dead required pools are blocked and explicit global off allows direct",
        async () => {
          const dead = (await proxies.create({
            ...payload,
            name: "dead",
            host: "dead.example",
            status: "dead",
          }))!;
          await proxies.assign("provider", "anthropic", dead.id);
          assert.equal(await proxies.resolveScope("provider", "anthropic"), null);
          assert.equal(await proxies.blockingProvider("anthropic"), true);
          assert.equal(await proxies.blockingConnection("missing-connection", "anthropic"), true);
          await database.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('settings','proxyEnabled','false') ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value"
            )
          );
          assert.equal(await proxies.blockingProvider("anthropic"), false);
          assert.deepEqual(
            await resolvePostgresProxyForConnection(
              database,
              "missing-connection",
              undefined,
              "anthropic"
            ),
            { proxy: null, level: "direct", levelId: null }
          );
          await database.transaction((tx) =>
            tx.query(
              "DELETE FROM omniroute.key_value WHERE namespace='settings' AND key='proxyEnabled'"
            )
          );
        }
      );
      await t.test("create and assign clears legacy config in the same transaction", async () => {
        await database.transaction((tx) =>
          tx.query(
            "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('proxyConfig','providers',$1)",
            [JSON.stringify({ openai: { host: "legacy.example" }, keep: { host: "keep.example" } })]
          )
        );
        const created = await proxies.mutateAndAssign(
          null,
          { ...payload, host: "atomic.example" },
          { scope: "provider", scopeId: "openai" }
        );
        assert.ok(created?.proxy);
        assert.equal(created.assignment!.proxyId, created.proxy.id);
        const raw = (
          await database.transaction((tx) =>
            tx.query<{ value: string }>(
              "SELECT value FROM omniroute.key_value WHERE namespace='proxyConfig' AND key='providers'"
            )
          )
        ).rows[0].value;
        assert.deepEqual(JSON.parse(raw), { keep: { host: "keep.example" } });
        await assert.rejects(
          proxies.mutateAndAssign(
            null,
            { ...payload, host: "rolled-back.example" },
            { scope: "account", scopeId: null }
          )
        );
        assert.equal(
          (await proxies.list({ includeSecrets: true })).items.some(
            (row) => row.host === "rolled-back.example"
          ),
          false
        );
      });
      await t.test(
        "free promotion is atomic and registry deletion clears permissive legacy links",
        async () => {
          const row = await free.upsert({
            source: "1proxy",
            host: "free.example",
            port: 8080,
            type: "http",
            countryCode: null,
            qualityScore: null,
            latencyMs: null,
            anonymity: null,
            lastValidated: null,
          });
          const id = await free.promote(row.id, {
            name: "free",
            type: "http",
            host: "free.example",
            port: 8080,
            source: "oneproxy",
          });
          assert.ok(id);
          assert.equal((await free.get(row.id))!.poolProxyId, id);
          assert.equal((await free.get(row.id))!.inPool, true);
          await proxies.delete(id, { force: true });
          assert.equal((await free.get(row.id))!.poolProxyId, null);
          assert.equal((await free.get(row.id))!.inPool, false);
          await free.markInPool(row.id, "unresolved-legacy-id");
          assert.equal((await free.get(row.id))!.poolProxyId, "unresolved-legacy-id");
          await free.recordSync("2026-10-11T01:02:03.000Z");
          assert.equal((await free.stats()).lastSyncAt, "2026-10-11T01:02:03.000Z");
        }
      );
      await t.test("upstream patch preserves unrelated fields and fallback backend", async () => {
        await upstream.upsert({
          providerId: "openai",
          mode: "fallback",
          fallbackBackend: "dario",
          cliproxyapiModelMapping: { a: "b" },
        });
        const row = await upstream.update("openai", { family: "ipv6" });
        assert.equal(row.fallbackBackend, "dario");
        assert.deepEqual(row.cliproxyapiModelMapping, { a: "b" });
        assert.equal(row.family, "ipv6");
      });
      await t.test(
        "relay usage is atomic, unknown auth attempts are recorded, and deletion cascades",
        async () => {
          const token = await relays.create({
            name: "relay",
            maxRequestsPerMinute: 2,
            maxRequestsPerDay: 3,
          });
          assert.ok(token.rawToken);
          assert.equal((await relays.byHash(token.tokenHash))!.id, token.id);
          assert.equal((await relays.check(token.id)).remaining, 2);
          await Promise.all([
            relays.record(token.id, { cost: 1, status: "success" }),
            relays.record(token.id, { cost: 2, status: "success" }),
          ]);
          assert.equal((await relays.check(token.id)).allowed, false);
          assert.deepEqual(await relays.usage(token.id, 0), { requestCount: 2, totalCost: 3 });
          await relays.record("unknown", { status: "auth_failed", statusCode: 401 });
          assert.equal((await relays.logs("unknown")).length, 1);
          await relays.delete(token.id);
          assert.equal(await relays.get(token.id), null);
          assert.deepEqual(await relays.logs(token.id), []);
        }
      );
      await t.test(
        "subscription secret omission preserves ciphertext and failures remain visible",
        async () => {
          const input = {
            id: "subscription",
            name: "subscription",
            url: "https://example.invalid/feed",
            enabled: 0,
            mode: "global",
            ruleProviders: null,
            localCoreEndpoint: null,
            updateIntervalMinutes: 60,
            controlUrl: "http://127.0.0.1:9090",
            coreConfigPath: null,
            controlSecretEnc: "ciphertext",
            gapSeconds: 60,
            now: "2026-10-11T00:00:00.000Z",
          };
          await subscriptions.insert(input);
          const { controlSecretEnc, ...patch } = input;
          await subscriptions.update({ ...patch, name: "renamed" });
          assert.equal((await subscriptions.get(input.id))!.control_secret_enc, "ciphertext");
          await subscriptions.setStatus(input.id, "ok", null, [], null, 0);
          await subscriptions.recordSwitch(
            { subscriptionId: input.id, result: "error", kind: "transport" },
            "switch-error"
          );
          assert.equal((await subscriptions.get(input.id))!.error, "switch-error");
          assert.equal((await subscriptions.get(input.id))!.status, "ok");
          await subscriptions.recordSwitch(
            { subscriptionId: input.id, result: "throttled" },
            "ignored"
          );
          assert.equal((await subscriptions.get(input.id))!.error, "switch-error");
        }
      );
      await t.test(
        "operator address caps preserve latest observation and invalid addresses are rejected",
        async () => {
          const now = new Date().toISOString();
          const result = await egress.upsert([
            {
              host: "unknown.example",
              port: 8080,
              observedAt: now,
              addresses: [
                "203.0.113.1",
                "bad-ip",
                ...Array.from({ length: 12 }, (_, i) => `203.0.113.${i + 2}`),
              ],
            },
          ]);
          assert.equal(result.ignored, 14);
          assert.ok(result.rejected.some((row) => row.reason === "unusable-address"));
          assert.equal((await egress.read("unknown.example", 8080)).addresses.size, 10);
          assert.equal(result.rejected.filter((row) => row.reason === "cap-evicted").length, 3);
          await egress.upsert([
            {
              host: "unknown.example",
              port: 8080,
              observedAt: "2000-01-01T00:00:00.000Z",
              addresses: ["203.0.113.13"],
            },
          ]);
          assert.equal((await egress.read("unknown.example", 8080)).freshest?.at, now);
        }
      );
      await t.test(
        "operator IPv6 storage round-trips without accepting malformed prefixes",
        async () => {
          const now = new Date().toISOString();
          await egress.upsert([
            {
              host: "ipv6.example",
              port: 8080,
              observedAt: now,
              addresses: ["2001:4860:4860::8888", "203.0.113.20"],
            },
          ]);
          assert.deepEqual(
            (await egress.read("ipv6.example", 8080)).addresses,
            new Set(["v6:2001:4860:4860:0", "203.0.113.20"])
          );
          await database.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.proxy_operator_egress(host,port,address,observed_at) VALUES('ipv6.example',8080,'v6:2001:4860:bad-host:0',$1)",
              [now]
            )
          );
          assert.equal((await egress.read("ipv6.example", 8080)).addresses.size, 2);
        }
      );
    } finally {
      await plane?.close();
      await other.close();
      await database.close();
      if (savedKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = savedKey;
    }
  }
);
