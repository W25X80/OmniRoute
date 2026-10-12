import assert from "node:assert/strict";
import test from "node:test";
import { initializePostgresControlPlane } from "../../src/lib/db/postgres/controlPlane.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import type { PostgresTransaction } from "../../src/lib/db/postgres/database.ts";
import { migratePostgresSchema } from "../../src/lib/db/postgres/migrations.ts";
import { createPostgresProxyRepository } from "../../src/lib/db/postgres/proxyRepository.ts";
import { resolvePostgresProxyForConnection } from "../../src/lib/db/postgres/proxyResolution.ts";
import * as registryGeneration from "../../src/lib/db/proxies/registryGeneration.ts";
import * as policy from "../../src/lib/db/settings/stableEgressPolicy.ts";
import {
  __resetProxyRefusalMemoryForTesting,
  noteProxyRefusal,
  proxyEgressKey,
} from "../../open-sse/utils/proxyRefusalMemory.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

test("proxy policy and registry generations are shared by independent module copies", async () => {
  const copyPolicy: typeof policy = await import(
    new URL("../../src/lib/db/settings/stableEgressPolicy.ts?second-bundle", import.meta.url).href
  );
  const copyRegistry: typeof registryGeneration = await import(
    new URL("../../src/lib/db/proxies/registryGeneration.ts?second-bundle", import.meta.url).href
  );
  assert.notEqual(copyPolicy.advanceProxyConfigGeneration, policy.advanceProxyConfigGeneration);
  assert.notEqual(
    copyRegistry.bumpProxyRegistryGeneration,
    registryGeneration.bumpProxyRegistryGeneration
  );
  const configBefore = policy.getProxyConfigGeneration();
  copyPolicy.advanceProxyConfigGeneration();
  assert.equal(policy.getProxyConfigGeneration(), configBefore + 1);
  assert.equal(copyPolicy.getProxyConfigGeneration(), configBefore + 1);
  const registryBefore = registryGeneration.getProxyRegistryGeneration();
  registryGeneration.bumpProxyRegistryGeneration();
  assert.equal(copyRegistry.getProxyRegistryGeneration(), registryBefore + 1);
  assert.equal(copyPolicy.requiresStableEgress("grok-web"), true);
  assert.equal(copyPolicy.requiresStableEgress("opencode"), true);
  assert.equal(copyPolicy.requiresStableEgress("openai"), false);
});

function host(result: { proxy: unknown }): string | null {
  return result.proxy && typeof result.proxy === "object"
    ? String((result.proxy as { host?: string }).host)
    : null;
}
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test(
  "native stable egress pins current eligible identity without freezing policy",
  {
    skip: !postgresTestsEnabled,
    timeout: 90_000,
  },
  async (t) => {
    const database = await createTestPostgresDatabase({ maxConnections: 8 });
    const saved = {
      key: process.env.STORAGE_ENCRYPTION_KEY,
      skip: process.env.PROXY_SKIP_RECENTLY_FAILED,
    };
    process.env.STORAGE_ENCRYPTION_KEY = "stable-egress-isolated-test";
    process.env.PROXY_SKIP_RECENTLY_FAILED = "1";
    const planes: Array<Awaited<ReturnType<typeof initializePostgresControlPlane>>> = [];
    try {
      await migratePostgresSchema(database);
      await database.transaction((tx) =>
        tx.query(
          "TRUNCATE omniroute.proxy_assignments,omniroute.proxy_scope_rotation,omniroute.proxy_registry,omniroute.provider_connections,omniroute.api_keys,omniroute.combos,omniroute.key_value RESTART IDENTITY CASCADE"
        )
      );
      for (let i = 0; i < 2; i++)
        planes.push(
          await initializePostgresControlPlane({
            connection: await testPostgresOptions({ maxConnections: 8 }),
            logger: { warn() {} },
            runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
          })
        );
      const repository = createPostgresProxyRepository(database);
      const proxies = await Promise.all(
        ["a", "b", "c"].map((name) =>
          repository.create({
            name,
            type: "http",
            host: `${name}.stable.example`,
            port: 8080,
            username: "user",
            password: `secret-${name}`,
            status: "active",
          })
        )
      );
      const [a, b, c] = proxies.map((row) => {
        assert.ok(row);
        return row;
      });
      let serial = 0;
      const connection = async (provider = "grok-web", members = [a.id, b.id]) => {
        const id = `stable-${++serial}`;
        await database.transaction((tx) =>
          tx.query(
            "INSERT INTO omniroute.provider_connections(id,provider,proxy_enabled,per_key_proxy_enabled,created_at,updated_at) VALUES($1,$2,1,1,$3,$3)",
            [id, provider, new Date().toISOString()]
          )
        );
        await repository.addToPool("account", id, members);
        return id;
      };
      const lookup = (id: string, apiKeyId?: string, providerId?: string, authority = 0) =>
        planes[authority].run(async () => {
          const { resolveProxyForConnection } = await import("../../src/lib/db/settings.ts");
          return resolveProxyForConnection(id, apiKeyId, providerId);
        });

      await t.test(
        "concurrent first calls pin grok-web and opencode, while ordinary calls rotate",
        async () => {
          for (const provider of ["grok-web", "opencode", "openai"]) {
            const id = await connection(provider);
            const results = await Promise.all(
              Array.from({ length: 6 }, () =>
                lookup(id, undefined, provider === "grok-web" ? "openai" : "grok-web")
              )
            );
            const hosts = results.map(host);
            assert.equal(new Set(hosts).size, provider === "openai" ? 2 : 1);
            if (provider === "openai") {
              assert.equal(hosts.filter((value) => value === a.host).length, 3);
              assert.equal(hosts.filter((value) => value === b.host).length, 3);
            }
          }
        }
      );

      await t.test(
        "each persistence identity owns its pin across request-scoped wrappers",
        async () => {
          const id = await connection();
          const first = host(await lookup(id));
          const other = host(await lookup(id, undefined, undefined, 1));
          assert.notEqual(first, other);
          assert.equal(host(await lookup(id)), first);
          assert.equal(host(await lookup(id, undefined, undefined, 1)), other);
        }
      );

      await t.test(
        "config and registry invalidation are independent, including same-scope strategy",
        async () => {
          const id = await connection();
          assert.equal(host(await lookup(id)), a.host);
          assert.equal(host(await lookup(id)), a.host);
          await planes[0].run((binding) => binding.settings.update({ proxyEnabled: true }));
          assert.equal(
            host(await lookup(id)),
            b.host,
            "acknowledged config generation invalidates the pin"
          );
          assert.equal(host(await lookup(id)), b.host);
          await repository.setStrategy("account", id, "round-robin");
          assert.equal(
            host(await lookup(id)),
            a.host,
            "same-scope registry mutation invalidates the pin"
          );
          await repository.assign("account", id, b.id);
          await repository.addToPool("account", id, [a.id]);
          assert.equal(host(await lookup(id)), b.host, "new pool ordering is observed");
        }
      );

      await t.test(
        "policy precedence stays fresh while lower-scope identity is pinned",
        async () => {
          const id = await connection();
          assert.equal(host(await lookup(id)), a.host);
          await database.transaction((tx) =>
            tx.query(
              "INSERT INTO omniroute.api_keys(id,name,key,created_at,proxy_id) VALUES('stable-key','stable','stable-key-value',$1,$2)",
              [new Date().toISOString(), c.id]
            )
          );
          await planes[0].run((binding) => binding.settings.update({ perKeyProxyEnabled: true }));
          assert.equal(host(await lookup(id, "stable-key")), c.host);
          await planes[0].run((binding) => binding.settings.update({ perKeyProxyEnabled: false }));
          assert.notEqual(host(await lookup(id, "stable-key")), c.host);
          await planes[0].run((binding) => binding.settings.update({ proxyEnabled: false }));
          assert.equal(host(await lookup(id, "stable-key")), null);
          await planes[0].run((binding) => binding.settings.update({ proxyEnabled: true }));
          await database.transaction((tx) =>
            tx.query("UPDATE omniroute.provider_connections SET proxy_enabled=0 WHERE id=$1", [id])
          );
          assert.equal(host(await lookup(id)), null);
          await database.transaction((tx) =>
            tx.query("UPDATE omniroute.provider_connections SET proxy_enabled=1 WHERE id=$1", [id])
          );
          assert.ok(host(await lookup(id)));
        }
      );

      await t.test(
        "current credentials and eligibility are checked independently of generation",
        async () => {
          const id = await connection();
          assert.equal(host(await lookup(id)), a.host);
          await database.transaction((tx) =>
            tx.query("UPDATE omniroute.proxy_registry SET password='fresh-secret' WHERE id=$1", [
              a.id,
            ])
          );
          const freshProxy = (await lookup(id)).proxy;
          assert.ok(freshProxy !== null && typeof freshProxy === "object");
          assert.equal(freshProxy.password, "fresh-secret");
          await database.transaction((tx) =>
            tx.query("UPDATE omniroute.proxy_registry SET status='dead' WHERE id=$1", [a.id])
          );
          assert.equal(host(await lookup(id)), b.host);
          await database.transaction((tx) =>
            tx.query("UPDATE omniroute.proxy_registry SET status='active' WHERE id=$1", [a.id])
          );
          assert.equal(
            host(await lookup(id)),
            b.host,
            "recovery does not itself rotate a healthy pin"
          );
        }
      );

      await t.test(
        "refusal alone switches the pin and exhausted required pools remain blocked",
        async () => {
          const id = await connection();
          const first = await lookup(id);
          noteProxyRefusal(proxyEgressKey(first.proxy), "proxy_unreachable");
          const second = await lookup(id);
          assert.notEqual(host(second), host(first));
          assert.equal(host(await lookup(id)), host(second));
          __resetProxyRefusalMemoryForTesting();
          await repository.update(a.id, { status: "dead" });
          await repository.update(b.id, { status: "dead" });
          assert.equal(await repository.resolveScope("account", id), null);
          assert.equal(await repository.blockingConnection(id, "grok-web"), true);
          await repository.update(a.id, { status: "active" });
          await repository.update(b.id, { status: "active" });
        }
      );

      await t.test(
        "provider, combo and global pools retain precedence and stable selection",
        async () => {
          const id = await connection("grok-web", []);
          await repository.addToPool("global", null, [a.id, b.id]);
          const global = await lookup(id);
          assert.equal(global.level, "global");
          assert.equal(host(await lookup(id)), host(global));
          await database.transaction((tx) =>
            tx.query("INSERT INTO omniroute.combos(id,data) VALUES('stable-combo',$1)", [
              JSON.stringify({ models: ["grok-web/model"] }),
            ])
          );
          await repository.addToPool("combo", "stable-combo", [b.id, c.id]);
          assert.equal((await lookup(id)).level, "combo");
          await repository.addToPool("provider", "grok-web", [a.id, c.id]);
          const provider = await lookup(id);
          assert.equal(provider.level, "provider");
          assert.equal(host(await lookup(id)), host(provider));
          await repository.addToPool("account", id, [b.id]);
          assert.equal((await lookup(id)).level, "account");
        }
      );

      await t.test(
        "a delayed selection receipt cannot publish across config or registry changes",
        async () => {
          for (const mutation of ["config", "registry"]) {
            const id = await connection();
            const committed = deferred(),
              release = deferred();
            let delayed = false;
            const wrapped = new Proxy(database, {
              get(target, property, receiver) {
                if (property !== "transaction") return Reflect.get(target, property, receiver);
                return async <T>(body: (tx: PostgresTransaction) => Promise<T>) => {
                  let selection = false;
                  const value = await target.transaction((tx) =>
                    body({
                      query: async <Row = Record<string, unknown>>(
                        sql: string,
                        values?: unknown[]
                      ) => {
                        selection ||= sql.startsWith(
                          "UPDATE omniroute.proxy_scope_rotation SET cursor"
                        );
                        return tx.query<Row>(sql, values);
                      },
                    })
                  );
                  if (selection && !delayed) {
                    delayed = true;
                    committed.resolve();
                    await release.promise;
                  }
                  return value;
                };
              },
            });
            await planes[0].run(async () => {
              const pending = withPostgresCatalogSnapshot(wrapped, () =>
                resolvePostgresProxyForConnection(wrapped, id)
              );
              try {
                await committed.promise;
                if (mutation === "config")
                  await planes[1].run((binding) => binding.settings.update({ proxyEnabled: true }));
                else await repository.setStrategy("account", id, "round-robin");
              } finally {
                release.resolve();
              }
              assert.equal(host(await pending), a.host);
            });
            assert.equal(
              host(await lookup(id)),
              b.host,
              "old receipt was not published into the new epoch"
            );
            assert.equal(host(await lookup(id)), b.host);
          }
        }
      );
    } finally {
      __resetProxyRefusalMemoryForTesting();
      for (const plane of planes) await plane.close();
      await database.close();
      if (saved.key === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = saved.key;
      if (saved.skip === undefined) delete process.env.PROXY_SKIP_RECENTLY_FAILED;
      else process.env.PROXY_SKIP_RECENTLY_FAILED = saved.skip;
    }
  }
);
