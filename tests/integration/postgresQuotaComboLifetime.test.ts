import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate as immediate, setTimeout as delay } from "node:timers/promises";
import {
  initializePostgresControlPlane,
  type PostgresControlPlane,
} from "../../src/lib/db/postgres/controlPlane.ts";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { withPostgresCatalogSnapshot } from "../../src/lib/db/postgres/catalogSnapshot.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../src/lib/db/postgres/database.ts";
import { PostgresQuotaRepository } from "../../src/lib/db/postgres/quotaRepository.ts";
import * as pools from "../../src/lib/db/quotaPools.ts";
import { quotaModelName } from "../../src/lib/quota/quotaModelNaming.ts";
import {
  createTestPostgresDatabase,
  postgresTestsEnabled,
  testPostgresOptions,
} from "../helpers/persistence/postgresTestDatabase.ts";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function bounded<T>(promise: Promise<T>): Promise<T> {
  const controller = new AbortController();
  try {
    return await Promise.race([
      promise,
      delay(10_000, undefined, { signal: controller.signal }).then(() => {
        throw new Error("Native hook oracle did not reach its expected boundary");
      }),
    ]);
  } finally {
    controller.abort();
  }
}

/** Pause a real native hook read AFTER COMMIT, without holding SQL locks or a pool lease. */
function interceptHook(groupId: string, mode: "delay" | "reject" | "unknown" = "delay") {
  const ready = deferred();
  const release = deferred();
  const mutationReady = deferred();
  let paused = false;
  let mutations = 0;
  let groupReads = 0;
  function wrap(database: PostgresDatabase): PostgresDatabase {
    const intercepted = Object.create(database) as PostgresDatabase;
    intercepted.transaction = async <T>(body: (tx: PostgresTransaction) => Promise<T>) => {
      const statements: Array<{ text: string; values: unknown[] }> = [];
      const result = await database.transaction((tx) =>
        body({
          query<Row>(text: string, values: unknown[] = []) {
            statements.push({ text: text.replace(/\s+/g, " ").trim(), values });
            return tx.query<Row>(text, values);
          },
        })
      );
      const mutation = statements.some(({ text }) =>
        /^(INSERT INTO|UPDATE|DELETE FROM) omniroute\.quota_(pools|allocations)\b/.test(text)
      );
      if (mutation) {
        mutations++;
        mutationReady.resolve();
        if (mode === "unknown") throw new PostgresError("commit_unknown");
      }
      const hookRead = statements.some(
        ({ text, values }) =>
          text.startsWith("SELECT id,name,created_at FROM omniroute.quota_groups WHERE id=") &&
          values[0] === groupId
      );
      if (hookRead) groupReads++;
      if (hookRead && !paused) {
        paused = true;
        ready.resolve();
        if (mode === "reject") throw new Error("isolated native combo hook rejection");
        await release.promise;
      }
      return result;
    };
    return intercepted;
  }
  return {
    wrap,
    ready: ready.promise,
    mutationReady: mutationReady.promise,
    release: release.resolve,
    get started() {
      return paused;
    },
    get mutations() {
      return mutations;
    },
    get groupReads() {
      return groupReads;
    },
  };
}

function selected<T>(
  plane: PostgresControlPlane,
  hook: ReturnType<typeof interceptHook> | undefined,
  body: () => Promise<T>
): Promise<T> {
  return plane.run((binding) => {
    const database = hook ? hook.wrap(binding.database!) : binding.database!;
    const scoped: PostgresControlPlaneBinding = { ...binding, database };
    return withPostgresControlPlane(scoped, () => withPostgresCatalogSnapshot(database, body));
  });
}

test(
  "native quota combo hooks stay within their admitted authority",
  {
    skip: !postgresTestsEnabled,
  },
  async (t) => {
    const previousKey = process.env.STORAGE_ENCRYPTION_KEY;
    process.env.STORAGE_ENCRYPTION_KEY = "l70-native-quota-hook-test-storage-key";
    let first: PostgresControlPlane | undefined;
    let second: PostgresControlPlane | undefined;
    let observer: PostgresDatabase | undefined;
    const fixtures: Array<{
      groupId: string;
      provider: string;
      connectionId: string;
      combo: string;
    }> = [];
    try {
      const options = {
        connection: await testPostgresOptions({ maxConnections: 12 }),
        logger: { warn() {} },
        runtime: { applyRuntimeSettings() {}, scheduleCatalogRevalidationAfterInit() {} },
      };
      first = await initializePostgresControlPlane(options);
      second = await initializePostgresControlPlane(options);
      observer = await createTestPostgresDatabase();
      const repository = new PostgresQuotaRepository(observer);
      const secondQuotaBundle = (await import(
        new URL("../../src/lib/db/quotaPools.ts?l70b-second-bundle", import.meta.url).href
      )) as typeof pools;
      assert.notEqual(
        secondQuotaBundle.updatePoolAsync,
        pools.updatePoolAsync,
        "the native oracle must evaluate an independent module instance"
      );

      async function fixture(label: string) {
        const id = crypto.randomUUID();
        const provider = `l70-${id}`;
        const connectionId = `${provider}-connection`;
        const group = await repository.createGroup(`L70 ${label} ${id}`);
        const model = "bounded-hook-model";
        const combo = quotaModelName(group.name, provider, model);
        await observer!.transaction(async (tx) => {
          await tx.query(
            "INSERT INTO omniroute.provider_connections" +
              "(id,provider,created_at,updated_at) VALUES($1,$2,$3,$3)",
            [connectionId, provider, new Date().toISOString()]
          );
          await tx.query(
            "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('customModels',$1,$2)",
            [provider, JSON.stringify([{ id: model, name: model }])]
          );
        });
        fixtures.push({ groupId: group.id, provider, connectionId, combo });
        return { connectionId, groupId: group.id, name: `${label}-${id}`, combo, provider };
      }

      async function comboExists(name: string): Promise<boolean> {
        return observer!.transaction(
          async (tx) =>
            (await tx.query("SELECT id FROM omniroute.combos WHERE name=$1", [name])).rows.length >
            0
        );
      }

      const operations = [
        "create",
        "ensure",
        "ensure-update",
        "update",
        "allocations",
        "provider",
      ] as const;
      for (const operation of operations) {
        await t.test(
          `${operation} does not reply before its real native hook finishes`,
          async () => {
            const f = await fixture(operation);
            const existing =
              operation === "create" || operation === "ensure"
                ? undefined
                : await repository.createPool(f);
            const hook = interceptHook(f.groupId);
            let settled = false;
            const request = selected(first!, hook, async () => {
              if (operation === "create") return pools.createPoolAsync(f);
              if (operation === "ensure") return pools.ensurePoolAsync(f);
              if (operation === "ensure-update")
                return pools.ensurePoolAsync({
                  ...f,
                  allocations: [{ apiKeyId: `${f.name}-key`, weight: 100, policy: "hard" }],
                });
              if (operation === "update")
                return pools.updatePoolAsync(existing!.id, {
                  name: `${f.name}-updated`,
                });
              if (operation === "allocations")
                return pools.upsertAllocationsAsync(existing!.id, []);
              return pools.syncQuotaCombosForProvider(f.provider);
            });
            void request.then(
              () => {
                settled = true;
              },
              () => {
                settled = true;
              }
            );
            try {
              await bounded(hook.ready);
              await immediate();
              assert.equal(
                settled,
                false,
                "the owning scope must remain admitted while the hook waits"
              );
              assert.equal(await comboExists(f.combo), false);
              if (operation !== "provider") {
                assert.equal(hook.mutations, 1, "the primary write already acknowledged once");
                assert((await repository.listPools()).items.some((p) => p.groupId === f.groupId));
              }
              hook.release();
              const reply = await bounded(request);
              if (operation === "create") assert(reply && "id" in reply);
              if (operation === "ensure") assert(reply && "created" in reply && reply.created);
              if (operation === "ensure-update") {
                assert(reply && "updated" in reply && reply.updated && !reply.created);
              }
              if (operation === "update") {
                assert(reply && "name" in reply && reply.name === `${f.name}-updated`);
              }
              assert.equal(
                await comboExists(f.combo),
                true,
                "the complete hook precedes CRUD reply"
              );
            } finally {
              hook.release();
              await request;
            }
          }
        );
      }

      await t.test("acknowledged CRUD survives a guarded native hook rejection", async () => {
        const f = await fixture("hook-rejection");
        const hook = interceptHook(f.groupId, "reject");
        const warnings = t.mock.method(console, "warn", () => {});
        try {
          const pool = await selected(first!, hook, () => pools.createPoolAsync(f));
          assert.equal((await repository.getPool(pool.id))?.name, f.name);
          assert.equal(hook.mutations, 1);
          assert.equal(hook.groupReads, 1);
          assert(
            warnings.mock.calls.some((call) =>
              String(call.arguments[0]).includes("syncQuotaCombos failed")
            )
          );
        } finally {
          warnings.mock.restore();
        }
      });

      await t.test("unknown primary acknowledgement does not invoke or retry hooks", async () => {
        const f = await fixture("commit-unknown");
        const hook = interceptHook(f.groupId, "unknown");
        await assert.rejects(
          selected(first!, hook, () => pools.createPoolAsync(f)),
          (error: unknown) => error instanceof PostgresError && error.kind === "commit_unknown"
        );
        await immediate();
        assert.equal(hook.mutations, 1);
        assert.equal(hook.groupReads, 0);
        assert.equal(
          (await repository.listPools()).items.filter((p) => p.name === f.name).length,
          1,
          "the injected unknown acknowledgement follows a real committed primary write"
        );
      });

      await t.test(
        "unchanged ensure preserves its reply and does not dispatch a hook",
        async () => {
          const f = await fixture("unchanged-ensure");
          const pool = await repository.createPool(f);
          const hook = interceptHook(f.groupId);
          const reply = await selected(first!, hook, () => pools.ensurePoolAsync(f));
          assert.equal(reply.pool.id, pool.id);
          assert.equal(reply.created, false);
          assert.equal(reply.updated, false);
          assert.equal(hook.groupReads, 0);
        }
      );

      await t.test(
        "separate run proxies of one plane serialize the same pool's maintenance",
        async () => {
          const f = await fixture("same-authority");
          const pool = await repository.createPool(f);
          const a = interceptHook(f.groupId);
          const b = interceptHook(f.groupId);
          const one = selected(first!, a, () =>
            pools.updatePoolAsync(pool.id, {
              name: `${f.name}-a`,
            })
          );
          let two: Promise<void> | undefined;
          try {
            await bounded(a.ready);
            two = selected(first!, b, () => secondQuotaBundle.upsertAllocationsAsync(pool.id, []));
            await bounded(b.mutationReady);
            await immediate();
            assert.equal(
              b.started,
              false,
              "a new guarded database proxy must not escape same-plane ordering"
            );
            a.release();
            await bounded(b.ready);
            b.release();
            await bounded(Promise.all([one, two]));
          } finally {
            a.release();
            b.release();
            await Promise.allSettled([one, ...(two ? [two] : [])]);
          }
        }
      );

      await t.test(
        "independent planes do not borrow each other's same-id maintenance",
        async () => {
          const f = await fixture("foreign-maintenance");
          const pool = await repository.createPool(f);
          const a = interceptHook(f.groupId);
          const b = interceptHook(f.groupId);
          const one = selected(first!, a, () =>
            pools.updatePoolAsync(pool.id, {
              name: `${f.name}-a`,
            })
          );
          let two: Promise<void> | undefined;
          try {
            await bounded(a.ready);
            two = selected(second!, b, () => secondQuotaBundle.upsertAllocationsAsync(pool.id, []));
            await bounded(b.ready);
            assert.equal(a.started, true, "first plane is still waiting independently");
            b.release();
            await bounded(two);
            a.release();
            await bounded(one);
          } finally {
            a.release();
            b.release();
            await Promise.allSettled([one, ...(two ? [two] : [])]);
          }
        }
      );

      await t.test("independent bundles share the same plane's deleting marker", async () => {
        const f = await fixture("same-authority-deleting");
        const pool = await repository.createPool(f);
        const hook = interceptHook(f.groupId);
        const deletion = selected(first!, hook, () => pools.deletePool(pool.id));
        try {
          await bounded(hook.ready);
          const changed = await selected(first!, undefined, () =>
            secondQuotaBundle.updatePoolAsync(pool.id, { name: `${f.name}-blocked` })
          );
          assert.equal(changed, null);
          assert.equal(
            await selected(first!, undefined, () => secondQuotaBundle.deletePool(pool.id)),
            false
          );
          assert.equal((await repository.getPool(pool.id))?.name, f.name);
          hook.release();
          assert.equal(await bounded(deletion), true);
        } finally {
          hook.release();
          await deletion;
        }
      });

      await t.test("one plane's deleting marker cannot veto another plane's update", async () => {
        const f = await fixture("foreign-deleting");
        const pool = await repository.createPool(f);
        const hook = interceptHook(f.groupId);
        const deletion = selected(first!, hook, () => pools.deletePool(pool.id));
        try {
          await bounded(hook.ready);
          const changed = await bounded(
            selected(second!, undefined, () =>
              secondQuotaBundle.updatePoolAsync(pool.id, { name: `${f.name}-foreign` })
            )
          );
          assert.equal(changed?.name, `${f.name}-foreign`);
          assert.equal((await repository.getPool(pool.id))?.name, `${f.name}-foreign`);
          hook.release();
          assert.equal(await bounded(deletion), true);
        } finally {
          hook.release();
          await deletion;
        }
      });

      await t.test(
        "plane shutdown drains the awaited hook before closing its native pool",
        async () => {
          const f = await fixture("shutdown");
          const hook = interceptHook(f.groupId);
          const request = selected(first!, hook, () => pools.createPoolAsync(f));
          let closing: Promise<void> | undefined;
          let closed = false;
          try {
            await bounded(hook.ready);
            closing = first!.close();
            void closing.then(() => {
              closed = true;
            });
            await immediate();
            assert.equal(
              closed,
              false,
              "the control plane retains the full admitted hook lifetime"
            );
            hook.release();
            await bounded(request);
            await bounded(closing);
            assert.equal(await comboExists(f.combo), true);
          } finally {
            hook.release();
            await Promise.allSettled([request, ...(closing ? [closing] : [])]);
          }
        }
      );
    } finally {
      if (observer) {
        for (const f of fixtures) {
          await observer.transaction(async (tx) => {
            await tx.query("DELETE FROM omniroute.combos WHERE name=$1", [f.combo]);
            await tx.query("DELETE FROM omniroute.quota_pools WHERE group_id=$1", [f.groupId]);
            await tx.query("DELETE FROM omniroute.quota_groups WHERE id=$1", [f.groupId]);
            await tx.query("DELETE FROM omniroute.provider_connections WHERE id=$1", [
              f.connectionId,
            ]);
            await tx.query(
              "DELETE FROM omniroute.key_value WHERE namespace='customModels' AND key=$1",
              [f.provider]
            );
          });
        }
        await observer.close();
      }
      await first?.close();
      await second?.close();
      if (previousKey === undefined) delete process.env.STORAGE_ENCRYPTION_KEY;
      else process.env.STORAGE_ENCRYPTION_KEY = previousKey;
    }
  }
);
