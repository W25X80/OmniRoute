import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { realpathSync } from "node:fs";
import { pathToFileURL } from "node:url";
import { setImmediate } from "node:timers/promises";
import test from "node:test";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../src/lib/db/controlPlaneContext.ts";
import { store } from "../helpers/persistence/skillRegistryPublicationFixture.ts";

process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";

const registryUrl = pathToFileURL(
  realpathSync(new URL("../../src/lib/skills/registry.ts", import.meta.url))
).href;
const routeUrl = pathToFileURL(
  realpathSync(new URL("../../src/app/api/skills/[id]/route.ts", import.meta.url))
).href;
const fixtureUrl = new URL(
  "../helpers/persistence/skillRegistryPublicationFixture.ts",
  import.meta.url
).href;
const hooks = registerHooks({
  resolve(specifier, context, next) {
    const parent = context.parentURL?.split("?")[0];
    if (
      parent === registryUrl &&
      ["../db/skillStorage", "../../../open-sse/utils/logger.ts"].includes(specifier)
    )
      return { url: fixtureUrl, shortCircuit: true };
    if (parent === routeUrl) {
      if (specifier === "@/lib/skills/registry") return { url: registryUrl, shortCircuit: true };
      if (
        [
          "@/lib/db/skills",
          "@/lib/api/requireManagementAuth",
          "@omniroute/open-sse/utils/error",
        ].includes(specifier)
      )
        return { url: fixtureUrl, shortCircuit: true };
    }
    return next(specifier, context);
  },
});
const { skillRegistry } = (await import(
  registryUrl
)) as typeof import("../../src/lib/skills/registry.ts");
const route = (await import(routeUrl)) as typeof import("../../src/app/api/skills/[id]/route.ts");
test.after(() => hooks.deregister());

function binding(assertReady = () => {}): PostgresControlPlaneBinding {
  return { databaseIdentity: {}, assertReady } as unknown as PostgresControlPlaneBinding;
}
function selected(body: () => Promise<void>) {
  return withPostgresControlPlane(binding(), body);
}
function skill(name: string, apiKeyId = "owner") {
  return {
    name,
    apiKeyId,
    schema: { input: {}, output: {} },
    handler: "handler",
    version: "1.0.0",
  };
}

test("older all-scope read cannot erase an acknowledged registration or stamp it fresh", () =>
  selected(async () => {
    const current = store();
    const gate = current.holdNextRead();
    const load = skillRegistry.loadFromDatabase();
    await gate.entered;
    const added = await skillRegistry.register(skill("fresh"));
    assert.equal(skillRegistry.getSkill(added.id, "owner")?.id, added.id);
    gate.release();
    await load;
    await skillRegistry.loadFromDatabase("owner");
    assert.equal(skillRegistry.getSkill(added.id, "owner")?.id, added.id);
    assert.equal(skillRegistry.resolveVersion("fresh", "^1.0.0", "owner")?.id, added.id);
    assert.equal(current.reads, 2, "one superseded read needs one fresh read");
  }));

test("older scoped read cannot resurrect a deleted skill or undo acknowledged enabled state", () =>
  selected(async () => {
    const removed = await skillRegistry.register(skill("removed"));
    const toggled = await skillRegistry.register({ ...skill("toggled"), enabled: false });
    const current = store();
    const gate = current.holdNextRead();
    const load = skillRegistry.loadFromDatabase("owner");
    await gate.entered;
    await skillRegistry.unregisterById(removed.id);
    await skillRegistry.setEnabledById(toggled.id, "owner", true);
    gate.release();
    await load;
    assert.equal(skillRegistry.getSkill(removed.id, "owner"), undefined);
    assert.equal(skillRegistry.getSkill(toggled.id, "owner")?.enabled, true);
    assert.equal(skillRegistry.getSkill(toggled.id, "owner")?.mode, "on");
  }));

test("cached setEnabled alone supersedes an old read", () =>
  selected(async () => {
    const added = await skillRegistry.register({ ...skill("toggle-only"), enabled: false });
    const gate = store().holdNextRead();
    const load = skillRegistry.loadFromDatabase("owner");
    await gate.entered;
    await skillRegistry.setEnabledById(added.id, "owner", true);
    gate.release();
    await load;
    assert.equal(skillRegistry.getSkill(added.id, "owner")?.enabled, true);
  }));

test("different-scope waiters share successor flights and preserve global visibility and tenant shadowing", () =>
  selected(async () => {
    const global = await skillRegistry.register(skill("same", "system"));
    const owned = await skillRegistry.register(skill("same", "b"));
    await skillRegistry.register(skill("private", "c"));
    const current = store();
    const gate = current.holdNextRead();
    const first = skillRegistry.loadFromDatabase("a");
    await gate.entered;
    const waiting = [
      skillRegistry.loadFromDatabase("b"),
      skillRegistry.loadFromDatabase("b"),
      skillRegistry.loadFromDatabase("c"),
    ];
    gate.release();
    await Promise.all([first, ...waiting]);
    assert.equal(current.maxActiveReads, 1);
    assert.equal(current.reads, 3, "one query per requested scope");
    assert.equal(skillRegistry.getSkill("same", "a")?.id, global.id);
    assert.equal(skillRegistry.getSkill("same", "b")?.id, owned.id);
    assert.equal(skillRegistry.getSkill("private", "b"), undefined);
    await skillRegistry.loadFromDatabase();
    assert.equal(skillRegistry.list().length, 3);
    const reads = current.reads;
    await skillRegistry.loadFromDatabase("b");
    assert.equal(current.reads, reads, "warm all-scope cache covers a tenant");
  }));

test("authority is checked again after a delayed read before local publication", async () => {
  let active = true;
  const ended = new Error("fixture scope ended");
  const scope = binding(() => {
    if (!active) throw ended;
  });
  await withPostgresControlPlane(scope, async () => {
    const gate = store().holdNextRead();
    const load = skillRegistry.loadFromDatabase();
    await gate.entered;
    active = false;
    gate.release();
    await assert.rejects(load, (error) => error === ended);
    active = true;
    assert.equal(skillRegistry.list().length, 0);
    await skillRegistry.loadFromDatabase();
  });
});

test("borrowed load cannot publish a foreign authority into its original instance", async () => {
  const first = binding(),
    second = binding();
  const load = withPostgresControlPlane(first, () => skillRegistry.loadFromDatabase);
  await withPostgresControlPlane(second, async () => {
    await skillRegistry.register(skill("foreign"));
    await assert.rejects(load(), /authority/i);
  });
  withPostgresControlPlane(first, () => assert.equal(skillRegistry.list().length, 0));
});

test("continuous mutation rejects bounded refresh without publishing old rows", () =>
  selected(async () => {
    const current = store();
    current.onRead = () => skillRegistry.invalidateCache();
    await assert.rejects(skillRegistry.loadFromDatabase(), /changed repeatedly|unavailable/i);
    assert.ok(current.reads > 0 && current.reads <= 4);
    current.onRead = undefined;
    await skillRegistry.loadFromDatabase();
  }));

test("read failure and uncertain mutation propagate once and do not poison a later load", () =>
  selected(async () => {
    const current = store();
    const fault = Object.assign(new Error("fixture commit outcome unknown"), {
      kind: "commit_unknown",
    });
    current.writeFailure = fault;
    current.commitBeforeInsertFailure = true;
    await assert.rejects(skillRegistry.register(skill("uncertain")), (error) => error === fault);
    assert.equal(current.inserts, 1);
    assert.equal(skillRegistry.getSkill("uncertain", "owner"), undefined);
    current.writeFailure = undefined;
    current.readFailure = fault;
    await assert.rejects(skillRegistry.loadFromDatabase(), (error) => error === fault);
    current.readFailure = undefined;
    await skillRegistry.loadFromDatabase();
    assert.equal(current.reads, 2);
    assert.equal(current.inserts, 1);
    assert.equal(current.rows.size, 1);
    assert.ok(
      skillRegistry.getSkill("uncertain", "owner"),
      "a later read may reconcile a committed row"
    );
  }));

test("scoped stale loads reload all global owners while excluding another tenant", () =>
  selected(async () => {
    const gate = store().holdNextRead();
    const load = skillRegistry.loadFromDatabase("a");
    await gate.entered;
    for (const owner of ["system", "skillsmp", "skillssh"])
      await skillRegistry.register(skill("global-" + owner, owner));
    await skillRegistry.register(skill("private-b", "b"));
    gate.release();
    await load;
    assert.deepEqual(
      skillRegistry
        .list("a")
        .map((entry) => entry.name)
        .sort(),
      ["global-skillsmp", "global-skillssh", "global-system"]
    );
    assert.equal(store().reads, 2);
    assert.ok(
      skillRegistry.getSkill("private-b", "b"),
      "scoped publication preserves another tenant's locally registered row"
    );
  }));

test("an accepted snapshot retains the existing 60 second TTL", () =>
  selected(async () => {
    const originalNow = Date.now;
    let now = 1_000_000;
    Date.now = () => now;
    try {
      await skillRegistry.register(skill("ttl"));
      await skillRegistry.loadFromDatabase("owner");
      now += 30_000;
      await skillRegistry.loadFromDatabase("owner");
      assert.equal(store().reads, 1);
      now += 30_001;
      await skillRegistry.loadFromDatabase("owner");
      assert.equal(store().reads, 2);
    } finally {
      Date.now = originalNow;
    }
  }));

test("PUT refreshes a warm registry after acknowledged update without a manual cache reset", () =>
  selected(async () => {
    const added = await skillRegistry.register({ ...skill("route-toggle"), enabled: false });
    await skillRegistry.loadFromDatabase();
    const response = await route.PUT(
      new Request("http://localhost/api/skills/id", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ enabled: true }),
      }),
      { params: Promise.resolve({ id: added.id }) }
    );
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { success: true, enabled: true });
    assert.equal(skillRegistry.getSkill(added.id, "owner")?.enabled, true);
    assert.equal(skillRegistry.getSkill(added.id, "owner")?.mode, "on");
    assert.equal(store().updates, 1);
  }));

test("PUT auth, validation and uncertain-write errors preserve the existing response boundary", () =>
  selected(async () => {
    const current = store();
    const request = (body: unknown) =>
      new Request("http://localhost/api/skills/id", {
        method: "PUT",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    const params = { params: Promise.resolve({ id: "missing" }) };
    current.denied = true;
    assert.equal((await route.PUT(request({ enabled: true }), params)).status, 401);
    current.denied = false;
    assert.equal((await route.PUT(request({ enabled: "yes" }), params)).status, 400);
    assert.equal(current.updates, 0);
    current.writeFailure = new Error("fixture uncertain commit");
    assert.equal((await route.PUT(request({ enabled: true }), params)).status, 500);
    assert.equal(current.updates, 1);
    assert.equal(current.reads, 0);
  }));

test("default and native authorities keep distinct cache instances", async () => {
  await skillRegistry.register(skill("default-only"));
  await selected(async () => {
    assert.equal(skillRegistry.getSkill("default-only", "owner"), undefined);
    await skillRegistry.register(skill("native-only"));
    await skillRegistry.loadFromDatabase("owner");
  });
  assert.equal(skillRegistry.getSkill("native-only", "owner"), undefined);
  assert.ok(skillRegistry.getSkill("default-only", "owner"));
  await setImmediate();
});
