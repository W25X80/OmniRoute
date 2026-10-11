import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const previousDataDir = process.env.DATA_DIR;
const dataDir = mkdtempSync(join(tmpdir(), "omniroute-l70-sqlite-quota-"));
process.env.DATA_DIR = dataDir;
const core = await import("../../src/lib/db/core.ts");
const pools = await import("../../src/lib/db/quotaPools.ts");
const groups = await import("../../src/lib/db/quotaGroups.ts");
const combos = await import("../../src/lib/db/combos.ts");
const { quotaModelName } = await import("../../src/lib/quota/quotaModelNaming.ts");

test("SQLite pool creation keeps its synchronous return and eventual combo hook", async () => {
  const provider = `l70-${crypto.randomUUID()}`;
  const connectionId = `${provider}-connection`;
  const model = "bounded-sqlite-model";
  try {
    await core.ensureDbInitialized();
    const db = core.getDbInstance();
    const now = new Date().toISOString();
    db.prepare(
      "INSERT INTO provider_connections(id,provider,created_at,updated_at) VALUES(?,?,?,?)"
    ).run(connectionId, provider, now, now);
    db.prepare("INSERT INTO key_value(namespace,key,value) VALUES('customModels',?,?)").run(
      provider,
      JSON.stringify([{ id: model, name: model }])
    );
    const group = groups.createGroup("L70 SQLite compatibility");
    const pool = pools.createPool({ connectionId, groupId: group.id, name: "SQLite pool" });
    assert.equal(typeof pool.id, "string");
    assert.equal("then" in pool, false, "legacy callers receive the pool synchronously");
    assert.equal(pools.getPool(pool.id)?.name, "SQLite pool");
    const expected = quotaModelName(group.name, provider, model);
    let created: Awaited<ReturnType<typeof combos.getComboByName>> = null;
    for (let attempt = 0; attempt < 200 && !created; attempt++) {
      created = await combos.getComboByName(expected);
      if (!created) await delay(10);
    }
    assert(created, "the original detached SQLite hook still mints its quota model");
    assert.equal(await pools.deletePool(pool.id), true);
    assert.equal(
      await combos.getComboByName(expected),
      null,
      "deletion still awaits scoped cleanup"
    );
  } finally {
    core.resetDbInstance();
    rmSync(dataDir, { recursive: true, force: true });
    if (previousDataDir === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = previousDataDir;
  }
});
