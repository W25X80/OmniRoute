import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import {
  fileFixture as io,
  obsidianFixture as db,
} from "../../helpers/persistence/servingFileFixture.mjs";
const owner = new URL("../../../src/lib/obsidianSync.ts", import.meta.url).href;
const dataPaths = new URL("../../../src/lib/dataPaths.ts", import.meta.url).href;
const fixtureUrl = new URL("../../helpers/persistence/servingFileFixture.mjs", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL?.split("?")[0];
    if (
      (parent === owner && specifier === "./db/obsidian") ||
      ([owner, dataPaths].includes(parent ?? "") &&
        ["node:fs", "node:fs/promises"].includes(specifier))
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const obsidian = await import("../../../src/lib/obsidianSync.ts");
const backend = process.env.OMNIROUTE_DATABASE_BACKEND,
  dataDir = process.env.DATA_DIR;
const turn = () => new Promise<void>((done) => setImmediate(done));
async function scenario(body: (directory: string, vault: string) => Promise<void>) {
  const directory = await mkdtemp(path.join(tmpdir(), "native-obsidian-files-")),
    vault = path.join(directory, "vault");
  await mkdir(vault);
  await mkdir(path.join(directory, "data"));
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.DATA_DIR = path.join(directory, "data");
  io.reset();
  io.allowSync = false;
  db.path = null;
  db.saved = null;
  db.clearCalls = 0;
  try {
    await body(directory, vault);
  } finally {
    io.reset();
    if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    if (dataDir === undefined) delete process.env.DATA_DIR;
    else process.env.DATA_DIR = dataDir;
    await rm(directory, { recursive: true, force: true });
  }
}
test.after(() => hooks.deregister());
test("native Obsidian enable waits for real stat and keeps vault/data physical overlap protection", async () =>
  scenario(async (directory, vault) => {
    const gate = io.block("stat", vault);
    let completed = false;
    const operation = obsidian.enableObsidianVaultSync(vault).then((value) => {
      completed = true;
      return value;
    });
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(completed, false);
      assert.equal(db.saved, null);
    } finally {
      gate.release.release();
    }
    const result = await operation;
    assert.equal(result.success, true);
    if (result.success) {
      assert.equal(result.vaultPath, vault);
      assert.equal(result.username.length, 12);
      assert.equal(result.password.length, 24);
    }
    const link = path.join(directory, "data-link");
    await symlink(process.env.DATA_DIR!, link);
    assert.equal(await obsidian.vaultPathOverlapsDataDirAsync(link), true);
    assert.equal((await obsidian.enableObsidianVaultSync(link)).success, false);
    assert.equal(
      (await obsidian.enableObsidianVaultSync(path.join(directory, "missing"))).success,
      false
    );
    assert.equal(io.syncCalls, 0);
  }));
test("native Obsidian disable awaits managed stignore deletion and leaves unmanaged files", async () =>
  scenario(async (_directory, vault) => {
    db.path = vault;
    const file = path.join(vault, ".stignore");
    await writeFile(file, "# Managed by OmniRoute\nmanaged");
    const gate = io.block("unlink", file);
    const operation = obsidian.disableObsidianVaultSync();
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(db.clearCalls, 0);
    } finally {
      gate.release.release();
    }
    assert.equal((await operation).success, true);
    await assert.rejects(readFile(file));
    assert.equal(db.clearCalls, 1);
    db.path = vault;
    await writeFile(file, "operator configuration");
    await obsidian.disableObsidianVaultSync();
    assert.equal(await readFile(file, "utf8"), "operator configuration");
    assert.equal(io.syncCalls, 0);
  }));
test("SQLite Obsidian control retains the synchronous filesystem path", async () =>
  scenario(async (_directory, vault) => {
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    io.allowSync = true;
    assert.equal((await obsidian.enableObsidianVaultSync(vault)).success, true);
    assert.ok(io.syncCalls > 0);
  }));
