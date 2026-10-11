import fs from "node:fs";
import * as files from "node:fs/promises";

function barrier() {
  /** @type {() => void} */
  let release = () => {};
  /** @type {Promise<void>} */
  const promise = new Promise((done) => {
    release = done;
  });
  return { promise, release };
}
export const fileFixture = {
  calls: [],
  syncCalls: 0,
  allowSync: true,
  gate: undefined,
  renameFailure: false,
  block(operation, target) {
    const gate = { operation, target, entered: barrier(), release: barrier() };
    this.gate = gate;
    return gate;
  },
  reset() {
    this.gate?.release.release();
    this.gate = undefined;
    this.calls = [];
    this.syncCalls = 0;
    this.allowSync = true;
    this.renameFailure = false;
  },
};
async function before(operation, target) {
  fileFixture.calls.push({ operation, target: String(target) });
  const gate = fileFixture.gate;
  if (gate?.operation === operation && (gate.target === String(target) || gate.target === "*")) {
    fileFixture.gate = undefined;
    gate.entered.release();
    await gate.release.promise;
  }
}
export async function access(...args) {
  await before("access", args[0]);
  return files.access(...args);
}
export async function readFile(...args) {
  await before("readFile", args[0]);
  return files.readFile(...args);
}
export async function readdir(...args) {
  await before("readdir", args[0]);
  return files.readdir(...args);
}
export async function realpath(...args) {
  await before("realpath", args[0]);
  return files.realpath(...args);
}
export async function stat(...args) {
  await before("stat", args[0]);
  return files.stat(...args);
}
export async function mkdir(...args) {
  await before("mkdir", args[0]);
  return files.mkdir(...args);
}
export async function writeFile(...args) {
  await before("writeFile", args[0]);
  return files.writeFile(...args);
}
export async function rm(...args) {
  await before("rm", args[0]);
  return files.rm(...args);
}
export async function unlink(...args) {
  await before("unlink", args[0]);
  return files.unlink(...args);
}
export async function rename(...args) {
  await before("rename", args[0]);
  if (fileFixture.renameFailure) throw new Error("Simulated rename failure");
  return files.rename(...args);
}
const synchronous = { ...fs };
for (const name of [
  "accessSync",
  "existsSync",
  "readFileSync",
  "readdirSync",
  "realpathSync",
  "statSync",
  "mkdirSync",
  "writeFileSync",
  "unlinkSync",
  "renameSync",
  "rmSync",
  "rmdirSync",
]) {
  const actual = fs[name];
  synchronous[name] = (...args) => {
    fileFixture.syncCalls++;
    if (!fileFixture.allowSync) throw new Error("Synchronous serving file operation");
    return actual(...args);
  };
}
synchronous.realpathSync.native = (...args) => {
  fileFixture.syncCalls++;
  if (!fileFixture.allowSync) throw new Error("Synchronous serving file operation");
  return fs.realpathSync.native(...args);
};
export default synchronous;
export const obsidianFixture = { path: null, saved: null, clearCalls: 0 };
export async function getWebdavConfigurationAsync() {
  return { enabled: false, vaultPath: obsidianFixture.path, username: null, password: null };
}
export async function getObsidianVaultPathAsync() {
  return obsidianFixture.path;
}
export async function saveObsidianSyncConfigurationAsync(path, username, password) {
  obsidianFixture.path = path;
  obsidianFixture.saved = { path, username, password };
}
export async function clearObsidianSyncConfigurationAsync() {
  obsidianFixture.path = null;
  obsidianFixture.clearCalls++;
}
