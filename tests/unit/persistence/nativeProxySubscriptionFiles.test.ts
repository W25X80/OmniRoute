import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import fs from "node:fs";
import asyncFs from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";
import {
  readPrivateConfigFile,
  readPrivateConfigFileAsync,
  writePrivateConfigFile,
  writePrivateConfigFileAsync,
} from "../../../src/lib/cli-helper/privateConfigFile.ts";
import { generateCoreConfigIntention } from "../../../src/lib/proxySubscription/coreConfig/sync.ts";
import {
  applyRendered,
  removeSubscriptionSideFilesAsync,
} from "../../../src/lib/proxySubscription/coreConfig/apply.ts";

function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const turn = () => new Promise<void>((done) => setImmediate(done));

async function scenario(t: TestContext, body: (directory: string) => Promise<void>) {
  const directory = await asyncFs.mkdtemp(join(tmpdir(), "native-subscription-files-"));
  const backend = process.env.OMNIROUTE_DATABASE_BACKEND;
  const binary = process.env.OMNIROUTE_PROXY_CORE_BINARY_PATH;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  delete process.env.OMNIROUTE_PROXY_CORE_BINARY_PATH;
  try {
    await body(directory);
  } finally {
    t.mock.restoreAll();
    if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    if (binary === undefined) delete process.env.OMNIROUTE_PROXY_CORE_BINARY_PATH;
    else process.env.OMNIROUTE_PROXY_CORE_BINARY_PATH = binary;
    await asyncFs.rm(directory, { recursive: true, force: true });
  }
}

function forbidSync(t: TestContext) {
  for (const name of [
    "lstatSync",
    "statSync",
    "readFileSync",
    "mkdirSync",
    "openSync",
    "writeFileSync",
    "fchmodSync",
    "fsyncSync",
    "closeSync",
    "renameSync",
    "unlinkSync",
    "accessSync",
  ] as const) {
    t.mock.method(fs, name, () => {
      throw new Error(`Forbidden synchronous I/O: ${name}`);
    });
  }
}

const feed = {
  nodes: [],
  needsCore: [
    {
      name: "native-node",
      rawProtocol: "vless" as const,
      detail: "requires local core",
      source: {
        kind: "object" as const,
        value: { type: "vless", server: "203.0.113.2", server_port: 443, uuid: "fixture" },
      },
    },
  ],
  format: "v2ray-json" as const,
};

test(
  "native subscription generation awaits fsync without blocking the event loop",
  { timeout: 10_000 },
  async (t) =>
    scenario(t, async (directory) => {
      const target = join(directory, "core.json");
      const original = '{"dns":{"servers":[]},"inbounds":[],"outbounds":[]}';
      await asyncFs.writeFile(target, original);
      const entered = barrier(),
        release = barrier();
      const open = asyncFs.open;
      t.mock.method(asyncFs, "open", async (...args: Parameters<typeof asyncFs.open>) => {
        const handle = await open(...args);
        if (String(args[0]).endsWith(".tmp")) {
          const sync = handle.sync.bind(handle);
          t.mock.method(handle, "sync", async () => {
            entered.resolve();
            await release.promise;
            await sync();
          });
        }
        return handle;
      });
      forbidSync(t);
      let completed = false;
      const work = generateCoreConfigIntention(
        {
          id: "native",
          coreConfigPath: target,
          localCoreEndpoint: "socks5://127.0.0.1:1080 selector=a",
        },
        feed
      ).then((result) => {
        completed = true;
        return result;
      });
      try {
        await Promise.race([
          entered.promise,
          work.then(() => {
            throw new Error("generation finished without awaiting native fsync");
          }),
        ]);
        await turn();
        assert.equal(completed, false, "the success response must await fsync and rename");
        await assert.rejects(asyncFs.access(`${target}.generated`), { code: "ENOENT" });
        release.resolve();
        const result = await work;
        assert.equal(result.warning, null);
        assert.equal(await asyncFs.readFile(target, "utf8"), original);
        assert.equal((await asyncFs.stat(`${target}.generated`)).mode & 0o777, 0o600);
        const generated = JSON.parse(await asyncFs.readFile(`${target}.generated`, "utf8"));
        assert.equal(
          generated.outbounds.filter((item: { type: string }) => item.type === "selector").length,
          1
        );
        assert.equal(
          (await asyncFs.readdir(directory)).filter((name) => name.endsWith(".tmp")).length,
          0
        );
      } finally {
        release.resolve();
        await work;
      }
    })
);

test(
  "native private config rejects final symlinks and keeps default synchronous signatures",
  { timeout: 10_000 },
  async (t) =>
    scenario(t, async (directory) => {
      const target = join(directory, "target.json"),
        link = join(directory, "link.json");
      writePrivateConfigFile(target, "original");
      assert.equal(readPrivateConfigFile(target), "original");
      await asyncFs.symlink(target, link);
      forbidSync(t);
      await assert.rejects(readPrivateConfigFileAsync(link), /symbolic link/);
      await assert.rejects(writePrivateConfigFileAsync(link, "replacement"), /symbolic link/);
      assert.equal(await asyncFs.readFile(target, "utf8"), "original");
    })
);

test(
  "native checked apply restores the adopted file if its final rename fails",
  { timeout: 10_000 },
  async (t) =>
    scenario(t, async (directory) => {
      const target = join(directory, "core.json"),
        binary = join(directory, "sing-box");
      await asyncFs.writeFile(target, "original", { mode: 0o640 });
      await asyncFs.writeFile(binary, "fixture", { mode: 0o700 });
      await asyncFs.writeFile(`${target}.generated`, "stale");
      const rename = asyncFs.rename;
      t.mock.method(asyncFs, "rename", async (from, to) => {
        if (String(from).startsWith(`${target}.check.`) && to === target)
          throw Object.assign(new Error("injected rename failure"), { code: "EIO" });
        await rename(from, to);
      });
      forbidSync(t);
      let checked = false;
      const result = await applyRendered({
        adoptedPath: target,
        binaryPath: binary,
        renderedText: "replacement",
        subscriptionId: "native",
        runCheck: async (_binary, args) => {
          checked = true;
          assert.equal(await asyncFs.readFile(args.at(-1)!, "utf8"), "replacement");
          return { stdout: "", stderr: "" };
        },
      });
      assert.equal(checked, true);
      assert.deepEqual(result, { status: "beside", beside: "write_failed" });
      assert.equal(await asyncFs.readFile(target, "utf8"), "original");
      assert.equal((await asyncFs.stat(target)).mode & 0o777, 0o640);
      assert.equal(
        (await asyncFs.readdir(directory)).some((name) => name.includes(".check.")),
        false
      );
    })
);

test(
  "native checked apply preserves mode and cleans only the side file",
  { timeout: 10_000 },
  async (t) =>
    scenario(t, async (directory) => {
      const target = join(directory, "core.json"),
        binary = join(directory, "sing-box");
      await asyncFs.writeFile(target, "original", { mode: 0o640 });
      await asyncFs.writeFile(binary, "fixture", { mode: 0o700 });
      await asyncFs.writeFile(`${target}.generated`, "stale");
      forbidSync(t);
      const result = await applyRendered({
        adoptedPath: target,
        binaryPath: binary,
        renderedText: "replacement",
        subscriptionId: "native",
        runCheck: async () => ({ stdout: "", stderr: "" }),
      });
      assert.deepEqual(result, { status: "replaced" });
      assert.equal(await asyncFs.readFile(target, "utf8"), "replacement");
      assert.equal(await asyncFs.readFile(`${target}.prev`, "utf8"), "original");
      assert.equal((await asyncFs.stat(target)).mode & 0o777, 0o640);
      await asyncFs.writeFile(`${target}.generated`, "delete only this");
      await removeSubscriptionSideFilesAsync(target, "native");
      await assert.rejects(asyncFs.access(`${target}.generated`), { code: "ENOENT" });
      assert.equal(await asyncFs.readFile(target, "utf8"), "replacement");
    })
);
