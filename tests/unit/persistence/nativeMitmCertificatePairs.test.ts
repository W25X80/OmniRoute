import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import { createPrivateKey, X509Certificate } from "node:crypto";
import fs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
import { generateCert } from "../../../src/mitm/cert/generate.ts";
import { loadOrCreateMitmCa } from "../../../src/mitm/cert/rootCa.ts";

function barrier() {
  let release!: () => void;
  return {
    promise: new Promise<void>((resolve) => {
      release = resolve;
    }),
    release: () => release(),
  };
}

test("native certificate regeneration and reuse serialize the complete real key/cert pair", async (context) => {
  await import("selfsigned");
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const entered = barrier();
  const blocked = barrier();
  const realWrite = fs.writeFile;
  let writes = 0;
  context.mock.method(fs, "writeFile", async (...args: Parameters<typeof realWrite>) => {
    await realWrite(...args);
    if (++writes === 1) {
      entered.release();
      await blocked.promise;
    }
  });
  const directoryWrites = context.mock.method(fs, "mkdir");
  const probes = context.mock.method(fs, "access");
  syncBuiltinESMExports();
  const accepted: Promise<unknown>[] = [];
  try {
    const first = generateCert({ force: true });
    accepted.push(first);
    await entered.promise;
    let finished = 0;
    const second = generateCert({ force: true }).then((value) => {
      finished++;
      return value;
    });
    const reused = generateCert().then((value) => {
      finished++;
      return value;
    });
    accepted.push(second, reused);
    await nextTurn();
    assert.equal(directoryWrites.mock.callCount(), 1, "the next generator has not entered");
    assert.equal(
      probes.mock.calls.filter((call) => /server\.(key|crt)$/.test(String(call.arguments[0])))
        .length,
      0,
      "reuse must not inspect an incomplete pair"
    );
    assert.equal(finished, 0);
    assert.equal(writes, 1);
    blocked.release();
    const [, final, existing] = await Promise.all([first, second, reused]);
    assert.deepEqual(existing, final);
    assert.equal(writes, 4, "two force calls publish exactly two complete pairs");
    const certificate = new X509Certificate(await fs.readFile(final.cert));
    assert.ok(certificate.checkPrivateKey(createPrivateKey(await fs.readFile(final.key))));
  } finally {
    blocked.release();
    await Promise.allSettled(accepted);
    context.mock.restoreAll();
    syncBuiltinESMExports();
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});

test("native root CA creation and normalized-directory reuse share one persisted pair", async (context) => {
  await import("selfsigned");
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const certDir = await fs.mkdtemp(path.join(process.env.DATA_DIR!, "ca-pair-"));
  const entered = barrier();
  const blocked = barrier();
  const realWrite = fs.writeFile;
  let writes = 0;
  context.mock.method(fs, "writeFile", async (...args: Parameters<typeof realWrite>) => {
    await realWrite(...args);
    if (++writes === 1) {
      entered.release();
      await blocked.promise;
    }
  });
  syncBuiltinESMExports();
  const accepted: Promise<unknown>[] = [];
  try {
    const creating = loadOrCreateMitmCa(certDir);
    accepted.push(creating);
    await entered.promise;
    let settled = false;
    const reuse = loadOrCreateMitmCa(`${certDir}/.`).then((value) => {
      settled = true;
      return value;
    });
    accepted.push(reuse);
    await nextTurn();
    assert.equal(settled, false);
    assert.equal(writes, 1);
    blocked.release();
    const [first, second] = await Promise.all([creating, reuse]);
    assert.equal(writes, 2, "concurrent reuse must not generate a second CA");
    assert.equal(second.key, first.key);
    assert.equal(second.cert, first.cert);
    const certificate = new X509Certificate(await fs.readFile(second.certPath));
    assert.ok(certificate.ca);
    assert.ok(certificate.checkPrivateKey(createPrivateKey(await fs.readFile(second.keyPath))));
    if (process.platform !== "win32")
      assert.equal((await fs.stat(second.keyPath)).mode & 0o777, 0o600);
  } finally {
    blocked.release();
    await Promise.allSettled(accepted);
    context.mock.restoreAll();
    syncBuiltinESMExports();
    await fs.rm(certDir, { recursive: true, force: true });
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});
