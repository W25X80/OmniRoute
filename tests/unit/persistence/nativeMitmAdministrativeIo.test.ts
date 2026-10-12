import "../../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import fs from "node:fs";
import asyncFs from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";
import { setImmediate as nextTurn } from "node:timers/promises";
import vm from "node:vm";
import ts from "typescript";
import { z } from "zod";
import { installTproxyCa, uninstallTproxyCa } from "../../../src/mitm/tproxy/caTrust.ts";
import * as nativeHostIo from "../../../src/mitm/nativeHostIo.ts";

function barrier() {
  let release!: () => void;
  return {
    promise: new Promise<void>((resolve) => {
      release = resolve;
    }),
    release: () => release(),
  };
}

test("native TPROXY trust awaits real async detection, staging and cleanup", async (context) => {
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const tmpDir = await asyncFs.mkdtemp(path.join(process.env.DATA_DIR!, "tproxy-ca-"));
  const probeEntered = barrier();
  const probeRelease = barrier();
  const writeEntered = barrier();
  const writeRelease = barrier();
  const cleanupEntered = barrier();
  const cleanupRelease = barrier();
  context.mock.method(asyncFs, "access", async () => {
    probeEntered.release();
    await probeRelease.promise;
  });
  const realWrite = asyncFs.writeFile;
  context.mock.method(asyncFs, "writeFile", async (...args: Parameters<typeof realWrite>) => {
    writeEntered.release();
    await writeRelease.promise;
    await realWrite(...args);
  });
  const realUnlink = asyncFs.unlink;
  context.mock.method(asyncFs, "unlink", async (...args: Parameters<typeof realUnlink>) => {
    cleanupEntered.release();
    await cleanupRelease.promise;
    await realUnlink(...args);
  });
  for (const name of ["existsSync", "writeFileSync", "unlinkSync"] as const)
    context.mock.method(fs, name, () => {
      throw new Error(`Native trust called ${name}`);
    });
  syncBuiltinESMExports();
  const calls: string[][] = [];
  const deps = {
    platform: () => "linux",
    tmpDir: () => tmpDir,
    run: async (_command: string, args: string[]) => {
      calls.push(args);
      if (args.includes("cp")) assert.equal(await asyncFs.readFile(args[2], "utf8"), "REAL-CA-PEM");
    },
  };
  try {
    let settled = false;
    const work = installTproxyCa("REAL-CA-PEM", "", deps).then(() => {
      settled = true;
    });
    await probeEntered.promise;
    await nextTurn();
    assert.equal(calls.length, 0);
    assert.equal(settled, false);
    probeRelease.release();
    await writeEntered.promise;
    await nextTurn();
    assert.equal(calls.length, 0, "privileged copy cannot overtake staging");
    writeRelease.release();
    await cleanupEntered.promise;
    await nextTurn();
    assert.equal(calls.length, 3);
    assert.equal(settled, false, "success must await accepted cleanup");
    cleanupRelease.release();
    await work;
    assert.deepEqual(await asyncFs.readdir(tmpDir), []);
    await uninstallTproxyCa("", deps);
    assert.equal(calls.length, 5);
  } finally {
    probeRelease.release();
    writeRelease.release();
    cleanupRelease.release();
    context.mock.restoreAll();
    syncBuiltinESMExports();
    await asyncFs.rm(tmpDir, { recursive: true, force: true });
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});

test("settings regeneration delegates forced replacement to the certificate owner", async () => {
  const source = await asyncFs.readFile(
    new URL("../../../src/app/api/settings/mitm/route.ts", import.meta.url),
    "utf8"
  );
  const parsed = ts.createSourceFile("route.ts", source, ts.ScriptTarget.ES2022, true);
  const post = parsed.statements.find(
    (statement): statement is ts.FunctionDeclaration =>
      ts.isFunctionDeclaration(statement) && statement.name?.text === "POST"
  );
  assert.ok(post);
  let options: { force?: boolean } | undefined;
  let deletes = 0;
  const exports: { POST?: (request: Request) => Promise<Response> } = {};
  vm.runInNewContext(
    ts.transpileModule(post.getText(parsed), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText,
    {
      exports,
      NextResponse: Response,
      requireManagementAuth: async () => null,
      regenerateSchema: z.object({ action: z.literal("regenerate-cert").optional() }),
      buildMitmResponse: async () => ({ running: false }),
      getCertPath: () => "server.crt",
      getKeyPath: () => "server.key",
      mitmFileExists: async () => true,
      removeMitmFile: async () => {
        deletes++;
      },
      toPublicSafeTunnelError: () => ({ error: "fixture failure" }),
      require: (name: string) => {
        if (name === "@/mitm/manager.runtime")
          return { getMitmStatus: async () => ({ running: false }) };
        if (name === "@/mitm/cert/generate")
          return {
            generateCert: async (value?: { force?: boolean }) => {
              options = value;
            },
          };
        throw new Error(`Unexpected regeneration dependency: ${name}`);
      },
    }
  );
  assert.ok(exports.POST);
  const response = await exports.POST(
    new Request("http://localhost/api/settings/mitm", {
      method: "POST",
      body: JSON.stringify({ action: "regenerate-cert" }),
    })
  );
  assert.equal(response.status, 200);
  assert.equal(options?.force, true);
  assert.equal(deletes, 0, "route must not delete a pair outside its owner lock");
});

test("native repair certificate presence awaits the actual async filesystem probe", async (context) => {
  const source = await asyncFs.readFile(
    new URL("../../../src/mitm/repair.ts", import.meta.url),
    "utf8"
  );
  const exports: { performRepairSteps?: (password: string) => Promise<string[]> } = {};
  let uninstalls = 0;
  const failures: unknown[] = [];
  // Execute the production repair owner with safe DB/DNS/trust ports. Its FS
  // adapter is real; no host trust store or hosts file is changed by this fixture.
  const ports: Record<string, unknown> = {
    path,
    fs,
    "./nativeHostIo.ts": nativeHostIo,
    "./dataDir.ts": { resolveMitmDataDir: () => process.env.DATA_DIR! },
    "./dns/dnsConfig.ts": { removeDNSEntry: async () => {}, removeDNSEntries: async () => {} },
    "./cert/install.ts": {
      uninstallCert: async () => {
        uninstalls++;
      },
    },
    "./targets/index.ts": { ALL_TARGETS: [] },
    "@/lib/db/inspectorCustomHosts.ts": { listCustomHostsAsync: async () => [] },
    "@/lib/db/providers.ts": { getGheCopilotHostsAsync: async () => [] },
    "@/shared/utils/logger.ts": {
      createLogger: () => ({ error: (value: { err: Error }) => failures.push(value.err.message) }),
    },
    "@/lib/inspector/captureState": { getSystemProxyState: () => ({ applied: false }) },
  };
  vm.runInNewContext(
    ts.transpileModule(source, {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText,
    {
      exports,
      require: (name: string) => {
        if (!(name in ports)) throw new Error(`Unexpected repair dependency: ${name}`);
        return ports[name];
      },
    }
  );
  assert.ok(exports.performRepairSteps);
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  const entered = barrier();
  const released = barrier();
  let probedPath = "";
  context.mock.method(asyncFs, "access", async (file) => {
    probedPath = String(file);
    entered.release();
    await released.promise;
  });
  context.mock.method(fs, "existsSync", () => {
    throw new Error("Sync repair probe called");
  });
  syncBuiltinESMExports();
  try {
    let settled = false;
    const repair = exports.performRepairSteps("").then((value) => {
      settled = true;
      return value;
    });
    assert.equal(
      await Promise.race([entered.promise.then(() => "probe"), repair.then(() => "done")]),
      "probe",
      JSON.stringify(failures)
    );
    await nextTurn();
    assert.equal(settled, false);
    assert.equal(uninstalls, 0);
    assert.equal(probedPath, path.join(process.env.DATA_DIR!, "mitm/server.crt"));
    released.release();
    assert.deepEqual(Array.from(await repair), ["dns", "cert"]);
    assert.equal(uninstalls, 1);
  } finally {
    released.release();
    context.mock.restoreAll();
    syncBuiltinESMExports();
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});

test("accepted native start completes compensation after its request scope is aborted", async () => {
  const source = await asyncFs.readFile(
    new URL("../../../src/mitm/manager.ts", import.meta.url),
    "utf8"
  );
  const parsed = ts.createSourceFile("manager.ts", source, ts.ScriptTarget.ES2022, true);
  const owner = parsed.statements.find(
    (statement): statement is ts.FunctionDeclaration =>
      ts.isFunctionDeclaration(statement) && statement.name?.text === "startMitm"
  );
  assert.ok(owner);
  const controller = new AbortController();
  const prior = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  let managedDnsEntries = 0;
  let lockHeld = false;
  const exports: {
    startMitm?: (
      key: string,
      password: string,
      options?: { signal?: AbortSignal }
    ) => Promise<unknown>;
  } = {};
  // Execute the real start ownership wrapper. The internal fixture represents
  // an accepted DNS effect, a request abort and a failed child requiring teardown.
  vm.runInNewContext(
    ts.transpileModule(owner.getText(parsed), {
      compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText,
    {
      exports,
      nativeMitmEnabled: nativeHostIo.nativeMitmEnabled,
      nativePidCleanup: Promise.resolve(),
      serverProcess: null,
      tryAcquireMitmStartLock: () => {
        lockHeld = true;
        return true;
      },
      releaseMitmStartLock: () => {
        lockHeld = false;
      },
      withNativeMitmCancellation: nativeHostIo.withNativeMitmCancellation,
      startMitmInternal: async () => {
        managedDnsEntries = 1;
        controller.abort();
        await nextTurn();
        nativeHostIo.assertNativeMitmNotAborted();
        assert.ok(lockHeld, "compensation still belongs to the accepted start");
        managedDnsEntries = 0;
        throw new Error("fixture child startup failed");
      },
    }
  );
  assert.ok(exports.startMitm);
  try {
    await assert.rejects(
      nativeHostIo.withNativeMitmCancellation(controller.signal, () =>
        exports.startMitm!("key", "password", { signal: controller.signal })
      ),
      /fixture child startup failed/
    );
    assert.equal(managedDnsEntries, 0);
    assert.equal(lockHeld, false);
  } finally {
    if (prior === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = prior;
  }
});
