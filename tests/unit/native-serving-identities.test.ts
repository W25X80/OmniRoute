import assert from "node:assert/strict";
import childProcess from "node:child_process";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import fs from "node:fs";
import fsPromises from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import * as tokens from "../../src/lib/machineToken.ts";
import * as rawIdentity from "../../src/shared/utils/machineId.ts";
import { hasValidLoopbackCliTokenAsync } from "../../src/server/authz/peerContext.ts";
import { publicPolicy } from "../../src/server/authz/policies/public.ts";
import { managementPolicy } from "../../src/server/authz/policies/management.ts";
import { isCliTokenAuthValid } from "../../src/lib/middleware/cliTokenAuth.ts";
import { CLI_TOKEN_HEADER, VIA_PROXY_HEADER } from "../../src/server/authz/headers.ts";
import type { PolicyContext } from "../../src/server/authz/context.ts";
import { readRunningBuildShaAsync } from "../../src/lib/monitoring/buildSha.ts";
import { resolveCodexCatalogAppVersionAsync } from "../../src/shared/services/codexCatalogRevalidation.ts";

const ENV_KEYS = [
  "OMNIROUTE_DATABASE_BACKEND",
  "DATA_DIR",
  "OMNIROUTE_CLI_SALT",
  "OMNIROUTE_DISABLE_CLI_TOKEN",
  "OMNIROUTE_PEER_STAMP_TOKEN",
  "OMNIROUTE_BUILD_SHA",
  "DISABLE_IOREG_STRATEGY",
  "SystemRoot",
  "OMNIROUTE_INTERNAL_SERVICE_TOKEN",
  "OMNIROUTE_INTERNAL_SERVICE_TOKEN_FILE",
];
const savedEnvironment = Object.fromEntries(ENV_KEYS.map((key) => [key, process.env[key]]));
const platformDescriptor = Object.getOwnPropertyDescriptor(process, "platform");
const platform = process.platform;
const fsRead = fsPromises.readFile.bind(fsPromises);
let directory = "";

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

function forbidSynchronousWork() {
  let calls = 0;
  for (const name of [
    "existsSync",
    "statSync",
    "readFileSync",
    "mkdirSync",
    "writeFileSync",
  ] as const) {
    const original = fs[name];
    test.mock.method(fs, name, (...args: unknown[]) => {
      calls++;
      return Reflect.apply(original, fs, args);
    });
  }
  for (const name of ["execSync", "execFileSync"] as const) {
    const original = childProcess[name];
    test.mock.method(childProcess, name, (...args: unknown[]) => {
      calls++;
      return Reflect.apply(original, childProcess, args);
    });
  }
  syncBuiltinESMExports();
  return () => calls;
}

function installIdentityProbe(gate?: Promise<void>) {
  let calls = 0;
  let mode: "ok" | "empty" | "error" = "ok";
  const rawId = "native-test-machine-id";
  const output =
    platform === "darwin"
      ? `IOPlatformUUID = "${rawId}"\n`
      : platform === "win32"
        ? `MachineGuid REG_SZ ${rawId}`
        : rawId;
  test.mock.method(childProcess, "exec", (...args: unknown[]) => {
    calls++;
    const callback = args[2] as (error: Error | null, stdout: string, stderr: string) => void;
    void (gate ?? Promise.resolve()).then(() => {
      callback(
        mode === "error" ? new Error("untrusted identity diagnostic") : null,
        mode === "empty" ? "" : output,
        ""
      );
    });
    return new EventEmitter();
  });
  syncBuiltinESMExports();
  return {
    rawId,
    calls: () => calls,
    mode: (value: typeof mode) => {
      mode = value;
    },
  };
}

function context(
  token: string,
  ip = "127.0.0.1",
  extraHeaders: Record<string, string> = {}
): PolicyContext {
  return {
    request: {
      method: "GET",
      headers: new Headers({ [CLI_TOKEN_HEADER]: token, ...extraHeaders }),
      ip,
    },
    classification: {
      routeClass: "PUBLIC",
      reason: "public_prefix",
      normalizedPath: "/api/monitoring/health",
    },
    requestId: "native-identity-test",
  };
}

test.beforeEach(async () => {
  directory = await fsPromises.mkdtemp(path.join(os.tmpdir(), "native-identities-"));
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.DATA_DIR = directory;
  process.env.OMNIROUTE_CLI_SALT = "native-test-salt";
  delete process.env.OMNIROUTE_DISABLE_CLI_TOKEN;
  delete process.env.OMNIROUTE_BUILD_SHA;
  delete process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN;
  delete process.env.OMNIROUTE_INTERNAL_SERVICE_TOKEN_FILE;
  for (const name of [
    "omniroute.nativeCliTokenState",
    "omniroute.nativeRawMachineId",
    "omniroute.nativeRunningBuildSha",
  ])
    Reflect.deleteProperty(globalThis, Symbol.for(name));
});

test.afterEach(async () => {
  test.mock.restoreAll();
  syncBuiltinESMExports();
  if (platformDescriptor) Object.defineProperty(process, "platform", platformDescriptor);
  for (const [key, value] of Object.entries(savedEnvironment)) {
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  await fsPromises.rm(directory, { recursive: true, force: true });
});

test("native public policy accepts both token formats and preserves locality/disabled gates", async () => {
  const probe = installIdentityProbe();
  const syncCalls = forbidSynchronousWork();
  const current = tokens.deriveMachineToken(probe.rawId, "native-test-salt");
  const legacy = tokens.deriveLegacyCliToken(
    createHash("sha256").update(probe.rawId).digest("hex"),
    "native-test-salt"
  );
  for (const token of [current, legacy]) {
    const result = await publicPolicy.evaluate(context(token));
    assert.equal(result.allow, true);
    if (result.allow) assert.equal(result.subject.label, "local-cli-token");
    const managementContext = context(token);
    managementContext.classification.routeClass = "MANAGEMENT";
    managementContext.classification.normalizedPath = "/api/settings";
    const management = await managementPolicy.evaluate(managementContext);
    assert.equal(management.allow, true);
    if (management.allow) assert.equal(management.subject.label, "local-cli-token");
  }
  const beforeRejected = probe.calls();
  assert.equal(await hasValidLoopbackCliTokenAsync(context(current, "203.0.113.7")), false);
  assert.equal(await hasValidLoopbackCliTokenAsync(context("")), false);
  process.env.OMNIROUTE_PEER_STAMP_TOKEN = "native-peer-stamp";
  assert.equal(
    await hasValidLoopbackCliTokenAsync(
      context(current, "127.0.0.1", { [VIA_PROXY_HEADER]: "native-peer-stamp|1" })
    ),
    false
  );
  process.env.OMNIROUTE_DISABLE_CLI_TOKEN = "true";
  assert.equal(await hasValidLoopbackCliTokenAsync(context(current)), false);
  assert.equal(
    probe.calls(),
    beforeRejected,
    "locality/header/disable rejection precedes identity lookup"
  );
  assert.equal(syncCalls(), 0);
});

test("native identity failure cannot authenticate with a cached successful token", async () => {
  const probe = installIdentityProbe();
  const token = await tokens.getMachineTokenAsync();
  assert.ok(token);
  assert.equal(await hasValidLoopbackCliTokenAsync(context(token)), true);
  const before = probe.calls();
  probe.mode("error");
  assert.equal(await tokens.getMachineTokenAsync(), "");
  assert.equal(await hasValidLoopbackCliTokenAsync(context(token)), false);
  assert.equal(
    probe.calls(),
    before + 3,
    "warm cached token still performs a fresh probe plus both auth probes"
  );
  const result = await publicPolicy.evaluate(context(token));
  assert.equal(result.allow, true);
  if (result.allow) assert.equal(result.subject.kind, "anonymous");
  assert.equal(JSON.stringify(result).includes("untrusted identity diagnostic"), false);
});

test("delayed native CLI identity keeps the event loop live and secondary CLI auth awaits it", async () => {
  const gate = deferred();
  const probe = installIdentityProbe(gate.promise);
  const syncCalls = forbidSynchronousWork();
  const token = tokens.deriveMachineToken(probe.rawId, "native-test-salt");
  const request = new Request("http://localhost/api/settings", {
    headers: { [CLI_TOKEN_HEADER]: token },
  });
  Object.assign(request, { socket: { remoteAddress: "127.0.0.1" } });
  let complete = false;
  const check = isCliTokenAuthValid(request).then((value) => {
    complete = true;
    return value;
  });
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(complete, false);
  gate.release();
  assert.equal(await check, true);
  assert.equal(probe.calls(), 2);
  assert.equal(syncCalls(), 0);
});

test("native salt first install uses private exclusive write and concurrent modules converge", async () => {
  delete process.env.OMNIROUTE_CLI_SALT;
  installIdentityProbe();
  const duplicate = await import(`../../src/lib/machineToken.ts?nativeSaltModule=${Date.now()}`);
  assert.notEqual(duplicate.getMachineTokenAsync, tokens.getMachineTokenAsync);
  const syncCalls = forbidSynchronousWork();
  const [first, second] = await Promise.all([
    tokens.getMachineTokenAsync(),
    tokens.getMachineTokenAsync(),
  ]);
  assert.ok(first);
  assert.equal(first, second);
  const file = path.join(directory, "cli-token-salt.json");
  const parsed: unknown = JSON.parse(await fsPromises.readFile(file, "utf8"));
  const salt = (parsed as { salt?: string }).salt;
  assert.match(salt as string, /^[0-9a-f]{64}$/);
  if (platform !== "win32") assert.equal((await fsPromises.stat(file)).mode & 0o777, 0o600);
  assert.equal(await duplicate.getMachineTokenAsync(), first);
  process.env.OMNIROUTE_CLI_SALT = "rotated-native-salt";
  assert.notEqual(await tokens.getMachineTokenAsync(), first);
  assert.equal(syncCalls(), 0);
});

test("native raw identity preserves Linux candidate order, salt hashing, cache and cross-bundle state", async () => {
  Object.defineProperty(process, "platform", { configurable: true, value: "linux" });
  const duplicate = await import(
    `../../src/shared/utils/machineId.ts?nativeRawModule=${Date.now()}`
  );
  assert.notEqual(duplicate.getRawMachineId, rawIdentity.getRawMachineId);
  const reads: string[] = [];
  test.mock.method(fsPromises, "readFile", async (file: unknown) => {
    const filename = String(file);
    reads.push(filename);
    assert.equal(filename, "/etc/machine-id");
    return "  LINUX-FIRST-IDENTITY\n";
  });
  const syncCalls = forbidSynchronousWork();
  assert.equal(await rawIdentity.getRawMachineId(), "linux-first-identity");
  assert.equal(
    await rawIdentity.getConsistentMachineId("test-salt"),
    createHash("sha256").update("linux-first-identitytest-salt").digest("hex").slice(0, 16)
  );
  assert.equal(await duplicate.getRawMachineId(), "linux-first-identity");
  assert.deepEqual(reads, ["/etc/machine-id"]);
  assert.equal(syncCalls(), 0);
});

test("native raw identity retains asynchronous Windows command arguments and source timeout", async () => {
  Object.defineProperty(process, "platform", { configurable: true, value: "win32" });
  process.env.SystemRoot = "C:\\NativeTestWindows";
  test.mock.method(fsPromises, "access", async (file: unknown) => {
    assert.equal(String(file), "C:\\NativeTestWindows\\System32\\REG.exe");
  });
  test.mock.method(childProcess, "execFile", (...args: unknown[]) => {
    assert.equal(args[0], "C:\\NativeTestWindows\\System32\\REG.exe");
    assert.deepEqual(args[1], [
      "QUERY",
      "HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography",
      "/v",
      "MachineGuid",
    ]);
    assert.deepEqual(args[2], { encoding: "utf8", timeout: 5000 });
    (args[3] as (error: null, stdout: string) => void)(
      null,
      "MachineGuid REG_SZ ABCDEF12-3456-7890"
    );
    return new EventEmitter();
  });
  const syncCalls = forbidSynchronousWork();
  assert.equal(await rawIdentity.getRawMachineId(), "abcdef12-3456-7890");
  assert.equal(syncCalls(), 0);
});

test("native health build SHA uses asynchronous file fallback and caches null", async () => {
  let reads = 0;
  test.mock.method(fsPromises, "readFile", async (...args: unknown[]) => {
    reads++;
    return Reflect.apply(fsRead, fsPromises, args);
  });
  const syncCalls = forbidSynchronousWork();
  assert.equal(await readRunningBuildShaAsync(directory), null);
  const firstReads = reads;
  await fsPromises.writeFile(path.join(directory, "BUILD_SHA"), "later-build\n");
  assert.equal(
    await readRunningBuildShaAsync(directory),
    null,
    "unknown identity is cached exactly like source"
  );
  assert.equal(reads, firstReads);
  assert.equal(syncCalls(), 0);
});

test("native health build SHA keeps sentinel priority and explicit env avoids file reads", async () => {
  await fsPromises.mkdir(path.join(directory, "dist"), { recursive: true });
  await fsPromises.writeFile(path.join(directory, "dist", "BUILD_SHA"), " dist-winner \n");
  await fsPromises.writeFile(path.join(directory, "BUILD_SHA"), "lower-priority\n");
  assert.equal(await readRunningBuildShaAsync(directory), "dist-winner");
  Reflect.deleteProperty(globalThis, Symbol.for("omniroute.nativeRunningBuildSha"));
  process.env.OMNIROUTE_BUILD_SHA = " env-winner ";
  test.mock.method(fsPromises, "readFile", async () => {
    assert.fail("explicit env must avoid filesystem lookup");
  });
  assert.equal(await readRunningBuildShaAsync(directory), "env-winner");
});

test("native Codex app identity is fresh and keeps build-file/package-env/override priority", async () => {
  await fsPromises.mkdir(path.join(directory, ".next"), { recursive: true });
  await fsPromises.writeFile(path.join(directory, ".next", "BUILD_ID"), "next-winner\n");
  await fsPromises.writeFile(
    path.join(directory, "package.json"),
    JSON.stringify({ version: "file-version" })
  );
  const options = { runtimeRoot: directory };
  const syncCalls = forbidSynchronousWork();
  assert.equal(
    await resolveCodexCatalogAppVersionAsync({ npm_package_version: "env-package" }, options),
    "next:next-winner"
  );
  await fsPromises.writeFile(path.join(directory, "BUILD_SHA"), "first-build\n");
  assert.equal(await resolveCodexCatalogAppVersionAsync({}, options), "build:first-build");
  await fsPromises.writeFile(path.join(directory, "BUILD_SHA"), "second-build\n");
  assert.equal(await resolveCodexCatalogAppVersionAsync({}, options), "build:second-build");
  await fsPromises.rm(path.join(directory, "BUILD_SHA"));
  await fsPromises.rm(path.join(directory, ".next", "BUILD_ID"));
  assert.equal(
    await resolveCodexCatalogAppVersionAsync({ OMNIROUTE_VERSION: "env-package" }, options),
    "pkg:env-package"
  );
  assert.equal(await resolveCodexCatalogAppVersionAsync({}, options), "pkg:file-version");
  assert.equal(
    await resolveCodexCatalogAppVersionAsync({}, { ...options, packageVersion: undefined }),
    null
  );
  assert.equal(syncCalls(), 0);
});

test("delayed native build fallback advances a ticker and captures resolver inputs", async () => {
  const gate = deferred();
  const reading = deferred();
  test.mock.method(fsPromises, "readFile", async () => {
    reading.release();
    await gate.promise;
    return "captured-build";
  });
  const environment = {
    BUILD_SHA: undefined as string | undefined,
    npm_package_version: "original-package",
  };
  let complete = false;
  const pending = resolveCodexCatalogAppVersionAsync(environment, { runtimeRoot: directory }).then(
    (value) => {
      complete = true;
      return value;
    }
  );
  await reading.promise;
  environment.BUILD_SHA = "late-env-change";
  await new Promise<void>((resolve) => setImmediate(resolve));
  assert.equal(complete, false);
  gate.release();
  assert.equal(await pending, "build:captured-build");
});

test("native dependency normalization/hash matches Linux, Windows and Darwin fixture output", async () => {
  const fixtures = [
    {
      platform: "linux",
      stdout: " AA-BB \n\r",
      raw: "aa-bb",
      command:
        "( cat /var/lib/dbus/machine-id /etc/machine-id 2> /dev/null || hostname ) | head -n 1 || :",
    },
    {
      platform: "win32",
      stdout: "MachineGuid REG_SZ AA-BB\r\n",
      raw: "aa-bb",
      command:
        "%windir%\\System32\\REG.exe QUERY HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Cryptography /v MachineGuid",
    },
    {
      platform: "darwin",
      stdout: 'IOPlatformUUID = "AA-BB"\n',
      raw: "aa-bb",
      command: "ioreg -rd1 -c IOPlatformExpertDevice",
    },
  ];
  let fixture = fixtures[0];
  test.mock.method(childProcess, "exec", (...args: unknown[]) => {
    assert.equal(args[0], fixture.command);
    assert.equal(Reflect.has(args[1] as object, "timeout"), false);
    assert.equal(Reflect.has(args[1] as object, "maxBuffer"), false);
    assert.equal(typeof (args[1] as { env?: object }).env, "object");
    (args[2] as (error: null, stdout: string, stderr: string) => void)(null, fixture.stdout, "");
    return new EventEmitter();
  });
  syncBuiltinESMExports();
  const syncCalls = forbidSynchronousWork();
  for (const next of fixtures) {
    fixture = next;
    Object.defineProperty(process, "platform", { configurable: true, value: fixture.platform });
    const salt = `native-${fixture.platform}-fixture`;
    assert.equal(
      await tokens.getMachineTokenAsync(salt),
      tokens.deriveMachineToken(fixture.raw, salt)
    );
    assert.equal(
      await tokens.getLegacyCliTokenAsync(salt),
      tokens.deriveLegacyCliToken(createHash("sha256").update(fixture.raw).digest("hex"), salt)
    );
  }
  assert.equal(syncCalls(), 0);
});

test("malformed successful Darwin/Windows command output is rejected without escaping its callback", async () => {
  for (const platform of ["darwin", "win32"]) {
    Object.defineProperty(process, "platform", { configurable: true, value: platform });
    test.mock.method(childProcess, "exec", (...args: unknown[]) => {
      queueMicrotask(() =>
        (args[2] as (error: null, stdout: string, stderr: string) => void)(
          null,
          "malformed-success-output",
          ""
        )
      );
      return new EventEmitter();
    });
    syncBuiltinESMExports();
    assert.equal(await tokens.getMachineTokenAsync(`malformed-${platform}`), "");
    assert.equal(await tokens.getLegacyCliTokenAsync(`malformed-${platform}`), "");
  }
});
