import "../_setup/isolateDataDir.ts";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  deferred,
  drainBackground,
  fileIOState,
} from "../helpers/persistence/nativeProviderFileIOState.ts";

const root = mkdtempSync(join(tmpdir(), "omniroute-native-provider-io-"));
const names = [
  "OMNIROUTE_DATABASE_BACKEND",
  "DATA_DIR",
  "XDG_CONFIG_HOME",
  "GROK_AUTH_PATH",
  "KIMI_CODING_DEVICE_ID",
  "NODE_ENV",
  "NODE_TEST_CONTEXT",
  "VITEST",
  "ADOBE_FIREFLY_SESSION_DISK",
  "ADOBE_FIREFLY_BROWSER_REFRESH",
  "OMNIROUTE_LOGIN_BROWSER_PATH",
  "OMNIROUTE_CODEX_APPSERVER_WS",
  "OMNIROUTE_CODEX_APPSERVER_WS_TOKEN",
  "OMNIROUTE_CODEX_APPSERVER_WS_TOKEN_FILE",
  "OMNIROUTE_CODEX_APPSERVER_CWD",
];
const previous = new Map(names.map((name) => [name, process.env[name]]));
const originalFetch = globalThis.fetch;
process.env.DATA_DIR = root;
process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
fileIOState.home = join(root, "home");
const owners = [
  "open-sse/utils/secureFileWrite.ts",
  "open-sse/services/adobeFireflySession.ts",
  "open-sse/services/adobeFireflyBrowserLogin.ts",
  "open-sse/services/adobeFireflyClient.ts",
  "open-sse/services/grokQuotaFetcher.ts",
  "open-sse/executors/codex/appServerConfig.ts",
  "open-sse/executors/codex.ts",
  "open-sse/executors/codex-app-server.ts",
  "src/app/api/providers/[id]/test/codexAppServerHealth.ts",
  "src/lib/oauth/providers/kimi-coding.ts",
  "open-sse/utils/kimiDevice.ts",
  "open-sse/services/tokenRefresh/providers/kimiCoding.ts",
  "src/lib/oauth/providers.ts",
  "src/lib/dataPaths.ts",
];
const fixture = (name: string) =>
  new URL(`../helpers/persistence/nativeProviderFileIO${name}.ts`, import.meta.url).href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    const parent = context.parentURL ? new URL(context.parentURL).pathname : "";
    if (owners.some((owner) => parent.endsWith(`/${owner}`))) {
      if (specifier === "fs" || specifier === "node:fs")
        return { url: fixture("Fs"), shortCircuit: true };
      if (specifier === "node:fs/promises") return { url: fixture("Promises"), shortCircuit: true };
      if (specifier === "os" || specifier === "node:os")
        return { url: fixture("Os"), shortCircuit: true };
      if (parent.endsWith("/open-sse/services/adobeFireflyBrowserLogin.ts")) {
        if (specifier === "node:child_process" || specifier === "node:net")
          return { url: fixture("BrowserTransport"), shortCircuit: true };
        if (specifier === "node:http") return { url: fixture("BrowserHttp"), shortCircuit: true };
      }
      if (parent.endsWith("/open-sse/utils/kimiDevice.ts") && specifier === "node:child_process")
        return { url: fixture("Process"), shortCircuit: true };
      if (specifier.includes("db/backendSelection"))
        return { url: fixture("Control"), shortCircuit: true };
      if (
        parent.endsWith("/open-sse/executors/codex.ts") &&
        specifier.includes("shared/utils/featureFlags")
      )
        return { url: fixture("Control"), shortCircuit: true };
      if (
        parent.endsWith("/open-sse/services/adobeFireflySession.ts") &&
        (specifier.includes("db/controlPlaneContext") ||
          specifier.includes("db/persistenceRuntime") ||
          specifier === "@/lib/db/providers" ||
          specifier === "./adobeFireflyBrowserLogin.ts")
      )
        return { url: fixture("Control"), shortCircuit: true };
    }
    return nextResolve(specifier, context);
  },
});
const secure = await import("../../open-sse/utils/secureFileWrite.ts");
const config = await import("../../open-sse/executors/codex/appServerConfig.ts");
const codex = await import("../../open-sse/executors/codex.ts");
const appServer = await import("../../open-sse/executors/codex-app-server.ts");
const health = await import("../../src/app/api/providers/[id]/test/codexAppServerHealth.ts");
const grok = await import("../../open-sse/services/grokQuotaFetcher.ts");
const kimi = await import("../../src/lib/oauth/providers/kimi-coding.ts");
const oauth = await import("../../src/lib/oauth/providers.ts");
const paths = await import("../../src/lib/dataPaths.ts");
const adobe = await import("../../open-sse/services/adobeFireflySession.ts");
const browser = await import("../../open-sse/services/adobeFireflyBrowserLogin.ts");

function restoreEnv() {
  for (const [name, value] of previous) {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
  }
}
function productionAdobeDisk() {
  delete process.env.NODE_TEST_CONTEXT;
  delete process.env.VITEST;
  process.env.NODE_ENV = "production";
  delete process.env.ADOBE_FIREFLY_SESSION_DISK;
  process.env.ADOBE_FIREFLY_BROWSER_REFRESH = "0";
}
function userToken() {
  return `eyJhbGciOiJSUzI1NiJ9.${Buffer.from(JSON.stringify({ user_id: "0EB@AdobeID", type: "access_token", client_id: "clio-playground-web", created_at: Date.now(), expires_in: 24 * 3600_000 })).toString("base64url")}.${"y".repeat(40)}`;
}
function session(fp: string, token = userToken()) {
  return {
    fingerprint: fp,
    accessToken: token,
    cookie: "",
    arpSessionId: "original-arp",
    tokenExpiresAt: Date.now() + 24 * 3600_000,
    updatedAt: Date.now(),
    browserSessionKey: "owned-connection",
    source: "paste" as const,
  };
}
function bodyInput(providerSpecificData: Record<string, unknown>) {
  return {
    model: "gpt-5.5",
    body: { model: "gpt-5.5", input: "hello", stream: false },
    stream: false,
    credentials: { providerSpecificData },
  };
}

test.beforeEach(() => {
  restoreEnv();
  process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
  process.env.DATA_DIR = join(root, randomUUID());
  mkdirSync(process.env.DATA_DIR, { recursive: true });
  fileIOState.reset();
  fileIOState.native = true;
  fileIOState.forbidSync = true;
  globalThis.fetch = async () => {
    throw new Error("Unexpected external fetch in native file oracle");
  };
  adobe.__resetAdobeFireflySessionCacheForTests();
});
test.afterEach(async () => {
  fileIOState.hold?.resolve();
  await drainBackground();
  globalThis.fetch = originalFetch;
  restoreEnv();
});
test.after(() => {
  hooks.deregister();
  rmSync(root, { recursive: true, force: true });
});

test("async secure persistence waits for file completion and preserves directory/file modes", async () => {
  const dir = join(process.env.DATA_DIR!, "secure");
  mkdirSync(dir, { mode: 0o777 });
  chmodSync(dir, 0o777);
  await secure.ensureSecureDirAsync(dir);
  assert.equal(statSync(dir).mode & 0o777, 0o700);
  fileIOState.holdOperation = "writeFile";
  fileIOState.hold = deferred<void>();
  fileIOState.entered = deferred<void>();
  let ended = false;
  const work = secure.writeSecureFileAsync(join(dir, "token.json"), "exact bytes").then(() => {
    ended = true;
  });
  await fileIOState.entered.promise;
  assert.equal(ended, false);
  fileIOState.hold.resolve();
  await work;
  assert.equal(readFileSync(join(dir, "token.json"), "utf8"), "exact bytes");
  assert.equal(statSync(join(dir, "token.json")).mode & 0o777, 0o600);
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("Codex token precedence, security binding and rotation agree between sync and native Async config", async () => {
  const psdFile = join(process.env.DATA_DIR!, "psd-token"),
    envFile = join(process.env.DATA_DIR!, "env-token");
  writeFileSync(psdFile, " psd-file \n");
  writeFileSync(envFile, " env-file \n");
  const cases = [
    {
      psd: { codexAppServerUrl: "http://invalid", codexAppServerTokenFile: psdFile },
      envToken: "",
      want: null,
    },
    {
      psd: {
        codexAppServerUrl: "ws://local",
        codexAppServerToken: "psd-inline",
        codexAppServerTokenFile: psdFile,
      },
      envToken: "env-inline",
      want: "psd-inline",
    },
    {
      psd: { codexAppServerUrl: "ws://local", codexAppServerTokenFile: psdFile },
      envToken: "env-inline",
      want: "env-inline",
    },
    {
      psd: { codexAppServerUrl: "ws://local", codexAppServerTokenFile: psdFile },
      envToken: "",
      want: "psd-file",
    },
    {
      psd: {
        codexAppServerUrl: "ws://local",
        codexAppServerTokenFile: join(process.env.DATA_DIR!, "missing"),
      },
      envToken: "",
      want: "env-file",
    },
    {
      psd: {
        codexAppServerUrl: "wss://remote.example.com",
        codexAppServerTokenFile: join(process.env.DATA_DIR!, "missing"),
      },
      envToken: "",
      want: null,
    },
  ];
  process.env.OMNIROUTE_CODEX_APPSERVER_WS_TOKEN_FILE = envFile;
  for (const c of cases) {
    process.env.OMNIROUTE_CODEX_APPSERVER_WS_TOKEN = c.envToken;
    fileIOState.native = false;
    fileIOState.forbidSync = false;
    const legacy = config.resolveAppServerConfig(c.psd);
    fileIOState.native = true;
    fileIOState.forbidSync = true;
    fileIOState.syncCalls.length = 0;
    const native = await config.resolveAppServerConfigAsync(c.psd);
    assert.deepEqual(native, legacy);
    assert.equal(native?.token ?? null, c.want);
    assert.deepEqual(fileIOState.syncCalls, []);
  }
  delete process.env.OMNIROUTE_CODEX_APPSERVER_WS_TOKEN;
  const psd = { codexAppServerUrl: "ws://local", codexAppServerTokenFile: psdFile };
  assert.equal((await config.resolveAppServerConfigAsync(psd))?.token, "psd-file");
  writeFileSync(psdFile, "rotated");
  assert.equal((await config.resolveAppServerConfigAsync(psd))?.token, "rotated");
  writeFileSync(psdFile, " \n");
  assert.equal((await config.resolveAppServerConfigAsync(psd))?.token, "env-file");
});

test("native Codex dispatch gate, app-server execute and health caller await token-file resolution", async () => {
  const tokenFile = join(process.env.DATA_DIR!, "capability");
  writeFileSync(tokenFile, "test-capability");
  const psd = {
    codexTransport: "app-server",
    codexAppServerUrl: "ws://local",
    codexAppServerTokenFile: tokenFile,
  };
  fileIOState.featureEnabled = false;
  assert.equal(await codex.isCodexAppServerRequiredAsync({ providerSpecificData: psd }), false);
  assert.equal(fileIOState.asyncCalls.length, 0);
  fileIOState.featureEnabled = true;
  assert.equal(
    await codex.isCodexAppServerRequiredAsync({
      providerSpecificData: { ...psd, codexTransport: "http" },
    }),
    false
  );
  assert.equal(fileIOState.asyncCalls.length, 0);
  const { withApiKeyCodexServiceMode } =
    await import("../../src/lib/providers/codexApiKeyServiceMode.ts");
  const input = bodyInput(psd);
  input.credentials = withApiKeyCodexServiceMode("codex", input.credentials, "default");
  const refused = await new codex.CodexExecutor().execute(input);
  assert.equal((refused instanceof Response ? refused : refused.response).status, 400);
  let wsCalls = 0;
  const executor = new appServer.CodexAppServerExecutor({
    websocketFn: async () => {
      wsCalls++;
      throw new Error("controlled offline transport");
    },
  });
  const result = await executor.execute(bodyInput(psd));
  assert.equal(wsCalls, 1);
  await (result instanceof Response ? result : result.response).text();
  globalThis.fetch = async (_url, init) => {
    assert.equal(new Headers(init?.headers).get("authorization"), "Bearer test-capability");
    return new Response("not ready", { status: 503 });
  };
  const tested = await health.testCodexAppServerConnection({
    provider: "codex-app-server",
    providerSpecificData: psd,
  });
  assert.equal(tested?.valid, false);
  assert.ok(
    fileIOState.asyncCalls.filter((c) => c.operation === "readFile" && c.path === tokenFile)
      .length >= 3
  );
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Grok auth refresh preserves file entries/newline, retries auth once and reuses quota TTL", async () => {
  const authPath = join(process.env.DATA_DIR!, "auth.json");
  process.env.GROK_AUTH_PATH = authPath;
  writeFileSync(
    authPath,
    JSON.stringify(
      {
        selected: {
          key: "old",
          refresh_token: "refresh",
          expires_at: new Date(Date.now() + 3600_000).toISOString(),
          oidc_issuer: "https://auth.x.ai",
          oidc_client_id: "client",
          extra: "keep",
        },
        other: { key: "other", expires_at: "2000-01-01", extra: 42 },
      },
      null,
      2
    ) + "\n"
  );
  let billing = 0,
    refreshes = 0;
  globalThis.fetch = async (url, init) => {
    const u = String(url);
    if (u.includes("openid-configuration"))
      return Response.json({ token_endpoint: "https://auth.x.ai/oauth2/token" });
    if (u.includes("oauth2/token")) {
      refreshes++;
      const concurrent = JSON.parse(readFileSync(authPath, "utf8"));
      concurrent.other.extra = 99;
      concurrent.selected.operator = "concurrent-edit";
      writeFileSync(authPath, JSON.stringify(concurrent));
      return Response.json({ access_token: "new", refresh_token: "rotated", expires_in: 3600 });
    }
    assert.equal(
      new Headers(init?.headers).get("authorization"),
      billing === 0 ? "Bearer old" : "Bearer new"
    );
    billing++;
    return billing === 1
      ? new Response("denied", { status: 401 })
      : Response.json({ config: { creditUsagePercent: 42 } });
  };
  const id = randomUUID();
  const quota = await grok.fetchGrokWebQuota(id);
  assert.equal(quota?.percentUsed, 0.42);
  assert.equal(refreshes, 1);
  assert.equal(billing, 2);
  const raw = readFileSync(authPath, "utf8"),
    saved = JSON.parse(raw);
  assert.ok(raw.endsWith("\n"));
  assert.equal(saved.selected.key, "new");
  assert.equal(saved.selected.refresh_token, "rotated");
  assert.equal(saved.selected.extra, "keep");
  assert.equal(saved.other.extra, 99);
  assert.equal(saved.selected.operator, "concurrent-edit");
  const calls = fileIOState.asyncCalls.length;
  assert.deepEqual(await grok.fetchGrokWebQuota(id), quota);
  assert.equal(fileIOState.asyncCalls.length, calls);
  assert.equal(billing, 2);
  grok.invalidateGrokWebQuotaCache(id);
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Grok missing/malformed auth fails open and write failure keeps refreshed in-memory auth", async () => {
  const path = join(process.env.DATA_DIR!, "auth.json");
  process.env.GROK_AUTH_PATH = path;
  assert.equal(await grok.fetchGrokWebQuota(randomUUID()), null);
  writeFileSync(path, "malformed");
  assert.equal(await grok.fetchGrokWebQuota(randomUUID()), null);
  writeFileSync(
    path,
    JSON.stringify({
      entry: { key: "old", refresh_token: "r", expires_at: "2000-01-01", oidc_client_id: "client" },
    })
  );
  fileIOState.failOperation = "writeFile";
  globalThis.fetch = async (url, init) => {
    if (String(url).includes("openid-configuration"))
      return Response.json({ token_endpoint: "https://auth.x.ai/oauth2/token" });
    if (String(url).includes("oauth2/token"))
      return Response.json({ access_token: "in-memory", expires_in: 3600 });
    assert.equal(new Headers(init?.headers).get("authorization"), "Bearer in-memory");
    return Response.json({ config: { creditUsagePercent: 10 } });
  };
  assert.equal((await grok.fetchGrokWebQuota(randomUUID()))?.percentUsed, 0.1);
  assert.equal(JSON.parse(readFileSync(path, "utf8")).entry.key, "old");
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native path resolver preserves legacy/XDG/cloud selection without mkdir or test redirection", async () => {
  delete process.env.DATA_DIR;
  fileIOState.home = join(root, randomUUID());
  const legacy = join(fileIOState.home, ".omniroute"),
    xdg = join(root, randomUUID());
  process.env.XDG_CONFIG_HOME = xdg;
  assert.equal(await paths.resolveDataDirAsync(), join(xdg, "omniroute"));
  mkdirSync(legacy, { recursive: true });
  assert.equal(await paths.resolveDataDirAsync(), legacy);
  rmSync(legacy, { recursive: true });
  const target = join(root, randomUUID());
  mkdirSync(target);
  symlinkSync(target, legacy);
  assert.equal(await paths.getDefaultDataDirAsync(), legacy);
  assert.equal(await paths.resolveDataDirAsync({ isCloud: true }), "/tmp");
  process.env.DATA_DIR = "  " + join(root, "configured") + "  ";
  assert.equal(await paths.resolveDataDirAsync(), join(root, "configured"));
  assert.equal(fileIOState.asyncCalls.filter((c) => c.operation === "mkdir").length, 0);
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Kimi concurrent first-touch grant and real generic poll preserve one persisted device identity", async () => {
  const identities: string[] = [];
  // The config URL is the exact registered source URL; classify it explicitly.
  globalThis.fetch = async (url, init) => {
    identities.push(new Headers(init?.headers).get("x-msh-device-id")!);
    return String(url) === kimi.kimiCoding.config.deviceCodeUrl
      ? Response.json({
          device_code: "device",
          user_code: "user",
          verification_uri_complete: "https://verify.test",
          expires_in: 600,
        })
      : Response.json({ access_token: "access", refresh_token: "refresh", expires_in: 3600 });
  };
  await Promise.all(
    Array.from({ length: 6 }, () => kimi.kimiCoding.requestDeviceCode(kimi.kimiCoding.config))
  );
  const result = await oauth.pollForToken("kimi-coding", "device", undefined, undefined);
  assert.equal(result.success, true);
  assert.ok(result.tokens && typeof result.tokens === "object");
  assert.equal(typeof result.tokens.then, "undefined");
  const device = readFileSync(
    join(process.env.DATA_DIR!, "oauth", "kimi-coding-device-id"),
    "utf8"
  );
  assert.ok(identities.every((id) => id === device));
  assert.equal(result.tokens.providerSpecificData.deviceId, device);
  assert.equal(
    statSync(join(process.env.DATA_DIR!, "oauth", "kimi-coding-device-id")).mode & 0o777,
    0o600
  );
  assert.equal(fileIOState.asyncCalls.filter((c) => c.operation === "writeFile").length, 1);
  assert.equal(fileIOState.commands.length, 0);
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Kimi env override performs no identity file I/O and existing compact ID normalizes", async () => {
  const captured: string[] = [];
  globalThis.fetch = async (_url, init) => {
    captured.push(new Headers(init?.headers).get("x-msh-device-id")!);
    return Response.json({
      device_code: "device",
      user_code: "user",
      verification_uri_complete: "https://verify.test",
    });
  };
  process.env.KIMI_CODING_DEVICE_ID = " operator-device ";
  await kimi.kimiCoding.requestDeviceCode(kimi.kimiCoding.config);
  assert.equal(captured[0], "operator-device");
  assert.equal(fileIOState.asyncCalls.length, 0);
  delete process.env.KIMI_CODING_DEVICE_ID;
  const dir = join(process.env.DATA_DIR!, "oauth");
  mkdirSync(dir);
  writeFileSync(join(dir, "kimi-coding-device-id"), "a".repeat(32));
  await kimi.kimiCoding.requestDeviceCode(kimi.kimiCoding.config);
  assert.equal(captured[1], "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
  assert.equal(fileIOState.asyncCalls.filter((c) => c.operation === "writeFile").length, 0);
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Kimi Darwin model lookup uses async command, coalesces, and falls back on timeout", async () => {
  fileIOState.osType = "Darwin";
  const url = new URL("../../open-sse/utils/kimiDevice.ts", import.meta.url).href;
  const success = (await import(
    url + "?model-success"
  )) as typeof import("../../open-sse/utils/kimiDevice.ts");
  assert.deepEqual(
    await Promise.all([success.getKimiDeviceModelAsync(), success.getKimiDeviceModelAsync()]),
    ["macOS 15.1 arm64", "macOS 15.1 arm64"]
  );
  assert.equal(fileIOState.commands.length, 1);
  assert.equal(fileIOState.commands[0].timeout, 1000);
  fileIOState.commandFailure = true;
  const failed = (await import(
    url + "?model-timeout"
  )) as typeof import("../../open-sse/utils/kimiDevice.ts");
  assert.equal(await failed.getKimiDeviceModelAsync(), "macOS 6.1-test arm64");
  assert.deepEqual(fileIOState.syncCalls, []);
  fileIOState.osType = "Linux";
});

test("native Adobe production disk save/hot ARP/fingerprint-only opt-out retain bytes and permissions", async () => {
  productionAdobeDisk();
  const token = userToken();
  const resolved = await adobe.ensureAdobeFireflySession({
    credentials: { apiKey: token },
    allowBrowserRefresh: false,
  });
  const path = join(
    process.env.DATA_DIR!,
    "adobe-firefly-sessions",
    resolved.fingerprint + ".json"
  );
  assert.equal(JSON.parse(readFileSync(path, "utf8")).accessToken, token);
  assert.equal(statSync(path).mode & 0o777, 0o600);
  assert.equal(statSync(join(process.env.DATA_DIR!, "adobe-firefly-sessions")).mode & 0o777, 0o700);
  await adobe.markAdobeFireflyArpSuccessAsync(resolved.fingerprint, "hot-arp");
  assert.equal(JSON.parse(readFileSync(path, "utf8")).arpSessionId, "hot-arp");
  adobe.__resetAdobeFireflySessionCacheForTests();
  process.env.ADOBE_FIREFLY_SESSION_DISK = "0";
  await adobe.markAdobeFireflyArpSuccessAsync(resolved.fingerprint, "uncached-arp");
  assert.equal(JSON.parse(readFileSync(path, "utf8")).arpSessionId, "uncached-arp");
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Adobe profile/executable facades use async filesystem and retain configured priority", async () => {
  const executable = join(process.env.DATA_DIR!, "fake-browser");
  writeFileSync(executable, "placeholder");
  process.env.OMNIROUTE_LOGIN_BROWSER_PATH = executable;
  assert.equal(await browser.resolveSystemBrowserExecutableAsync(), executable);
  const profile = await browser.resolveAdobeFireflyBrowserProfileDirAsync("account");
  assert.equal(
    profile,
    join(
      process.env.DATA_DIR!,
      "adobe-chrome-profiles",
      browser.adobeFireflyBrowserSessionKey("account")
    )
  );
  assert.ok(statSync(profile).isDirectory());
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Adobe optional write-back is admitted before caller return, uses captured identity and observes refusal", async () => {
  productionAdobeDisk();
  process.env.ADOBE_FIREFLY_BROWSER_REFRESH = "1";
  delete process.env.NODE_TEST_CONTEXT;
  const original = session("native-owner");
  fileIOState.browserResult = {
    accessToken: original.accessToken,
    cookie:
      "ff_session_guid=s; arkose=a; forterToken=" +
      "a".repeat(32) +
      "_" +
      Date.now() +
      "__UDF43-v2_tt",
    arpSessionId: "warmed-arp",
  };
  const warnings: unknown[][] = [];
  const log = { warn: (...args: unknown[]) => warnings.push(args) };
  const identity = fileIOState.identity;
  const warmed = await adobe.refreshAdobeSessionViaBrowser(original, log);
  assert.ok(warmed);
  assert.equal(fileIOState.background.length, 1);
  assert.equal(fileIOState.writes.length, 0);
  fileIOState.identity = {};
  fileIOState.workerIdentity = identity;
  await drainBackground();
  assert.equal(fileIOState.writes.length, 1);
  assert.equal(warnings.length, 0);
  fileIOState.workerIdentity = {};
  await adobe.refreshAdobeSessionViaBrowser(original, log);
  await drainBackground();
  assert.equal(fileIOState.writes.length, 1);
  assert.ok(warnings.some((row) => String(row[1]) === "credential write-back was not completed"));
  fileIOState.refuseBackground = true;
  await adobe.refreshAdobeSessionViaBrowser(original, log);
  await Promise.resolve();
  assert.ok(warnings.length >= 2);
  assert.ok(warnings.every((row) => !JSON.stringify(row).includes(original.accessToken)));
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native CDP refresh awaits real cookie-file read/write while controlled browser callbacks preserve the jar", async () => {
  const { ControlledBrowserWebSocket } =
    await import("../helpers/persistence/nativeProviderFileIOBrowserTransport.ts");
  const executable = join(process.env.DATA_DIR!, "controlled-browser");
  writeFileSync(executable, "placeholder");
  process.env.OMNIROUTE_LOGIN_BROWSER_PATH = executable;
  const key = "cookie-account",
    dir = join(process.env.DATA_DIR!, "adobe-browser-sessions"),
    file = join(dir, browser.adobeFireflyBrowserSessionKey(key) + ".json");
  mkdirSync(dir, { mode: 0o777 });
  chmodSync(dir, 0o777);
  const seed = {
    name: "adobe-sso",
    value: "seed",
    domain: ".adobelogin.com",
    path: "/",
    secure: true,
  };
  writeFileSync(file, JSON.stringify([seed]), { mode: 0o600 });
  const base = Date.now();
  fileIOState.browserClock = base;
  fileIOState.browserCookies = [
    seed,
    { name: "ff_session_guid", value: "sid", domain: "firefly.adobe.com", path: "/" },
    { name: "arkose", value: "ark", domain: "firefly.adobe.com", path: "/" },
    {
      name: "forterToken",
      value: "a_" + (base + 10_000) + "__UDF43-v2_tt",
      domain: "firefly.adobe.com",
      path: "/",
    },
    { name: "foreign", value: "drop", domain: "evil.example", path: "/" },
  ];
  const originalSocket = globalThis.WebSocket,
    originalNow = Date.now;
  globalThis.WebSocket = ControlledBrowserWebSocket as unknown as typeof WebSocket;
  Date.now = () => fileIOState.browserClock;
  fileIOState.holdOperation = "writeFile";
  fileIOState.hold = deferred<void>();
  fileIOState.entered = deferred<void>();
  globalThis.fetch = async () => Response.json({ email: "cookie-account@example.test" });
  try {
    let ended = false;
    const work = browser
      .refreshAdobeFireflyViaCdp({ sessionKey: key, accessToken: userToken() })
      .then((result) => {
        ended = true;
        return result;
      });
    await fileIOState.entered.promise;
    assert.equal(ended, false, "public CDP refresh owns cookie persistence through completion");
    fileIOState.hold.resolve();
    const result = await work;
    assert.ok(result?.cookie.includes("ff_session_guid=sid"));
    assert.deepEqual(fileIOState.seededCookies[0], [seed]);
    const saved = JSON.parse(readFileSync(file, "utf8"));
    assert.equal(saved.length, 4);
    assert.ok(saved.some((entry: { name: string }) => entry.name === "adobe-sso"));
    assert.ok(!saved.some((entry: { name: string }) => entry.name === "foreign"));
    assert.equal(statSync(dir).mode & 0o777, 0o700);
    assert.equal(statSync(file).mode & 0o777, 0o600);
    assert.equal(fileIOState.spawns.length, 1);
    assert.deepEqual(fileIOState.syncCalls, []);
  } finally {
    Date.now = originalNow;
    globalThis.WebSocket = originalSocket;
  }
});

test("native Kimi grant uses async default-path resolution when DATA_DIR is absent", async () => {
  delete process.env.DATA_DIR;
  fileIOState.home = join(root, randomUUID());
  const xdg = join(root, randomUUID());
  process.env.XDG_CONFIG_HOME = xdg;
  const identities: string[] = [];
  globalThis.fetch = async (_url, init) => {
    identities.push(new Headers(init?.headers).get("x-msh-device-id")!);
    return Response.json({
      device_code: "device",
      user_code: "user",
      verification_uri_complete: "https://verify.test",
    });
  };
  await kimi.kimiCoding.requestDeviceCode(kimi.kimiCoding.config);
  const file = join(xdg, "omniroute", "oauth", "kimi-coding-device-id");
  assert.equal(readFileSync(file, "utf8"), identities[0]);
  assert.ok(fileIOState.asyncCalls.some((call) => call.operation === "stat"));
  assert.deepEqual(fileIOState.syncCalls, []);
  rmSync(file);
  fileIOState.failOperation = "writeFile";
  await kimi.kimiCoding.requestDeviceCode(kimi.kimiCoding.config);
  assert.match(identities[1], /^[a-f0-9-]{36}$/i);
  assert.throws(() => readFileSync(file));
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Adobe cold legacy cache rekeys to the connection fingerprint without replacing a healthy paste", async () => {
  productionAdobeDisk();
  const token = userToken(),
    fp = adobe.fingerprintAdobeCredential(token),
    dir = join(process.env.DATA_DIR!, "adobe-firefly-sessions");
  mkdirSync(dir);
  writeFileSync(join(dir, fp + ".json"), JSON.stringify(session(fp, token)));
  const resolved = await adobe.ensureAdobeFireflySession({
    credentials: { apiKey: token, connectionId: "stable-connection" },
    allowBrowserRefresh: false,
  });
  assert.equal(resolved.fingerprint, adobe.fingerprintAdobeCredential("conn:stable-connection"));
  assert.equal(resolved.accessToken, token);
  assert.equal(resolved.arpSessionId, "original-arp");
  assert.equal(
    JSON.parse(readFileSync(join(dir, resolved.fingerprint + ".json"), "utf8")).fingerprint,
    resolved.fingerprint
  );
  assert.deepEqual(fileIOState.syncCalls, []);
});

test("native Adobe same-file saves serialize and snapshot ARP at admission", async () => {
  productionAdobeDisk();
  const resolved = await adobe.ensureAdobeFireflySession({
    credentials: { apiKey: userToken() },
    allowBrowserRefresh: false,
  });
  fileIOState.fileWrites.length = 0;
  fileIOState.holdOperation = "writeFile";
  fileIOState.hold = deferred<void>();
  fileIOState.entered = deferred<void>();
  const first = adobe.markAdobeFireflyArpSuccessAsync(resolved.fingerprint, "first-arp");
  await fileIOState.entered.promise;
  const second = adobe.markAdobeFireflyArpSuccessAsync(resolved.fingerprint, "second-arp");
  await Promise.resolve();
  assert.equal(
    fileIOState.fileWrites.length,
    1,
    "second save cannot enter while the first file section is held"
  );
  assert.equal(JSON.parse(fileIOState.fileWrites[0].contents).arpSessionId, "first-arp");
  fileIOState.hold.resolve();
  await Promise.all([first, second]);
  assert.equal(fileIOState.fileWrites.length, 2);
  assert.equal(JSON.parse(fileIOState.fileWrites[1].contents).arpSessionId, "second-arp");
  assert.equal(
    JSON.parse(readFileSync(fileIOState.fileWrites[1].path, "utf8")).arpSessionId,
    "second-arp"
  );
  assert.deepEqual(fileIOState.syncCalls, []);
});
