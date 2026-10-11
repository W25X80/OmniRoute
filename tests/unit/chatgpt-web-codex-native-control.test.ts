import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, writeFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { setImmediate as immediate } from "node:timers/promises";
import test from "node:test";
import ts from "typescript";
import {
  controls,
  deferred,
  resetControls,
  socketMetadata,
} from "../helpers/persistence/browserControlFixture.ts";

test("native registered browser control awaits small files and retains source safety predicates", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "omniroute-browser-control-"));
  const previousHome = process.env.CODEX_CHATGPT_WEB_HOME;
  const previousBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
  const previousTokenFile = process.env.CDP_PROXY_TOKEN_FILE;
  const previousToken = process.env.CDP_PROXY_TOKEN;
  const previousChrome = process.env.CHROME_PATH;
  const previousDedicatedChrome = process.env.CHATGPT_WEB_CODEX_CHROME_PATH;
  process.env.CODEX_CHATGPT_WEB_HOME = directory;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  delete process.env.CDP_PROXY_TOKEN;
  const fixtureUrl = new URL("../helpers/persistence/browserControlFixture.ts", import.meta.url)
    .href;
  async function compile(name: string, path: string, replacements: Record<string, string> = {}) {
    const source = await readFile(new URL("../../" + path, import.meta.url), "utf8");
    let compiled = ts.transpileModule(source, {
      compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
    }).outputText;
    for (const [specifier, url] of Object.entries(replacements)) {
      compiled = compiled.replaceAll(`from "${specifier}"`, `from "${url}"`);
    }
    const output = join(directory, `${name}.mjs`);
    await writeFile(output, compiled);
    return pathToFileURL(output).href;
  }
  const vendor = "open-sse/vendor/codex-chatgpt-web/";
  const fsPorts = { "node:fs": fixtureUrl, "node:fs/promises": fixtureUrl };
  try {
    const version = await compile("version", vendor + "version.ts");
    const backend = await compile("backend", "src/lib/db/backendSelection.ts");
    const config = await compile("config", vendor + "config.ts", {
      ...fsPorts,
      "./version": version,
    });
    const storageWrite = await compile("storage-write", vendor + "storage-state-write.ts");
    const secondStorageWrite = await compile("storage-write-b", vendor + "storage-state-write.ts");
    const cdpUrl = await compile("cdp", vendor + "cdp-auth.ts", fsPorts);
    const loginUrl = await compile("login", vendor + "browser-login.ts", {
      ...fsPorts,
      "playwright-core": fixtureUrl,
      "./config": config,
      "./storage-state-write": storageWrite,
      "./cdp-auth": cdpUrl,
      "./chatgpt-session": fixtureUrl,
      "../../../src/lib/db/backendSelection.ts": backend,
    });
    const storageReplacements = {
      ...fsPorts,
      "../../vendor/codex-chatgpt-web/config.ts": config,
      "../../vendor/codex-chatgpt-web/storage-state-write.ts": storageWrite,
      "../../vendor/codex-chatgpt-web/browser-login.ts": loginUrl,
    };
    const storageUrl = await compile(
      "storage-a",
      "open-sse/executors/chatgpt-web-codex/storageState.ts",
      storageReplacements
    );
    const secondStorageUrl = await compile(
      "storage-b",
      "open-sse/executors/chatgpt-web-codex/storageState.ts",
      {
        ...storageReplacements,
        "../../vendor/codex-chatgpt-web/storage-state-write.ts": secondStorageWrite,
      }
    );
    const compaction = await compile(
      "compaction",
      vendor + "adapters/chatgpt-web/compaction-transaction.ts"
    );
    const brokerUrl = await compile("broker", vendor + "adapters/chatgpt-web/turn-broker.ts", {
      ...fsPorts,
      "node:net": fixtureUrl,
      "../../config": config,
      "./compaction-transaction": compaction,
      "../../../../../src/lib/db/backendSelection.ts": backend,
    });
    const chromeUrl = await compile(
      "chrome",
      "open-sse/executors/chatgpt-web-codex/chromeExecutable.ts",
      { "node:fs/promises": fixtureUrl }
    );
    // Exercise the exact selected worker methods without importing its unrelated tokenizer/DOM ports.
    const workerSource = await readFile(
      new URL(
        "../../open-sse/vendor/codex-chatgpt-web/adapters/chatgpt-web/browser-worker.ts",
        import.meta.url
      ),
      "utf8"
    );
    const workerAst = ts.createSourceFile(
      "browser-worker.ts",
      workerSource,
      ts.ScriptTarget.ES2022,
      true
    );
    const workerClass = workerAst.statements.find(
      (node): node is ts.ClassDeclaration =>
        ts.isClassDeclaration(node) && node.name?.text === "ChatGptBrowserWorker"
    );
    assert(workerClass);
    const controlMethods = ["ensurePage", "ensureManagedBrowser"].map((name) => {
      const member = workerClass.members.find((node) => node.name?.getText(workerAst) === name);
      assert(member && ts.isMethodDeclaration(member));
      return member.getText(workerAst);
    });
    const pathHelper = workerAst.statements.find(
      (node): node is ts.FunctionDeclaration =>
        ts.isFunctionDeclaration(node) && node.name?.text === "browserControlPathExists"
    );
    assert(pathHelper);
    const workerControlSource = `
      import { existsSync, access, chromium } from "${fixtureUrl}";
      import { parseDatabaseBackend } from "${backend}";
      import { browserContextForStoredState, loginVerificationMarkerPath } from "${loginUrl}";
      import { cdpConnectOptions, cdpConnectOptionsAsync } from "${cdpUrl}";
      function connectLauncherBrowserHost(): never { throw new Error("unqualified launcher port"); }
      ${pathHelper.getText(workerAst)}
      export class WorkerControl {
        private page?: Page;
        private browser?: Browser;
        private context?: BrowserContext;
        private managedBrowserReady?: Promise<{ browser: Browser; context: BrowserContext }>;
        constructor(private readonly config: BrowserLoginConfig) {}
        ${controlMethods.join("\n")}
        openPage() { return this.ensurePage(); }
        openManaged() { return this.ensureManagedBrowser(); }
      }
    `;
    const workerControlPath = join(directory, "worker-controls.mjs");
    await writeFile(
      workerControlPath,
      ts.transpileModule(workerControlSource, {
        compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
      }).outputText
    );
    const workerControl = (await import(pathToFileURL(workerControlPath).href)) as {
      WorkerControl: new (config: Record<string, unknown>) => {
        openPage(): Promise<unknown>;
        openManaged(): Promise<unknown>;
      };
    };
    const storage = (await import(
      storageUrl
    )) as typeof import("../../open-sse/executors/chatgpt-web-codex/storageState.ts");
    const secondStorage = (await import(secondStorageUrl)) as typeof storage;
    const login = (await import(
      loginUrl
    )) as typeof import("../../open-sse/vendor/codex-chatgpt-web/browser-login.ts");
    const cdp = (await import(
      cdpUrl
    )) as typeof import("../../open-sse/vendor/codex-chatgpt-web/cdp-auth.ts");
    const broker = (await import(
      brokerUrl
    )) as typeof import("../../open-sse/vendor/codex-chatgpt-web/adapters/chatgpt-web/turn-broker.ts");
    const chrome = (await import(
      chromeUrl
    )) as typeof import("../../open-sse/executors/chatgpt-web-codex/chromeExecutable.ts");

    await t.test(
      "state/marker pair waits for async I/O and serializes one connection across bundles",
      async () => {
        resetControls();
        assert.notEqual(
          storage.ensureConnectionStorageStateAsync,
          secondStorage.ensureConnectionStorageStateAsync
        );
        const paths = storage.connectionRuntimePaths("shared-connection");
        const gate = deferred();
        controls.asyncGate = { operation: "open", match: paths.storageStatePath + ".tmp-", gate };
        const first = storage.ensureConnectionStorageStateAsync("shared-connection", "session-a");
        let second: Promise<string> | undefined;
        try {
          await gate.ready;
          second = secondStorage.ensureConnectionStorageStateAsync(
            "shared-connection",
            "session-b"
          );
          await immediate();
          assert.equal(controls.asyncCalls.filter((x) => x.startsWith("open:")).length, 1);
          assert.equal(controls.syncCalls.length, 0);
          gate.release();
          await Promise.all([first, second]);
          const state = await storage.readConnectionStorageStateAsync(paths.storageStatePath);
          assert.equal((state.cookies as Array<{ value: string }>)[0].value, "session-b");
          const marker = JSON.parse(
            await readFile(login.loginVerificationMarkerPath(paths.storageStatePath), "utf8")
          );
          assert.equal(
            marker.cookieFingerprint,
            createHash("sha256").update("session-b").digest("hex")
          );
          assert.equal(marker.pendingBrowserVerification, true);
          assert.equal((await stat(paths.storageStatePath)).mode & 0o077, 0);
          assert.equal(await login.browserLoginStateExistsAsync(paths), false);
        } finally {
          gate.release();
          await Promise.allSettled([first, ...(second ? [second] : [])]);
        }
      }
    );

    await t.test(
      "browser marker publication waits for the independently bundled state writer",
      async () => {
        resetControls();
        const paths = storage.connectionRuntimePaths("cross-writer");
        const gate = deferred();
        controls.asyncGate = { operation: "open", match: paths.storageStatePath + ".tmp-", gate };
        const stateWrite = secondStorage.ensureConnectionStorageStateAsync(
          "cross-writer",
          "session-guarded"
        );
        let markerWrite: Promise<void> | undefined;
        try {
          await gate.ready;
          markerWrite = login.writeVerificationMarkerAsync(paths.storageStatePath, {
            solAvailable: false,
            proAvailable: true,
          });
          await immediate();
          assert.equal(
            controls.asyncCalls.filter((call) => call.startsWith("readFile:")).length,
            0
          );
          assert.equal(controls.asyncCalls.filter((call) => call.startsWith("open:")).length, 1);
          gate.release();
          await Promise.all([stateWrite, markerWrite]);
          const state = await storage.readConnectionStorageStateAsync(paths.storageStatePath);
          const marker = JSON.parse(
            await readFile(login.loginVerificationMarkerPath(paths.storageStatePath), "utf8")
          );
          assert.equal(marker.pendingBrowserVerification, false);
          assert.equal(
            marker.cookieFingerprint,
            createHash("sha256").update("session-guarded").digest("hex")
          );
          assert.equal(
            marker.storageStateFingerprint,
            createHash("sha256").update(JSON.stringify(state)).digest("hex")
          );
          assert.equal(marker.proAvailable, true);
          assert.equal(controls.syncCalls.length, 0);
        } finally {
          gate.release();
          await Promise.allSettled([stateWrite, ...(markerWrite ? [markerWrite] : [])]);
        }
      }
    );

    await t.test(
      "credential normalization, marker verification and finalization preserve protected fields",
      async () => {
        resetControls();
        const validationId = "validation-0123456789abcdef01234567";
        const statePath = await storage.ensureConnectionStorageStateAsync(
          validationId,
          "session-c"
        );
        await login.writeVerificationMarkerAsync(statePath, {
          solAvailable: false,
          proAvailable: true,
        });
        assert.equal(
          await login.browserLoginStateExistsAsync({ storageStatePath: statePath }),
          true
        );
        await assert.rejects(
          storage.finalizeValidatedChatGptWebCodexSecretsAsync(
            JSON.stringify({ cookie: "wrong-cookie" }),
            validationId
          ),
          /does not match/
        );
        const finalized = await storage.finalizeValidatedChatGptWebCodexSecretsAsync(
          JSON.stringify({ cookie: "session-c", runtimeKey: " runtime-key " }),
          validationId
        );
        const encoded = JSON.parse(finalized.encodedCredential);
        assert.equal(encoded.version, 2);
        assert.equal(encoded.runtimeKey, "runtime-key");
        assert.deepEqual(encoded.storageState, finalized.storageState);
        await assert.rejects(readFile(statePath), { code: "ENOENT" });
        const normalizedPath = await storage.ensureConnectionStorageStateFromCredentialAsync(
          "host-cookie",
          {
            storageState: {
              cookies: [{ name: "__Host-test", value: "v", url: "https://chatgpt.com" }],
              origins: [],
            },
          }
        );
        const normalized = await storage.readConnectionStorageStateAsync(normalizedPath);
        assert.deepEqual((normalized.cookies as unknown[])[0], {
          name: "__Host-test",
          value: "v",
          domain: "chatgpt.com",
          path: "/",
          secure: true,
        });
        await assert.rejects(
          storage.ensureConnectionStorageStateFromCredentialAsync("invalid", {
            storageState: { cookies: [] },
          }),
          /Encrypted ChatGPT browser storage state is invalid/
        );
        assert.equal(controls.syncCalls.length, 0);
      }
    );

    await t.test(
      "CDP reconnect reads rotated token, direct env wins and read failure stays empty",
      async () => {
        resetControls();
        const tokenPath = join(directory, "cdp-token");
        const env = { CDP_PROXY_TOKEN_FILE: tokenPath };
        await writeFile(tokenPath, " first-token \n");
        assert.deepEqual(await cdp.cdpConnectOptionsAsync(env), {
          headers: { [cdp.CDP_TOKEN_HEADER]: "first-token" },
        });
        await writeFile(tokenPath, "second-token\n");
        assert.equal(await cdp.resolveCdpProxyTokenAsync(env), "second-token");
        assert.equal(
          await cdp.resolveCdpProxyTokenAsync({ ...env, CDP_PROXY_TOKEN: " direct " }),
          "direct"
        );
        assert.equal(
          await cdp.resolveCdpProxyTokenAsync({ CDP_PROXY_TOKEN_FILE: join(directory, "missing") }),
          ""
        );
        assert.equal(controls.syncCalls.length, 0);
      }
    );

    await t.test(
      "native browser verification awaits files and reuses the persistent CDP context",
      async () => {
        resetControls();
        const statePath = await storage.ensureConnectionStorageStateAsync(
          "browser-interface",
          "session-d"
        );
        const tokenPath = join(directory, "browser-token");
        process.env.CDP_PROXY_TOKEN_FILE = tokenPath;
        await writeFile(tokenPath, "browser-token-one");
        const config = {
          appName: "fixture",
          storageStatePath: statePath,
          headed: false,
          proAvailable: false,
          autoApproveToolCalls: false,
          cdpEndpoint: "http://fixture-cdp",
        };
        const gate = deferred();
        controls.asyncGate = { operation: "access", match: statePath, gate };
        const check = login.inspectBrowserLoginCapabilities(config);
        try {
          await gate.ready;
          await immediate();
          assert.equal(controls.browserConnections.length, 0);
          gate.release();
          assert.deepEqual(await check, { solAvailable: false, proAvailable: true });
          assert.equal(controls.contextCloses, 0);
          assert.equal(controls.pageCloses, 1);
          assert.equal(controls.browserCloses, 1);
          await writeFile(tokenPath, "browser-token-two");
          await login.inspectBrowserLoginCapabilities(config);
          assert.deepEqual(
            controls.browserConnections.map((x) => x.options),
            [
              { headers: { [cdp.CDP_TOKEN_HEADER]: "browser-token-one" } },
              { headers: { [cdp.CDP_TOKEN_HEADER]: "browser-token-two" } },
            ]
          );
          assert.equal(controls.syncCalls.length, 0);
        } finally {
          gate.release();
          await check;
        }
      }
    );

    for (const method of ["openPage", "openManaged"] as const) {
      await t.test(
        `native worker ${method} awaits state checks and reads the token for each connect`,
        async () => {
          resetControls();
          const statePath = await storage.ensureConnectionStorageStateAsync(
            `worker-${method}`,
            "worker-session"
          );
          const tokenPath = join(directory, `worker-token-${method}`);
          process.env.CDP_PROXY_TOKEN_FILE = tokenPath;
          await writeFile(tokenPath, "worker-token-one");
          const config = {
            storageStatePath: statePath,
            browserHost: "managed-chrome",
            cdpEndpoint: "http://fixture-cdp",
          };
          const gate = deferred();
          controls.asyncGate = { operation: "access", match: statePath, gate };
          const opening = new workerControl.WorkerControl(config)[method]();
          try {
            await gate.ready;
            assert.equal(controls.browserConnections.length, 0);
            gate.release();
            await opening;
            await writeFile(tokenPath, "worker-token-two");
            await new workerControl.WorkerControl(config)[method]();
            assert.deepEqual(
              controls.browserConnections.map((connection) => connection.options),
              [
                { headers: { [cdp.CDP_TOKEN_HEADER]: "worker-token-one" } },
                { headers: { [cdp.CDP_TOKEN_HEADER]: "worker-token-two" } },
              ]
            );
            assert.equal(controls.syncCalls.length, 0);
          } finally {
            gate.release();
            await opening;
          }
        }
      );
    }

    await t.test(
      "Chrome async detection preserves candidate priority without a positive cache",
      async () => {
        resetControls();
        process.env.CHATGPT_WEB_CODEX_CHROME_PATH = "/virtual/dedicated";
        process.env.CHROME_PATH = "/virtual/environment";
        controls.existingPaths.add("/virtual/explicit");
        controls.existingPaths.add("/virtual/dedicated");
        controls.existingPaths.add("/virtual/environment");
        assert.equal(
          await chrome.detectChromeExecutableAsync("/virtual/explicit"),
          "/virtual/explicit"
        );
        controls.existingPaths.delete("/virtual/explicit");
        assert.equal(
          await chrome.detectChromeExecutableAsync("/virtual/explicit"),
          "/virtual/dedicated"
        );
        controls.existingPaths.delete("/virtual/dedicated");
        assert.equal(await chrome.detectChromeExecutableAsync(), "/virtual/environment");
        assert.equal(controls.syncCalls.length, 0);
      }
    );

    for (const [kind, changes, message] of [
      ["regular", { isSocket: () => false }, /not a socket/],
      ["foreign-uid", { uid: (process.getuid?.() ?? 0) + 1 }, /not owned by the current user/],
      ["unsafe-mode", { mode: 0o666 }, /unsafe permissions/],
    ] as const) {
      await t.test(`native broker rejects ${kind} before an owner probe`, async () => {
        resetControls();
        const path = `/virtual/${kind}.sock`;
        controls.sockets.set(path, socketMetadata(changes));
        const owner = broker.TurnBroker.forSocket(path);
        await assert.rejects(owner.listen(), message);
        assert.equal(controls.probeCount, 0);
        assert.equal(controls.servers.length, 0);
        await owner.close();
        assert(controls.sockets.has(path));
        assert.equal(controls.syncCalls.length, 0);
      });
    }

    for (const probe of ["connected", "timeout", "denied"] as const) {
      await t.test(
        `native broker retains ${probe} probe rejection and its timeout budget`,
        async () => {
          resetControls();
          const path = `/virtual/${probe}.sock`;
          controls.sockets.set(path, socketMetadata());
          controls.probe = probe;
          const owner = broker.TurnBroker.forSocket(path);
          await assert.rejects(
            owner.listen(),
            probe === "connected"
              ? /already owned by another process/
              : probe === "timeout"
                ? /Timed out/
                : /Could not verify/
          );
          assert.deepEqual(controls.probeTimeouts, [2_000]);
          assert.equal(controls.servers.length, 0);
          await owner.close();
          assert(controls.sockets.has(path));
        }
      );
    }

    await t.test(
      "stale broker socket is rechecked and an intervening inode is retained",
      async () => {
        resetControls();
        const path = "/virtual/replaced-during-probe.sock";
        controls.sockets.set(path, socketMetadata());
        const replacement = socketMetadata();
        controls.beforeProbe = () => {
          controls.sockets.set(path, replacement);
        };
        const owner = broker.TurnBroker.forSocket(path);
        await assert.rejects(owner.listen(), /already owned by another process/);
        await owner.close();
        assert.equal(controls.sockets.get(path)?.ino, replacement.ino);
        assert.equal(controls.asyncCalls.filter((x) => x.startsWith("unlink:")).length, 0);
      }
    );

    await t.test(
      "native broker replaces only the verified stale socket and cleans its own inode",
      async () => {
        resetControls();
        const path = "/virtual/verified-stale.sock";
        const stale = socketMetadata();
        controls.sockets.set(path, stale);
        const owner = broker.TurnBroker.forSocket(path);
        await owner.listen();
        assert.notEqual(controls.sockets.get(path)?.ino, stale.ino);
        assert.equal(controls.sockets.get(path)?.mode, 0o600);
        assert.deepEqual(controls.probeTimeouts, [2_000]);
        await owner.close();
        assert.equal(controls.sockets.has(path), false);
        assert.equal(controls.asyncCalls.filter((x) => x.startsWith("unlink:")).length, 2);
        assert.equal(controls.syncCalls.length, 0);
      }
    );

    await t.test(
      "native broker retains path-length rejection before filesystem or probe work",
      async () => {
        resetControls();
        const owner = broker.TurnBroker.forSocket(`/virtual/${"x".repeat(120)}.sock`);
        await assert.rejects(owner.listen(), /over the.*limit this platform allows/);
        assert.equal(controls.asyncCalls.length, 0);
        assert.equal(controls.probeCount, 0);
        assert.equal(controls.servers.length, 0);
        await owner.close();
      }
    );

    await t.test(
      "native listen waits for permissions and close retains a foreign replacement",
      async () => {
        resetControls();
        const path = "/virtual/permissions.sock";
        const gate = deferred();
        controls.asyncGate = { operation: "chmod", match: path, gate };
        const owner = broker.TurnBroker.forSocket(path);
        let replied = false;
        const starting = owner.listen().then(() => {
          replied = true;
        });
        try {
          await gate.ready;
          await immediate();
          assert.equal(replied, false);
          gate.release();
          await starting;
          assert.equal(controls.sockets.get(path)?.mode, 0o600);
          const replacement = socketMetadata();
          controls.sockets.set(path, replacement);
          await owner.close();
          assert.equal(controls.sockets.get(path)?.ino, replacement.ino);
          assert.equal(controls.syncCalls.length, 0);
        } finally {
          gate.release();
          await starting;
          await owner.close();
        }
      }
    );

    await t.test(
      "rejected post-listen chmod retires its server, connections, inode and listeners",
      async () => {
        resetControls();
        const path = "/virtual/failed-permissions.sock";
        const failure = Object.assign(new Error("isolated permission rejection"), {
          code: "EACCES",
        });
        controls.asyncFailure = { operation: "chmod", match: path, error: failure };
        controls.acceptOnListen = true;
        const owner = broker.TurnBroker.forSocket(path);
        await assert.rejects(owner.listen(), (error) => error === failure);
        assert.equal(controls.servers[0].listening, false);
        assert.equal(controls.servers[0].accepted[0].destroyed, true);
        assert.equal(controls.servers[0].accepted[0].listenerCount("close"), 0);
        assert.equal(controls.servers[0].listenerCount("error"), 0);
        assert.deepEqual(controls.serverCloseActiveCounts, [0]);
        assert.equal(controls.sockets.has(path), false);
        assert.equal(controls.syncCalls.length, 0);
        await owner.close();
      }
    );

    await t.test(
      "startup rejection keeps the inode that replaced its captured socket",
      async () => {
        resetControls();
        const path = "/virtual/failed-replaced.sock";
        const gate = deferred();
        const failure = new Error("isolated setup rejection");
        controls.asyncGate = { operation: "chmod", match: path, gate };
        controls.asyncFailure = { operation: "chmod", match: path, error: failure };
        const owner = broker.TurnBroker.forSocket(path);
        const starting = owner.listen();
        const rejected = assert.rejects(starting, (error) => error === failure);
        try {
          await gate.ready;
          const replacement = socketMetadata();
          controls.sockets.set(path, replacement);
          gate.release();
          await rejected;
          assert.equal(controls.servers[0].listening, false);
          assert.equal(controls.sockets.get(path)?.ino, replacement.ino);
          assert.equal(controls.asyncCalls.filter((call) => call.startsWith("unlink:")).length, 0);
          await owner.close();
          assert.equal(controls.sockets.get(path)?.ino, replacement.ino);
        } finally {
          gate.release();
          await rejected;
          await owner.close();
        }
      }
    );

    await t.test(
      "close overtaking post-listen setup retires old connections and keeps the new startup",
      async () => {
        resetControls();
        const path = "/virtual/overtaken-start.sock";
        const gate = deferred();
        controls.asyncGate = { operation: "chmod", match: path, gate };
        controls.acceptOnListen = true;
        const oldOwner = broker.TurnBroker.forSocket(path);
        const oldStarting = oldOwner.listen();
        const rejected = assert.rejects(oldStarting, /closed before startup completed/);
        let newOwner: ReturnType<typeof broker.TurnBroker.forSocket> | undefined;
        try {
          await gate.ready;
          await oldOwner.close();
          assert.equal(controls.servers[0].accepted[0].destroyed, true);
          assert.equal(controls.sockets.has(path), false);
          controls.acceptOnListen = false;
          newOwner = broker.TurnBroker.forSocket(path);
          await newOwner.listen();
          const newInode = controls.sockets.get(path)?.ino;
          gate.release();
          await rejected;
          assert.equal(controls.servers[0].listening, false);
          assert.equal(controls.servers[0].listenerCount("error"), 0);
          assert.equal(controls.servers[1].listening, true);
          assert.equal(controls.sockets.get(path)?.ino, newInode);
          assert.equal(controls.syncCalls.length, 0);
        } finally {
          gate.release();
          await rejected;
          await newOwner?.close();
        }
      }
    );

    await t.test("close during async setup prevents a later native listen", async () => {
      resetControls();
      const path = "/virtual/cancelled-start.sock";
      const gate = deferred();
      controls.asyncGate = { operation: "mkdir", match: "/virtual", gate };
      const owner = broker.TurnBroker.forSocket(path);
      const starting = owner.listen();
      const rejected = assert.rejects(starting, /closed before startup completed/);
      try {
        await gate.ready;
        await owner.close();
        gate.release();
        await rejected;
        assert.equal(controls.servers.length, 0);
      } finally {
        gate.release();
        await rejected;
      }
    });

    await t.test(
      "public synchronous credential/marker APIs remain available to default CLI callers",
      async () => {
        resetControls();
        controls.nativeOnly = false;
        process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
        const statePath = storage.ensureConnectionStorageState("cli-default", "session-cli");
        assert.equal(typeof statePath, "string");
        login.writeVerificationMarker(statePath, { solAvailable: true, proAvailable: false });
        assert.equal(login.browserLoginStateExists({ storageStatePath: statePath }), true);
        assert.equal(Array.isArray(storage.readConnectionStorageState(statePath).cookies), true);
        assert(controls.syncCalls.length > 0);
      }
    );
  } finally {
    for (const [name, value] of [
      ["CODEX_CHATGPT_WEB_HOME", previousHome],
      ["OMNIROUTE_DATABASE_BACKEND", previousBackend],
      ["CDP_PROXY_TOKEN_FILE", previousTokenFile],
      ["CDP_PROXY_TOKEN", previousToken],
      ["CHROME_PATH", previousChrome],
      ["CHATGPT_WEB_CODEX_CHROME_PATH", previousDedicatedChrome],
    ]) {
      if (value === undefined) delete process.env[name!];
      else process.env[name!] = value;
    }
    await rm(directory, { recursive: true, force: true });
  }
});
