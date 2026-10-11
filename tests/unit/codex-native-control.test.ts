import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { unzipSync, zipSync } from "fflate";
import { atomicWriteFileAsync } from "../../open-sse/vendor/codex-chatgpt-web/config";
import {
  expandPreviousResponseInputAsync,
  previousResponseReplayPrefixLength,
  rememberResponseStateAsync,
  rememberResponseState,
} from "../../open-sse/vendor/codex-chatgpt-web/responses/state";
import {
  closeResponseStateWorker,
  runResponseStateOperation,
  resolveResponseStateWorker,
} from "../../open-sse/vendor/codex-chatgpt-web/responses/stateWorkerClient";
import {
  extractTunnelBinaryAsync,
  resolveTunnelZipWorker,
} from "../../open-sse/executors/chatgpt-web-codex/tunnelZipClient";
import {
  runTunnelCommandAsync,
  acquireTunnelSupervisorLeaseAsync,
  tunnelSupervisorLeaseStatusAsync,
  ensureTunnelClientInstalled,
  tunnelClientPaths,
  tunnelPlatformAsset,
} from "../../open-sse/executors/chatgpt-web-codex/tunnelClient";
import { bridgeToResponsesSSE } from "../../open-sse/vendor/codex-chatgpt-web/bridge";
import type { AdapterEvent } from "../../open-sse/vendor/codex-chatgpt-web/types";

test("native Codex control", async (t) => {
  const home = await mkdtemp(join(tmpdir(), "codex-native-control-"));
  const oldHome = process.env.CODEX_CHATGPT_WEB_HOME;
  const oldBackend = process.env.OMNIROUTE_DATABASE_BACKEND;
  process.env.CODEX_CHATGPT_WEB_HOME = home;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  t.after(async () => {
    await closeResponseStateWorker();
    if (oldHome === undefined) delete process.env.CODEX_CHATGPT_WEB_HOME;
    else process.env.CODEX_CHATGPT_WEB_HOME = oldHome;
    if (oldBackend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = oldBackend;
    await rm(home, { recursive: true, force: true });
  });
  const delay = (ms: number) => new Promise((resolveDelay) => setTimeout(resolveDelay, ms));

  await t.test(
    "native cache ACK preserves namespace, replay provenance and cache state",
    async () => {
      await rememberResponseStateAsync(
        { input: "question", store: false },
        {
          id: "resp_native",
          status: "completed",
          output: [{ role: "assistant", content: "answer" }],
        },
        { force: true, namespace: "account:thread:turn" }
      );
      const input = { previous_response_id: "resp_native", input: "next" };
      const expanded = (await expandPreviousResponseInputAsync(input, "account:thread:turn")) as {
        input: unknown[];
      };
      assert.equal(expanded.input.length, 3);
      assert.equal(previousResponseReplayPrefixLength(expanded), 2);
      assert.equal(await expandPreviousResponseInputAsync(input, "foreign"), input);
      const snapshot = JSON.parse(await readFile(join(home, "responses-state.json"), "utf8")) as {
        version: number;
        states: unknown[];
      };
      assert.equal(snapshot.version, 1);
      assert.equal(snapshot.states.length, 1);
      await rememberResponseStateAsync(
        { input: "filtered" },
        {
          id: "filtered",
          status: "incomplete",
          incomplete_details: { reason: "content_filter" },
          output: [],
        },
        { force: true, namespace: "account:thread:turn" }
      );
      const filtered = { previous_response_id: "filtered", input: [] };
      assert.equal(
        await expandPreviousResponseInputAsync(filtered, "account:thread:turn"),
        filtered
      );
      process.env.CODEX_CHATGPT_WEB_HOME = `${home}-foreign`;
      assert.equal(await expandPreviousResponseInputAsync(input, "account:thread:turn"), input);
      process.env.CODEX_CHATGPT_WEB_HOME = home;
      assert.notEqual(await expandPreviousResponseInputAsync(input, "account:thread:turn"), input);
    }
  );

  await t.test(
    "separate bundles share native cache authority without sharing replay bodies",
    async () => {
      const nonce = `${process.pid}-${Date.now()}`;
      const moduleUrl = new URL(
        "../../open-sse/vendor/codex-chatgpt-web/responses/state.ts",
        import.meta.url
      ).href;
      const first = await import(`${moduleUrl}?native-first=${nonce}`);
      const second = await import(`${moduleUrl}?native-second=${nonce}`);
      await first.rememberResponseStateAsync(
        { input: "cross-bundle" },
        { id: "cross_bundle", output: [], status: "completed" },
        { force: true, namespace: "cross" }
      );
      const body = { previous_response_id: "cross_bundle", input: "next" };
      const expanded = await second.expandPreviousResponseInputAsync(body, "cross");
      assert.notEqual(expanded, body);
      assert.equal(second.previousResponseReplayPrefixLength(expanded), 1);
      assert.equal(await second.expandPreviousResponseInputAsync(body, "foreign"), body);
    }
  );

  await t.test(
    "held source snapshot lock leaves main loop responsive and eight-job admission bounded",
    async () => {
      const warmup = await runResponseStateOperation({
        kind: "lookup",
        responseId: "missing-lock-progress",
        namespace: "lock",
      });
      assert.equal(warmup.kind, "lookup");
      const lock = join(home, "responses-state.json.lock");
      await writeFile(lock, "", { flag: "wx" });
      let completed = false;
      const started = performance.now();
      const operation = {
        kind: "remember" as const,
        request: { input: "lock" },
        response: { id: "locked", output: [], status: "completed" },
        options: { force: true, namespace: "lock" },
      };
      const first = runResponseStateOperation(operation);
      void first.then(
        () => {
          completed = true;
        },
        () => {
          completed = true;
        }
      );
      const queued = Array.from({ length: 7 }, () => runResponseStateOperation(operation));
      const drain = Promise.all([first, ...queued]);
      // Observe rejection immediately; cleanup still propagates any accepted-job failure.
      void drain.catch(() => {});
      let deadline: ReturnType<typeof setTimeout> | undefined;
      const progressDiagnostic = () =>
        `elapsedMs=${Math.round(performance.now() - started)}, settled=${completed}`;
      try {
        await assert.rejects(runResponseStateOperation(operation), /unavailable/);
        await Promise.race([
          new Promise<void>((resolveTurn) => setImmediate(resolveTurn)),
          new Promise<never>((_resolve, reject) => {
            // Test harness deadline; the source snapshot lock still expires after two seconds.
            deadline = setTimeout(
              () =>
                reject(
                  new Error(`Main-loop progress observation timed out (${progressDiagnostic()})`)
                ),
              5_000
            );
          }),
        ]);
        assert.equal(
          completed,
          false,
          `Accepted cache operation must remain pending at a future main-loop turn (${progressDiagnostic()})`
        );
        t.diagnostic(`Native cache progress observed (${progressDiagnostic()})`);
      } finally {
        if (deadline) clearTimeout(deadline);
        await rm(lock, { force: true });
        await drain;
      }
    }
  );

  await t.test("direct source lock falsifies the main-loop-progress property", async () => {
    const lock = join(home, "responses-state.json.lock");
    await writeFile(lock, "", { flag: "wx" });
    let ticks = 0;
    const beat = setInterval(() => ticks++, 5);
    const started = performance.now();
    try {
      rememberResponseState(
        { input: "direct" },
        { id: "direct", output: [], status: "completed" },
        { force: true, namespace: "direct" }
      );
      assert.equal(ticks, 0);
      assert.ok(performance.now() - started >= 1_900);
    } finally {
      clearInterval(beat);
      await rm(lock, { force: true });
    }
  });

  await t.test("async atomic helper keeps replacement bytes, mode and temp cleanup", async () => {
    const path = join(home, "atomic", "state.json");
    await atomicWriteFileAsync(path, "first");
    await atomicWriteFileAsync(path, new TextEncoder().encode("second"));
    assert.equal(await readFile(path, "utf8"), "second");
    if (process.platform !== "win32") assert.equal((await stat(path)).mode & 0o777, 0o600);
  });

  await t.test("native command is asynchronous, output/timeout cannot report ready", async () => {
    let settled = false;
    const work = runTunnelCommandAsync(
      process.execPath,
      ["-e", "setTimeout(()=>console.log('ready'),100)"],
      5_000
    ).then((result) => {
      settled = true;
      return result;
    });
    await delay(30);
    assert.equal(settled, false);
    const result = await work;
    assert.equal(result.status, 0);
    assert.match(result.stdout, /ready/);
    assert.notEqual(
      (await runTunnelCommandAsync(process.execPath, ["-e", "setInterval(()=>{},1000)"], 25))
        .status,
      0
    );
    assert.notEqual(
      (
        await runTunnelCommandAsync(
          process.execPath,
          ["-e", "process.stdout.write('x'.repeat(1100000))"],
          500
        )
      ).status,
      0
    );
    assert.notEqual((await runTunnelCommandAsync(join(home, "missing"), [], 100)).status, 0);
  });

  await t.test("native lease remains single owner with concurrent acquisition", async () => {
    await Promise.all([acquireTunnelSupervisorLeaseAsync(), acquireTunnelSupervisorLeaseAsync()]);
    const lease = await tunnelSupervisorLeaseStatusAsync();
    assert.equal(lease.ownedByCurrentProcess, true);
    assert.equal(lease.conflict, false);
  });

  await t.test(
    "native installation preserves integrity, pinned upgrade and rollback without network",
    { skip: process.platform === "win32" },
    async () => {
      const paths = tunnelClientPaths();
      const oldBinary = new TextEncoder().encode(`#!${process.execPath}\nconsole.log("0.0.12");\n`);
      const nextBinary = new TextEncoder().encode(
        `#!${process.execPath}\nconsole.log("0.0.13");\n`
      );
      const oldManifest = JSON.stringify({
        version: 1,
        tunnelClientVersion: "0.0.12",
        asset: "old",
        archiveSha256: "old",
        binarySha256: createHash("sha256").update(oldBinary).digest("hex"),
      });
      await atomicWriteFileAsync(paths.binary, oldBinary);
      await (await import("node:fs/promises")).chmod(paths.binary, 0o700);
      await atomicWriteFileAsync(paths.manifest, oldManifest);
      const asset = tunnelPlatformAsset();
      const archive = zipSync({ "nested/tunnel-client": nextBinary });
      const checksum = createHash("sha256").update(archive).digest("hex");
      const originalFetch = globalThis.fetch;
      let downloads = 0;
      globalThis.fetch = async (input) => {
        downloads++;
        const url = String(input);
        assert.ok(
          url.startsWith("https://github.com/openai/tunnel-client/releases/download/v0.0.13/")
        );
        if (url.endsWith("SHA256SUMS.txt")) return new Response(`${checksum}  ${asset}\n`);
        assert.ok(url.endsWith(asset));
        return new Response(archive.slice());
      };
      try {
        await Promise.all([ensureTunnelClientInstalled(), ensureTunnelClientInstalled()]);
        assert.equal(downloads, 2, "one serialized install owns one archive/checksum pair");
        assert.deepEqual(new Uint8Array(await readFile(paths.binary)), nextBinary);
        const manifest = JSON.parse(await readFile(paths.manifest, "utf8")) as {
          tunnelClientVersion: string;
          archiveSha256: string;
          binarySha256: string;
        };
        assert.equal(manifest.tunnelClientVersion, "0.0.13");
        assert.equal(manifest.archiveSha256, checksum);
        assert.equal(manifest.binarySha256, createHash("sha256").update(nextBinary).digest("hex"));
        downloads = 0;
        await ensureTunnelClientInstalled();
        assert.equal(downloads, 0, "trusted current version is reused");
        await atomicWriteFileAsync(paths.binary, "tampered");
        await assert.rejects(ensureTunnelClientInstalled(), /integrity validation/);
        assert.equal(downloads, 0);
        await atomicWriteFileAsync(paths.binary, oldBinary);
        await (await import("node:fs/promises")).chmod(paths.binary, 0o700);
        await atomicWriteFileAsync(paths.manifest, oldManifest);
        globalThis.fetch = async (input) => {
          if (String(input).endsWith("SHA256SUMS.txt"))
            return new Response(`${"0".repeat(64)}  ${asset}\n`);
          return new Response(archive.slice());
        };
        await assert.rejects(ensureTunnelClientInstalled(), /Checksum mismatch/);
        assert.deepEqual(new Uint8Array(await readFile(paths.binary)), oldBinary);
        assert.equal(await readFile(paths.manifest, "utf8"), oldManifest);
        let obstructed = false;
        globalThis.fetch = async (input) => {
          if (!obstructed) {
            obstructed = true;
            await rm(paths.manifest);
            await (await import("node:fs/promises")).mkdir(paths.manifest);
          }
          if (String(input).endsWith("SHA256SUMS.txt"))
            return new Response(`${checksum}  ${asset}\n`);
          return new Response(archive.slice());
        };
        await assert.rejects(ensureTunnelClientInstalled());
        assert.deepEqual(
          new Uint8Array(await readFile(paths.binary)),
          oldBinary,
          "binary rollback runs before obstructed manifest restoration"
        );
        await rm(paths.manifest, { recursive: true });
        await atomicWriteFileAsync(paths.manifest, oldManifest);
      } finally {
        globalThis.fetch = originalFetch;
      }
    }
  );

  await t.test(
    "real ZIP worker handles incompressible >512KiB through source entry selection",
    async () => {
      const binary = randomBytes(2 * 1024 * 1024);
      const archive = zipSync({
        "nested/tunnel-client": binary,
        ignored: new Uint8Array([1, 2]),
      });
      const expected = createHash("sha256").update(binary).digest("hex");
      let completed = false;
      const work = extractTunnelBinaryAsync(archive, "tunnel-client").then((result) => {
        completed = true;
        return result;
      });
      await new Promise<void>((resolveTick) => setImmediate(resolveTick));
      assert.equal(completed, false, "archive operation is off the main thread");
      const actual = await work;
      assert.equal(createHash("sha256").update(actual).digest("hex"), expected);
      await assert.rejects(
        extractTunnelBinaryAsync(zipSync({ other: binary }), "tunnel-client"),
        /does not contain/
      );
      // Direct source synchronous extraction cannot satisfy the pending-at-next-tick oracle.
      let directCompleted = false;
      const original = Promise.resolve().then(() => {
        unzipSync(zipSync({ "tunnel-client": binary }));
        directCompleted = true;
      });
      await new Promise<void>((resolveTick) => setImmediate(resolveTick));
      assert.equal(directCompleted, true);
      await original;
    }
  );

  await t.test(
    "resolvers accept packaged workers and reject missing artifact without fallback",
    async () => {
      assert.match((await resolveResponseStateWorker()).workerFile, /stateWorker\.(js|ts)$/);
      assert.match((await resolveTunnelZipWorker()).workerFile, /tunnelZipWorker\.(js|ts)$/);
      const missing = {
        moduleDir: join(home, "missing"),
        cwd: join(home, "missing"),
        entryFile: "",
      };
      await assert.rejects(resolveResponseStateWorker(missing), /unavailable/);
      await assert.rejects(resolveTunnelZipWorker(missing), /unavailable/);
    }
  );

  await t.test(
    "native SSE waits for cache ACK without watchdog failure or later cancellation terminal",
    async () => {
      let finish!: () => void;
      const ack = new Promise<void>((resolveAck) => {
        finish = resolveAck;
      });
      async function* events(): AsyncGenerator<AdapterEvent> {
        yield { type: "done" };
      }
      const stream = bridgeToResponsesSSE(
        events(),
        "test",
        undefined,
        undefined,
        undefined,
        undefined,
        5,
        { stallTimeoutSec: 1, onCompletedResponseAsync: () => ack }
      );
      const reader = stream.getReader();
      const decoder = new TextDecoder();
      let text = "";
      let terminal = false;
      const consuming = (async () => {
        for (;;) {
          const next = await reader.read();
          if (next.done) break;
          text += decoder.decode(next.value);
          terminal = text.includes('"type":"response.completed"');
        }
      })();
      await delay(1_100);
      assert.equal(terminal, false);
      assert.equal(text.includes("response.failed"), false);
      finish();
      await consuming;
      assert.equal(terminal, true);
      assert.equal((text.match(/event: response.completed/g) ?? []).length, 1);

      let finishCancelled!: () => void;
      const cancelledAck = new Promise<void>((resolveAck) => {
        finishCancelled = resolveAck;
      });
      let terminalCalls = 0;
      const cancelled = bridgeToResponsesSSE(
        events(),
        "test",
        undefined,
        undefined,
        undefined,
        undefined,
        5,
        {
          onCompletedResponseAsync: () => cancelledAck,
          onTerminal: () => terminalCalls++,
        }
      );
      const cancelledReader = cancelled.getReader();
      await cancelledReader.read();
      const pending = cancelledReader.read();
      await delay(15);
      await cancelledReader.cancel();
      finishCancelled();
      await pending;
      await delay(10);
      assert.equal(terminalCalls, 0);
    }
  );
  await t.test(
    "closed cache admission cannot resurrect a worker from a late terminal",
    async () => {
      await closeResponseStateWorker();
      await assert.rejects(
        runResponseStateOperation({
          kind: "lookup",
          responseId: "resp_native",
          namespace: "account:thread:turn",
        }),
        /unavailable/
      );
      await closeResponseStateWorker();
    }
  );
});
