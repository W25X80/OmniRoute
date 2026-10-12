import assert from "node:assert/strict";
import type { ChildProcess, SpawnOptions } from "node:child_process";
import { AsyncResource } from "node:async_hooks";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { createRequire, registerHooks, syncBuiltinESMExports } from "node:module";
import { mkdtemp, mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fixture } from "../../helpers/persistence/runtimeOwnedWorkFixture.ts";
import { headroomFixture } from "../../helpers/persistence/nativeHeadroomFixture.ts";

const require = createRequire(import.meta.url);
const childProcess = require("node:child_process") as typeof import("node:child_process");
const asyncFs = require("node:fs/promises") as typeof import("node:fs/promises");
const runtimeUrl = import.meta.resolve("../../../src/lib/db/persistenceRuntime.ts");
const fixtureUrl = import.meta.resolve("../../helpers/persistence/runtimeOwnedWorkFixture.ts");
const headroomFixtureUrl = import.meta
  .resolve("../../helpers/persistence/nativeHeadroomFixture.ts");
const statusUrl = import.meta.resolve("../../../src/app/api/headroom/status/route.ts");
const startUrl = import.meta.resolve("../../../src/app/api/headroom/start/route.ts");
const runtimeDependencies = new Set([
  "./postgres/environment",
  "./postgres/controlPlane",
  "../artifacts/s3ArtifactConfiguration",
  "../artifacts/s3ArtifactStore",
  "@/shared/utils/circuitBreaker",
  "@/domain/quotaCache",
  "./postgres/catalogSnapshot",
]);
const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (context.parentURL?.split("?")[0] === runtimeUrl && runtimeDependencies.has(specifier))
      return { url: fixtureUrl, shortCircuit: true };
    if (
      [statusUrl, startUrl].includes(context.parentURL?.split("?")[0] ?? "") &&
      specifier === "@/lib/db/settings"
    )
      return { url: headroomFixtureUrl, shortCircuit: true };
    return next(specifier, context);
  },
});

async function waitFor(predicate: () => Promise<boolean> | boolean): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error("Controlled Headroom did not reach its barrier");
    await delay(5);
  }
}
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

test(
  "native Headroom keeps async admission, authority and owned child/FD shutdown",
  { timeout: 45000 },
  async (t) => {
    const directory = await mkdtemp(
      join(process.env.HEADROOM_TEST_DIR || tmpdir(), "omniroute-native-headroom-")
    );
    const previous = Object.fromEntries(
      [
        "OMNIROUTE_DATABASE_BACKEND",
        "DATA_DIR",
        "HEADROOM_PYTHON",
        "OMNIROUTE_BUILDING",
        "STORAGE_ENCRYPTION_KEY",
        "JWT_SECRET",
        "API_KEY_SECRET",
      ].map((key) => [key, process.env[key]])
    );
    const originalRuntime = globalThis.__omniroutePersistenceRuntime;
    const ownerSymbol = Symbol.for("omniroute.nativeHeadroomProcess");
    const globals = globalThis as typeof globalThis & { [ownerSymbol]?: unknown };
    const oldHeadroom = globals[ownerSymbol];
    delete globals[ownerSymbol];
    delete globalThis.__omniroutePersistenceRuntime;
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    delete process.env.OMNIROUTE_BUILDING;
    process.env.STORAGE_ENCRYPTION_KEY = "isolated-native-headroom-fixture-key";
    process.env.DATA_DIR = join(directory, "data");
    process.env.HEADROOM_PYTHON = join(directory, "controlled-python");
    fixture.reset();
    const runner = join(directory, "controlled.cjs");
    const binary = join(directory, "controlled-headroom");
    const ready = join(directory, "ready"),
      term = join(directory, "term"),
      release = join(directory, "release");
    await writeFile(
      runner,
      String.raw`const fs = require("node:fs");
const mode = process.argv[2];
if (mode === "which") { process.stdout.write(process.env.HEADROOM_TEST_BINARY + "\n"); }
else if (mode === "python") { process.stdout.write("Python 3.12.7\n"); }
else if (mode === "exit") { process.exitCode = 1; }
else {
 fs.writeFileSync(process.env.HEADROOM_TEST_READY, String(process.pid));
 const timer = setInterval(() => {
   if (fs.existsSync(process.env.HEADROOM_TEST_RELEASE)) { clearInterval(timer); process.exit(0); }
 }, 5);
 process.on("SIGTERM", () => fs.writeFileSync(process.env.HEADROOM_TEST_TERM, "term"));
}`
    );
    const spawn = childProcess.spawn.bind(childProcess);
    const calls: Array<{ command: string; args: readonly string[]; options: SpawnOptions }> = [];
    const children: ChildProcess[] = [];
    const closedChildren = new Set<ChildProcess>();
    const childJoins = new Map<ChildProcess, Promise<void>>();
    function track(child: ChildProcess): ChildProcess {
      children.push(child);
      childJoins.set(
        child,
        new Promise<void>((done) =>
          child.once("close", () => {
            closedChildren.add(child);
            done();
          })
        )
      );
      return child;
    }
    const handles: Array<Awaited<ReturnType<typeof asyncFs.open>>> = [];
    let hangDetection = false;
    let earlyExitStartup = false;
    t.mock.method(
      childProcess,
      "spawn",
      (command: string, args: readonly string[], options: SpawnOptions = {}) => {
        calls.push({ command, args, options });
        if (command === "headroom-cap-fixture") {
          const child = new EventEmitter() as EventEmitter & {
            stdin: PassThrough;
            stdout: PassThrough;
            stderr: PassThrough;
            kill(): boolean;
          };
          child.stdin = new PassThrough();
          child.stdout = new PassThrough();
          child.stderr = new PassThrough();
          let closed = false;
          child.kill = () => {
            if (!closed) {
              closed = true;
              queueMicrotask(() => child.emit("close", null));
            }
            return true;
          };
          return child as unknown as ChildProcess;
        }
        const mapped =
          command === "which"
            ? hangDetection
              ? "hang"
              : "which"
            : args.length === 1 && args[0] === "--version"
              ? "python"
              : command === binary
                ? earlyExitStartup
                  ? "exit"
                  : "daemon"
                : null;
        const child = mapped
          ? spawn(process.execPath, [runner, mapped, ...args], {
              ...options,
              env: {
                ...options.env,
                HEADROOM_TEST_BINARY: binary,
                HEADROOM_TEST_READY: ready,
                HEADROOM_TEST_TERM: term,
                HEADROOM_TEST_RELEASE: release,
              },
            })
          : spawn(command, [...args], options);
        return track(child);
      }
    );
    const open = asyncFs.open.bind(asyncFs);
    t.mock.method(asyncFs, "open", async (...args: Parameters<typeof asyncFs.open>) => {
      const handle = await open(...args);
      handles.push(handle);
      return handle;
    });
    syncBuiltinESMExports();
    const runtime = await import("../../../src/lib/db/persistenceRuntime.ts");
    const native = await import("../../../src/lib/headroom/nativeProcess.ts");
    const detection = await import("../../../src/lib/headroom/detect.ts");
    const probes = await import("../../../src/lib/nativeHostProbes.ts");
    const status = await import("../../../src/app/api/headroom/status/route.ts");
    const start = await import("../../../src/app/api/headroom/start/route.ts");
    const exists = async (file: string) => {
      try {
        await readFile(file);
        return true;
      } catch {
        return false;
      }
    };
    try {
      await runtime.ensurePersistenceReady();
      await t.test(
        "status and external-sidecar refusal use native probes without a synchronous CLI call",
        async () => {
          let synchronous = 0;
          const trap = t.mock.method(childProcess, "execFileSync", () => {
            synchronous++;
            throw new Error("sync CLI forbidden in native status");
          });
          syncBuiltinESMExports();
          const fetch = t.mock.method(
            globalThis,
            "fetch",
            async () => new Response("healthy", { status: 200 })
          );
          try {
            await runtime.runPersistenceRequest(async () => {
              const response = await status.GET();
              assert.equal(synchronous, 0);
              assert.equal(response.status, 200);
              const body = await response.json();
              assert.equal(body.installed, true);
              assert.equal(body.python, process.env.HEADROOM_PYTHON);
              assert.equal(body.running, true);
              assert.equal(body.canStart, true);
              const python = calls.find((call) => call.command === process.env.HEADROOM_PYTHON)!;
              assert.equal(python.options.env?.PATH, detection.buildPythonSearchPath());
              assert.deepEqual(python.args, ["--version"]);
              headroomFixture.url = "http://headroom:8787";
              const before = calls.length;
              const refused = await start.POST();
              assert.equal(refused.status, 400);
              assert.equal(calls.length, before);
            });
          } finally {
            trap.mock.restore();
            syncBuiltinESMExports();
            fetch.mock.restore();
            headroomFixture.url = "http://localhost:8787";
          }
        }
      );
      await t.test(
        "probe cancellation joins the actual child before returning and the event loop stays live",
        async () => {
          await rm(ready, { force: true });
          hangDetection = true;
          const signal = new AbortController();
          let heartbeat = false;
          const work = runtime.runPersistenceRequest(() =>
            detection.findHeadroomBinaryAsync({ signal: signal.signal })
          );
          void work.catch(() => {});
          await new Promise<void>((done) =>
            setImmediate(() => {
              heartbeat = true;
              done();
            })
          );
          await waitFor(() => exists(ready));
          const pid = Number(await readFile(ready, "utf8"));
          signal.abort(new Error("controlled cancellation"));
          await assert.rejects(work, /controlled cancellation/);
          assert.equal(heartbeat, true);
          assert.equal(alive(pid), false);
          hangDetection = false;
        }
      );
      await t.test("finite probe timeout joins a real hung executable", async () => {
        await rm(ready, { force: true });
        const work = runtime.runPersistenceRequest(() =>
          probes.probeNativeHostCapability(process.execPath, [runner, "hang"], {
            timeoutMs: 1000,
            env: {
              ...process.env,
              HEADROOM_TEST_READY: ready,
              HEADROOM_TEST_TERM: term,
              HEADROOM_TEST_RELEASE: release,
            },
          })
        );
        await waitFor(() => exists(ready));
        const result = await work;
        assert.equal(result.failure, "timeout");
        assert.equal(result.success, false);
        assert.equal(closedChildren.has(children.at(-1)!), true);
      });
      await t.test(
        "shared probe admission is capped before another executable can start",
        async () => {
          await runtime.runPersistenceRequest(async () => {
            const controllers = Array.from({ length: 64 }, () => new AbortController());
            const admitted = controllers.map((controller) =>
              probes.probeNativeHostCapability("headroom-cap-fixture", [], {
                signal: controller.signal,
              })
            );
            try {
              const before = calls.length;
              assert.deepEqual(await probes.probeNativeHostCapability("headroom-cap-fixture", []), {
                success: false,
                stdout: "",
                failure: "busy",
              });
              assert.equal(calls.length, before);
            } finally {
              controllers.forEach((controller) => controller.abort());
              await Promise.all(admitted);
            }
          });
        }
      );
      await t.test("stale authority fails before subprocess admission", async () => {
        let stale!: AsyncResource;
        await runtime.runPersistenceRequest(async () => {
          stale = new AsyncResource("headroom-stale-fixture");
        });
        const before = calls.length;
        try {
          await assert.rejects(
            stale.runInAsyncScope(() => detection.findHeadroomBinaryAsync()),
            /scope.*ended|scope ended/
          );
        } finally {
          stale.emitDestroy();
        }
        assert.equal(calls.length, before);
      });
      await t.test(
        "a probe completed after its caller scope ended cannot publish its result",
        async () => {
          await rm(ready, { force: true });
          hangDetection = true;
          let late!: Promise<string | null>;
          try {
            await runtime.runPersistenceRequest(async () => {
              late = detection.findHeadroomBinaryAsync();
              void late.catch(() => {});
              await waitFor(() => exists(ready));
            });
            await writeFile(release, "close the already accepted probe");
            await assert.rejects(late, /scope.*ended|scope ended/);
          } finally {
            hangDetection = false;
            await rm(release, { force: true });
          }
        }
      );
      await t.test("native operation admission is finite", async () => {
        await runtime.runPersistenceRequest(async () => {
          const admitted = Array.from({ length: 16 }, () => native.getManagedHeadroomPidAsync());
          await assert.rejects(native.getManagedHeadroomPidAsync(), { code: "BUSY" });
          assert.equal(
            (await Promise.all(admitted)).every((pid) => pid === null),
            true
          );
        });
      });
      await t.test(
        "explicit stop waits for an adopted process without claiming application ownership",
        async () => {
          const adoptedReady = join(directory, "manual-ready"),
            adoptedRelease = join(directory, "manual-release"),
            adoptedTerm = join(directory, "manual-term");
          const adopted = track(
            spawn(process.execPath, [runner, "adopted"], {
              env: {
                ...process.env,
                HEADROOM_TEST_READY: adoptedReady,
                HEADROOM_TEST_TERM: adoptedTerm,
                HEADROOM_TEST_RELEASE: adoptedRelease,
              },
              stdio: "ignore",
            })
          );
          await waitFor(() => exists(adoptedReady));
          const pidFile = join(process.env.DATA_DIR!, "headroom/proxy.pid");
          await mkdir(join(process.env.DATA_DIR!, "headroom"), { recursive: true });
          await writeFile(pidFile, String(adopted.pid));
          let settled = false;
          const stopping = runtime
            .runPersistenceRequest(() => native.stopHeadroomProxyAsync())
            .then((result) => {
              settled = true;
              return result;
            });
          await waitFor(() => exists(adoptedTerm));
          assert.equal(settled, false);
          await writeFile(adoptedRelease, "release adopted process exit");
          assert.deepEqual(await stopping, { stopped: true, pid: adopted.pid });
          assert.equal(alive(adopted.pid!), false);
          assert.equal(await exists(pidFile), false);
        }
      );
      await t.test(
        "canceled startup kills and joins its own child and closes the parent log handle",
        async () => {
          await rm(ready, { force: true });
          const abort = new AbortController();
          const work = runtime.runPersistenceRequest(() =>
            native.startHeadroomProxyAsync({ signal: abort.signal })
          );
          void work.catch(() => {});
          await waitFor(() => exists(ready));
          const pid = Number(await readFile(ready, "utf8"));
          abort.abort(new Error("cancel startup"));
          await assert.rejects(
            work,
            (error: unknown) =>
              error instanceof Error &&
              error.name === "AbortError" &&
              error.cause === abort.signal.reason
          );
          assert.equal(alive(pid), false);
          assert.equal(await exists(join(process.env.DATA_DIR!, "headroom/proxy.pid")), false);
          assert.equal(handles.length > 0, true);
          assert.equal(
            handles.every((handle) => handle.fd === -1),
            true
          );
        }
      );
      await t.test(
        "early startup failure closes its actual child and parent log handle",
        async () => {
          earlyExitStartup = true;
          try {
            await assert.rejects(
              runtime.runPersistenceRequest(() => native.startHeadroomProxyAsync()),
              { code: "EARLY_EXIT" }
            );
            assert.equal(closedChildren.has(children.at(-1)!), true);
            assert.equal(await exists(join(process.env.DATA_DIR!, "headroom/proxy.pid")), false);
            assert.equal(
              handles.every((handle) => handle.fd === -1),
              true
            );
          } finally {
            earlyExitStartup = false;
          }
        }
      );
      await t.test(
        "shutdown drains admitted startup, joins its owned daemon and preserves replacement adopted PID",
        async () => {
          await rm(ready, { force: true });
          const starting = runtime.runPersistenceRequest(() => native.startHeadroomProxyAsync());
          void starting.catch(() => {});
          await waitFor(() => exists(ready));
          const ownedPid = Number(await readFile(ready, "utf8"));
          const adoptedReady = join(directory, "adopted-ready"),
            adoptedRelease = join(directory, "adopted-release"),
            adoptedTerm = join(directory, "adopted-term");
          const adopted = track(
            spawn(process.execPath, [runner, "adopted"], {
              env: {
                ...process.env,
                HEADROOM_TEST_READY: adoptedReady,
                HEADROOM_TEST_TERM: adoptedTerm,
                HEADROOM_TEST_RELEASE: adoptedRelease,
              },
              stdio: "ignore",
            })
          );
          await waitFor(() => exists(adoptedReady));
          await writeFile(join(process.env.DATA_DIR!, "headroom/proxy.pid"), String(adopted.pid));
          native.beginNativeHeadroomClose();
          await runtime.runPersistenceRequest(async () => {
            await assert.rejects(native.getManagedHeadroomPidAsync(), { code: "STOPPING" });
          });
          let closed = false;
          const closing = native.stopNativeHeadroomProcesses().then(() => {
            closed = true;
          });
          await new Promise<void>((done) => setImmediate(done));
          assert.equal(closed, false, "accepted startup must drain before child teardown");
          assert.equal(alive(ownedPid), true);
          assert.equal(
            handles.some((handle) => handle.fd !== -1),
            true
          );
          const accepted = await starting;
          assert.equal(accepted.pid, ownedPid);
          assert.equal(accepted.alreadyRunning, false);
          assert.equal(
            handles.every((handle) => handle.fd === -1),
            true
          );
          await waitFor(() => exists(term));
          assert.equal(closed, false, "sending SIGTERM is not a child close acknowledgement");
          assert.equal(alive(adopted.pid!), true);
          await writeFile(release, "release actual owned child exit");
          await closing;
          assert.equal(alive(accepted.pid), false);
          assert.equal(alive(adopted.pid!), true);
          assert.equal(
            await readFile(join(process.env.DATA_DIR!, "headroom/proxy.pid"), "utf8"),
            String(adopted.pid)
          );
          assert.equal(fixture.queries, 0);
        }
      );
    } finally {
      await writeFile(release, "cleanup");
      for (const child of children) {
        if (!closedChildren.has(child)) child.kill("SIGKILL");
      }
      await Promise.all(childJoins.values());
      await native.stopNativeHeadroomProcesses().catch(() => {});
      await probes.stopNativeHostProbes();
      await runtime.closePersistenceRuntime();
      hooks.deregister();
      t.mock.restoreAll();
      syncBuiltinESMExports();
      globalThis.__omniroutePersistenceRuntime = originalRuntime;
      if (oldHeadroom === undefined) delete globals[ownerSymbol];
      else globals[ownerSymbol] = oldHeadroom;
      for (const [key, value] of Object.entries(previous)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      await rm(directory, { recursive: true, force: true });
    }
  }
);
