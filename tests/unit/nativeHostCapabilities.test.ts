import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { probeNativeHostCapability, stopNativeHostProbes } from "../../src/lib/nativeHostProbes";
import {
  BUILT_IN_AGENT_IDS,
  getRegisteredAgentById,
  setCustomAgents,
  detectInstalledAgentsAsync,
  refreshAgentCacheAsync,
  resolveVersionProbe,
} from "../../src/lib/acp/registry";

const directory = mkdtempSync(join(tmpdir(), "omniroute-native-probes-"));
const originalPath = process.env.PATH;
function executable(name: string, body: string): string {
  const file = join(directory, name);
  writeFileSync(file, `#!${process.execPath}\n${body}\n`, { mode: 0o700 });
  return file;
}
test.after(async () => {
  await stopNativeHostProbes();
  if (originalPath === undefined) delete process.env.PATH;
  else process.env.PATH = originalPath;
  rmSync(directory, { recursive: true, force: true });
});

test("async version probes observe stdin EOF, UTF-8, exit status and event-loop progress", async () => {
  const file = executable(
    "eof",
    `process.stdin.resume(); process.stdin.on("end", () => setTimeout(() => {
    const value = Buffer.from("agent café v1.2.3-beta\\n");
    process.stdout.write(value.subarray(0, 10)); process.stdout.write(value.subarray(10));
  }, 60));`
  );
  let ticks = 0;
  const interval = setInterval(() => {
    ticks += 1;
  }, 1);
  try {
    const result = await probeNativeHostCapability(file, ["--version"]);
    assert.equal(result.success, true);
    assert.equal(result.stdout, "agent café v1.2.3-beta\n");
    assert.ok(ticks > 0, "main event loop advances while the child awaits input/end and a timer");
    const failure = await probeNativeHostCapability(executable("failure", "process.exit(3);"), []);
    assert.equal(failure.success, false);
    assert.equal(failure.failure, "failed");
  } finally {
    clearInterval(interval);
  }
});

test("native probes cap each piped output, ignore lookup output, and join a hard timeout close", async () => {
  const huge = executable("huge", "process.stdout.write(Buffer.alloc(1100000, 120));");
  assert.equal((await probeNativeHostCapability(huge, [])).failure, "output_limit");
  assert.equal((await probeNativeHostCapability(huge, [], { ignoreOutput: true })).success, true);
  const stderr = executable("huge-stderr", "process.stderr.write(Buffer.alloc(1100000, 120));");
  assert.equal((await probeNativeHostCapability(stderr, [])).failure, "output_limit");
  const hang = executable("hang", 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000);');
  const started = Date.now();
  const result = await probeNativeHostCapability(hang, ["--version"]);
  assert.equal(result.failure, "timeout");
  assert.equal(result.success, false);
  assert.ok(Date.now() - started >= 4500, "actual default five-second probe deadline was used");
});

test("ACP async detection preserves validated definitions/order/version and fences stale scans", async () => {
  process.env.PATH = directory;
  for (const id of BUILT_IN_AGENT_IDS) {
    const definition = getRegisteredAgentById(id)!;
    executable(definition.binary, 'console.log("agent v2.3.4-dev");');
  }
  const old = executable("old-agent", 'setTimeout(() => console.log("old 1.2.3"), 100);');
  const fresh = executable("fresh-agent", 'console.log("fresh 3.4.5");');
  const base = {
    name: "fixture",
    providerAlias: "fixture",
    spawnArgs: [],
    protocol: "stdio" as const,
  };
  setCustomAgents([{ ...base, id: "old", binary: old, versionCommand: `"${old}" --version` }]);
  const older = detectInstalledAgentsAsync();
  setCustomAgents([
    { ...base, id: "fresh", binary: fresh, versionCommand: `"${fresh}" --version` },
  ]);
  const current = await detectInstalledAgentsAsync();
  await older;
  assert.deepEqual(
    current.slice(0, BUILT_IN_AGENT_IDS.length).map((agent) => agent.id),
    BUILT_IN_AGENT_IDS
  );
  assert.ok(
    current
      .slice(0, BUILT_IN_AGENT_IDS.length)
      .every((agent) => agent.installed && agent.version === "2.3.4-dev")
  );
  assert.equal(current.at(-1)?.version, "3.4.5");
  assert.equal((await detectInstalledAgentsAsync()).at(-1)?.id, "fresh");
  assert.equal(
    await detectInstalledAgentsAsync(),
    current,
    "source TTL cache reuses the published array"
  );
  const marker = join(directory, "unsafe-executed");
  const unsafe = executable(
    "unsafe",
    `require("node:fs").writeFileSync(${JSON.stringify(marker)}, "bad");`
  );
  assert.equal(resolveVersionProbe(unsafe, `"${unsafe}" -e payload`, true), null);
  setCustomAgents([
    { ...base, id: "unsafe", binary: unsafe, versionCommand: `"${unsafe}" -e payload` },
  ]);
  const rejected = await refreshAgentCacheAsync();
  assert.equal(rejected.at(-1)?.installed, false);
  assert.equal(existsSync(marker), false, "unrecognized custom argv never reaches a child");
});

test("shutdown closes new probe admission and waits for the child's close event", async () => {
  const waiting = executable("waiting", "setInterval(() => {}, 1000);");
  const pending = probeNativeHostCapability(waiting, ["--version"]);
  await new Promise((resolve) => setTimeout(resolve, 30));
  await stopNativeHostProbes();
  assert.equal((await pending).success, false);
  assert.equal((await probeNativeHostCapability(waiting, [])).failure, "closed");
});
