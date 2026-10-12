import {
  DEFAULT_COMPRESSION_CONFIG,
  DEFAULT_RTK_CONFIG,
} from "../../../open-sse/services/compression/types";
import type {
  CompressionResult,
  CompressionEngineId,
} from "../../../open-sse/services/compression/types";
import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import files from "node:fs/promises";
import { registerHooks } from "node:module";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { setImmediate as yieldToEventLoop } from "node:timers/promises";
import { workerFixture } from "../../helpers/persistence/nativeCompressionWorkerFixture";

const hooks = registerHooks({
  resolve(specifier, context, next) {
    if (
      context.parentURL?.endsWith("/compression/strategySelector.ts") &&
      specifier === "./compressionWorkerPool.ts"
    ) {
      return {
        url: new URL("../../helpers/persistence/nativeCompressionWorkerFixture.ts", import.meta.url)
          .href,
        shortCircuit: true,
      };
    }
    return next(specifier, context);
  },
});
const { applyCompressionAsync, applyCompression } =
  await import("../../../open-sse/services/compression/strategySelector");
const { cavemanCompress, cavemanCompressAsync } =
  await import("../../../open-sse/services/compression/caveman");
const { applyRtkCompression, applyRtkCompressionAsync } =
  await import("../../../open-sse/services/compression/engines/rtk/index");
const { loadRtkFiltersAsync, matchPreparedRtkFilter } =
  await import("../../../open-sse/services/compression/engines/rtk/filterLoader");
const { registerEngine, unregisterCompressionEngine } =
  await import("../../../open-sse/services/compression/engines/registry");
const { resetRtkRawOutputPurgeThrottle, closeRtkRawOutputPurgeScheduler } =
  await import("../../../open-sse/services/compression/engines/rtk/rawOutput");

test.after(() => hooks.deregister());
function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
function normalize(value: CompressionResult): CompressionResult {
  const stats = value.stats
    ? JSON.parse(
        JSON.stringify(value.stats, (key, entry) =>
          key === "timestamp" || /(?:[Dd]uration|[Ll]atency|[Tt]ime)[Mm]s$/.test(key) ? 0 : entry
        )
      )
    : null;
  return { ...value, stats };
}

function noSyncFiles(t: test.TestContext) {
  let calls = 0;
  for (const name of [
    "existsSync",
    "statSync",
    "readdirSync",
    "readFileSync",
    "mkdirSync",
    "writeFileSync",
  ] as const) {
    const original = fs[name];
    t.mock.method(fs, name, (...args: unknown[]) => {
      if (/(?:filterLoader|rawOutput|ruleLoader)\.(?:ts|js):/.test(new Error().stack ?? "")) {
        calls++;
        throw new Error("Unexpected synchronous compression filesystem operation");
      }
      return Reflect.apply(original, fs, args);
    });
  }
  return () => calls;
}
async function withNative(t: test.TestContext, run: (directory: string) => Promise<void>) {
  const symbols = [
    Symbol.for("omniroute.rtkRawOutputPurge"),
    Symbol.for("omniroute.rtkRawOutputPurgeAt"),
  ];
  const priorPurge = symbols.map((symbol) => Object.getOwnPropertyDescriptor(globalThis, symbol));
  for (const symbol of symbols) Reflect.deleteProperty(globalThis, symbol);
  const directory = await files.mkdtemp(path.join(os.tmpdir(), "native-inline-compression-"));
  const old = new Map(
    ["DATA_DIR", "OMNIROUTE_DATABASE_BACKEND", "APP_LOG_TO_FILE"].map((name) => [
      name,
      process.env[name],
    ])
  );
  process.env.DATA_DIR = directory;
  process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
  process.env.APP_LOG_TO_FILE = "false";
  try {
    await run(directory);
  } finally {
    await closeRtkRawOutputPurgeScheduler();
    t.mock.restoreAll();
    resetRtkRawOutputPurgeThrottle();
    for (const [name, value] of old)
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    await files.rm(directory, { recursive: true, force: true });
    symbols.forEach((symbol, index) => {
      if (priorPurge[index]) Object.defineProperty(globalThis, symbol, priorPurge[index]!);
      else Reflect.deleteProperty(globalThis, symbol);
    });
  }
}
function toolBody() {
  return {
    messages: [
      {
        role: "assistant",
        tool_calls: [
          {
            id: "t",
            function: { name: "bash", arguments: JSON.stringify({ command: "customcmd" }) },
          },
        ],
      },
      {
        role: "tool",
        tool_call_id: "t",
        content: "noise repeated\n".repeat(30) + "KEEP valuable result\n",
      },
    ],
  };
}
async function installFilter(directory: string, id = "global-filter") {
  const filter = {
    id,
    label: id,
    category: "generic",
    priority: 100,
    match: { outputTypes: [], commands: ["^customcmd\\b"], patterns: ["KEEP"] },
    rules: { dropPatterns: ["^noise"], maxLines: 20, headLines: 10, tailLines: 10 },
    preserve: { errorPatterns: ["error"], summaryPatterns: [] },
    tests: [{ name: "sample", input: "noise\nKEEP", expected: "KEEP" }],
  };
  const filename = path.join(directory, "rtk", "filters.json");
  await files.mkdir(path.dirname(filename), { recursive: true });
  await files.writeFile(filename, JSON.stringify(filter));
}

test("cold EN Caveman prepares real rule files asynchronously while retaining output and rule policy", async (t) =>
  withNative(t, async () => {
    const body = {
      messages: [
        {
          role: "user",
          content:
            "Could you please provide a detailed explanation of the architecture? Thank you so much.".repeat(
              3
            ),
        },
      ],
    };
    const options = { enabled: true, minMessageLength: 1, intensity: "full" as const };
    const entered = barrier();
    const release = barrier();
    const original = files.readFile;
    let reads = 0;
    t.mock.method(files, "readFile", async (...args: Parameters<typeof files.readFile>) => {
      if (String(args[0]).includes(path.join("compression", "rules"))) {
        reads++;
        entered.resolve();
        await release.promise;
      }
      return Reflect.apply(original, files, args);
    });
    const calls = noSyncFiles(t);
    const pending = cavemanCompressAsync(body, options);
    try {
      await Promise.race([entered.promise, pending]);
      await yieldToEventLoop();
      assert.ok(reads > 0);
      release.resolve();
      const actual = await pending;
      assert.equal(calls(), 0);
      t.mock.restoreAll();
      const expected = cavemanCompress(body, options);
      assert.deepEqual(normalize(actual), normalize(expected));
      assert.equal(actual.compressed, true);
    } finally {
      release.resolve();
    }
  }));

test("native RTK awaits retained output, preserves source receipts, and omits failed capture receipts", async (t) =>
  withNative(t, async (directory) => {
    await installFilter(directory);
    t.mock.method(Date, "now", () => 1700000000000);
    const body = toolBody();
    const config = { enabled: true, rawOutputRetention: "always" as const };
    const expected = applyRtkCompression(body, { config });
    const calls = noSyncFiles(t);
    const actual = await applyRtkCompressionAsync(body, { config });
    assert.deepEqual(normalize(actual), normalize(expected));
    assert.equal(calls(), 0);
    const pointer = actual.stats?.rtkRawOutputPointers?.[0];
    assert.ok(pointer);
    assert.equal(await files.readFile(pointer.path, "utf8"), body.messages[1].content);
    const write = files.writeFile;
    t.mock.method(files, "writeFile", async (...args: Parameters<typeof files.writeFile>) => {
      if (String(args[0]).endsWith(".log")) throw new Error("Controlled unavailable file");
      return Reflect.apply(write, files, args);
    });
    const failed = await applyRtkCompressionAsync(body, { config });
    assert.deepEqual(failed.body, expected.body);
    assert.equal(failed.stats?.rtkRawOutputPointers, undefined);
    assert.ok(!failed.stats?.techniquesUsed.includes("rtk-raw-output-retention"));
    assert.equal(calls(), 0);
  }));

test("successful worker avoids inline preparation; fast fault retains native compression and timeout does not retry", async (t) =>
  withNative(t, async (directory) => {
    await installFilter(directory);
    const body = toolBody();
    const calls = noSyncFiles(t);
    const read = files.readFile;
    let inlineReads = 0;
    t.mock.method(files, "readFile", async (...args: Parameters<typeof files.readFile>) => {
      inlineReads++;
      return Reflect.apply(read, files, args);
    });
    workerFixture.mode = "success";
    workerFixture.calls = 0;
    const bailout = { enabled: true, minGainPercent: 0 };
    const accepted = await applyCompressionAsync(body, "rtk", { bailout });
    assert.equal(accepted.body.workerReceipt, true);
    assert.equal(inlineReads, 0);
    assert.deepEqual(workerFixture.options?.bailout, bailout);
    workerFixture.mode = "fast";
    const recovered = await applyCompressionAsync(body, "rtk");
    assert.equal(recovered.compressed, true);
    workerFixture.mode = "timeout";
    const expired = await applyCompressionAsync(body, "rtk");
    assert.equal(expired.body, body);
    assert.equal(expired.stats, null);
    assert.equal(calls(), 0);
    assert.equal(workerFixture.calls, 3);
  }));

test("mixed inline stack and TV1 bailout preserve compression rather than dropping the target", async (t) =>
  withNative(t, async (directory) => {
    await installFilter(directory);
    const body = toolBody();
    const calls = noSyncFiles(t);
    registerEngine({
      id: "throwing-inline",
      name: "throwing",
      description: "fixture",
      icon: "",
      targets: ["messages"],
      stackable: true,
      stackPriority: 1,
      metadata: {
        id: "throwing-inline",
        name: "throwing",
        description: "fixture",
        inputScope: "messages",
        targetLatencyMs: 1,
        supportsPreview: true,
        stable: true,
      },
      apply() {
        throw new Error("Controlled engine failure");
      },
      compress() {
        throw new Error("Controlled engine failure");
      },
      getConfigSchema() {
        return [];
      },
      validateConfig() {
        return { valid: true, errors: [] };
      },
    });
    try {
      const options: NonNullable<Parameters<typeof applyCompressionAsync>[2]> = {
        config: {
          ...DEFAULT_COMPRESSION_CONFIG,
          stackedPipeline: [
            { engine: "throwing-inline" as CompressionEngineId },
            { engine: "rtk" },
          ],
          rtkConfig: { ...DEFAULT_RTK_CONFIG, enabled: true },
        },
        bailout: { enabled: true, minGainPercent: 0 },
      };
      const result = await applyCompressionAsync(body, "stacked", options);
      assert.equal(result.compressed, true);
      assert.ok(JSON.stringify(result.body).includes("KEEP"));
      assert.equal(calls(), 0);
    } finally {
      unregisterCompressionEngine("throwing-inline");
    }
  }));

test("prepared RTK matches the trusted exact project bytes ahead of global sources", async (t) =>
  withNative(t, async (directory) => {
    await installFilter(directory);
    const cwd = process.cwd();
    const project = path.join(directory, "project");
    await files.mkdir(path.join(project, ".rtk"), { recursive: true });
    const source = await files.readFile(path.join(directory, "rtk", "filters.json"), "utf8");
    const projectSource = source.replaceAll("global-filter", "project-filter");
    await files.writeFile(path.join(project, ".rtk", "filters.json"), projectSource);
    await files.writeFile(
      path.join(project, ".rtk", "trust.json"),
      JSON.stringify({
        filtersSha256: crypto.createHash("sha256").update(projectSource).digest("hex"),
      })
    );
    process.chdir(project);
    const calls = noSyncFiles(t);
    try {
      const filters = await loadRtkFiltersAsync({ refresh: true });
      assert.equal(
        matchPreparedRtkFilter(filters, "noise\nKEEP", "customcmd")?.id,
        "project-filter"
      );
      await files.writeFile(path.join(project, ".rtk", "filters.json"), projectSource + " ");
      const changed = await loadRtkFiltersAsync({ refresh: true });
      assert.equal(
        matchPreparedRtkFilter(changed, "noise\nKEEP", "customcmd")?.id,
        "global-filter"
      );
      assert.equal(calls(), 0);
    } finally {
      process.chdir(cwd);
    }
  }));

test("native fast-fault delegation retains QuantumLock and RiskGate output/stat contracts", async (t) =>
  withNative(t, async (directory) => {
    await installFilter(directory);
    const pem =
      "-----BEGIN PRIVATE KEY-----\nMIIBVQ0123456789abcdefBODY\n-----END PRIVATE KEY-----";
    const body = {
      messages: [
        { role: "system", content: "Agent. Session 550e8400-e29b-41d4-a716-446655440000 active." },
        {
          role: "user",
          content: `${"The quick brown fox jumps over the lazy dog. ".repeat(20)}\n${pem}`,
        },
        ...toolBody().messages,
      ],
    };
    const options = {
      config: { ...DEFAULT_COMPRESSION_CONFIG, quantumLock: { enabled: true } },
      riskGate: { enabled: true },
      cachingContext: { provider: "anthropic" },
    };
    workerFixture.mode = "fast";
    for (const mode of ["rtk", "standard"] as const) {
      const calls = noSyncFiles(t);
      const actual = await applyCompressionAsync(body, mode, options);
      assert.equal(calls(), 0);
      assert.deepEqual(workerFixture.options?.riskGate, options.riskGate);
      t.mock.restoreAll();
      const expected = applyCompression(body, mode, options);
      assert.deepEqual(normalize(actual), normalize(expected));
      assert.ok(JSON.stringify(actual.body).includes("BEGIN PRIVATE KEY"));
      assert.equal(actual.stats?.riskGate?.categories.private_key, 1);
      assert.equal(actual.stats?.quantumLock?.fragments, 1);
    }
  }));

test("native Caveman language metadata retains source ordering and overlay counts asynchronously", async (t) =>
  withNative(t, async () => {
    const { listCavemanRulePacksAsync, listCavemanRulePacks } =
      await import("../../../open-sse/services/compression/ruleLoader");
    const calls = noSyncFiles(t);
    const actual = await listCavemanRulePacksAsync();
    assert.equal(calls(), 0);
    t.mock.restoreAll();
    assert.deepEqual(actual, listCavemanRulePacks());
    assert.ok(actual.some((pack) => pack.language === "en"));
  }));

test("native cold RTK inline matching never borrows the synchronous filter loader cache", async (t) =>
  withNative(t, async (directory) => {
    await installFilter(directory);
    const body = toolBody();
    const calls = noSyncFiles(t);
    const actual = await applyRtkCompressionAsync(body, { config: { enabled: true } });
    assert.equal(calls(), 0);
    assert.equal(actual.compressed, true);
    t.mock.restoreAll();
    assert.deepEqual(
      normalize(actual),
      normalize(applyRtkCompression(body, { config: { enabled: true } }))
    );
  }));

test("global RTK import refreshes every warmed native trust variant without sharing project trust", async (t) =>
  withNative(t, async (directory) => {
    const { installGlobalRtkTomlV1Async } =
      await import("../../../open-sse/services/compression/engines/rtk/tomlCompatibility");
    const globalToml = (id: string, replacement: boolean) =>
      [
        "schema_version = 1",
        "[filters." + id + "]",
        'match_command = "^customcmd"',
        replacement
          ? 'strip_lines_matching = ["^noise", "^verbose progress details"]'
          : 'strip_lines_matching = ["^noise"]',
        "[[tests." + id + "]]",
        'name = "removes noise"',
        'input = "noise\\nkept"',
        'expected = "kept"',
      ].join("\n");
    await installFilter(directory);
    const project = path.join(directory, "project");
    await files.mkdir(path.join(project, ".rtk"), { recursive: true });
    const projectFilter = JSON.parse(
      await files.readFile(path.join(directory, "rtk", "filters.json"), "utf8")
    ) as {
      id: string;
      label: string;
      match: { outputTypes: string[]; commands: string[]; patterns: string[] };
    };
    projectFilter.id = "project-only";
    projectFilter.label = "project-only";
    projectFilter.match.commands = ["^projectcmd\\b"];
    projectFilter.match.patterns = [];
    await files.writeFile(
      path.join(project, ".rtk", "filters.json"),
      JSON.stringify(projectFilter)
    );
    // No trust.json: only the explicitly trusting variant may admit this overlay.
    const cwd = process.cwd();
    process.chdir(project);
    try {
      await installGlobalRtkTomlV1Async(globalToml("global-old", false));
      const body = toolBody();
      body.messages[1].content =
        "noise repeated\n".repeat(30) + "verbose progress details\nKEEP valuable result\n";
      const compress = (trustProjectFilters: boolean) =>
        applyCompressionAsync(body, "stacked", {
          config: {
            ...DEFAULT_COMPRESSION_CONFIG,
            stackedPipeline: [{ engine: "rtk" }, { engine: "lite" }],
            rtkConfig: {
              ...DEFAULT_RTK_CONFIG,
              enabled: true,
              customFiltersEnabled: true,
              trustProjectFilters,
              rawOutputRetention: "never",
            },
          },
        });
      for (const trustProjectFilters of [false, true]) {
        const filters = await loadRtkFiltersAsync({ trustProjectFilters });
        assert.equal(matchPreparedRtkFilter(filters, "noise\nKEEP", "customcmd")?.id, "global-old");
        assert.equal(
          matchPreparedRtkFilter(filters, "noise\nKEEP", "projectcmd")?.id === "project-only",
          trustProjectFilters
        );
        assert.ok(
          JSON.stringify((await compress(trustProjectFilters)).body).includes(
            "verbose progress details"
          )
        );
      }

      await assert.rejects(installGlobalRtkTomlV1Async(globalToml("global-new", true)));
      const replacement = await installGlobalRtkTomlV1Async(globalToml("global-new", true), {
        overwrite: true,
      });
      assert.equal(replacement.backupCreated, true);
      // Exact publication sequence used by POST /api/context/rtk/import.
      await loadRtkFiltersAsync({ refresh: true });
      for (const trustProjectFilters of [false, true]) {
        const filters = await loadRtkFiltersAsync({ trustProjectFilters });
        assert.equal(matchPreparedRtkFilter(filters, "noise\nKEEP", "customcmd")?.id, "global-new");
        assert.equal(
          matchPreparedRtkFilter(filters, "noise\nKEEP", "projectcmd")?.id === "project-only",
          trustProjectFilters,
          "refresh must revalidate each variant instead of borrowing another variant's trust"
        );
        const result = await compress(trustProjectFilters);
        assert.ok(JSON.stringify(result.body).includes("KEEP valuable result"));
        assert.ok(!JSON.stringify(result.body).includes("verbose progress details"));
      }
    } finally {
      process.chdir(cwd);
    }
  }));
