import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { registerHooks } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { fileFixture as io } from "../../helpers/persistence/servingFileFixture.mjs";

const owners = new Set(
  ["catalog", "generator", "generatorAsync", "openapiParser", "cliRegistryParser"].map(
    (name) => new URL(`../../../src/lib/agentSkills/${name}.ts`, import.meta.url).href
  )
);
const fixtureUrl = new URL("../../helpers/persistence/servingFileFixture.mjs", import.meta.url)
  .href;
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (
      owners.has(context.parentURL?.split("?")[0] ?? "") &&
      ["node:fs", "node:fs/promises"].includes(specifier)
    )
      return { url: fixtureUrl, shortCircuit: true };
    return nextResolve(specifier, context);
  },
});
const catalog = await import("../../../src/lib/agentSkills/catalog.ts");
const generator = await import("../../../src/lib/agentSkills/generator.ts");
const openapi = await import("../../../src/lib/agentSkills/openapiParser.ts");
const cli = await import("../../../src/lib/agentSkills/cliRegistryParser.ts");
const cwd = process.cwd(),
  backend = process.env.OMNIROUTE_DATABASE_BACKEND;
const turn = () => new Promise<void>((done) => setImmediate(done));
async function scenario(body: (directory: string) => Promise<void>) {
  const directory = await realpath(await mkdtemp(path.join(tmpdir(), "native-agent-skill-files-")));
  await mkdir(path.join(directory, "docs"));
  await mkdir(path.join(directory, "bin/cli/commands"), { recursive: true });
  await writeFile(
    path.join(directory, "docs/openapi.yaml"),
    'openapi: 3.0.0\npaths:\n  /api/models:\n    get:\n      summary: List models\n      responses:\n        "200":\n          description: OK\n'
  );
  await writeFile(
    path.join(directory, "bin/cli/commands/health.mjs"),
    'program.command("health").description("Check health").option("--json", "JSON");'
  );
  process.chdir(directory);
  io.reset();
  try {
    await body(directory);
  } finally {
    io.reset();
    process.chdir(cwd);
    if (backend === undefined) delete process.env.OMNIROUTE_DATABASE_BACKEND;
    else process.env.OMNIROUTE_DATABASE_BACKEND = backend;
    await rm(directory, { recursive: true, force: true });
  }
}
test.after(() => hooks.deregister());
test("native coverage and parsers preserve synchronous filesystem projections", async () =>
  scenario(async (directory) => {
    await mkdir(path.join(directory, "skills/cli-health"), { recursive: true });
    await writeFile(path.join(directory, "skills/cli-health/SKILL.md"), "# health");
    const expected = catalog.computeCoverage();
    const expectedApi = openapi.parseOpenapi(),
      expectedCli = cli.parseCliRegistry();
    assert.ok(expectedApi.paths.has("GET /api/models"));
    assert.ok(expectedCli.commands.has("health"));
    assert.ok(io.syncCalls > 0);
    io.reset();
    io.allowSync = false;
    const actual = await catalog.computeCoverageAsync();
    assert.deepEqual({ ...actual, generatedAt: "clock" }, { ...expected, generatedAt: "clock" });
    assert.deepEqual(await openapi.parseOpenapiAsync(), expectedApi);
    assert.deepEqual(await cli.parseCliRegistryAsync(), expectedCli);
    assert.equal(io.syncCalls, 0);
  }));
test("native Markdown waits for real local read before fallback and preserves frontmatter", async () =>
  scenario(async (directory) => {
    const file = path.join(directory, "skills/cli-health/SKILL.md");
    await mkdir(path.dirname(file), { recursive: true });
    await writeFile(file, '---\nname: "health"\ndescription: "Health info"\n---\n# Local health');
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    const expected = await catalog.fetchSkillMarkdown("cli-health");
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.reset();
    io.allowSync = false;
    const gate = io.block("readFile", file);
    let completed = false;
    const operation = catalog.fetchSkillMarkdown("cli-health").then((value) => {
      completed = true;
      return value;
    });
    await gate.entered.promise;
    try {
      await turn();
      assert.equal(completed, false);
    } finally {
      gate.release.release();
    }
    const result = await operation;
    assert.deepEqual({ ...result, fetchedAt: "clock" }, { ...expected, fetchedAt: "clock" });
    assert.equal(io.syncCalls, 0);
    const fetch = globalThis.fetch;
    globalThis.fetch = async () =>
      new Response("---\nname: fallback\ndescription: Remote\n---\n# Fallback");
    try {
      await rm(file);
      assert.equal((await catalog.fetchSkillMarkdown("cli-health")).source, "github");
    } finally {
      globalThis.fetch = fetch;
    }
  }));
test("native generator preserves dry-run, custom markers, bytes, prune and idempotent reports", async () =>
  scenario(async (directory) => {
    const old = "<!-- skill:custom-start -->\nKeep this custom text.\n<!-- skill:custom-end -->";
    for (const dir of ["sync", "native"]) {
      await mkdir(path.join(directory, dir, "cli-health"), { recursive: true });
      await mkdir(path.join(directory, dir, "orphan"));
      await writeFile(path.join(directory, dir, "cli-health/SKILL.md"), old);
    }
    process.env.OMNIROUTE_DATABASE_BACKEND = "sqlite";
    const expected = await generator.generateAgentSkills({
      dryRun: false,
      prune: true,
      outputDir: "sync",
      onlyIds: ["cli-health"],
    });
    const expectedText = await readFile(path.join(directory, "sync/cli-health/SKILL.md"), "utf8");
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.reset();
    io.allowSync = false;
    const preview = await generator.generateAgentSkills({
      dryRun: true,
      prune: false,
      outputDir: "native",
      onlyIds: ["cli-health"],
    });
    assert.deepEqual(preview.generated, ["cli-health"]);
    assert.equal(await readFile(path.join(directory, "native/cli-health/SKILL.md"), "utf8"), old);
    const actual = await generator.generateAgentSkills({
      dryRun: false,
      prune: true,
      outputDir: "native",
      onlyIds: ["cli-health"],
    });
    assert.deepEqual(actual, expected);
    assert.equal(
      await readFile(path.join(directory, "native/cli-health/SKILL.md"), "utf8"),
      expectedText
    );
    const repeat = await generator.generateAgentSkills({
      dryRun: false,
      prune: true,
      outputDir: "native",
      onlyIds: ["cli-health"],
    });
    assert.deepEqual(repeat.unchanged, ["cli-health"]);
    assert.deepEqual(repeat.generated, []);
    assert.equal(io.syncCalls, 0);
  }));
test("native generation serializes the complete file RMW, including prune and comparison", async () =>
  scenario(async (directory) => {
    await mkdir(path.join(directory, "skills"));
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.allowSync = false;
    const file = path.join(directory, "skills/cli-health/SKILL.md");
    const gate = io.block("writeFile", file);
    const first = generator.generateAgentSkills({
      dryRun: false,
      prune: true,
      onlyIds: ["cli-health"],
    });
    await gate.entered.promise;
    const second = generator.generateAgentSkills({
      dryRun: false,
      prune: true,
      onlyIds: ["cli-health"],
    });
    try {
      await turn();
      assert.equal(
        io.calls.filter(
          (call: { operation: string; target: string }) =>
            call.operation === "readdir" && call.target === path.join(directory, "skills")
        ).length,
        1
      );
    } finally {
      gate.release.release();
    }
    const [a, b] = await Promise.all([first, second]);
    assert.deepEqual(a.generated, ["cli-health"]);
    assert.deepEqual(b.unchanged, ["cli-health"]);
    assert.equal(io.syncCalls, 0);
  }));

test("native HTTP, MCP and A2A callers await the actual coverage filesystem operation", async () =>
  scenario(async (directory) => {
    await mkdir(path.join(directory, "skills/cli-health"), { recursive: true });
    await writeFile(path.join(directory, "skills/cli-health/SKILL.md"), "# health");
    process.env.OMNIROUTE_DATABASE_BACKEND = "postgres";
    io.allowSync = false;
    const http = await import("../../../src/app/api/agent-skills/route.ts");
    const coverage = await import("../../../src/app/api/agent-skills/coverage/route.ts");
    const { agentSkillTools } =
      await import("../../../open-sse/mcp-server/tools/agentSkillTools.ts");
    const { executeListCapabilities } =
      await import("../../../src/lib/a2a/skills/listCapabilities.ts");
    const callers: Array<() => Promise<unknown>> = [
      async () => (await http.GET(new Request("http://localhost/api/agent-skills"))).json(),
      async () => (await coverage.GET()).json(),
      () => agentSkillTools.omniroute_agent_skills_list.handler({}),
      () => agentSkillTools.omniroute_agent_skills_coverage.handler({}),
      () => executeListCapabilities({} as Parameters<typeof executeListCapabilities>[0]),
    ];
    for (const call of callers) {
      const gate = io.block("readdir", path.join(directory, "skills"));
      let complete = false;
      const operation = call().then((value) => {
        complete = true;
        return value;
      });
      await gate.entered.promise;
      try {
        await turn();
        assert.equal(complete, false);
      } finally {
        gate.release.release();
      }
      assert.ok(await operation);
    }
    assert.equal(io.syncCalls, 0);
  }));
