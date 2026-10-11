import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { getCatalog, refreshCatalog } from "./catalog";
import { assembleFileContent, buildSkillMarkdown } from "./generator";
import { parseOpenapiAsync } from "./openapiParser";
import { parseCliRegistryAsync } from "./cliRegistryParser";
import type { GeneratorOptions, GeneratorReport } from "./types";

const GENERATIONS = Symbol.for("omniroute.agentSkillFileGenerations");
const generationGlobals = globalThis as typeof globalThis & {
  [GENERATIONS]?: Map<string, Promise<void>>;
};

/** Preserve the synchronous generator's non-interleaving file operation per output base. */
export function generateAgentSkillsWithAsyncFiles(
  opts: GeneratorOptions
): Promise<GeneratorReport> {
  const outputDir = opts.outputDir ?? "skills";
  const outputBase = path.isAbsolute(outputDir) ? outputDir : path.join(process.cwd(), outputDir);
  const key = path.resolve(outputBase);
  const options = { ...opts, onlyIds: opts.onlyIds ? [...opts.onlyIds] : undefined };
  const pending = (generationGlobals[GENERATIONS] ??= new Map());
  const work = (pending.get(key) ?? Promise.resolve()).then(() =>
    generateFiles(options, outputBase)
  );
  const settled = work.then(
    () => {},
    () => {}
  );
  pending.set(key, settled);
  void settled.then(() => {
    if (pending.get(key) === settled) pending.delete(key);
  });
  return work;
}

async function readOptionalFile(file: string): Promise<string | undefined> {
  try {
    return await readFile(file, "utf-8");
  } catch {
    return undefined;
  }
}

async function generateFiles(opts: GeneratorOptions, outputBase: string): Promise<GeneratorReport> {
  const { dryRun = true, prune = false, onlyIds } = opts;
  const catalog = getCatalog();
  const catalogIds = new Set(catalog.map((skill) => skill.id));
  const skills = onlyIds ? catalog.filter((skill) => onlyIds.includes(skill.id)) : catalog;
  const report: GeneratorReport = {
    generated: [],
    unchanged: [],
    pruned: [],
    orphansDetected: [],
    errors: [],
  };
  if (prune) {
    let directories: string[] = [];
    try {
      directories = (await readdir(outputBase, { withFileTypes: true }))
        .filter((entry) => entry.isDirectory())
        .map((entry) => entry.name);
    } catch {
      /* missing output directory has no orphans */
    }
    for (const directory of directories) {
      if (catalogIds.has(directory)) continue;
      report.orphansDetected.push(directory);
      if (!dryRun) {
        try {
          await rm(path.join(outputBase, directory), { recursive: true, force: true });
          report.pruned.push(directory);
        } catch (error) {
          report.errors.push({
            id: directory,
            error: `Failed to prune: ${sanitizeErrorMessage(error instanceof Error ? error.message : String(error))}`,
          });
        }
      }
    }
  }
  const sources = {
    openapi: await parseOpenapiAsync().catch(() => ({ paths: new Map(), areas: new Map() })),
    cliRegistry: await parseCliRegistryAsync().catch(() => ({
      commands: new Map(),
      families: new Map(),
    })),
  };
  for (const skill of skills) {
    try {
      const skillDir = path.join(outputBase, skill.id);
      const skillFile = path.join(skillDir, "SKILL.md");
      const existing = await readOptionalFile(skillFile);
      const { frontmatter, body, references } = buildSkillMarkdown(skill.id, sources, existing);
      const content = assembleFileContent(frontmatter, body);
      const stale = references.length === 0 ? "references/endpoints.md" : undefined;
      let referenceChanged = false;
      for (const reference of references) {
        if ((await readOptionalFile(path.join(skillDir, reference.path))) !== reference.content)
          referenceChanged = true;
      }
      if (stale && (await readOptionalFile(path.join(skillDir, stale))) !== undefined)
        referenceChanged = true;
      if (existing === content && !referenceChanged) {
        report.unchanged.push(skill.id);
        continue;
      }
      if (dryRun) {
        report.generated.push(skill.id);
        continue;
      }
      await mkdir(skillDir, { recursive: true });
      await writeFile(skillFile, content, "utf-8");
      for (const reference of references) {
        const file = path.join(skillDir, reference.path);
        await mkdir(path.dirname(file), { recursive: true });
        await writeFile(file, reference.content, "utf-8");
      }
      if (stale) await rm(path.join(skillDir, stale), { force: true });
      report.generated.push(skill.id);
    } catch (error) {
      report.errors.push({
        id: skill.id,
        error: sanitizeErrorMessage(error instanceof Error ? error.message : String(error)),
      });
    }
  }
  if (!dryRun && report.generated.length > 0) refreshCatalog();
  return report;
}
