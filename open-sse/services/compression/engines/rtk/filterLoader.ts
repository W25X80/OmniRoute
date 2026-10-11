import fs from "node:fs";
import fsp from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import crypto from "node:crypto";
import { parseDatabaseBackend } from "../../../../../src/lib/db/backendSelection";

import { detectCommandType } from "./commandDetector.ts";
import { validateRtkFilter, type RtkFilterDefinition } from "./filterSchema.ts";
import { parseRtkTomlV1, RtkTomlCompatibilityError } from "./tomlCompatibility.ts";

let cache: RtkFilterDefinition[] | null = null;
let cacheKey: string | null = null;
let diagnostics: RtkFilterLoadDiagnostic[] = [];

// RegExp cache for matchPatterns — same pattern strings are tested against
// many different lines across requests; avoid compiling them every time.
const regexCache = new Map<string, RegExp>();

function cachedMatchPattern(pattern: string, value: string): boolean {
  const key = `${pattern}::im`;
  let re = regexCache.get(key);
  if (!re) {
    try {
      re = new RegExp(pattern, "im");
      regexCache.set(key, re);
    } catch {
      return false;
    }
  }
  return re.test(value);
}

export interface RtkFilterLoadDiagnostic {
  source: "project" | "global" | "builtin";
  format?: "omniroute-json" | "rtk-toml-v1";
  path?: string;
  level: "warning" | "error";
  message: string;
}

interface FilterSource {
  source: "project" | "global" | "builtin";
  path: string;
  trusted: boolean;
  format: "omniroute-json" | "rtk-toml-v1";
}

interface RtkFilterLoadOptions {
  refresh?: boolean;
  customFiltersEnabled?: boolean;
  trustProjectFilters?: boolean;
}

function getModuleDir(): string {
  const anchors = [process.cwd()];
  const argv1 = process.argv[1];
  if (typeof argv1 === "string" && argv1) anchors.push(path.dirname(argv1));
  const rel = path.join("open-sse", "services", "compression");
  for (const anchor of anchors) {
    let dir = path.resolve(anchor);
    for (let i = 0; i <= 8; i++) {
      if (fs.existsSync(path.join(dir, rel))) return dir;
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }
  }
  return path.join(os.homedir(), ".omniroute");
}

function getFiltersDir(): string {
  const root = getModuleDir();
  const candidates = [
    path.join(root, "open-sse", "services", "compression", "engines", "rtk", "filters"),
    path.join(root, "app", "open-sse", "services", "compression", "engines", "rtk", "filters"),
  ];
  return (
    candidates.find((candidate, index) => {
      return candidates.indexOf(candidate) === index && fs.existsSync(candidate);
    }) ?? candidates[0]
  );
}

function getDataDir(): string {
  return process.env.DATA_DIR || path.join(os.homedir(), ".omniroute");
}

function sha256(value: string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function projectFiltersTrusted(
  filtersPath: string,
  trustProjectFilters = false
): boolean | "changed" {
  if (trustProjectFilters) return true;
  if (process.env.OMNIROUTE_RTK_TRUST_PROJECT_FILTERS === "1") return true;
  const trustPath = path.join(path.dirname(filtersPath), "trust.json");
  if (!fs.existsSync(trustPath)) return false;
  try {
    const filtersHash = sha256(fs.readFileSync(filtersPath, "utf8"));
    const trust = JSON.parse(fs.readFileSync(trustPath, "utf8")) as Record<string, unknown>;
    const isToml = filtersPath.endsWith(".toml");
    const trustedHash = isToml
      ? typeof trust.filtersTomlSha256 === "string"
        ? trust.filtersTomlSha256
        : null
      : typeof trust.filtersSha256 === "string"
        ? trust.filtersSha256
        : typeof trust.trustedFiltersSha256 === "string"
          ? trust.trustedFiltersSha256
          : null;
    if (!trustedHash) return false;
    return trustedHash === filtersHash ? true : "changed";
  } catch {
    return false;
  }
}

function collectFilterSources(options: RtkFilterLoadOptions = {}): FilterSource[] {
  const sources: FilterSource[] = [];
  if (options.customFiltersEnabled !== false) {
    collectProjectFilterSources(sources, options);
    collectGlobalFilterSources(sources);
  }
  collectBuiltinFilterSources(sources);
  return sources;
}

function collectProjectFilterSources(sources: FilterSource[], options: RtkFilterLoadOptions): void {
  const projectCandidates = [
    {
      path: path.join(process.cwd(), ".rtk", "filters.toml"),
      format: "rtk-toml-v1" as const,
    },
    {
      path: path.join(process.cwd(), ".rtk", "filters.json"),
      format: "omniroute-json" as const,
    },
  ];
  for (const candidate of projectCandidates) {
    if (!fs.existsSync(candidate.path)) continue;
    const trusted = projectFiltersTrusted(candidate.path, options.trustProjectFilters === true);
    if (trusted === true) {
      sources.push({ source: "project", ...candidate, trusted: true });
      continue;
    }
    diagnostics.push({
      source: "project",
      format: candidate.format,
      path: candidate.path,
      level: "warning",
      message:
        trusted === "changed"
          ? "Project RTK filters changed after trust and were skipped"
          : "Project RTK filters are untrusted and were skipped",
    });
  }
}

function collectGlobalFilterSources(sources: FilterSource[]): void {
  const globalCandidates = [
    {
      path: path.join(getDataDir(), "rtk", "filters.toml"),
      format: "rtk-toml-v1" as const,
    },
    {
      path: path.join(getDataDir(), "rtk", "filters.json"),
      format: "omniroute-json" as const,
    },
  ];
  for (const candidate of globalCandidates) {
    if (fs.existsSync(candidate.path)) {
      sources.push({ source: "global", ...candidate, trusted: true });
    }
  }
}

function collectBuiltinFilterSources(sources: FilterSource[]): void {
  const builtinDir = getFiltersDir();
  if (fs.existsSync(builtinDir)) {
    let builtinFiles: string[] = [];
    try {
      builtinFiles = fs
        .readdirSync(builtinDir)
        .filter((entry) => entry.endsWith(".json"))
        .sort();
    } catch {
      // A read failure on the builtin dir (lost permission, TOCTOU after existsSync, NFS
      // timeout, read-only container) must not fail the request — degrade to the filters
      // already collected. listRtkCommandSamples guards readdirSync the same way.
      builtinFiles = [];
    }
    for (const file of builtinFiles) {
      sources.push({
        source: "builtin",
        path: path.join(builtinDir, file),
        trusted: true,
        format: "omniroute-json",
      });
    }
  }
}

function parseFilterFile(source: FilterSource): RtkFilterDefinition[] {
  return parseFilterSource(source, () => fs.readFileSync(source.path, "utf8"), diagnostics);
}

function parseFilterSource(
  source: FilterSource,
  read: () => string,
  report: RtkFilterLoadDiagnostic[]
): RtkFilterDefinition[] {
  try {
    const content = read();
    const definitions =
      source.format === "rtk-toml-v1"
        ? (() => {
            const result = parseRtkTomlV1(content);
            if (!result.passed) {
              throw new Error("one or more inline tests failed");
            }
            for (const warning of result.warnings) {
              report.push({
                source: source.source,
                format: source.format,
                path: source.path,
                level: "warning",
                message: warning,
              });
            }
            return result.filters;
          })()
        : (() => {
            const parsed = JSON.parse(content);
            const entries = Array.isArray(parsed) ? parsed : [parsed];
            return entries.map(validateRtkFilter);
          })();
    return definitions.map((definition) => ({
      ...definition,
      source: source.source,
      sourceFormat: definition.sourceFormat ?? source.format,
    }));
  } catch (error) {
    const message =
      error instanceof RtkTomlCompatibilityError
        ? error.publicMessage
        : error instanceof Error
          ? error.message
          : String(error);
    if (source.source === "builtin") {
      throw new Error(`Invalid RTK filter ${path.basename(source.path)}: ${message}`);
    }
    report.push({
      source: source.source,
      format: source.format,
      path: source.path,
      level: "warning",
      message: `Invalid custom RTK filter skipped: ${message}`,
    });
    return [];
  }
}

export function loadRtkFilters(options: RtkFilterLoadOptions = {}): RtkFilterDefinition[] {
  const currentCacheKey = [
    process.cwd(),
    getDataDir(),
    options.customFiltersEnabled === false ? "builtin-only" : "custom",
    options.trustProjectFilters === true ? "trusted-project" : "trust-file",
    process.env.OMNIROUTE_RTK_TRUST_PROJECT_FILTERS === "1" ? "env-trust" : "env-normal",
  ].join("|");
  if (cache && cacheKey === currentCacheKey && !options.refresh) return cache;
  diagnostics = [];
  const filters: RtkFilterDefinition[] = [];
  for (const source of collectFilterSources(options)) {
    filters.push(...parseFilterFile(source));
  }

  const sourceRank = { project: 3, global: 2, builtin: 1 } as const;
  const formatRank = { "rtk-toml-v1": 2, "omniroute-json": 1 } as const;
  const sorted = filters.sort(
    (a, b) =>
      sourceRank[b.source ?? "builtin"] - sourceRank[a.source ?? "builtin"] ||
      formatRank[b.sourceFormat ?? "omniroute-json"] -
        formatRank[a.sourceFormat ?? "omniroute-json"] ||
      b.priority - a.priority ||
      a.id.localeCompare(b.id)
  );
  cache = sorted;
  cacheKey = currentCacheKey;
  return sorted;
}

export function getRtkFilterLoadDiagnostics(): RtkFilterLoadDiagnostic[] {
  loadRtkFilters();
  return diagnostics.map((entry) => ({ ...entry }));
}

export function getRtkFilterCatalog(): Array<
  Pick<
    RtkFilterDefinition,
    | "id"
    | "name"
    | "description"
    | "commandTypes"
    | "category"
    | "priority"
    | "source"
    | "sourceFormat"
  >
> {
  return loadRtkFilters().map((filter) => ({
    id: filter.id,
    name: filter.name,
    description: filter.description,
    commandTypes: filter.commandTypes,
    category: filter.category,
    priority: filter.priority,
    source: filter.source,
    sourceFormat: filter.sourceFormat,
  }));
}

export function matchRtkFilter(
  text: string,
  command?: string | null,
  options: RtkFilterLoadOptions = {}
): RtkFilterDefinition | null {
  return matchPreparedRtkFilter(loadRtkFilters(options), text, command);
}

/** Pure matching over the exact trusted snapshot selected by the caller. */
export function matchPreparedRtkFilter(
  filters: readonly RtkFilterDefinition[],
  text: string,
  command?: string | null
): RtkFilterDefinition | null {
  const detection = detectCommandType(text, command);
  const detectedCommand = detection.command ?? command ?? "";
  for (const source of ["project", "global", "builtin"] as const) {
    const scoped = filters.filter((filter) => (filter.source ?? "builtin") === source);
    const matched =
      scoped.find(
        (filter) =>
          filter.sourceFormat === "rtk-toml-v1" &&
          detectedCommand &&
          filter.commandPatterns.some((pattern) => cachedMatchPattern(pattern, detectedCommand))
      ) ??
      scoped.find((filter) => filter.commandTypes.includes(detection.type)) ??
      scoped.find(
        (filter) =>
          detectedCommand &&
          filter.commandPatterns.some((pattern) => cachedMatchPattern(pattern, detectedCommand))
      ) ??
      scoped.find((filter) =>
        filter.matchPatterns.some((pattern) => cachedMatchPattern(pattern, text))
      );
    if (matched) return matched;
  }
  return filters.find((filter) => filter.commandTypes.includes("generic-output")) ?? null;
}

interface NativeFilterSnapshot {
  filters: RtkFilterDefinition[];
  diagnostics: RtkFilterLoadDiagnostic[];
}
interface NativeFilterCache {
  snapshot?: NativeFilterSnapshot;
  pending?: Promise<NativeFilterSnapshot>;
}
const NATIVE_FILTERS = Symbol.for("omniroute.nativeRtkFilterCaches");
const filterGlobals = globalThis as typeof globalThis & {
  [NATIVE_FILTERS]?: Map<string, NativeFilterCache>;
};

async function existsAsync(filename: string): Promise<boolean> {
  try {
    await fsp.access(filename);
    return true;
  } catch {
    return false;
  }
}

async function builtinFiltersDirAsync(): Promise<string> {
  const anchors = [process.cwd()];
  const argv1 = process.argv[1];
  if (typeof argv1 === "string" && argv1) anchors.push(path.dirname(argv1));
  const relative = path.join("open-sse", "services", "compression");
  let root = path.join(os.homedir(), ".omniroute");
  outer: for (const anchor of anchors) {
    let dir = path.resolve(anchor);
    for (let i = 0; i <= 8; i++) {
      if (await existsAsync(path.join(dir, relative))) {
        root = dir;
        break outer;
      }
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }
  }
  const candidates = [
    path.join(root, "open-sse", "services", "compression", "engines", "rtk", "filters"),
    path.join(root, "app", "open-sse", "services", "compression", "engines", "rtk", "filters"),
  ];
  for (const candidate of candidates) if (await existsAsync(candidate)) return candidate;
  return candidates[0];
}

async function loadNativeFilterSnapshot(
  options: RtkFilterLoadOptions
): Promise<NativeFilterSnapshot> {
  const report: RtkFilterLoadDiagnostic[] = [];
  const loaded: Array<{ source: FilterSource; content: string | Error }> = [];
  const readCandidate = async (source: FilterSource) => {
    try {
      loaded.push({ source, content: await fsp.readFile(source.path, "utf8") });
    } catch (error) {
      loaded.push({
        source,
        content: error instanceof Error ? error : new Error(String(error)),
      });
    }
  };
  if (options.customFiltersEnabled !== false) {
    for (const [name, format] of [
      ["filters.toml", "rtk-toml-v1"],
      ["filters.json", "omniroute-json"],
    ] as const) {
      const filename = path.join(process.cwd(), ".rtk", name);
      if (!(await existsAsync(filename))) continue;
      let content: string | Error;
      try {
        content = await fsp.readFile(filename, "utf8");
      } catch (error) {
        content = error instanceof Error ? error : new Error(String(error));
      }
      let trusted: boolean | "changed" =
        options.trustProjectFilters === true ||
        process.env.OMNIROUTE_RTK_TRUST_PROJECT_FILTERS === "1";
      if (!trusted && !(content instanceof Error)) {
        try {
          const trust = JSON.parse(
            await fsp.readFile(path.join(path.dirname(filename), "trust.json"), "utf8")
          ) as Record<string, unknown>;
          const trustedHash =
            format === "rtk-toml-v1"
              ? typeof trust.filtersTomlSha256 === "string"
                ? trust.filtersTomlSha256
                : null
              : typeof trust.filtersSha256 === "string"
                ? trust.filtersSha256
                : typeof trust.trustedFiltersSha256 === "string"
                  ? trust.trustedFiltersSha256
                  : null;
          trusted = trustedHash ? (trustedHash === sha256(content) ? true : "changed") : false;
        } catch {
          trusted = false;
        }
      }
      if (trusted === true) {
        // Parse the SAME bytes whose hash was authorized, avoiding a trust-check/read race.
        loaded.push({
          source: { source: "project", path: filename, trusted: true, format },
          content,
        });
      } else {
        report.push({
          source: "project",
          path: filename,
          format,
          level: "warning",
          message:
            trusted === "changed"
              ? "Project RTK filters changed after trust and were skipped"
              : "Project RTK filters are untrusted and were skipped",
        });
      }
    }
    for (const [name, format] of [
      ["filters.toml", "rtk-toml-v1"],
      ["filters.json", "omniroute-json"],
    ] as const) {
      const filename = path.join(getDataDir(), "rtk", name);
      if (await existsAsync(filename))
        await readCandidate({
          source: "global",
          path: filename,
          trusted: true,
          format,
        });
    }
  }
  const builtinDir = await builtinFiltersDirAsync();
  if (await existsAsync(builtinDir)) {
    let files: string[];
    try {
      files = (await fsp.readdir(builtinDir)).filter((entry) => entry.endsWith(".json")).sort();
    } catch {
      files = [];
    }
    for (const filename of files)
      await readCandidate({
        source: "builtin",
        path: path.join(builtinDir, filename),
        trusted: true,
        format: "omniroute-json",
      });
  }
  const filters = loaded.flatMap(({ source, content }) =>
    parseFilterSource(
      source,
      () => {
        if (content instanceof Error) throw content;
        return content;
      },
      report
    )
  );
  const sourceRank = { project: 3, global: 2, builtin: 1 } as const;
  const formatRank = { "rtk-toml-v1": 2, "omniroute-json": 1 } as const;
  filters.sort(
    (a, b) =>
      sourceRank[b.source ?? "builtin"] - sourceRank[a.source ?? "builtin"] ||
      formatRank[b.sourceFormat ?? "omniroute-json"] -
        formatRank[a.sourceFormat ?? "omniroute-json"] ||
      b.priority - a.priority ||
      a.id.localeCompare(b.id)
  );
  return { filters, diagnostics: report };
}

async function nativeFilterSnapshot(
  options: RtkFilterLoadOptions = {}
): Promise<NativeFilterSnapshot> {
  if (parseDatabaseBackend() !== "postgres") {
    const filters = loadRtkFilters(options);
    return { filters, diagnostics: diagnostics.map((entry) => ({ ...entry })) };
  }
  const key = [
    process.cwd(),
    getDataDir(),
    options.customFiltersEnabled === false ? "builtin-only" : "custom",
    options.trustProjectFilters === true ? "trusted-project" : "trust-file",
    process.env.OMNIROUTE_RTK_TRUST_PROJECT_FILTERS === "1" ? "env-trust" : "env-normal",
  ].join("|");
  const caches = (filterGlobals[NATIVE_FILTERS] ??= new Map());
  let state = caches.get(key);
  if (!state) {
    state = {};
    caches.set(key, state);
  }
  const selectedState = state;
  if (selectedState.snapshot && !options.refresh && !selectedState.pending)
    return selectedState.snapshot;
  if (selectedState.pending && !options.refresh) return selectedState.pending;
  const previous = selectedState.pending;
  const reading = (async () => {
    await previous?.catch(() => {});
    const snapshot = await loadNativeFilterSnapshot(options);
    selectedState.snapshot = snapshot;
    return snapshot;
  })();
  selectedState.pending = reading;
  try {
    return await reading;
  } finally {
    if (selectedState.pending === reading) selectedState.pending = undefined;
  }
}

export async function loadRtkFiltersAsync(
  options: RtkFilterLoadOptions = {}
): Promise<RtkFilterDefinition[]> {
  return (await nativeFilterSnapshot(options)).filters;
}

export async function getRtkFilterCatalogSnapshotAsync(): Promise<{
  filters: ReturnType<typeof getRtkFilterCatalog>;
  diagnostics: RtkFilterLoadDiagnostic[];
}> {
  const snapshot = await nativeFilterSnapshot();
  return {
    filters: snapshot.filters.map((filter) => ({
      id: filter.id,
      name: filter.name,
      description: filter.description,
      commandTypes: filter.commandTypes,
      category: filter.category,
      priority: filter.priority,
      source: filter.source,
      sourceFormat: filter.sourceFormat,
    })),
    diagnostics: snapshot.diagnostics.map((entry) => ({ ...entry })),
  };
}
