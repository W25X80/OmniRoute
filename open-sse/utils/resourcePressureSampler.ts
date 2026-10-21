import fs from "node:fs/promises";
import path from "node:path";
import v8 from "node:v8";
import type { ResourceSignals } from "./resourcePressurePolicy.ts";

const DEFAULT_CGROUP_ROOT = "/sys/fs/cgroup";

export type ResourcePressureFs = {
  readText: (filePath: string) => Promise<string | null>;
};

export type SampleResourceSignalsDeps = {
  nowMs?: () => number;
  memoryUsage?: () => NodeJS.MemoryUsage;
  heapStatistics?: () => { heap_size_limit: number; used_heap_size?: number };
  availableMemory?: () => number | undefined;
  constrainedMemory?: () => number | undefined;
  fs?: ResourcePressureFs;
};

type Cgroup2Mount = { root: string; mountpoint: string };

async function defaultReadText(filePath: string): Promise<string | null> {
  try {
    return await fs.readFile(filePath, "utf8");
  } catch {
    return null;
  }
}

export function sanitizeMemoryBytes(value: unknown): number | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed || trimmed === "max" || !/^\d+$/.test(trimmed) || trimmed.length > 15) {
      return null;
    }
    value = Number(trimmed);
  }
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
  if (value >= Number.MAX_SAFE_INTEGER) return null;
  return Math.floor(value);
}

function safeNumber(call: (() => number | undefined) | undefined): number | null {
  try {
    return call ? sanitizeMemoryBytes(call()) : null;
  } catch {
    return null;
  }
}

export function decodeMountInfoPath(value: string): string | null {
  if (value.includes("\0")) return null;
  try {
    return value.replace(/\\([0-7]{3})/g, (_match, octal: string) =>
      String.fromCharCode(Number.parseInt(octal, 8))
    );
  } catch {
    return null;
  }
}

export function parseCgroupV2Path(contents: string | null): string | null {
  if (!contents) return null;
  for (const rawLine of contents.split("\n")) {
    const line = rawLine.trim();
    if (!line.startsWith("0::")) continue;
    const relativePath = line.slice(3);
    if (!relativePath.startsWith("/") || relativePath.includes("\0")) return null;
    return relativePath;
  }
  return null;
}

export function parseCgroup2Mount(contents: string | null): Cgroup2Mount | null {
  if (!contents) return null;
  for (const rawLine of contents.split("\n")) {
    const separator = rawLine.indexOf(" - ");
    if (separator < 0) continue;
    const left = rawLine.slice(0, separator).trim().split(/\s+/);
    const right = rawLine
      .slice(separator + 3)
      .trim()
      .split(/\s+/);
    if (right[0] !== "cgroup2" || left.length < 5) continue;
    const root = decodeMountInfoPath(left[3]);
    const mountpoint = decodeMountInfoPath(left[4]);
    if (!root?.startsWith("/") || !mountpoint?.startsWith("/")) return null;
    return { root, mountpoint };
  }
  return null;
}

function isContained(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function hasTraversalSegment(value: string): boolean {
  let decoded = value;
  try {
    decoded = decodeURIComponent(value);
  } catch {
    return true;
  }
  return decoded.split("/").some((segment) => segment === ".." || segment === ".");
}

function resolveFromMount(cgroupPath: string, mount: Cgroup2Mount): string | null {
  if (
    cgroupPath.includes("\0") ||
    mount.root.includes("\0") ||
    mount.mountpoint.includes("\0") ||
    hasTraversalSegment(cgroupPath)
  ) {
    return null;
  }
  const resolvedRoot = path.resolve(mount.root);
  const resolvedCgroup = path.resolve(cgroupPath);
  if (!isContained(resolvedRoot, resolvedCgroup)) return null;
  const suffix = path.relative(resolvedRoot, resolvedCgroup);
  const resolvedMountpoint = path.resolve(mount.mountpoint);
  const candidate = path.resolve(resolvedMountpoint, suffix);
  return isContained(resolvedMountpoint, candidate) ? candidate : null;
}

export async function resolveCgroupDirectory(
  readText: ResourcePressureFs["readText"],
  options: { allowDefaultFallback?: boolean } = {}
): Promise<string | null> {
  try {
    const [cgroupContents, mountInfo] = await Promise.all([
      readText("/proc/self/cgroup"),
      readText("/proc/self/mountinfo"),
    ]);
    const cgroupPath = parseCgroupV2Path(cgroupContents);
    const mount = parseCgroup2Mount(mountInfo);
    if (cgroupPath && mount) {
      const candidate = resolveFromMount(cgroupPath, mount);
      if (candidate && (await readText(path.join(candidate, "memory.current"))) != null) {
        return candidate;
      }
      if (!candidate) return null;
    }
    if (options.allowDefaultFallback === false) return null;
    return (await readText(path.join(DEFAULT_CGROUP_ROOT, "memory.current"))) != null
      ? DEFAULT_CGROUP_ROOT
      : null;
  } catch {
    return null;
  }
}

async function resolveCgroupV1Directory(
  readText: ResourcePressureFs["readText"]
): Promise<string | null> {
  const [contents, mountInfo] = await Promise.all([
    readText("/proc/self/cgroup"),
    readText("/proc/self/mountinfo"),
  ]);
  const entry = contents
    ?.split("\n")
    .find((line) => line.split(":")[1]?.split(",").includes("memory"));
  if (entry) {
    const cgroupPath = entry.slice(entry.indexOf(":", entry.indexOf(":") + 1) + 1);
    for (const line of mountInfo?.split("\n") ?? []) {
      const separator = line.indexOf(" - ");
      if (separator < 0) continue;
      const left = line.slice(0, separator).trim().split(/\s+/);
      const right = line
        .slice(separator + 3)
        .trim()
        .split(/\s+/);
      if (right[0] !== "cgroup" || !right[2]?.split(",").includes("memory") || left.length < 5)
        continue;
      const root = decodeMountInfoPath(left[3]);
      const mountpoint = decodeMountInfoPath(left[4]);
      if (!root?.startsWith("/") || !mountpoint?.startsWith("/") || !cgroupPath.startsWith("/"))
        return null;
      const candidate = resolveFromMount(cgroupPath, { root, mountpoint });
      if (!candidate) return null;
      if ((await readText(path.join(candidate, "memory.usage_in_bytes"))) != null) return candidate;
    }
  }
  const fallback = path.join(DEFAULT_CGROUP_ROOT, "memory");
  return (await readText(path.join(fallback, "memory.usage_in_bytes"))) != null ? fallback : null;
}

function statFields(text: string | null): Map<string, string> {
  return new Map(
    (text ?? "")
      .trim()
      .split("\n")
      .map((line) => {
        const [key, value] = line.trim().split(/\s+/, 2);
        return [key, value ?? ""];
      })
  );
}

function finiteMinimum(...values: Array<number | null>): number | null {
  const finite = values.filter((value): value is number => value !== null);
  return finite.length ? Math.min(...finite) : null;
}

async function sampleCgroup(readText: ResourcePressureFs["readText"]): Promise<{
  signals: ResourceSignals["cgroup"];
  pressure: string | null;
}> {
  const directory = await resolveCgroupDirectory(readText);
  if (directory) {
    const [current, max, high, events, stat, pressure] = await Promise.all(
      [
        "memory.current",
        "memory.max",
        "memory.high",
        "memory.events",
        "memory.stat",
        "memory.pressure",
      ].map((file) => readText(path.join(directory, file)))
    );
    return {
      signals: {
        currentBytes: sanitizeMemoryBytes(current),
        maxBytes: sanitizeMemoryBytes(max),
        highBytes: sanitizeMemoryBytes(high),
        fileBytes: parseMemoryStatFileBytes(stat),
        events: parseMemoryEvents(events),
      },
      pressure,
    };
  }
  const legacy = await resolveCgroupV1Directory(readText);
  if (!legacy)
    return {
      signals: {
        currentBytes: null,
        maxBytes: null,
        highBytes: null,
        fileBytes: null,
        events: null,
      },
      pressure: null,
    };
  const [current, limit, stat, failcnt, oom] = await Promise.all(
    [
      "memory.usage_in_bytes",
      "memory.limit_in_bytes",
      "memory.stat",
      "memory.failcnt",
      "memory.oom_control",
    ].map((file) => readText(path.join(legacy, file)))
  );
  const fields = statFields(stat);
  const cache = fields.get("total_cache") ?? fields.get("cache");
  const failed = failcnt == null ? null : parseEventCounter(failcnt);
  const killed = statFields(oom).get("oom_kill");
  const oomKills = killed == null ? null : parseEventCounter(killed);
  return {
    signals: {
      currentBytes: sanitizeMemoryBytes(current),
      // ECS exposes an unlimited leaf with a finite task limit in the hierarchy.
      maxBytes: finiteMinimum(
        sanitizeMemoryBytes(limit),
        sanitizeMemoryBytes(fields.get("hierarchical_memory_limit"))
      ),
      highBytes: null, // v1 soft_limit is not the v2 throttling mechanism.
      fileBytes: cache === "0" ? 0 : sanitizeMemoryBytes(cache),
      events:
        failed == null && oomKills == null
          ? null
          : { low: null, high: null, max: failed, oom: null, oom_kill: oomKills },
    },
    pressure: null,
  };
}

function parseEventCounter(value: string): number | null {
  const parsed = Number(value.trim());
  return Number.isFinite(parsed) && parsed >= 0 && parsed < Number.MAX_SAFE_INTEGER
    ? Math.floor(parsed)
    : null;
}

function parseMemoryEvents(text: string | null): ResourceSignals["cgroup"]["events"] {
  if (!text) return null;
  const values = { low: null, high: null, max: null, oom: null, oom_kill: null } as Record<
    "low" | "high" | "max" | "oom" | "oom_kill",
    number | null
  >;
  let matched = false;
  for (const line of text.split("\n")) {
    const [key, rawValue] = line.trim().split(/\s+/, 2);
    if (!(key in values) || rawValue == null) continue;
    values[key as keyof typeof values] = parseEventCounter(rawValue);
    matched = true;
  }
  return matched ? values : null;
}

function parsePsiNumber(line: string, name: string): number | null {
  const match = new RegExp(`(?:^|\\s)${name}=([0-9.]+)`).exec(line);
  const parsed = match ? Number(match[1]) : Number.NaN;
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function parseMemoryStatFileBytes(text: string | null): number | null {
  if (!text) return null;
  for (const line of text.split("\n")) {
    const [key, rawValue] = line.trim().split(/\s+/, 2);
    if (key !== "file" || rawValue == null) continue;
    // sanitizeMemoryBytes rejects every falsy magnitude INCLUDING zero, but a
    // zero file cache is a valid reading (workingSetBytes falls back to the
    // raw ratio for it). The zero short-circuit below is coupled to that
    // contract on purpose; if sanitizeMemoryBytes ever accepts zero, this
    // branch becomes dead but harmless.
    if (rawValue === "0") return 0;
    return sanitizeMemoryBytes(rawValue);
  }
  return null;
}

function parsePsi(text: string | null): ResourceSignals["psi"] {
  if (!text) return null;
  const result: NonNullable<ResourceSignals["psi"]> = {
    someAvg10: null,
    someAvg60: null,
    someAvg300: null,
    fullAvg10: null,
    fullAvg60: null,
    fullAvg300: null,
  };
  let matched = false;
  for (const line of text.split("\n")) {
    const kind = line.startsWith("some ") ? "some" : line.startsWith("full ") ? "full" : null;
    if (!kind) continue;
    result[`${kind}Avg10`] = parsePsiNumber(line, "avg10");
    result[`${kind}Avg60`] = parsePsiNumber(line, "avg60");
    result[`${kind}Avg300`] = parsePsiNumber(line, "avg300");
    matched = true;
  }
  return matched ? result : null;
}

function firstParseablePsi(
  ...samples: Array<string | null>
): { text: string; psiSource: "cgroup" | "host" } | null {
  const sources = ["cgroup", "host"] as const;
  for (let index = 0; index < samples.length; index += 1) {
    const sample = samples[index];
    if (sample != null && parsePsi(sample)) {
      return { text: sample, psiSource: sources[Math.min(index, sources.length - 1)] };
    }
  }
  return null;
}

export async function sampleResourceSignals(
  deps: SampleResourceSignalsDeps = {}
): Promise<ResourceSignals> {
  const sourceRead = deps.fs?.readText ?? defaultReadText;
  const readText: ResourcePressureFs["readText"] = async (filePath) => {
    try {
      return await sourceRead(filePath);
    } catch {
      return null;
    }
  };
  let memory: NodeJS.MemoryUsage;
  try {
    memory = (deps.memoryUsage ?? process.memoryUsage)();
  } catch {
    memory = { rss: 0, heapTotal: 0, heapUsed: 0, external: 0, arrayBuffers: 0 };
  }

  let heapUsed = Math.max(0, Math.floor(memory.heapUsed || 0));
  let heapLimit = 0;
  try {
    const heap = (deps.heapStatistics ?? v8.getHeapStatistics)();
    heapLimit = sanitizeMemoryBytes(heap.heap_size_limit) ?? 0;
    if (Number.isFinite(heap.used_heap_size)) {
      heapUsed = Math.max(0, Math.floor(heap.used_heap_size ?? heapUsed));
    }
  } catch {
    /* retain process heap sample */
  }

  const cgroup = await sampleCgroup(readText);
  // /proc/pressure/memory is host-wide. Inside Docker/cgroup that stalls the
  // chat admission gate (503 resource_pressure) when the *machine* is swapping
  // even if this container is idle. Prefer this unit's cgroup PSI; fall back
  // to the host file only when the cgroup sample is missing or unparseable
  // (bare metal, cgroup v1, or a stub filesystem).
  const hostPsi = await readText("/proc/pressure/memory").catch(() => null);
  const psiWinner = firstParseablePsi(cgroup.pressure, hostPsi);
  const parsedPsi = psiWinner ? parsePsi(psiWinner.text) : null;

  return {
    observedAtMs: (deps.nowMs ?? Date.now)(),
    v8: { heapUsedBytes: heapUsed, heapLimitBytes: heapLimit },
    process: {
      rssBytes: Math.max(0, Math.floor(memory.rss || 0)),
      externalBytes: Math.max(0, Math.floor(memory.external || 0)),
      arrayBuffersBytes: Math.max(0, Math.floor(memory.arrayBuffers || 0)),
      availableBytes: safeNumber(deps.availableMemory ?? (() => process.availableMemory?.())),
      constrainedBytes: safeNumber(deps.constrainedMemory ?? (() => process.constrainedMemory?.())),
    },
    cgroup: cgroup.signals,
    psi: parsedPsi ? { ...parsedPsi, psiSource: psiWinner?.psiSource ?? null } : null,
  };
}
