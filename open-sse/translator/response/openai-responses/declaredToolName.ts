// Pure, stateless helpers: map a provider-injected, dotted tool-call name back to
// a tool name the request actually declared.
//
// Some Chat Completions upstreams echo tool calls with a harmony-style prefix the
// client never declared (`functions.exec` while the request declared a flat
// `exec`). A Responses client dispatches on the exact declared name, so match
// dotted suffixes against declared names only; never guess or fuzzily rewrite.

type JsonRecord = Record<string, unknown>;

const TOOL_NAME_PREFIX_SEPARATOR = ".";

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonRecord) : null;
}

function addName(names: Set<string>, value: unknown) {
  if (typeof value !== "string") return;
  const name = value.trim();
  if (name) names.add(name);
}

/** Collect every tool name declared by a request body's tools and namespace children. */
export function collectDeclaredToolNames(body: unknown): Set<string> | null {
  const tools = asRecord(body)?.tools;
  if (!Array.isArray(tools)) return null;

  const names = new Set<string>();
  for (const tool of tools) {
    const item = asRecord(tool);
    if (!item) continue;
    addName(names, asRecord(item.function)?.name);
    addName(names, item.name);
    if (item.type === "namespace" && Array.isArray(item.tools)) {
      for (const child of item.tools) addName(names, asRecord(child)?.name);
    }
  }
  return names.size > 0 ? names : null;
}

/** Names carried by a request-scoped tool-name map or identity ledger. */
export function toolNameSideChannelNames(map: unknown): string[] {
  const entries =
    map instanceof Map
      ? [...map.entries()]
      : asRecord(map)
        ? Object.entries(map as JsonRecord)
        : [];
  const names: string[] = [];
  for (const [key, value] of entries) {
    if (typeof key === "string") names.push(key);
    if (typeof value === "string") names.push(value);
  }
  return names;
}

/** Union every non-empty string from the given request-scoped sources. */
export function mergeDeclaredToolNames(
  ...sources: Array<Iterable<unknown> | null | undefined>
): Set<string> | null {
  const names = new Set<string>();
  for (const source of sources) {
    if (!source) continue;
    for (const value of source) addName(names, value);
  }
  return names.size > 0 ? names : null;
}

export type CanonicalToolName = {
  name: string;
  unresolved: boolean;
};

/**
 * Canonicalize an emitted name only when a dotted suffix exactly matches a declared name.
 * Exact declarations win, and unresolved names are forwarded unchanged.
 */
export function canonicalizeDeclaredToolName(
  name: string,
  declaredNames: ReadonlySet<string> | null | undefined
): CanonicalToolName {
  if (!name || !declaredNames || declaredNames.size === 0) return { name, unresolved: false };
  if (!name.includes(TOOL_NAME_PREFIX_SEPARATOR) || declaredNames.has(name)) {
    return { name, unresolved: false };
  }

  const parts = name.split(TOOL_NAME_PREFIX_SEPARATOR);
  for (let i = 1; i < parts.length; i++) {
    const candidate = parts.slice(i).join(TOOL_NAME_PREFIX_SEPARATOR);
    if (candidate && declaredNames.has(candidate)) return { name: candidate, unresolved: false };
  }
  return { name, unresolved: true };
}
