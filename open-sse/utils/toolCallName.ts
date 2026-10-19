/** Tool-call name accumulation and resolution against request-declared spellings. */

/** Return the leaf following the last flattened namespace separator. */
function leafOf(name: string): string {
  const separator = name.lastIndexOf("__");
  return separator >= 0 ? name.slice(separator + 2) : name;
}

function toNameSet(values: Iterable<string> | null | undefined): Set<string> {
  const set = new Set<string>();
  if (!values) return set;
  for (const value of values) {
    if (typeof value === "string" && value) set.add(value);
  }
  return set;
}

/** Accumulate streamed function-name fragments, repeats, progressive resends, and suffix resends. */
export function appendToolCallNameDelta(
  current: unknown,
  incoming: unknown,
  declaredNames?: Iterable<string> | null
): string {
  const previous = typeof current === "string" ? current.trim() : "";
  const delta = typeof incoming === "string" ? incoming.trim() : "";
  if (!delta) return previous;
  if (!previous) return delta;
  if (delta === previous) return previous;
  if (delta.startsWith(previous)) return delta;

  // Prefer a continuation that still fits a declaration before applying the
  // suffix-resend rule; repeated letters at a boundary are otherwise ambiguous.
  const declared = toNameSet(declaredNames);
  const combined = previous + delta;
  if ([...declared].some((name) => name.startsWith(combined))) return combined;
  if (previous.endsWith(delta)) return previous;
  return combined;
}

/** Resolve exact, dotted, or unambiguous bare names without guessing ambiguous leaves. */
export function resolveDeclaredToolName(
  declaredNames: Iterable<string> | null | undefined,
  wireName: unknown,
  explicitNames?: Iterable<string> | null
): string | null {
  const name = typeof wireName === "string" ? wireName.trim() : "";
  if (!name) return null;
  const declared = toNameSet(declaredNames);
  if (declared.size === 0) return null;
  if (declared.has(name)) return name;

  const dot = name.lastIndexOf(".");
  if (dot > 0 && dot < name.length - 1) {
    const flattened = `${name.slice(0, dot)}__${name.slice(dot + 1)}`;
    if (declared.has(flattened)) return flattened;
  }

  if (name.includes("__") || toNameSet(explicitNames).has(name)) return null;
  const leafMatches: string[] = [];
  for (const candidate of declared) {
    if (leafOf(candidate) === name) leafMatches.push(candidate);
  }
  return leafMatches.length === 1 ? leafMatches[0] : null;
}

/** Test whether a wire name denotes a declared custom tool rather than a flat function. */
export function matchesCustomToolDeclaration(options: {
  customToolNames?: Iterable<string> | null;
  declaredFunctionSchemas?: unknown;
  toolName: unknown;
}): boolean {
  const { customToolNames, declaredFunctionSchemas, toolName } = options;
  if (!customToolNames) return false;
  const explicitNames =
    declaredFunctionSchemas instanceof Map
      ? declaredFunctionSchemas.keys()
      : declaredFunctionSchemas &&
          typeof (declaredFunctionSchemas as { keys?: unknown }).keys === "function"
        ? (declaredFunctionSchemas as Map<string, unknown>).keys()
        : null;
  return resolveDeclaredToolName(customToolNames, toolName, explicitNames) !== null;
}
