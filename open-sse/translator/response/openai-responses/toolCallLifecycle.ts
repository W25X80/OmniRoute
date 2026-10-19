import { canonicalizeDeclaredToolName } from "./declaredToolName.ts";
import { appendToolCallNameDelta, resolveDeclaredToolName } from "../../../utils/toolCallName.ts";
import { resolveRequestToolIdentity } from "./requestToolIdentity.ts";

type ToolMetadata = {
  declaredToolNames?: Iterable<string> | null;
  customToolNames?: Iterable<string> | null;
  toolSchemas?: ReadonlyMap<string, unknown> | null;
  requestToolIdentityMap?: unknown;
};
type SelectedTool = { name: string; namespace?: string; custom: boolean; wireName: string };

/** Per-response decisions: defer undecided names, then freeze published identity and type. */
export class ToolCallLifecycle {
  private readonly metadata: ToolMetadata & { customToolNames: Set<string> };
  private readonly declaredNames: Set<string>;
  private readonly explicitNames: Set<string>;
  private readonly wireSpellings: Set<string>;
  private readonly selected = new Map<string, SelectedTool>();
  private readonly identities = new Map<string, { namespace: string; name: string }>();
  private readonly aliases = new Map<string, Set<string>>();

  constructor(
    metadata: ToolMetadata,
    private readonly nativeApplyPatch = false
  ) {
    this.metadata = { ...metadata, customToolNames: new Set(metadata.customToolNames || []) };
    this.explicitNames = new Set(metadata.toolSchemas?.keys() || []);
    const ledger = this.metadata.requestToolIdentityMap;
    const identityNames =
      ledger instanceof Map
        ? [...ledger.keys()]
        : ledger && typeof ledger === "object"
          ? Object.keys(ledger)
          : [];
    this.declaredNames = new Set([
      ...(metadata.declaredToolNames || []),
      ...(this.metadata.customToolNames || []),
      ...(metadata.toolSchemas?.keys() || []),
      ...identityNames,
    ]);
    const entries = ledger instanceof Map ? [...ledger] : Object.entries(ledger || {});
    for (const [wireName, value] of entries) this.addIdentity(wireName, value);
    this.wireSpellings = new Set([...this.declaredNames, ...this.aliases.keys()]);
    for (const name of this.declaredNames) {
      if (this.identities.has(name)) continue;
      const separator = name.lastIndexOf("__");
      if (separator > 0) {
        const leaf = name.slice(separator + 2);
        this.wireSpellings.add(leaf);
        this.wireSpellings.add(`${name.slice(0, separator)}.${leaf}`);
      }
    }
  }

  private addIdentity(wireName: unknown, value: unknown): void {
    if (
      typeof wireName !== "string" ||
      !wireName ||
      !value ||
      typeof value !== "object" ||
      Array.isArray(value)
    ) {
      return;
    }
    const { namespace, name } = value as Record<string, unknown>;
    if (typeof namespace !== "string" || !namespace || typeof name !== "string" || !name) return;
    this.identities.set(wireName, { namespace, name });
    for (const alias of [name, `${namespace}.${name}`]) {
      const names = this.aliases.get(alias) ?? new Set<string>();
      names.add(wireName);
      this.aliases.set(alias, names);
    }
  }

  private uniqueAlias(name: string): string | null {
    const aliases = this.aliases.get(name);
    return aliases?.size === 1 ? [...aliases][0] : null;
  }

  private declaredName(name: string): string | null {
    // Exact flat or qualified declarations outrank aliases and prefix matching.
    if (this.declaredNames.has(name)) return name;
    const directAlias = this.uniqueAlias(name);
    if (directAlias) return directAlias;
    const resolved = resolveDeclaredToolName(this.declaredNames, name, this.explicitNames);
    if (resolved) return resolved;

    // A provider may prepend a dotted marker such as `functions.exec` to a flat
    // declaration, or prepend another dotted segment to a declared namespace alias.
    const canonical = canonicalizeDeclaredToolName(name, this.declaredNames);
    if (canonical.name !== name) {
      if (this.declaredNames.has(canonical.name)) return canonical.name;
      const suffixAlias = this.uniqueAlias(canonical.name);
      if (suffixAlias) return suffixAlias;
    }
    if (name.includes(".")) {
      const parts = name.split(".");
      for (let i = 1; i < parts.length - 1; i++) {
        const suffix = parts.slice(i).join(".");
        const alias = this.uniqueAlias(suffix);
        if (alias) return alias;
        const suffixName = resolveDeclaredToolName(this.declaredNames, suffix, this.explicitNames);
        if (suffixName) return suffixName;
      }
    }
    return null;
  }

  appendName(index: string | number, previous: string, incoming: unknown): string {
    if (this.selected.has(String(index))) return previous;
    return appendToolCallNameDelta(previous, incoming, this.wireSpellings);
  }

  reset(index: string | number): void {
    this.selected.delete(String(index));
  }

  get(index: string | number): SelectedTool | undefined {
    return this.selected.get(String(index));
  }

  select(index: string | number, name: string, terminal = false): SelectedTool | null {
    const published = this.get(index);
    if (published) return published;
    const declared = this.declaredName(name);
    const nativePatch =
      this.nativeApplyPatch &&
      /^(apply_patch|applypatch)$/i.test(name) &&
      !this.metadata.toolSchemas?.has(name);
    const isPrefix = [...this.wireSpellings].some(
      (candidate) => candidate !== name && candidate.startsWith(name)
    );
    if (!terminal && ((!declared && !nativePatch) || isPrefix)) return null;
    const selection = this.resolveSelection(name, declared, nativePatch);
    this.selected.set(String(index), selection);
    return selection;
  }

  private resolveSelection(
    name: string,
    declared: string | null,
    nativePatch: boolean
  ): SelectedTool {
    const custom = nativePatch || Boolean(declared && this.metadata.customToolNames.has(declared));
    const literalFlat = this.metadata.toolSchemas?.has(name) && !this.identities.has(name);
    const identity =
      !literalFlat && (declared || this.declaredNames.size === 0)
        ? (this.identities.get(declared || "") ??
          resolveRequestToolIdentity(this.metadata.requestToolIdentityMap, name, {
            explicitNames: this.explicitNames,
          }))
        : null;
    return {
      name: identity?.name ?? declared ?? name,
      ...(identity ? { namespace: identity.namespace } : {}),
      custom,
      wireName: declared ?? name,
    };
  }
}

const lifecycles = new WeakMap<object, ToolCallLifecycle>();

/** Keep generic translator lifecycle state private to one response state object. */
export function translatorToolCallLifecycle(state: ToolMetadata): ToolCallLifecycle {
  let lifecycle = lifecycles.get(state);
  if (!lifecycle) {
    lifecycle = new ToolCallLifecycle(state, true);
    lifecycles.set(state, lifecycle);
  }
  return lifecycle;
}
