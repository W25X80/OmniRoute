import { types } from "node:util";

const MAX_GRAPH_DEPTH = 64;
const MAX_GRAPH_VALUES = 100_000;

/**
 * Inspect plain data before structuredClone. Count UTF-16 storage, slots and deliberately
 * generous node/property overhead; this is an admission estimate, not an RSS ceiling.
 * Reject accessors, proxies, exotic objects and excessive traversal without invoking them.
 */
export function retainedPlainDataBytes(
  value: unknown,
  maximum: number,
  initial: number
): number | null {
  let bytes = initial;
  let values = 0;
  const seen = new WeakSet<object>();
  const visit = (item: unknown, depth: number): boolean => {
    if (++values > MAX_GRAPH_VALUES || depth > MAX_GRAPH_DEPTH) return false;
    if (typeof item === "string") bytes += 64 + item.length * 2;
    else if (
      item === null ||
      item === undefined ||
      typeof item === "boolean" ||
      typeof item === "number"
    )
      bytes += 16;
    else if (typeof item !== "object" || types.isProxy(item)) return false;
    else {
      bytes += 16;
      if (bytes > maximum || seen.has(item)) return bytes <= maximum;
      seen.add(item);
      const array = Array.isArray(item);
      const prototype = Object.getPrototypeOf(item);
      if (prototype !== (array ? Array.prototype : Object.prototype) && prototype !== null)
        return false;
      bytes += 128 + (array ? item.length * 8 : 0);
      if (bytes > maximum) return false;
      // Do not first allocate Object.entries/descriptors for an arbitrarily large graph.
      for (const key in item) {
        const descriptor = Object.getOwnPropertyDescriptor(item, key);
        if (!descriptor?.enumerable) continue;
        if (!("value" in descriptor)) return false;
        bytes += 64 + key.length * 2;
        if (bytes > maximum || !visit(descriptor.value, depth + 1)) return false;
      }
    }
    return bytes <= maximum;
  };
  return visit(value, 0) ? bytes : null;
}
