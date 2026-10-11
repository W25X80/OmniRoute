import { createHash } from "node:crypto";

export const APPLICATION_IMPORT_DIGEST_ALGORITHM = "sha256-canonical-json-v1" as const;

/** Hash JSON semantics, including array order, independently of object insertion order. */
export function applicationImportDigest(input: unknown): string {
  const digest = createHash("sha256"),
    active = new Set<object>();
  const append = (value: unknown): void => {
    if (value === null) {
      digest.update("null");
      return;
    }
    if (typeof value === "string" || typeof value === "boolean" || typeof value === "number") {
      if (typeof value === "number" && !Number.isFinite(value))
        throw new TypeError("Import digest requires finite JSON numbers");
      digest.update(JSON.stringify(value));
      return;
    }
    if (typeof value !== "object" || active.has(value))
      throw new TypeError("Import digest requires acyclic JSON values");
    active.add(value);
    try {
      if (Array.isArray(value)) {
        digest.update("[");
        for (let index = 0; index < value.length; index++) {
          if (index) digest.update(",");
          append(value[index] === undefined ? null : value[index]);
        }
        digest.update("]");
      } else {
        if (
          Object.getPrototypeOf(value) !== Object.prototype &&
          Object.getPrototypeOf(value) !== null
        )
          throw new TypeError("Import digest requires plain JSON objects");
        const record = value as Record<string, unknown>,
          keys = Object.keys(record)
            .filter((key) => record[key] !== undefined)
            .sort();
        digest.update("{");
        for (let index = 0; index < keys.length; index++) {
          if (index) digest.update(",");
          const key = keys[index];
          digest.update(JSON.stringify(key));
          digest.update(":");
          append(record[key]);
        }
        digest.update("}");
      }
    } finally {
      active.delete(value);
    }
  };
  append(input);
  return digest.digest("hex");
}
