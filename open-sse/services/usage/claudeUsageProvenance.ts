/** Server-only provenance for Claude usage observations. It is deliberately non-enumerable. */
export type ClaudeUsageSource =
  "oauth" | "legacy" | "success-memo" | "stale-429" | "error" | "superseded";

export interface ClaudeUsageProvenance {
  source: ClaudeUsageSource;
  observedAt: number | null;
  producerId: number | null;
  generation: number;
}

const provenanceByResult = new WeakMap<object, ClaudeUsageProvenance>();
let producerSequence = 0;
let usageGeneration = 0;

/** Assign a unique identity to a new network poll, independent of the wall clock. */
export function beginClaudeUsageProducer(): number {
  producerSequence += 1;
  return producerSequence;
}

/** Latest poll identity, used to fence invalidations against already-running reads. */
export function currentClaudeUsageProducerId(): number {
  return producerSequence;
}

export function currentClaudeUsageGeneration(): number {
  return usageGeneration;
}

/** Fence callers already holding results when the usage cache is reset. */
export function advanceClaudeUsageGeneration(): number {
  usageGeneration += 1;
  return usageGeneration;
}

export function setClaudeUsageProvenance<T extends object>(
  result: T,
  provenance: ClaudeUsageProvenance
): T {
  provenanceByResult.set(result, Object.freeze({ ...provenance }));
  return result;
}

export function getClaudeUsageProvenance(value: unknown): ClaudeUsageProvenance | null {
  return value !== null && typeof value === "object"
    ? (provenanceByResult.get(value) ?? null)
    : null;
}

/** Preserve server-only metadata when a consumer sanitizes/clones a usage DTO. */
export function copyClaudeUsageProvenance<T extends object>(source: unknown, target: T): T {
  const provenance = getClaudeUsageProvenance(source);
  if (provenance) provenanceByResult.set(target, provenance);
  return target;
}

/** Only direct, current network observations may update routing or durable state. */
export function isFreshClaudeUsage(value: unknown): boolean {
  const provenance = getClaudeUsageProvenance(value);
  return Boolean(
    provenance &&
    (provenance.source === "oauth" || provenance.source === "legacy") &&
    provenance.producerId !== null &&
    Number.isFinite(provenance.producerId) &&
    provenance.generation === usageGeneration &&
    provenance.observedAt !== null &&
    Number.isFinite(provenance.observedAt)
  );
}

export function isClaudeUsageDisplayOnly(value: unknown): boolean {
  const source = getClaudeUsageProvenance(value)?.source;
  return source === "success-memo" || source === "stale-429";
}

/** Strip cached quota authority before a non-display usage consumer sees it. */
export function redactClaudeUsageForPolicy<T extends object>(
  value: T
): T | Record<string, unknown> {
  if (isFreshClaudeUsage(value)) return value;
  const usage = value as Record<string, unknown>;
  const provenance = getClaudeUsageProvenance(value);
  const isReplay = isClaudeUsageDisplayOnly(value);
  return copyClaudeUsageProvenance(value, {
    message:
      typeof usage.message === "string" ? usage.message : "Claude usage has no fresh observation.",
    ...(isReplay || (provenance !== null && provenance.observedAt !== null)
      ? { _stale: true }
      : {}),
    ...(typeof usage._staleSince === "string"
      ? { _staleSince: usage._staleSince }
      : provenance?.observedAt !== null && provenance?.observedAt !== undefined
        ? { _staleSince: new Date(provenance.observedAt).toISOString() }
        : {}),
    ...(typeof usage._staleReason === "string" ? { _staleReason: usage._staleReason } : {}),
    ...(usage._cached === true ? { _cached: true } : {}),
    ...(typeof usage._cachedAt === "string" ? { _cachedAt: usage._cachedAt } : {}),
  });
}
