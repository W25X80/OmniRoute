import type { EmbeddingResolution, EmbeddingResult } from "../../../src/lib/memory/embedding/types";

export const reindexResolution: EmbeddingResolution = {
  source: "static",
  model: "reindex-race-fixture",
  dimensions: 2,
  signature: "static:reindex-race-fixture:2",
  reason: "deterministic reindex fixture",
};

// Only embedding/configuration are replaced. The batch and PostgreSQL owners remain real.
export async function getMemorySettings() {
  return { embeddingSource: "static", staticEnabled: true };
}

export function resolveEmbeddingSource(): EmbeddingResolution {
  return reindexResolution;
}

export function withMeasuredDimensions(
  resolution: EmbeddingResolution,
  dimensions: number
): EmbeddingResolution {
  return { ...resolution, dimensions };
}

export async function embed(): Promise<EmbeddingResult> {
  return {
    vector: new Float32Array([1, 0]),
    source: "static",
    model: "reindex-race-fixture",
    dimensions: 2,
    latencyMs: 0,
    cached: false,
  };
}
