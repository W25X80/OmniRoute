import { isEgressBucketedLockScope } from "@omniroute/open-sse/config/providerErrorRules.ts";

/** Egress-bucketed quota and Cloudflare session cookies require a stable pool member. */
export function requiresStableEgress(provider: string | null): boolean {
  return Boolean(
    provider && (isEgressBucketedLockScope(provider) || provider.toLowerCase() === "grok-web")
  );
}

// Native pins and independently bundled SQLite caches observe the same invalidation.
const key = Symbol.for("omniroute.proxy-config-generation.v1");
const shared = globalThis as typeof globalThis & { [key]?: { generation: number } };
const state = (shared[key] ??= { generation: 0 });

export function getProxyConfigGeneration(): number {
  return state.generation;
}

export function advanceProxyConfigGeneration(): void {
  state.generation++;
}
