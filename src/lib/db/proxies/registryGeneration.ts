// Shared registry-generation counter for the proxy registry. Split out so both
// `proxies.ts` and `proxySubscriptions.ts` can bump/read it without a cyclic
// import between the two sibling modules.
const key = Symbol.for("omniroute.proxy-registry-generation.v1");
const shared = globalThis as typeof globalThis & { [key]?: { generation: number } };
const state = (shared[key] ??= { generation: 0 });

export function bumpProxyRegistryGeneration() {
  state.generation++;
}

export function getProxyRegistryGeneration() {
  return state.generation;
}
