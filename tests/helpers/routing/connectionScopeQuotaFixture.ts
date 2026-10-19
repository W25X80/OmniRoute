export const quotaScopeFixture = { ids: [] as string[], reads: 0, failure: null as Error | null };

export async function resolveQuotaKeyScope() {
  quotaScopeFixture.reads++;
  if (quotaScopeFixture.failure) throw quotaScopeFixture.failure;
  return { connectionIds: [...quotaScopeFixture.ids], providers: [], poolSlugs: [] };
}
