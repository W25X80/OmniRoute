/** Admission/drain oracle: it deliberately does not execute network/domain business work. */
export const schedulerFixture = {
  identity: {},
  admissions: [] as Array<() => Promise<unknown>>,
  barrier: undefined as Promise<void> | undefined,
  providers: [] as Array<Record<string, unknown>>,
};
export async function runPersistenceBackgroundWork<T>(body: () => Promise<T>): Promise<T> {
  schedulerFixture.admissions.push(body);
  await schedulerFixture.barrier;
  return { success: false, ok: false, error: "Fixture completion" } as T;
}
export function isArenaEloSyncEnabled(): boolean {
  return true;
}
export { sanitizeErrorMessage } from "../../../open-sse/utils/errorSanitization.ts";
function unusedDomainWork(): never {
  throw new Error("Domain work is outside the admission oracle");
}
export const getCodexUsage = unusedDomainWork;
export const refreshAndUpdateCredentialsWithResolver = unusedDomainWork;
export const getCircuitBreaker = unusedDomainWork;
export const getSettings = unusedDomainWork;
export const resolveProxyForConnection = unusedDomainWork;
export async function getProviderConnections(): Promise<Array<Record<string, unknown>>> {
  return schedulerFixture.providers;
}
export const updateProviderConnection = unusedDomainWork;
export const isConnectionUnavailableToAuxiliaryActivity = unusedDomainWork;
export const isModelSelectable = unusedDomainWork;
export const runWithProxyContext = unusedDomainWork;
export const getResolvedModelCapabilities = unusedDomainWork;
export const listMemoriesForDecayAsync = unusedDomainWork;
export const deleteMemory = unusedDomainWork;

export function getSelectedPersistenceIdentity(): object {
  return schedulerFixture.identity;
}
