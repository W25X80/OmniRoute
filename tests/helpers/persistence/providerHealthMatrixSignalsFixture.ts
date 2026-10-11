export const matrixSignalsFixture = { aliases: new Map<string, string>() };
export async function getProviderConnections() {
  return [];
}
export async function getSyncedAvailableModelsByConnection() {
  return {};
}
export function getAllCircuitBreakerStatuses() {
  return [];
}
export function getAllModelLockouts() {
  return [];
}
export function resolveProviderAlias(provider: string) {
  return matrixSignalsFixture.aliases.get(provider) ?? provider;
}
export function getWebSessionPoolHealth() {
  return { providers: [] };
}
