import { fileIOState } from "./nativeProviderFileIOState.ts";
export const parseDatabaseBackend = () => (fileIOState.native ? "postgres" : "sqlite");
export const getSelectedPersistenceIdentity = () => fileIOState.identity;
export const isFeatureFlagEnabled = () => fileIOState.featureEnabled;
export function runPersistenceBackgroundWork<T>(body: () => Promise<T>): Promise<T> {
  if (fileIOState.refuseBackground)
    return Promise.reject(new Error("Controlled admission refusal"));
  return new Promise<T>((resolve, reject) => {
    fileIOState.background.push({ body, resolve: (value) => resolve(value as T), reject });
  });
}
export async function updateProviderConnection(id: string, update: Record<string, unknown>) {
  fileIOState.writes.push({ id, update });
  return { id, ...update };
}
export async function refreshAdobeFireflyViaCdp() {
  return fileIOState.browserResult;
}
