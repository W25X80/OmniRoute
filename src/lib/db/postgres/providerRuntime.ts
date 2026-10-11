import type { PostgresDatabase, PostgresTransaction } from "./database";
import { lockProviders } from "./providerConnectionSemantics";
import { lockApiKeyPolicy } from "./apiKeyPolicyState";

/** Native provider writes use provider namespace 4 before key-policy namespace 5. */
export function writeProviderRuntime<T>(
  database: PostgresDatabase,
  body: (transaction: PostgresTransaction) => Promise<T>
): Promise<T> {
  return database.transaction(async (transaction) => {
    await lockProviders(transaction);
    await lockApiKeyPolicy(transaction);
    return body(transaction);
  });
}

export class ProviderRuntimeEffectsError extends Error {
  readonly committed = true;
  constructor(readonly operation: string) {
    super("Provider runtime write committed but cache invalidation failed");
    this.name = "ProviderRuntimeEffectsError";
  }
}

/** No raw callback/driver diagnostics, SQL replay or effects inside the transaction. */
export async function afterProviderRuntimeWrite(
  operation: string,
  effect: () => void | Promise<void>
): Promise<void> {
  try {
    await effect();
  } catch {
    throw new ProviderRuntimeEffectsError(operation);
  }
}
