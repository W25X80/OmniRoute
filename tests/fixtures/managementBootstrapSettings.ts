import { updateSettings as persistSettings } from "../../src/lib/db/settings.ts";
import { SettingsRevisionConflictError } from "../../src/lib/db/settings/persistenceSemantics.ts";
import { PostgresError } from "../../src/lib/db/postgres/database.ts";
import { PostgresSettingsEffectsError } from "../../src/lib/db/postgres/settingsRepository.ts";

export { getSettings, SettingsRevisionConflictError } from "../../src/lib/db/settings.ts";

export type BootstrapMutationMode =
  | "acknowledged"
  | "conflict"
  | "rollback"
  | "commit_unknown"
  | "committed_effect"
  | "unbranded"
  | "unknown";

let mode: BootstrapMutationMode = "acknowledged";
export let writes = 0;
export function selectBootstrapMutationMode(next: BootstrapMutationMode): void {
  mode = next;
  writes = 0;
}

/** Caller-outcome injection only; real PostgreSQL reply loss is tested separately. */
export async function updateSettings(...args: Parameters<typeof persistSettings>) {
  writes++;
  if (mode === "conflict") throw new SettingsRevisionConflictError(0);
  if (mode === "rollback") throw new PostgresError("transaction_failed");
  if (mode === "unbranded")
    throw Object.assign(new Error("Unbranded storage failure"), { kind: "transaction_failed" });
  if (mode === "unknown") throw new Error("Unclassified storage failure");
  const result = await persistSettings(...args);
  if (mode === "commit_unknown") throw new PostgresError("commit_unknown");
  if (mode === "committed_effect") throw new PostgresSettingsEffectsError(["settings_cache"]);
  return result;
}
