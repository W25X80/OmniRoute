import { getSelectedControlPlane } from "../controlPlaneContext";
import { decrypt } from "../encryption";
import { interpretSettingsRows, needsHeadlessOnboarding } from "../settings/persistenceSemantics";
import type { SettingsEnvironment, SettingsRecord } from "../settings/persistenceSemantics";
import {
  catalogNamespaceRows,
  hasPostgresCatalogSnapshot,
  catalogEnvironmentValue,
} from "./catalogSnapshot";

/** Pure projection of one prepared settings row set; defaults and decryption share SQLite policy. */
export function projectPostgresSettingsSnapshot(
  rows: readonly unknown[],
  environment: SettingsEnvironment,
  decryptSecret: (value: string) => string | null | undefined
): SettingsRecord {
  return interpretSettingsRows(rows, environment, decryptSecret);
}

/** Undefined asks the facade to use its live repository, including initial onboarding writes. */
export function readPostgresSettingsSnapshot(
  environment?: SettingsEnvironment
): SettingsRecord | undefined {
  const database = getSelectedControlPlane()?.database;
  if (!database || !hasPostgresCatalogSnapshot(database)) return undefined;
  const settings = projectPostgresSettingsSnapshot(
    catalogNamespaceRows("settings"),
    environment ?? {
      OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS: catalogEnvironmentValue(
        "OMNIROUTE_PREFER_CLAUDE_CODE_FOR_UNPREFIXED_CLAUDE_MODELS"
      ),
      MAX_BODY_SIZE_BYTES: catalogEnvironmentValue("MAX_BODY_SIZE_BYTES"),
      INITIAL_PASSWORD: catalogEnvironmentValue("INITIAL_PASSWORD"),
    },
    decrypt
  );
  return needsHeadlessOnboarding(
    settings,
    environment ?? { INITIAL_PASSWORD: catalogEnvironmentValue("INITIAL_PASSWORD") }
  )
    ? undefined
    : settings;
}
