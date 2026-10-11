import type {
  SettingsRecord,
  SettingsRepository,
  UpdateSettingsOptions,
} from "@/domain/persistence/settingsRepository";
import { decrypt, encrypt, looksEncrypted } from "../encryption";
import {
  interpretSettingsRows,
  needsHeadlessOnboarding,
  parseSettingsRevision,
  SETTINGS_REVISION_KEY,
  SettingsRevisionConflictError,
  type SettingsEnvironment,
} from "../settings/persistenceSemantics";
import type { PostgresDatabase, PostgresTransaction } from "./database";

/** Required composition callbacks. Tests must supply explicit inert callbacks. */
export interface PostgresSettingsEffects {
  invalidateSettingsCache(): void | Promise<void>;
  bumpProxyConfigGeneration(): void | Promise<void>;
  /** Best effort, matching the existing SQLite hot-reload behavior. */
  applyRuntimeSettings(
    settings: SettingsRecord,
    options: { source: "settings:update" }
  ): void | Promise<void>;
  /** Best effort and scheduled without delaying the update acknowledgement. */
  scheduleCatalogRevalidationAfterInit(): void | Promise<void>;
}

export interface PostgresSettingsOptions {
  environment?: () => SettingsEnvironment;
  /** Internal composition only: awaited SQL work on this transaction, never external effects. */
  markPolicyChanged?: (transaction: PostgresTransaction) => Promise<void>;
}

export class PostgresSettingsEffectsError extends Error {
  readonly committed = true;

  constructor(readonly failedEffects: readonly string[]) {
    super("Settings update committed but follow-up operations failed");
    this.name = "PostgresSettingsEffectsError";
  }
}

async function lockSettings(transaction: PostgresTransaction): Promise<void> {
  // This lock also covers the absent revision row and headless-onboarding writes.
  await transaction.query("SELECT pg_advisory_xact_lock(18765, 3)");
}

async function readSettingsRows(transaction: PostgresTransaction) {
  return (
    await transaction.query<{ key: string; value: string }>(
      "SELECT key, value FROM omniroute.key_value WHERE namespace = 'settings'"
    )
  ).rows;
}

async function putSetting(
  transaction: PostgresTransaction,
  key: string,
  value: unknown
): Promise<void> {
  await transaction.query(
    `INSERT INTO omniroute.key_value(namespace, key, value) VALUES ('settings', $1, $2)
     ON CONFLICT (namespace, key) DO UPDATE SET value = EXCLUDED.value`,
    [key, JSON.stringify(value)]
  );
}

export function createPostgresSettingsRepository(
  database: PostgresDatabase,
  effects: PostgresSettingsEffects,
  options: PostgresSettingsOptions = {}
): SettingsRepository {
  const environment = options.environment ?? (() => process.env);

  async function get(): Promise<SettingsRecord> {
    return database.transaction(async (transaction) => {
      await lockSettings(transaction);
      const env = environment();
      const settings = interpretSettingsRows(await readSettingsRows(transaction), env, decrypt);
      if (needsHeadlessOnboarding(settings, env)) {
        await putSetting(transaction, "setupComplete", true);
        await putSetting(transaction, "requireLogin", true);
        settings.setupComplete = true;
        settings.requireLogin = true;
      }
      return settings;
    });
  }

  async function update(
    updates: SettingsRecord,
    updateOptions?: UpdateSettingsOptions
  ): Promise<SettingsRecord> {
    // Match SQLite: detecting first setup completion includes its ordinary get behavior.
    let setupJustCompleted = false;
    if (updates.setupComplete === true) {
      try {
        setupJustCompleted = (await get()).setupComplete !== true;
      } catch {
        setupJustCompleted = true;
      }
    }

    await database.transaction(async (transaction) => {
      await lockSettings(transaction);
      const rows = await readSettingsRows(transaction);
      const currentRevision = parseSettingsRevision(
        rows.find((row) => row.key === SETTINGS_REVISION_KEY)?.value
      );
      if (
        updateOptions?.expectedRevision !== undefined &&
        updateOptions.expectedRevision !== currentRevision
      ) {
        throw new SettingsRevisionConflictError(currentRevision);
      }
      await options.markPolicyChanged?.(transaction);
      for (const [key, value] of Object.entries(updates)) {
        const persisted = key === "oidcClientSecret" ? encrypt(value as string) : value;
        if (
          key === "oidcClientSecret" &&
          typeof value === "string" &&
          value &&
          !looksEncrypted(persisted)
        ) {
          throw new Error("Settings credential encryption failed");
        }
        await putSetting(transaction, key, persisted);
      }
      await putSetting(transaction, SETTINGS_REVISION_KEY, currentRevision + 1);
    });

    // COMMIT acknowledgement precedes every effect. Cache failures are independent;
    // try all applicable effects and report durable success separately from effect failure.
    const failed: string[] = [];
    try {
      await effects.invalidateSettingsCache();
    } catch {
      failed.push("settings_cache");
    }
    if (
      Object.keys(updates).some((key) => key === "proxyEnabled" || key === "perKeyProxyEnabled")
    ) {
      try {
        await effects.bumpProxyConfigGeneration();
      } catch {
        failed.push("proxy_config");
      }
    }

    let nextSettings: SettingsRecord;
    try {
      nextSettings = await get();
    } catch {
      failed.push("settings_readback");
      throw new PostgresSettingsEffectsError(failed);
    }
    try {
      await effects.applyRuntimeSettings(nextSettings, { source: "settings:update" });
    } catch {
      // Existing runtime hot reload is best effort; durable settings remain authoritative.
    }
    if (setupJustCompleted) {
      void Promise.resolve()
        .then(() => effects.scheduleCatalogRevalidationAfterInit())
        .catch(() => {
          // Existing one-shot setup catalog scheduling is best effort.
        });
    }
    if (failed.length > 0) throw new PostgresSettingsEffectsError(failed);
    return nextSettings;
  }

  return {
    get,
    getRevision: () =>
      database.transaction(async (transaction) => {
        const { rows } = await transaction.query<{ value: string }>(
          "SELECT value FROM omniroute.key_value WHERE namespace = 'settings' AND key = $1",
          [SETTINGS_REVISION_KEY]
        );
        return parseSettingsRevision(rows[0]?.value);
      }),
    update,
  };
}
