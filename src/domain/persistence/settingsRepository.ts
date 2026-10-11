export type SettingsRecord = Record<string, unknown>;

export interface UpdateSettingsOptions {
  expectedRevision?: number;
}

/**
 * System settings, including the existing headless-onboarding read behavior.
 * update atomically checks the optional revision, persists all fields and increments
 * the revision once, even for an empty update. Conflict applies none of the explicit fields.
 * Acknowledged writes precede application effects; a failed effect must never replay them.
 */
export interface SettingsRepository {
  get(): Promise<SettingsRecord>;
  getRevision(): Promise<number>;
  update(updates: SettingsRecord, options?: UpdateSettingsOptions): Promise<SettingsRecord>;
}
