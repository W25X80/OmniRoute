import { randomUUID } from "node:crypto";
export const AUTH_GENERATION_SETTING = "managementAuthGeneration";

export function readAuthGeneration(settings: Record<string, unknown>): string | null {
  const value = settings[AUTH_GENERATION_SETTING];
  return typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)
    ? value
    : null;
}

export interface AuthSettingsSnapshot {
  settings: Record<string, unknown>;
  revision: number;
  generation: string | null;
}

/** Internal callers may supply their own acknowledged mutation epoch. */
export function withPasswordGeneration(updates: Record<string, unknown>): Record<string, unknown> {
  return typeof updates.password === "string"
    ? { ...updates, [AUTH_GENERATION_SETTING]: readAuthGeneration(updates) ?? randomUUID() }
    : updates;
}
