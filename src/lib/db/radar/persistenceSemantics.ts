/** Shared Radar identity and display-name validation; no database imports. */

// ---------------------------------------------------------------------------
// radar_local_model_state
// ---------------------------------------------------------------------------

const RADAR_PROVIDER_PATTERN = /^[a-z0-9][a-z0-9._-]{0,99}$/i;

const CONTROL_CHARACTER_PATTERN = /[\u0000-\u001f\u007f]/;

export function normalizeRadarIdentity(
  provider: unknown,
  modelId: unknown
): { provider: string; modelId: string } | null {
  const normalizedProvider = typeof provider === "string" ? provider.trim() : "";
  const normalizedModelId = typeof modelId === "string" ? modelId.trim() : "";
  if (!RADAR_PROVIDER_PATTERN.test(normalizedProvider)) return null;
  if (
    normalizedModelId.length < 1 ||
    normalizedModelId.length > 200 ||
    CONTROL_CHARACTER_PATTERN.test(normalizedModelId)
  ) {
    return null;
  }
  return { provider: normalizedProvider, modelId: normalizedModelId };
}

export function normalizeDisplayName(value: unknown): string | null | undefined {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (typeof value !== "string") return undefined;
  const normalized = value.trim();
  if (
    normalized.length < 1 ||
    normalized.length > 160 ||
    CONTROL_CHARACTER_PATTERN.test(normalized)
  ) {
    return undefined;
  }
  return normalized;
}
