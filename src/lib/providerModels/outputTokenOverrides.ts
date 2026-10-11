import { assertSqliteBackendAllowed } from "../db/controlPlaneContext";
import { removeModelCapabilityOverrideAsync } from "@/lib/db/modelCapabilityOverrides";
import { setModelCapabilityOverrideAsync } from "@/lib/db/modelCapabilityOverrides";
import {
  listModelCapabilityOverrides,
  removeModelCapabilityOverride,
  setModelCapabilityOverride,
} from "@/lib/db/modelCapabilityOverrides";

/** Provider-page overrides share the runtime's max_output_tokens key. */
export function listProviderOutputOverrides(provider: string | null) {
  if (!provider) return [];
  return listModelCapabilityOverrides().flatMap((row) =>
    row.provider === provider && row.key === "max_output_tokens"
      ? [{ modelId: row.modelId, maxOutputTokenOverride: row.value }]
      : []
  );
}

/** Call only after validating the provider/model mutation target. */
export async function persistOutputTokenOverrideAsync(
  provider: string,
  modelId: string,
  value: number | null | undefined
): Promise<{ maxOutputTokenOverride?: number | null }> {
  if (value === undefined) return {};
  const target = `${provider}/${modelId}`;
  if (value === null) await removeModelCapabilityOverrideAsync(target, "max_output_tokens");
  else if (!(await setModelCapabilityOverrideAsync(target, "max_output_tokens", value))) {
    throw new Error("Failed to persist output token override");
  }
  return { maxOutputTokenOverride: value };
}

export function persistOutputTokenOverride(
  provider: string,
  modelId: string,
  value: number | null | undefined
): { maxOutputTokenOverride?: number | null } {
  assertSqliteBackendAllowed();
  if (value === undefined) return {};
  const target = `${provider}/${modelId}`;
  if (value === null) removeModelCapabilityOverride(target, "max_output_tokens");
  else if (!setModelCapabilityOverride(target, "max_output_tokens", value)) {
    throw new Error("Failed to persist output token override");
  }
  return { maxOutputTokenOverride: value };
}
