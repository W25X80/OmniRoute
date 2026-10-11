import { normalizeEgressAddress } from "@/shared/network/egressAddress";

/** Decode the canonical /64 form written by the operator-egress store. */
export function normalizeStoredOperatorEgressAddress(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const text = value.trim().toLowerCase();
  if (!text.startsWith("v6:")) return normalizeEgressAddress(value);
  const prefix = /^v6:((?:[0-9a-f]{1,4}:){3}[0-9a-f]{1,4})$/.exec(text);
  return prefix ? normalizeEgressAddress(`${prefix[1]}::`) : null;
}
