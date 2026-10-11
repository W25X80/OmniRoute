import { decrypt as sourceDecrypt } from "../../../src/lib/db/encryption.ts";
import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import type { PostgresControlPlaneBinding } from "../../../src/lib/db/controlPlaneContext.ts";

export const proxyControlSecretProbe = {
  values: [] as (string | null | undefined)[],
  bindings: [] as PostgresControlPlaneBinding[],
  observed: undefined as (() => void) | undefined,
  reset() {
    this.values = [];
    this.bindings = [];
    this.observed = undefined;
  },
};

/** Record the real decrypt result without replacing cryptography or selected-scope checks. */
export function decrypt(
  ...args: Parameters<typeof sourceDecrypt>
): ReturnType<typeof sourceDecrypt> {
  const binding = getSelectedControlPlane();
  if (binding) proxyControlSecretProbe.bindings.push(binding);
  const value = sourceDecrypt(...args);
  proxyControlSecretProbe.values.push(value);
  proxyControlSecretProbe.observed?.();
  return value;
}
