import type {
  getProviderCredentialsWithQuotaPreflight as sourceGetProviderCredentials,
  markAccountUnavailable as sourceMarkAccountUnavailable,
} from "../../../src/sse/services/auth.ts";
export * from "../../../src/sse/services/auth.ts";

export const meteredGateFixture = {
  credentialSelections: 0,
  connectionCooldowns: 0,
  reset() {
    this.credentialSelections = 0;
    this.connectionCooldowns = 0;
  },
};
export async function getProviderCredentialsWithQuotaPreflight(
  ..._args: Parameters<typeof sourceGetProviderCredentials>
): Promise<Awaited<ReturnType<typeof sourceGetProviderCredentials>>> {
  meteredGateFixture.credentialSelections++;
  return null;
}
export async function markAccountUnavailable(
  ..._args: Parameters<typeof sourceMarkAccountUnavailable>
): Promise<Awaited<ReturnType<typeof sourceMarkAccountUnavailable>>> {
  meteredGateFixture.connectionCooldowns++;
  throw new Error("Unexpected connection cooldown for local budget gate");
}
