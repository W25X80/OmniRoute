import { getProviderCredentialsWithQuotaPreflight as selectCredentials } from "../../../src/sse/services/scopedCredentials.ts";

export const audioSelectionFailurePorts = {
  failure: null as { value: unknown } | null,
  calls: [] as Array<Parameters<typeof selectCredentials>>,
  onSelection: null as (() => void) | null,
};

/** Only the audio helper's credential port is intercepted, after real combo gates. */
export async function getProviderCredentialsWithQuotaPreflight(
  ...args: Parameters<typeof selectCredentials>
) {
  audioSelectionFailurePorts.calls.push(args);
  audioSelectionFailurePorts.onSelection?.();
  if (audioSelectionFailurePorts.failure) throw audioSelectionFailurePorts.failure.value;
  return selectCredentials(...args);
}
