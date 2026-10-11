import {
  resolveDataDir,
  resolveDataDirAsync as resolveOwnerAsync,
} from "../../../src/lib/dataPaths";

export { resolveDataDir };
export const pathFixture = {
  calls: 0,
  entered: undefined as (() => void) | undefined,
  release: undefined as Promise<void> | undefined,
};

export async function resolveDataDirAsync(options?: { isCloud?: boolean }): Promise<string> {
  pathFixture.calls++;
  // Call the real owner before suspending: configuration choices belong to this operation.
  const resolved = resolveOwnerAsync(options);
  pathFixture.entered?.();
  await pathFixture.release;
  return resolved;
}
