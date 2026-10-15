/**
 * Record persisted LKGP pins after a combo target succeeds.
 *
 * Non-blocking by design: the fallback loop never waits on these owned
 * writes. A failed write is not silent — it logs a warning carrying the
 * combo, the execution key and the provider. The returned promise never
 * rejects: routing callers ignore it, tests await it.
 */

import { runPersistenceBackgroundWork } from "../../../src/lib/db/persistenceRuntime.ts";

type WarnLogger = { warn?: (tag: string, msg: string, data?: unknown) => void } | null;
type SetLkgp = (
  comboName: string,
  modelKey: string,
  provider: string,
  connectionId?: string
) => Promise<void>;

async function writePins(
  comboName: string,
  executionKey: string,
  comboId: string | null | undefined,
  provider: string,
  connectionId: string | undefined,
  setLKGPFn: SetLkgp | undefined
): Promise<void> {
  const comboKey = comboId || comboName;
  const keys = comboKey === comboName ? [comboKey] : [...new Set([executionKey, comboKey])];
  if (setLKGPFn) {
    for (const key of keys) await setLKGPFn(comboName, key, provider, connectionId);
  } else {
    const { setLKGPPins } = await import("../../../src/lib/db/settings/lkgp.ts");
    await setLKGPPins(comboName, keys, provider, connectionId);
  }
}

export function recordLkgpPin(opts: {
  comboName: string;
  executionKey: string;
  comboId: string | null | undefined;
  provider: string;
  connectionId: string | undefined;
  log?: WarnLogger;
  tag?: string;
  /** Test seam; the routing path always resolves the writer from @/lib/db/settings. */
  setLKGP?: SetLkgp;
}): Promise<void> {
  const { comboName, executionKey, comboId, provider, connectionId } = opts;
  const log = opts.log;
  const tag = opts.tag ?? "COMBO";
  const setLKGP = opts.setLKGP;
  return runPersistenceBackgroundWork(
    () => writePins(comboName, executionKey, comboId, provider, connectionId, setLKGP),
    { owner: "lkgp.record" }
  ).catch((err: unknown) => {
    log?.warn?.(tag, "Failed to record Last Known Good Provider. This is non-fatal.", {
      combo: comboName,
      comboId: comboId ?? null,
      executionKey,
      provider,
      connectionId: connectionId ?? null,
      err,
    });
  });
}
