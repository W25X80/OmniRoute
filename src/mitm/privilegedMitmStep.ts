import { nativeMitmEnabled, assertNativeMitmNotAborted } from "./nativeHostIo.ts";
import { createLogger } from "@/shared/utils/logger.ts";
import { canRunPrivilegedMitmSteps, isMitmSudoPasswordRequiredAsync } from "./sudoGate.ts";

const log = createLogger("mitm-manager");

/** Run a privileged MITM step when sudo is available; log and skip otherwise (#7938). */
export async function runPrivilegedMitmStep(
  sudoPassword: string,
  skipLog: string,
  step: () => Promise<void>
): Promise<void> {
  assertNativeMitmNotAborted();
  const allowed = nativeMitmEnabled()
    ? !(await isMitmSudoPasswordRequiredAsync(sudoPassword))
    : canRunPrivilegedMitmSteps(sudoPassword);
  if (!allowed) {
    log.info(skipLog);
    return;
  }
  await step();
}
