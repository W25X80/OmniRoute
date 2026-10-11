import { getSelectedProxySubscriptionStore } from "../db/proxySubscriptionStore";
import { getDbInstance } from "../db/core";
import { decrypt } from "../db/encryption";
import { getSelectedPersistenceIdentity } from "../db/controlPlaneContext";
import {
  isApplicationPersistenceIdentity,
  runPersistenceBackgroundWork,
} from "../db/persistenceRuntime";
import { reloadCore, type SecretProvider } from "./coreConfig/reload";
/** Control secret for one subscription (deferred reloads re-read via this). */
export function readControlSecret(id: string): string | null {
  try {
    const row = getDbInstance()
      .prepare("SELECT control_secret_enc FROM proxy_subscriptions WHERE id = ?")
      .get(id) as { control_secret_enc?: unknown } | undefined;
    const enc = row?.control_secret_enc;
    if (typeof enc !== "string" || !enc) return null;
    try {
      const dec = decrypt(enc);
      return typeof dec === "string" && dec.length > 0 ? dec : null;
    } catch {
      return null;
    }
  } catch {
    return null;
  }
}

/** Encode a reload failure as a sync warning; the sync itself stays `ok`. */
export function encodeReloadWarning(
  code: "CORE_RELOAD_FAILED" | "CORE_RELOAD_UNDECLARED",
  detail?: string
): string {
  return JSON.stringify(detail ? { code, detail } : { code });
}

/**
 * Reload the core after a verified replacement. The secret is read now and
 * re-read at a deferred deadline via the async provider. Never throws.
 */
export async function reloadAfterReplace(
  subscriptionId: string,
  controlUrl: string | null,
  configPath: string
): Promise<string | null> {
  let outcome;
  try {
    const identity = getSelectedPersistenceIdentity();
    const applicationOwned = isApplicationPersistenceIdentity(identity);
    const secretProvider: SecretProvider = async () => {
      try {
        if (applicationOwned) {
          if (!isApplicationPersistenceIdentity(identity))
            throw new Error("Proxy control secret persistence authority changed");
          return await runPersistenceBackgroundWork(async () => {
            if (
              !isApplicationPersistenceIdentity(identity) ||
              getSelectedPersistenceIdentity() !== identity
            )
              throw new Error("Proxy control secret persistence authority changed");
            return readControlSecretAsync(subscriptionId);
          });
        }
        // Standalone native callers retain their explicit-await scope contract.
        return await readControlSecretAsync(subscriptionId);
      } catch {
        if (identity) {
          console.warn("[ProxySubscription] Deferred control secret read failed");
          throw new Error("Deferred proxy control secret read failed");
        }
        return null;
      }
    };
    outcome = await reloadCore({
      subscriptionId,
      controlUrl,
      secret: await readControlSecretAsync(subscriptionId),
      configPath,
      secretProvider,
    });
  } catch {
    return encodeReloadWarning("CORE_RELOAD_FAILED", "exit-code");
  }
  if (outcome.kind === "undeclared") return encodeReloadWarning("CORE_RELOAD_UNDECLARED");
  if (outcome.kind === "failed")
    return encodeReloadWarning("CORE_RELOAD_FAILED", outcome.reason ?? "exit-code");
  return null;
}

export async function readControlSecretAsync(id: string): Promise<string | null> {
  const store = getSelectedProxySubscriptionStore();
  if (!store) return readControlSecret(id);
  const enc = (await store.get(id))?.control_secret_enc;
  if (typeof enc !== "string" || !enc) return null;
  const value = decrypt(enc);
  return typeof value === "string" && value.length > 0 ? value : null;
}
