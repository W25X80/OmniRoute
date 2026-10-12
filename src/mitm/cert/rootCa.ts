import { mkdir, writeFile, chmod } from "node:fs/promises";
import {
  nativeMitmEnabled,
  existsAsync,
  readTextAsync,
  nativeMitmSignal,
} from "../nativeHostIo.ts";
import path from "path";
import fs from "fs";
import { resolveMitmDataDir } from "../dataDir.ts";
import { generateMitmCa, type CaPair } from "../tproxy/dynamicCert.ts";

// #6684: persisted local root CA for the AgentBridge static server, so it can
// issue a per-host leaf for every host in `MITM_TOOL_HOSTS` (not just the 4
// antigravity hosts the legacy single self-signed leaf covers) without
// re-prompting the OS trust store on every restart. Reuses the exact
// `generateMitmCa()` crypto already proven for the TPROXY capture mode
// (`../tproxy/dynamicCert.ts`) — this module only adds the disk-persistence
// layer (load-if-present, generate-once-otherwise, restrictive file mode on
// the private key).

export interface MitmCaPair extends CaPair {
  keyPath: string;
  certPath: string;
}

const CA_KEY_FILE = "ca.key";
const CA_CERT_FILE = "ca.crt";

/** Directory the CA key/cert pair (and legacy leaf) live under. */
export function resolveMitmCertDir(): string {
  return path.join(resolveMitmDataDir(), "mitm");
}

function caPaths(certDir: string): { keyPath: string; certPath: string } {
  return {
    keyPath: path.join(certDir, CA_KEY_FILE),
    certPath: path.join(certDir, CA_CERT_FILE),
  };
}

/**
 * Load the persisted MITM root CA from disk if both `ca.key`/`ca.crt` exist;
 * otherwise generate a fresh CA (via `generateMitmCa()`) and persist it. The
 * private key file is chmod'd `0o600` (owner read/write only) immediately
 * after writing — it must never be group/world-readable.
 *
 * Idempotent across restarts: once written, a later call returns the exact
 * same key/cert bytes without regenerating, so the OS trust-store install
 * only has to happen once per machine.
 */
export async function loadOrCreateMitmCa(
  certDir: string = resolveMitmCertDir()
): Promise<MitmCaPair> {
  const { keyPath, certPath } = caPaths(certDir);

  if (
    nativeMitmEnabled()
      ? (await existsAsync(keyPath)) && (await existsAsync(certPath))
      : fs.existsSync(keyPath) && fs.existsSync(certPath)
  ) {
    return {
      key: nativeMitmEnabled() ? await readTextAsync(keyPath) : fs.readFileSync(keyPath, "utf-8"),
      cert: nativeMitmEnabled()
        ? await readTextAsync(certPath)
        : fs.readFileSync(certPath, "utf-8"),
      keyPath,
      certPath,
    };
  }

  const ca = await generateMitmCa();

  if (nativeMitmEnabled()) await mkdir(certDir, { recursive: true });
  else if (!fs.existsSync(certDir)) fs.mkdirSync(certDir, { recursive: true });

  if (nativeMitmEnabled()) {
    nativeMitmSignal()?.throwIfAborted();
    await writeFile(keyPath, ca.key, { mode: 0o600 });
    await chmod(keyPath, 0o600);
    await writeFile(certPath, ca.cert);
  } else {
    fs.writeFileSync(keyPath, ca.key);
    fs.writeFileSync(certPath, ca.cert);
  }
  // Owner-only read/write — the CA private key must never be group/world-
  // readable (it can sign a trusted leaf for any host). No-op on Windows,
  // which does not honor POSIX chmod bits.
  if (!nativeMitmEnabled()) fs.chmodSync(keyPath, 0o600);

  return { key: ca.key, cert: ca.cert, keyPath, certPath };
}
