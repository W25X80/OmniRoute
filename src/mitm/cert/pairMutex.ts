import path from "node:path";
import { createKeyedMutex } from "../../shared/utils/keyedMutex.ts";

const PAIR_MUTEX = Symbol.for("omniroute.mitmCertificatePairMutex");
const globals = globalThis as typeof globalThis & {
  [PAIR_MUTEX]?: ReturnType<typeof createKeyedMutex>;
};
const mutex = (globals[PAIR_MUTEX] ??= createKeyedMutex());

/** Keep generation, publication and reuse of one certificate pair indivisible. */
export function withCertificatePair<T>(
  keyPath: string,
  certPath: string,
  body: () => Promise<T>
): Promise<T> {
  const key = JSON.stringify([path.resolve(keyPath), path.resolve(certPath)]);
  return mutex.run(key, body) as Promise<T>;
}
