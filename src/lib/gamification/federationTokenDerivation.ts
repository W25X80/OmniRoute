import { pbkdf2 } from "node:crypto";

/** Match the persisted federation derivation exactly; callers select the CPU profile. */
export function deriveFederationTokenHashAsync(token: string): Promise<string> {
  return new Promise((resolve, reject) => {
    pbkdf2(token, "omniroute-federation-salt", 120000, 32, "sha256", (error, key) => {
      if (error) reject(error);
      else resolve(key.toString("hex"));
    });
  });
}
