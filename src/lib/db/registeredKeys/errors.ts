/** Shared quota error identity for both persistence implementations. */
import type { QuotaCheckResult } from "../registeredKeys";

export class RegisteredKeyQuotaError extends Error {
  constructor(readonly result: QuotaCheckResult) {
    super(result.errorMessage || "Registered-key quota exceeded");
    this.name = "RegisteredKeyQuotaError";
  }
}
