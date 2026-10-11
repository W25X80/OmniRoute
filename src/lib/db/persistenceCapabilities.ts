import { sanitizeErrorMessage } from "@omniroute/open-sse/utils/error";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";

export type SqliteMaintenanceCapability =
  | "sqlite-file-backup"
  | "sqlite-file-restore"
  | "sqlite-file-import"
  | "sqlite-file-export"
  | "sqlite-optimization"
  | "legacy-json-import"
  | "sqlite-integrity-repair";

const CAPABILITY_ERROR = Symbol.for("omniroute.persistence.capability.error");

export class PersistenceCapabilityError extends Error {
  readonly backend = "postgresql";
  readonly status = 501;

  constructor(readonly capability: SqliteMaintenanceCapability) {
    super(`The PostgreSQL backend does not support ${capability}`);
    this.name = "PersistenceCapabilityError";
    Object.defineProperty(this, CAPABILITY_ERROR, { value: true });
  }
}

export function requireSqliteMaintenance(capability: SqliteMaintenanceCapability): void {
  if (getSelectedPostgresDatabase()) throw new PersistenceCapabilityError(capability);
}

export function persistenceCapabilityResponse(error: unknown):
  | {
      error: {
        message: string;
        code: "persistence_capability_unavailable";
        backend: string;
        capability: string;
      };
    }
  | undefined {
  if (!error || typeof error !== "object") return undefined;
  const candidate = error as Partial<PersistenceCapabilityError> & Record<symbol, unknown>;
  if (candidate[CAPABILITY_ERROR] !== true) return undefined;
  return {
    error: {
      message: sanitizeErrorMessage(error),
      code: "persistence_capability_unavailable",
      backend: candidate.backend!,
      capability: candidate.capability!,
    },
  };
}

export function isDatabaseFileMaintenanceAvailable(): boolean {
  return getSelectedPostgresDatabase() === undefined;
}
