import { backupDbFile } from "../backup";
import { invalidateModelCatalogCache } from "../readCache";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";

export function finishModelCatalogWriteWithBackup(): void {
  if (getSelectedPostgresDatabase()) {
    return;
  }
  backupDbFile("pre-write");
  invalidateModelCatalogCache();
}

export function finishModelCatalogWriteWithoutBackup(): void {
  if (getSelectedPostgresDatabase()) {
    return;
  }
  invalidateModelCatalogCache();
}
