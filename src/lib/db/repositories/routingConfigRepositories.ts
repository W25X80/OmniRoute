import type {
  ComboRepository,
  ModelComboMappingRepository,
} from "@/domain/persistence/comboRepositories";
import {
  getCombosCount as getSqliteCombosCount,
  sqliteComboRepository,
} from "./sqliteComboRepository";
import { sqliteModelComboMappingRepository } from "./sqliteModelComboMappingRepository";
import { getSelectedControlPlane, UnsupportedPersistenceDomainError } from "../controlPlaneContext";

export interface RoutingConfigRepositories {
  combos: ComboRepository;
  modelComboMappings: ModelComboMappingRepository;
  legacySync: {
    getCombosCount(): number;
  };
}

/**
 * Domain selection is evaluated at each operation, including after module import.
 * Legacy synchronous counts retain their SQLite implementation.
 */
export const routingConfigRepositories: RoutingConfigRepositories = {
  get combos() {
    const selected = getSelectedControlPlane();
    if (!selected) return sqliteComboRepository;
    if (!selected.combos) throw new UnsupportedPersistenceDomainError();
    return selected.combos;
  },
  get modelComboMappings() {
    const selected = getSelectedControlPlane();
    if (!selected) return sqliteModelComboMappingRepository;
    if (!selected.modelComboMappings) throw new UnsupportedPersistenceDomainError();
    return selected.modelComboMappings;
  },
  legacySync: {
    getCombosCount: getSqliteCombosCount,
  },
};
