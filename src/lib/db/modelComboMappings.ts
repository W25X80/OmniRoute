/**
 * Compatibility facade for model-to-combo mapping persistence.
 */

import type {
  CreateModelComboMappingInput,
  ModelComboMapping,
  ModelComboMappingPage,
  UpdateModelComboMappingInput,
} from "@/domain/persistence/comboRepositories";
import { refreshSelectedPostgresCatalogAfterCommit } from "./postgres/catalogSnapshot";
import { routingConfigRepositories } from "./repositories/routingConfigRepositories";

export type { ModelComboMapping } from "@/domain/persistence/comboRepositories";

function selectedRepository() {
  return routingConfigRepositories.modelComboMappings;
}

export function getModelComboMappings(options?: {
  limit?: number;
  offset?: number;
}): Promise<ModelComboMappingPage> {
  return selectedRepository().list(options);
}

export function getModelComboMappingById(id: string): Promise<ModelComboMapping | null> {
  return selectedRepository().findById(id);
}

export async function createModelComboMapping(
  data: CreateModelComboMappingInput
): Promise<ModelComboMapping> {
  const result = await selectedRepository().create(data);
  if (result) await refreshSelectedPostgresCatalogAfterCommit();
  return result;
}

export async function updateModelComboMapping(
  id: string,
  data: UpdateModelComboMappingInput
): Promise<ModelComboMapping | null> {
  const result = await selectedRepository().update(id, data);
  if (result) await refreshSelectedPostgresCatalogAfterCommit();
  return result;
}

export async function deleteModelComboMapping(id: string): Promise<boolean> {
  const result = await selectedRepository().deleteById(id);
  if (result) await refreshSelectedPostgresCatalogAfterCommit();
  return result;
}

export function resolveComboForModel(model: string): Promise<Record<string, unknown> | null> {
  return selectedRepository().resolveForModel(model);
}
