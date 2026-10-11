export type ProviderRecord = Record<string, unknown>;

export interface ProviderConnectionDisplayMetadata {
  id: string;
  name: string | null;
  displayName: string | null;
  email: string | null;
}

export interface ProviderConnectionUpdateOptions {
  skipModelCatalog?: boolean;
}

/** Raw reads retain ciphertext; list returns the existing lazy credential view. */
export interface ProviderConnectionRepository {
  list(
    filter?: ProviderRecord,
    limit?: number,
    offset?: number,
    columns?: string[]
  ): Promise<ProviderRecord[]>;
  listRaw(
    filter?: ProviderRecord,
    limit?: number,
    offset?: number,
    columns?: string[]
  ): Promise<ProviderRecord[]>;
  count(filter?: ProviderRecord): Promise<number>;
  findById(id: string): Promise<ProviderRecord | null>;
  displayMetadata(ids: readonly string[]): Promise<ProviderConnectionDisplayMetadata[]>;
  create(data: ProviderRecord): Promise<ProviderRecord | null>;
  update(
    id: string,
    data: ProviderRecord,
    options?: ProviderConnectionUpdateOptions
  ): Promise<ProviderRecord | null>;
  reorder(provider: string): Promise<void>;
  deleteById(id: string): Promise<boolean>;
  deleteByIds(ids: string[]): Promise<number>;
  deleteByProvider(provider: string): Promise<number>;
  mergeProviderSpecificData(id: string, patch: ProviderRecord): Promise<void>;
  touchLastUsed(id: string, consecutiveUseCount: number): Promise<void>;
  touchSyncedModelsAt(id: string): Promise<void>;
  resetBackoff(id: string): Promise<void>;
  distinctGroups(): Promise<string[]>;
}

export interface ProviderNodeRepository {
  list(filter?: ProviderRecord, limit?: number, offset?: number): Promise<ProviderRecord[]>;
  count(filter?: ProviderRecord): Promise<number>;
  findById(id: string): Promise<ProviderRecord | null>;
  resolveForConnection(idOrType: string): Promise<ProviderRecord | null>;
  create(data: ProviderRecord): Promise<ProviderRecord>;
  update(id: string, data: ProviderRecord): Promise<ProviderRecord | null>;
  deleteById(id: string): Promise<ProviderRecord | null>;
}
