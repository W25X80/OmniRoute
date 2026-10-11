import { AsyncLocalStorage } from "node:async_hooks";
import type { ApiKeyGroupsRepository } from "@/domain/persistence/apiKeyGroupsRepository";
import type { ApiKeyRepository } from "@/domain/persistence/apiKeyRepository";
import type {
  ProviderConnectionRepository,
  ProviderNodeRepository,
} from "@/domain/persistence/providerRepositories";
import type { SettingsRepository } from "@/domain/persistence/settingsRepository";
import type {
  ComboRepository,
  ModelComboMappingRepository,
} from "@/domain/persistence/comboRepositories";
import type { PostgresDatabase } from "./postgres/database";
import type { SecretRepository } from "./postgres/secretRepository";
import { parseDatabaseBackend } from "./backendSelection";

/** Explicit, operation-scoped selection until every durable application owner is ported. */
export interface PostgresControlPlaneBinding {
  readonly settings: SettingsRepository;
  readonly providers: ProviderConnectionRepository;
  readonly nodes: ProviderNodeRepository;
  readonly apiKeys: ApiKeyRepository;
  readonly keyGroups: ApiKeyGroupsRepository;
  readonly secrets: SecretRepository;
  /** Internal backend primitive for native implementations in db/, never route callers. */
  readonly database?: PostgresDatabase;
  readonly databaseIdentity?: object;
  readonly combos?: ComboRepository;
  readonly modelComboMappings?: ModelComboMappingRepository;
  assertReady(): void;
}

declare global {
  var __omniroutePersistenceContext:
    | {
        scopes: AsyncLocalStorage<PostgresControlPlaneBinding>;
        application?: PostgresControlPlaneBinding;
      }
    | undefined;
}

// Next emits independent server bundles. Selection must share their process identity.
const context: NonNullable<typeof globalThis.__omniroutePersistenceContext> =
  (globalThis.__omniroutePersistenceContext ??= {
    scopes: new AsyncLocalStorage<PostgresControlPlaneBinding>(),
  });

export function getSelectedControlPlane(): PostgresControlPlaneBinding | undefined {
  const binding = context.scopes.getStore() ?? context.application;
  binding?.assertReady();
  if (!binding && parseDatabaseBackend() === "postgres" && !isPersistenceBuildPhase()) {
    throw new Error("PostgreSQL application persistence is not ready");
  }
  return binding;
}

function isPersistenceBuildPhase(): boolean {
  return (
    process.env.NEXT_PHASE === "phase-production-build" || process.env.OMNIROUTE_BUILDING === "1"
  );
}

/** Native repository composition only; an incomplete binding must never fall back to SQLite. */
export function getSelectedPostgresDatabase(): PostgresDatabase | undefined {
  const binding = getSelectedControlPlane();
  if (!binding) return undefined;
  if (!binding.database) throw new UnsupportedPersistenceDomainError();
  return binding.database;
}

/** Stable opaque authority within one runtime; it exposes no unguarded database methods. */
export function getSelectedPersistenceIdentity(): object | undefined {
  const binding = getSelectedControlPlane();
  if (!binding) return undefined;
  if (!binding.databaseIdentity) throw new UnsupportedPersistenceDomainError();
  return binding.databaseIdentity;
}

export function installApplicationPersistence(binding: PostgresControlPlaneBinding): () => void {
  binding.assertReady();
  if (context.application) throw new Error("Application persistence is already selected");
  context.application = binding;
  return () => {
    if (context.application === binding) context.application = undefined;
  };
}

export function withPostgresControlPlane<T>(
  binding: PostgresControlPlaneBinding,
  body: () => T
): T {
  binding.assertReady();
  return context.scopes.run(binding, body);
}

export class UnsupportedPersistenceDomainError extends Error {
  constructor() {
    super("This operation has not been ported to the selected PostgreSQL backend");
    this.name = "UnsupportedPersistenceDomainError";
  }
}

/** Guard even a previously initialized singleton; never allow implicit mixed persistence. */
export function assertSqliteBackendAllowed(): void {
  if (getSelectedControlPlane()) throw new UnsupportedPersistenceDomainError();
}
