export const fixture = {
  identity: {} as object | undefined,
  native: true,
  privacyAllowed: true,
  noLog: false,
  sqliteReads: 0,
};

export function getDbInstance() {
  fixture.sqliteReads++;
  throw new Error("Unexpected SQLite access");
}

export function getSelectedPersistenceIdentity() {
  return fixture.identity;
}

export function getSelectedPostgresDatabase() {
  return fixture.native
    ? {
        transaction: async <T>(body: (tx: { query: () => Promise<{ rows: [] }> }) => T) =>
          body({ query: async () => ({ rows: [] }) }),
      }
    : undefined;
}

export function cloneLogPayload<T>(value: T): T {
  return structuredClone(value);
}

export function isExplicitNoLogApiKey() {
  return fixture.noLog;
}

export async function withSelectedContinuationPrivacy<T>(
  _apiKeyId: string | null,
  body: () => T
): Promise<T | null> {
  return fixture.privacyAllowed && !fixture.noLog ? body() : null;
}

export function readCallArtifact() {
  return { artifact: null, state: "missing" as const };
}

export const readCallArtifactAsync = async () => readCallArtifact();
