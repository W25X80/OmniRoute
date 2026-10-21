import { createHash } from "node:crypto";
import type { NativeArtifactStore } from "../../../src/domain/persistence/artifactStore.ts";
import type { PostgresDatabase } from "../../../src/lib/db/postgres/database.ts";
import type { ArtifactRecoveryRow } from "../../../src/lib/db/postgres/artifactRecoveryRepository.ts";

const bytes = Buffer.from("replacement-orchestration");
const sha256 = createHash("sha256").update(bytes).digest("hex");
const reference = `s3/00000000-0000-4000-8000-000000000001_${sha256}.json`;
const row = (): ArtifactRecoveryRow => ({
  relpath: reference,
  publication_owner: "00000000-0000-4000-8000-000000000002",
  dispatch_owner: "00000000-0000-4000-8000-000000000003",
  call_log_id: "replacement-call",
  call_log_order: "7",
  expected_sha256: sha256,
  expected_size_bytes: String(bytes.byteLength),
  store_scope: "a".repeat(64),
  publication_state: "publishing",
  upload_state: "uncertain",
  staged_payload: "enc:v1:unit-boundary-only",
});

export const artifactReplacementFixture = {
  bytes,
  row: row(),
  calls: [] as string[],
  active: false,
  positive: false,
  attach: true,
  getError: undefined as unknown,
  replaceError: undefined as unknown,
  reset() {
    this.row = row();
    this.calls = [];
    this.active = false;
    this.positive = false;
    this.attach = true;
    this.getError = undefined;
    this.replaceError = undefined;
  },
};

export const database = {} as PostgresDatabase;
export const store: NativeArtifactStore = {
  storageScope: "a".repeat(64),
  isPutActive: () => artifactReplacementFixture.active,
  async put() {
    artifactReplacementFixture.calls.push("put");
    throw new Error("Successor must not be sent inline");
  },
  async get() {
    artifactReplacementFixture.calls.push("get");
    if (artifactReplacementFixture.getError) throw artifactReplacementFixture.getError;
    return artifactReplacementFixture.positive ? bytes : null;
  },
  async delete() {
    artifactReplacementFixture.calls.push("delete");
    return true;
  },
  async close() {},
};
export const getSelectedPostgresDatabase = () => database;
export const getPersistenceArtifactStore = () => store;
export const runPersistenceBackgroundWork = async (run: () => Promise<unknown>) => run();
export const readArtifactRecovery = async () => {
  artifactReplacementFixture.calls.push("read");
  return { ...artifactReplacementFixture.row };
};
export const listArtifactRecovery = async () => [];
export const claimArtifactRecoveryDispatch = async () => {
  artifactReplacementFixture.calls.push("claim");
  return true;
};
export const deferArtifactRecovery = async () => {
  artifactReplacementFixture.calls.push("defer");
};
export const attachArtifactRecovery = async () => {
  artifactReplacementFixture.calls.push("attach");
  return artifactReplacementFixture.attach;
};
export const replaceUncertainArtifactRecovery = async (
  _database: PostgresDatabase,
  expected: ArtifactRecoveryRow,
  isLive: () => boolean
) => {
  artifactReplacementFixture.calls.push("replace");
  if (expected.relpath !== artifactReplacementFixture.row.relpath)
    throw new Error("Recovery changed its captured identity");
  if (artifactReplacementFixture.replaceError) throw artifactReplacementFixture.replaceError;
  return !isLive();
};
export const confirmArtifactUpload = async () => {
  artifactReplacementFixture.calls.push("confirm-deletion");
};
export const deleteNativeCallArtifacts = async () => {
  artifactReplacementFixture.calls.push("cleanup");
};
export const createPostgresCleanupRepository = () => ({});
