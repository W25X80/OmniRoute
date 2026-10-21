import assert from "node:assert/strict";
import { createHash, randomUUID as nativeRandomUUID } from "node:crypto";
import { Readable } from "node:stream";
import {
  S3Client as ActualS3Client,
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
  type S3ClientConfig,
} from "@aws-sdk/client-s3";
import {
  withPostgresControlPlane,
  type PostgresControlPlaneBinding,
} from "../../../src/lib/db/controlPlaneContext.ts";
import {
  PostgresError,
  type PostgresDatabase,
  type PostgresTransaction,
} from "../../../src/lib/db/postgres/database.ts";
import type { PostgresControlPlane } from "../../../src/lib/db/postgres/controlPlane.ts";
import type { ArtifactPublicationRow } from "../../../src/lib/db/postgres/artifactPublicationRepository.ts";
export { createHash, PutObjectCommand, GetObjectCommand, DeleteObjectCommand };
export function randomUUID() {
  return fixture.fixedReferenceUuid ?? nativeRandomUUID();
}
export function barrier() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
type Request = {
  method: string;
  protocol: string;
  hostname: string;
  path: string;
  headers: Record<string, string>;
  body?: unknown;
};
type Intent = ArtifactPublicationRow & {
  nextDue: number;
  staged_payload?: string | null;
  staged_bytes?: number;
  dispatch_owner?: string | null;
};
export const configuration = {
  bucket: "artifact-publication-test",
  region: "eu-central-1",
  prefix: "test/",
  expectedBucketOwner: "123456789012",
};
export const fixture = {
  objects: new Map<string, Buffer>(),
  requests: [] as Request[],
  clients: [] as S3ClientConfig[],
  intents: new Map<string, Intent>(),
  logs: new Map<string, Record<string, unknown>>(),
  legacy: new Set<string>(),
  events: [] as string[],
  errors: [] as string[],
  noLog: false,
  keyExists: true,
  putMode: "ack" as "ack" | "lost" | "late" | "lost_before_effect",
  getFailure: false,
  getRetries: 0,
  deleteRetries: 0,
  writeGate: undefined as ReturnType<typeof barrier> | undefined,
  putEntered: undefined as ReturnType<typeof barrier> | undefined,
  prepareGate: undefined as ReturnType<typeof barrier> | undefined,
  prepareEntered: undefined as ReturnType<typeof barrier> | undefined,
  fixedReferenceUuid: undefined as string | undefined,
  remoteEffect: undefined as Promise<void> | undefined,
  sqlFailure: undefined as "definite" | "commit_unknown" | undefined,
  prepareCommitLost: false,
  collisions: 0,
  beforePut: undefined as ((request: Request) => void) | undefined,
  authority: Object.freeze({}),
  factory: undefined as
    | typeof import("../../../src/lib/artifacts/s3ArtifactStore.ts").createS3ArtifactStore
    | undefined,
  reset() {
    this.objects.clear();
    this.requests = [];
    this.clients = [];
    this.intents.clear();
    this.logs.clear();
    this.legacy.clear();
    this.events = [];
    this.errors = [];
    this.noLog = false;
    this.keyExists = true;
    this.putMode = "ack";
    this.getFailure = false;
    this.getRetries = 0;
    this.deleteRetries = 0;
    this.writeGate = undefined;
    this.putEntered = undefined;
    this.prepareGate = undefined;
    this.prepareEntered = undefined;
    this.fixedReferenceUuid = undefined;
    this.remoteEffect = undefined;
    this.sqlFailure = undefined;
    this.prepareCommitLost = false;
    this.collisions = 0;
    this.beforePut = undefined;
  },
};
function response(statusCode: number, body = "", headers: Record<string, string> = {}) {
  return { response: { statusCode, headers, body: Readable.from(body ? [body] : []) } };
}
export class S3Client extends ActualS3Client {
  constructor(options: S3ClientConfig) {
    fixture.clients.push(options);
    super({
      ...options,
      credentials: { accessKeyId: "PUBLICATIONTEST", secretAccessKey: "synthetic-test-only" },
      requestHandler: {
        async handle(request: Request) {
          fixture.requests.push(request);
          if (request.method === "PUT") {
            fixture.events.push("put-send");
            fixture.beforePut?.(request);
            fixture.putEntered?.resolve();
            assert.ok(request.body instanceof Uint8Array);
            if (fixture.putMode === "lost_before_effect") {
              const bytes = Buffer.from(request.body),
                gate = fixture.writeGate?.promise ?? Promise.resolve();
              fixture.remoteEffect = gate.then(() => {
                fixture.objects.set(request.path, bytes);
                fixture.events.push("put-effect");
              });
              throw Object.assign(new Error("synthetic remote ACK loss before effect"), {
                name: "TimeoutError",
              });
            }
            await fixture.writeGate?.promise;
            assert.ok(request.body instanceof Uint8Array);
            fixture.objects.set(request.path, Buffer.from(request.body));
            fixture.events.push("put-effect");
            if (fixture.putMode === "lost")
              throw Object.assign(new Error("synthetic ACK loss"), { name: "TimeoutError" });
            return response(200);
          }
          if (request.method === "GET") {
            if (fixture.getRetries-- > 0)
              return response(500, "<Error><Code>InternalError</Code></Error>");
            if (fixture.getFailure)
              return response(403, "<Error><Code>AccessDenied</Code></Error>");
            const bytes = fixture.objects.get(request.path);
            if (!bytes) return response(404, "<Error><Code>NoSuchKey</Code></Error>");
            return {
              response: {
                statusCode: 200,
                headers: {
                  "content-length": String(bytes.length),
                  "x-amz-checksum-sha256": createHash("sha256").update(bytes).digest("base64"),
                },
                body: Readable.from([bytes]),
              },
            };
          }
          assert.equal(request.method, "DELETE");
          if (fixture.deleteRetries-- > 0)
            return response(500, "<Error><Code>InternalError</Code></Error>");
          fixture.objects.delete(request.path);
          fixture.events.push("delete-effect");
          return response(204);
        },
        destroy() {
          fixture.events.push("sdk-close");
        },
      },
    });
  }
}
function result<Row>(rows: unknown[] = [], rowCount = rows.length) {
  return { rows: rows as Row[], rowCount, command: "SELECT" };
}
async function query<Row>(
  text: string,
  values: unknown[] = []
): Promise<ReturnType<typeof result<Row>>> {
  const sql = text.replace(/\s+/g, " ").trim();
  const ref = String(values[0]);
  if (sql.startsWith("SELECT pg_advisory")) return result<Row>();
  if (sql.startsWith("SELECT EXISTS"))
    return result<Row>([
      {
        present:
          [...fixture.logs.values()].some((row) => row.artifact_relpath === ref) ||
          fixture.legacy.has(ref),
      },
    ]);
  if (sql.startsWith("SELECT COALESCE(SUM(staged_bytes)"))
    return result<Row>([
      {
        used: String(
          [...fixture.intents.values()].reduce((sum, row) => sum + (row.staged_bytes ?? 0), 0)
        ),
      },
    ]);
  if (sql.startsWith("SELECT 1 FROM omniroute.call_logs")) {
    const row = fixture.logs.get(ref);
    return result<Row>(row && String(row.insertion_order) === String(values[1]) ? [{}] : []);
  }
  if (sql.startsWith("SELECT (NULLIF(c.api_key_id")) {
    const row = fixture.logs.get(ref);
    return result<Row>(
      row && String(row.insertion_order) === String(values[1]) && !row.artifact_relpath
        ? [{ allowed: fixture.keyExists && !fixture.noLog }]
        : []
    );
  }
  if (sql.startsWith("SELECT no_log"))
    return result<Row>(fixture.keyExists ? [{ no_log: fixture.noLog ? 1 : 0 }] : []);
  if (sql.startsWith("SELECT * FROM omniroute.artifact_deletion_queue"))
    return result<Row>(fixture.intents.has(ref) ? [{ ...fixture.intents.get(ref) }] : []);
  if (
    sql.startsWith("SELECT relpath FROM omniroute.artifact_deletion_queue") &&
    sql.includes("WHERE call_log_order=$1")
  )
    return result<Row>(
      [...fixture.intents.values()]
        .filter(
          (row) => row.call_log_order === String(values[0]) && row.call_log_id === String(values[1])
        )
        .slice(0, 2)
        .map((row) => ({ relpath: row.relpath }))
    );
  if (sql.startsWith("WITH source AS MATERIALIZED")) {
    const row = fixture.intents.get(ref);
    if (
      !row ||
      row.publication_state !== "publishing" ||
      row.upload_state !== "uncertain" ||
      row.nextDue > Date.now() ||
      row.publication_owner !== values[3] ||
      row.dispatch_owner !== values[4] ||
      row.call_log_id !== values[5] ||
      row.call_log_order !== values[6] ||
      row.store_scope !== values[7] ||
      row.expected_size_bytes !== values[8] ||
      row.expected_sha256 !== values[9] ||
      !row.staged_payload
    )
      return result<Row>([], 0);
    if (fixture.intents.has(String(values[1]))) throw new Error("Duplicate artifact identity");
    const successor = {
      ...row,
      relpath: String(values[1]),
      publication_state: "publishing" as const,
      upload_state: "not_sent" as const,
      publication_owner: String(values[2]),
      dispatch_owner: null,
      nextDue: Date.now() + 30000,
    };
    Object.assign(row, {
      publication_state: "delete_pending",
      staged_payload: null,
      staged_bytes: 0,
      nextDue: Date.now() + 300000,
    });
    fixture.intents.set(successor.relpath, successor);
    return result<Row>([{ relpath: successor.relpath }]);
  }
  if (sql.startsWith("SELECT relpath FROM omniroute.artifact_deletion_queue"))
    return result<Row>(
      [...fixture.intents.values()]
        .filter((row) => row.publication_state !== "attached" && row.nextDue <= Date.now())
        .sort((a, b) => a.nextDue - b.nextDue)
        .slice(0, Number(values[0]))
        .map((row) => ({ relpath: row.relpath }))
    );
  if (sql.startsWith("INSERT INTO omniroute.artifact_deletion_queue")) {
    const old = fixture.intents.get(ref),
      abandon = sql.includes("DO UPDATE");
    if (old && !abandon) return result<Row>([], 0);
    if (old && old.publication_owner !== values[2]) return result<Row>([], 0);
    const row: Intent = abandon
      ? {
          relpath: ref,
          publication_state: "delete_pending",
          upload_state:
            old?.upload_state === "acknowledged"
              ? "acknowledged"
              : (values[1] as Intent["upload_state"]),
          publication_owner: String(values[2]),
          store_scope: String(values[3]),
          expected_size_bytes: String(values[4]),
          expected_sha256: String(values[5]),
          nextDue: Date.now(),
        }
      : {
          relpath: ref,
          publication_state: "publishing",
          upload_state: "uncertain",
          publication_owner: String(values[1]),
          store_scope: String(values[2]),
          expected_size_bytes: String(values[3]),
          expected_sha256: String(values[4]),
          nextDue: Date.now() + 300000,
        };
    if (sql.includes("staged_payload"))
      Object.assign(row, {
        upload_state: "not_sent",
        call_log_id: String(values[5]),
        call_log_order: String(values[6]),
        staged_payload: String(values[7]),
        staged_bytes: Number(values[8]),
        dispatch_owner: null,
        nextDue: Date.now(),
      });
    fixture.intents.set(ref, row);
    fixture.events.push(abandon ? "abandon" : "prepare-insert");
    if (!abandon) {
      fixture.prepareEntered?.resolve();
      await fixture.prepareGate?.promise;
    }
    return result<Row>([], 1);
  }
  if (sql.startsWith("INSERT INTO omniroute.call_logs(")) {
    if (fixture.collisions-- > 0) return result<Row>([], 0);
    if (fixture.sqlFailure === "definite") throw new PostgresError("query_failed");
    const columns = /call_logs\(([^)]+)\)/.exec(sql)![1].split(","),
      row = Object.fromEntries(columns.map((column, at) => [column, values[at]]));
    row.insertion_order = fixture.logs.size + 1;
    fixture.logs.set(String(row.id), row);
    fixture.events.push("log-insert");
    return result<Row>([{ id: row.id, insertion_order: String(row.insertion_order) }], 1);
  }
  if (sql.startsWith("UPDATE omniroute.call_logs")) {
    if (sql.includes("SET detail_state='ready'")) {
      const log = fixture.logs.get(ref);
      if (!log || String(log.insertion_order) !== String(values[1])) return result<Row>([], 0);
      Object.assign(log, {
        detail_state: "ready",
        detail_reason: null,
        artifact_relpath: values[2],
        artifact_size_bytes: values[3],
        artifact_sha256: values[4],
      });
    } else if (sql.includes("SET detail_reason='publication_pending'")) {
      const log = fixture.logs.get(ref);
      if (!log || String(log.insertion_order) !== String(values[1]) || log.artifact_relpath)
        return result<Row>([], 0);
      log.detail_reason = "publication_pending";
    } else if (sql.includes("FROM omniroute.artifact_deletion_queue")) {
      const intent = fixture.intents.get(ref),
        log = intent?.call_log_id ? fixture.logs.get(intent.call_log_id) : undefined;
      if (log && intent?.publication_state === "publishing")
        log.detail_reason =
          intent.upload_state === "uncertain" ? "upload_unknown" : "publication_pending";
    }
    return result<Row>([], 1);
  }
  const row = fixture.intents.get(ref);
  if (sql.startsWith("UPDATE omniroute.artifact_deletion_queue")) {
    if (!row) return result<Row>([], 0);
    if (sql.includes("publication_owner=$2") && row.publication_owner !== values[1])
      return result<Row>([], 0);
    if (sql.includes("staged_payload=NULL")) {
      row.staged_payload = null;
      row.staged_bytes = 0;
    }
    if (sql.includes("SET upload_state='uncertain'")) {
      row.upload_state = "uncertain";
      row.dispatch_owner = String(values[1]);
      row.nextDue = Date.now() + 30000;
    } else if (sql.includes("upload_state=CASE WHEN $3")) {
      if (values[2] && row.dispatch_owner === values[1]) {
        row.upload_state = "not_sent";
        row.dispatch_owner = null;
      }
      row.nextDue = Date.now() + 30000;
    } else if (sql.includes("publication_state=CASE")) {
      if (!values[1]) row.publication_state = "attached";
      row.nextDue = Date.now() + 300000;
    } else if (sql.includes("publication_state='attached'")) {
      row.publication_state = "attached";
      if (sql.includes("upload_state='acknowledged'")) row.upload_state = "acknowledged";
    } else if (sql.includes("publication_state='delete_pending'")) {
      row.publication_state = "delete_pending";
      if (sql.includes("upload_state='acknowledged'")) row.upload_state = "acknowledged";
      row.nextDue = Date.now();
    } else if (sql.includes("SET upload_state='acknowledged'")) row.upload_state = "acknowledged";
    else if (sql.includes("next_attempt_at=")) row.nextDue = Date.now() + 300000;
    return result<Row>([], 1);
  }
  if (sql.startsWith("DELETE FROM omniroute.artifact_deletion_queue")) {
    if (
      !row ||
      row.publication_state !== "delete_pending" ||
      !["acknowledged", "not_sent"].includes(row.upload_state)
    )
      return result<Row>([], 0);
    fixture.intents.delete(ref);
    fixture.events.push("identity-retired");
    return result<Row>([], 1);
  }
  throw new Error("Unmodeled SQL operand: " + sql);
}
let transactionTail = Promise.resolve();
const database = {
  async transaction<T>(body: (tx: PostgresTransaction) => Promise<T>): Promise<T> {
    const prior = transactionTail;
    let release!: () => void;
    transactionTail = new Promise<void>((done) => {
      release = done;
    });
    await prior;
    const beforeIntents = new Map([...fixture.intents].map(([key, row]) => [key, { ...row }])),
      beforeLogs = new Map(fixture.logs);
    let inserted = false,
      prepared = false;
    const tx: PostgresTransaction = {
      async query<Row>(text, values) {
        inserted ||= text.includes("INSERT INTO omniroute.call_logs(");
        prepared ||= text.includes("VALUES($1,'publishing'");
        return query<Row>(text, values);
      },
    };
    try {
      const value = await body(tx);
      if (
        (prepared && fixture.prepareCommitLost) ||
        (inserted && fixture.sqlFailure === "commit_unknown")
      ) {
        fixture.prepareCommitLost = false;
        fixture.sqlFailure = undefined;
        throw new PostgresError("commit_unknown");
      }
      fixture.events.push("sql-ack");
      return value;
    } catch (error) {
      if (!(error instanceof PostgresError && error.kind === "commit_unknown")) {
        fixture.intents = beforeIntents;
        fixture.logs = beforeLogs;
      }
      throw error;
    } finally {
      release();
    }
  },
} as unknown as PostgresDatabase;
export async function initializePostgresControlPlane(): Promise<PostgresControlPlane> {
  let closed = false;
  return {
    state: "ready",
    bootstrapSecrets: Object.freeze({ jwtSecret: "synthetic-jwt", apiKeySecret: "synthetic-api" }),
    async run(body) {
      if (closed) throw new Error("Fixture plane closed");
      let alive = true;
      const binding = {
        database,
        databaseIdentity: fixture.authority,
        assertReady() {
          if (!alive) throw new Error("Fixture scope ended");
        },
      } as unknown as PostgresControlPlaneBinding;
      try {
        return await withPostgresControlPlane(binding, () => body(binding));
      } finally {
        alive = false;
      }
    },
    async close() {
      closed = true;
      fixture.events.push("pool-close");
    },
  } as PostgresControlPlane;
}
export async function readPostgresEnvironment() {
  return {};
}
export function readS3ArtifactConfiguration() {
  return configuration;
}
export async function initializeCircuitBreakerPersistence() {}
export async function initializeQuotaSnapshotCache() {}
export async function flushCircuitBreakerPersistence() {}
export async function withPostgresCatalogSnapshot<T>(
  _db: PostgresDatabase,
  body: () => T | Promise<T>
) {
  return body();
}
export const shouldPersistToDisk = true;
export function getDbInstance(): never {
  throw new Error("SQLite escaped the native composition boundary");
}
export function updateRequestTokensById() {}
export async function isNoLogAsync() {
  return fixture.noLog;
}
export function isNoLog() {
  return fixture.noLog;
}
export function isPiiSanitizationEnabled() {
  return false;
}
export function sanitizePII(text: string) {
  return { text };
}
export function seedPendingContinuationState() {}
export function clearPendingContinuationState() {}
export async function getRequestDetailLogByCallLogIdAsync() {
  return undefined;
}
export function cleanupOrphanCallLogFiles() {}
export function cleanupOverflowCallLogFiles() {}
export function deleteCallLogsBefore() {}
export function trimCallLogsToMaxRows() {}
export function rotateCallLogs() {}
export function scheduleCallLogRotation() {}
export function clearArtifactReferenceAsync() {}
export function scheduleCallLogRotationAsync() {
  fixture.events.push("rotation-scheduled");
}
export async function closeCallLogArtifactWriter() {}
export async function serializeCallArtifactAsync(artifact: unknown) {
  return Buffer.from(JSON.stringify(artifact));
}
/** CPU serialization is excluded from this composition oracle; domain/SDK/SQL owners are real. */
export async function writeCallArtifactAsync(
  artifact: unknown,
  options?: Parameters<
    typeof import("../../../src/lib/usage/callLogArtifacts.ts").writeCallArtifactToNativeStore
  >[1]
) {
  const owner = await import("../../../src/lib/usage/callLogArtifacts.ts");
  return owner.writeCallArtifactToNativeStore(Buffer.from(JSON.stringify(artifact)), options);
}

export function createS3ArtifactStore() {
  if (!fixture.factory) throw new Error("Native store factory is not installed in fixture");
  return fixture.factory(configuration);
}
