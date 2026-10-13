import { stageArtifactRecovery, type ArtifactRecoveryIntent } from "./artifactRecoveryRepository";
import { acquirePostgresLock } from "./locks";
import { queuePostgresArtifactDeletions } from "./cleanupRepository";
import {
  requireArtifactPublication,
  finishArtifactPublication,
} from "./artifactPublicationRepository";
import { getSelectedPostgresDatabase } from "../controlPlaneContext";
import type { PostgresDatabase } from "./database";
import {
  queryNamedUsageRows,
  queryUsageRows,
  safeUsageNumber,
  bindSelectedUsageRepository,
} from "./usageQueries";

const CALL_NUMBERS = [
  "duration",
  "ttft_ms",
  "tokens_in",
  "tokens_out",
  "tokens_cache_read",
  "tokens_cache_creation",
  "tokens_reasoning",
  "tokens_compressed",
  "reasoning_chars",
  "reasoning_duration_ms",
  "artifact_size_bytes",
  "added_wait_ms",
  "insertion_order",
  "row_id",
];
const INSERT_COLUMNS = {
  id: "id",
  timestamp: "timestamp",
  method: "method",
  path: "path",
  status: "status",
  model: "model",
  requestedModel: "requested_model",
  provider: "provider",
  account: "account",
  connectionId: "connection_id",
  duration: "duration",
  ttftMs: "ttft_ms",
  tokensIn: "tokens_in",
  tokensOut: "tokens_out",
  tokensCacheRead: "tokens_cache_read",
  tokensCacheCreation: "tokens_cache_creation",
  tokensReasoning: "tokens_reasoning",
  tokensCompressed: "tokens_compressed",
  reasoningSource: "reasoning_source",
  reasoningChars: "reasoning_chars",
  reasoningDurationMs: "reasoning_duration_ms",
  reasoningEffortRequested: "reasoning_effort_requested",
  reasoningEffortUpstream: "reasoning_effort_upstream",
  reasoningEncrypted: "reasoning_encrypted",
  cacheSource: "cache_source",
  requestType: "request_type",
  sourceFormat: "source_format",
  targetFormat: "target_format",
  apiKeyId: "api_key_id",
  apiKeyName: "api_key_name",
  comboName: "combo_name",
  comboStepId: "combo_step_id",
  comboExecutionKey: "combo_execution_key",
  errorSummary: "error_summary",
  detailState: "detail_state",
  detailReason: "detail_reason",
  artifactRelPath: "artifact_relpath",
  artifactSizeBytes: "artifact_size_bytes",
  artifactSha256: "artifact_sha256",
  hasRequestBody: "has_request_body",
  hasResponseBody: "has_response_body",
  hasPipelineDetails: "has_pipeline_details",
  requestSummary: "request_summary",
  correlationId: "correlation_id",
  modelPinned: "model_pinned",
  sessionTag: "session_tag",
  responseId: "response_id",
  errorType: "error_type",
  videoContentRemoved: "video_content_removed",
  hasContent: "has_content",
  usageProvenance: "usage_provenance",
  addedWaitMs: "added_wait_ms",
  addedWaitCause: "added_wait_cause",
  resilienceActions: "resilience_actions",
} as const;
const RESOLVED_ACCOUNT = "COALESCE(NULLIF(pc.name,''),NULLIF(pc.email,''),cl.account)";
const SUMMARY_SELECT = `SELECT cl.*,pn.name AS provider_node_name,pn.prefix AS provider_node_prefix,
  ${RESOLVED_ACCOUNT} AS resolved_account FROM omniroute.call_logs cl
  LEFT JOIN omniroute.provider_nodes pn ON pn.id=cl.provider
  LEFT JOIN omniroute.provider_connections pc ON pc.id=cl.connection_id`;

export class PostgresCallLogRepository {
  constructor(private readonly database: PostgresDatabase) {}

  async insert(
    parameters: Record<string, unknown>,
    intent?: ArtifactRecoveryIntent
  ): Promise<{ payloadAccepted: boolean; inserted: boolean; intentAccepted: boolean }> {
    return this.database.transaction(
      async (transaction) => {
        await acquirePostgresLock(transaction, "keyPolicy", "shared");
        // Serialized allocation keeps the export cursor in commit order, including concurrent writes.
        await acquirePostgresLock(transaction, "accounting");
        const stored = { ...parameters };
        const artifactReference =
          typeof stored.artifactRelPath === "string" ? stored.artifactRelPath : null;
        if (artifactReference) {
          await requireArtifactPublication(
            transaction,
            artifactReference,
            stored.artifactPublicationOwner,
            stored.artifactSizeBytes,
            stored.artifactSha256
          );
        }
        let payloadAccepted = true;
        if (typeof stored.apiKeyId === "string" && stored.apiKeyId) {
          const row = (
            await transaction.query<{ no_log: number }>(
              "SELECT no_log FROM omniroute.api_keys WHERE id=$1",
              [stored.apiKeyId]
            )
          ).rows[0];
          payloadAccepted = Boolean(row && row.no_log !== 1);
          if (!payloadAccepted) {
            Object.assign(stored, {
              detailState: "none",
              artifactRelPath: null,
              artifactSizeBytes: null,
              artifactSha256: null,
              hasRequestBody: 0,
              hasResponseBody: 0,
              hasPipelineDetails: 0,
              requestSummary: null,
              errorSummary: null,
              detailReason: "privacy",
              hasContent: null,
            });
          }
        }
        const keys = Object.keys(INSERT_COLUMNS) as Array<keyof typeof INSERT_COLUMNS>;
        const inserted = await transaction.query(
          `INSERT INTO omniroute.call_logs(${keys.map((key) => INSERT_COLUMNS[key]).join(",")})
        VALUES(${keys.map((_, index) => `$${index + 1}`).join(",")}) ON CONFLICT(id) DO NOTHING RETURNING id,insertion_order`,
          keys.map((key) => stored[key] ?? null)
        );
        if (inserted.rowCount === 1 && artifactReference) {
          await finishArtifactPublication(
            transaction,
            artifactReference,
            stored.artifactPublicationOwner as string,
            payloadAccepted
          );
        }
        let intentAccepted = false;
        if (inserted.rowCount === 1 && intent && payloadAccepted) {
          intentAccepted = await stageArtifactRecovery(
            transaction,
            intent,
            String(stored.id),
            String(inserted.rows[0].insertion_order)
          );
          if (!intentAccepted)
            await transaction.query(
              "UPDATE omniroute.call_logs SET detail_state='missing',detail_reason='staging_capacity' WHERE id=$1",
              [stored.id]
            );
        }
        return { payloadAccepted, inserted: inserted.rowCount === 1, intentAccepted };
      },
      { operation: "callLog.admit" }
    );
  }

  async list(filter: Record<string, unknown> = {}): Promise<Record<string, unknown>[]> {
    const params: Record<string, unknown> = {};
    const conditions: string[] = [];
    if (filter.status === "error")
      conditions.push("(cl.status>=400 OR cl.error_summary IS NOT NULL)");
    else if (filter.status === "ok") conditions.push("cl.status>=200 AND cl.status<300");
    else if (filter.status) {
      const status = Number.parseInt(String(filter.status), 10);
      if (!Number.isNaN(status)) {
        conditions.push("cl.status=@status");
        params.status = status;
      }
    }
    for (const [field, columns] of Object.entries({
      model: ["model", "requested_model"],
      provider: ["provider"],
      account: ["account", "resolved"],
      apiKey: ["api_key_name", "api_key_id"],
      correlationId: ["correlation_id"],
      sessionTag: ["session_tag"],
    })) {
      if (!filter[field]) continue;
      params[field] = `%${String(filter[field])}%`;
      conditions.push(
        "(" +
          columns
            .map(
              (column) =>
                `${column === "resolved" ? RESOLVED_ACCOUNT : "cl." + column} COLLATE "C" ILIKE @${field}`
            )
            .join(" OR ") +
          ")"
      );
    }
    if (filter.combo) conditions.push("cl.combo_name IS NOT NULL");
    if (filter.excludeTests) conditions.push("(cl.path LIKE '/v1/%' OR cl.path LIKE '/api/v1/%')");
    if (filter.since) {
      params.since =
        filter.since instanceof Date ? filter.since.toISOString() : String(filter.since);
      conditions.push("cl.timestamp>=@since");
    }
    if (filter.until) {
      params.until =
        filter.until instanceof Date ? filter.until.toISOString() : String(filter.until);
      conditions.push("cl.timestamp<=@until");
    }
    if (filter.search) {
      params.search = `%${String(filter.search)}%`;
      const fields = [
        "model",
        "path",
        "account",
        "requested_model",
        "provider",
        "api_key_name",
        "api_key_id",
        "combo_name",
        "combo_step_id",
        "combo_execution_key",
        "error_summary",
        "correlation_id",
      ];
      conditions.push(
        "(" +
          fields
            .map((field) => `cl.${field} COLLATE "C" ILIKE @search`)
            .concat([
              `${RESOLVED_ACCOUNT} COLLATE "C" ILIKE @search`,
              "CAST(cl.status AS text) LIKE @search",
            ])
            .join(" OR ") +
          ")"
      );
    }
    params.limit =
      Number.isSafeInteger(filter.limit) && Number(filter.limit) > 0 ? filter.limit : 200;
    params.offset =
      Number.isSafeInteger(filter.offset) && Number(filter.offset) > 0 ? filter.offset : 0;
    return queryNamedUsageRows(
      this.database,
      SUMMARY_SELECT +
        (conditions.length ? " WHERE " + conditions.join(" AND ") : "") +
        " ORDER BY cl.timestamp DESC,cl.id DESC LIMIT @limit OFFSET @offset",
      params,
      CALL_NUMBERS
    );
  }
  async byId(id: string): Promise<Record<string, unknown> | undefined> {
    return (
      await queryUsageRows<Record<string, unknown>>(
        this.database,
        SUMMARY_SELECT + " WHERE cl.id=$1",
        [id],
        CALL_NUMBERS
      )
    )[0];
  }
  async legacyById(id: string): Promise<Record<string, unknown> | undefined> {
    return (
      await queryUsageRows<Record<string, unknown>>(
        this.database,
        "SELECT request_body,response_body,error FROM omniroute.call_logs_v1_legacy WHERE id=$1",
        [id]
      )
    )[0];
  }
  async setDetailState(id: string, state: string): Promise<void> {
    await this.database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "accounting");
      await transaction.query("UPDATE omniroute.call_logs SET detail_state=$2 WHERE id=$1", [
        id,
        state,
      ]);
    });
  }
  async clearReference(relativePath: string, nextState: string): Promise<void> {
    await this.database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "accounting");
      await transaction.query(
        `UPDATE omniroute.call_logs SET detail_state=$2,
        artifact_relpath=NULL,artifact_size_bytes=NULL,artifact_sha256=NULL WHERE artifact_relpath=$1`,
        [relativePath, nextState]
      );
      await queuePostgresArtifactDeletions(transaction, [relativePath]);
    });
  }
  async countSince(since: string): Promise<number> {
    return safeUsageNumber(
      (
        await queryUsageRows<{ count: string }>(
          this.database,
          "SELECT COUNT(*) AS count FROM omniroute.call_logs WHERE timestamp >= $1",
          [since]
        )
      )[0].count
    );
  }
  async idsSince(since: string, limit: number): Promise<string[]> {
    return (
      await queryUsageRows<{ id: string }>(
        this.database,
        "SELECT id FROM omniroute.call_logs WHERE timestamp >= $1 ORDER BY timestamp DESC,id DESC LIMIT $2",
        [since, limit]
      )
    ).map((row) => row.id);
  }
  async maxCursor(): Promise<number> {
    return safeUsageNumber(
      (
        await queryUsageRows<{ maximum: string }>(
          this.database,
          "SELECT COALESCE(MAX(insertion_order),0) AS maximum FROM omniroute.call_logs"
        )
      )[0].maximum
    );
  }
  async countAfter(cursor: number): Promise<number> {
    return safeUsageNumber(
      (
        await queryUsageRows<{ count: string }>(
          this.database,
          "SELECT COUNT(*) AS count FROM omniroute.call_logs WHERE insertion_order>$1",
          [cursor]
        )
      )[0].count
    );
  }
  async exportAfter(cursor: number, limit: number): Promise<Record<string, unknown>[]> {
    return queryUsageRows(
      this.database,
      SUMMARY_SELECT.replace("cl.*,", "cl.insertion_order AS row_id,cl.*,") +
        " WHERE cl.insertion_order>$1 ORDER BY cl.insertion_order ASC LIMIT $2",
      [cursor, limit],
      CALL_NUMBERS
    );
  }
  async references(): Promise<Set<string>> {
    const references = new Set<string>();
    let cursor = 0;
    while (true) {
      const rows = await queryUsageRows<{ artifact_relpath: string; insertion_order: string }>(
        this.database,
        `SELECT artifact_relpath,insertion_order FROM omniroute.call_logs
        WHERE insertion_order>$1 AND artifact_relpath IS NOT NULL ORDER BY insertion_order LIMIT 1000`,
        [cursor]
      );
      for (const row of rows) references.add(row.artifact_relpath);
      if (rows.length < 1000) break;
      cursor = safeUsageNumber(rows[rows.length - 1].insertion_order);
    }
    return references;
  }
  async findReferences(paths: string[]): Promise<Set<string>> {
    return new Set(
      (
        await queryUsageRows<{ artifact_relpath: string }>(
          this.database,
          "SELECT DISTINCT artifact_relpath FROM omniroute.call_logs WHERE artifact_relpath=ANY($1::text[])",
          [paths]
        )
      ).map((row) => row.artifact_relpath)
    );
  }
  async deleteBefore(
    cutoff: string,
    limit: number
  ): Promise<{ deletedRows: number; paths: string[] }> {
    return this.database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "accounting");
      const rows = (
        await transaction.query<{ artifact_relpath: string | null }>(
          `DELETE FROM omniroute.call_logs WHERE id IN
        (SELECT id FROM omniroute.call_logs WHERE timestamp<$1 ORDER BY timestamp,id LIMIT $2 FOR UPDATE)
        RETURNING artifact_relpath`,
          [cutoff, limit]
        )
      ).rows;
      const paths = rows
        .map((row) => row.artifact_relpath)
        .filter((value): value is string => Boolean(value));
      const unreferenced = (
        await transaction.query<{ relpath: string }>(
          `SELECT DISTINCT relpath FROM unnest($1::text[]) AS removed(relpath)
        WHERE NOT EXISTS(SELECT 1 FROM omniroute.call_logs WHERE artifact_relpath=removed.relpath
          UNION ALL SELECT 1 FROM omniroute.call_logs_v1_legacy WHERE artifact_relpath=removed.relpath)`,
          [paths]
        )
      ).rows.map((row) => row.relpath);
      await queuePostgresArtifactDeletions(transaction, unreferenced);
      return { deletedRows: rows.length, paths: unreferenced };
    });
  }
  async trim(maxRows: number, limit: number): Promise<{ deletedRows: number; paths: string[] }> {
    return this.database.transaction(async (transaction) => {
      await acquirePostgresLock(transaction, "accounting");
      const excess = safeUsageNumber(
        (
          await transaction.query<{ count: string }>(
            "SELECT GREATEST(COUNT(*)-$1::bigint,0) AS count FROM omniroute.call_logs",
            [maxRows]
          )
        ).rows[0].count
      );
      const rows = (
        await transaction.query<{ artifact_relpath: string | null }>(
          `DELETE FROM omniroute.call_logs WHERE id IN
        (SELECT id FROM omniroute.call_logs ORDER BY timestamp,id LIMIT $1 FOR UPDATE) RETURNING artifact_relpath`,
          [Math.min(excess, limit)]
        )
      ).rows;
      const paths = rows
        .map((row) => row.artifact_relpath)
        .filter((value): value is string => Boolean(value));
      const unreferenced = (
        await transaction.query<{ relpath: string }>(
          `SELECT DISTINCT relpath FROM unnest($1::text[]) AS removed(relpath)
        WHERE NOT EXISTS(SELECT 1 FROM omniroute.call_logs WHERE artifact_relpath=removed.relpath
          UNION ALL SELECT 1 FROM omniroute.call_logs_v1_legacy WHERE artifact_relpath=removed.relpath)`,
          [paths]
        )
      ).rows.map((row) => row.relpath);
      await queuePostgresArtifactDeletions(transaction, unreferenced);
      return { deletedRows: rows.length, paths: unreferenced };
    });
  }
  async requestArtifactPaths(
    connection: string,
    model: string,
    since: string
  ): Promise<Array<{ artifact_relpath: string | null }>> {
    return queryUsageRows(
      this.database,
      "SELECT artifact_relpath FROM omniroute.call_logs WHERE connection_id=$1 AND model=$2 AND timestamp>=$3 ORDER BY timestamp DESC,id DESC LIMIT 5",
      [connection, model, since]
    );
  }
  async filterOptions(): Promise<{
    providers: Array<{ value: unknown }>;
    models: Array<{ value: unknown }>;
    accounts: Array<{ value: unknown }>;
    apiKeys: Array<{ id: string | null; name: string | null }>;
  }> {
    return this.database.transaction(async (transaction) => {
      const providers = (
        await transaction.query<{ value: unknown }>(
          "SELECT DISTINCT provider AS value FROM omniroute.call_logs"
        )
      ).rows;
      const models = (
        await transaction.query<{ value: unknown }>(
          "SELECT model AS value FROM omniroute.call_logs UNION SELECT requested_model AS value FROM omniroute.call_logs"
        )
      ).rows;
      const accounts = (
        await transaction.query<{ value: unknown }>(
          `SELECT DISTINCT ${RESOLVED_ACCOUNT} AS value FROM omniroute.call_logs cl LEFT JOIN omniroute.provider_connections pc ON pc.id=cl.connection_id`
        )
      ).rows;
      const apiKeys = (
        await transaction.query<{
          id: string | null;
          name: string | null;
        }>(`SELECT DISTINCT NULLIF(api_key_id,'') AS id,NULLIF(api_key_name,'') AS name FROM omniroute.call_logs
        WHERE NULLIF(api_key_id,'') IS NOT NULL OR NULLIF(api_key_name,'') IS NOT NULL ORDER BY id NULLS FIRST,name NULLS FIRST`)
      ).rows;
      return { providers, models, accounts, apiKeys };
    });
  }
}
export function getSelectedCallLogRepository(): PostgresCallLogRepository | undefined {
  const database = getSelectedPostgresDatabase();
  return database
    ? bindSelectedUsageRepository(new PostgresCallLogRepository(database))
    : undefined;
}
