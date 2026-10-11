import { encrypt, decrypt, requireExplicitStorageEncryptionKey } from "../encryption";
import type { ApiKeyContextSource } from "../apiKeyContextSources";
import type {
  AgentBridgeMappingRow,
  AgentBridgeStateRow,
  AgentBridgeBypassRow,
} from "../_rowTypes";
import type {
  PluginCreateInput,
  PluginRow,
  PluginExecutionRow,
  PluginAnalyticsSummary,
} from "../plugins";
import type { PluginMetricRow } from "../pluginMetrics";
import type { SkillPatch } from "../skills";
import type { PostgresDatabase, PostgresTransaction } from "./database";

function encodeContextToken(token: string): string {
  requireExplicitStorageEncryptionKey();
  const ciphertext = encrypt(token);
  if (typeof ciphertext !== "string") throw new Error("Failed to encrypt context-source token");
  return ciphertext;
}

function context(row: Record<string, unknown>): ApiKeyContextSource {
  requireExplicitStorageEncryptionKey();
  const storedToken = row.token == null ? null : String(row.token);
  const token = storedToken == null ? null : (decrypt(storedToken) ?? null);
  if (storedToken?.startsWith("enc:v1:") && token === null) {
    throw new Error("Context-source token could not be decrypted");
  }
  return {
    apiKeyId: String(row.api_key_id),
    sourceType: String(row.source_type),
    token,
    baseUrl: row.base_url as string | null,
    vaultPath: row.vault_path as string | null,
    enabled: row.enabled === 1,
  };
}
function state(row: Record<string, unknown>): AgentBridgeStateRow {
  return {
    agent_id: String(row.agent_id),
    dns_enabled: row.dns_enabled === 1,
    cert_trusted: row.cert_trusted === 1,
    setup_completed: row.setup_completed === 1,
    last_started_at: row.last_started_at as string | null,
    last_error: row.last_error as string | null,
  };
}
function plugin(row: Record<string, unknown>): PluginRow {
  return {
    id: String(row.id),
    name: String(row.name),
    version: String(row.version),
    description: row.description as string | null,
    author: row.author as string | null,
    license: row.license as string,
    main: String(row.main),
    source: String(row.source),
    tags: row.tags as string,
    status: row.status as PluginRow["status"],
    enabled: Number(row.enabled),
    manifest: String(row.manifest),
    config: row.config as string,
    configSchema: row.config_schema as string,
    hooks: row.hooks as string,
    permissions: row.permissions as string,
    pluginDir: String(row.plugin_dir),
    errorMessage: row.error_message as string | null,
    installedAt: String(row.installed_at),
    updatedAt: String(row.updated_at),
    activatedAt: row.activated_at as string | null,
  };
}

export function createPostgresAgentMetadataRepository(database: PostgresDatabase) {
  function operation<T>(write: boolean, body: (tx: PostgresTransaction) => Promise<T>) {
    return database.transaction(async (tx) => {
      await tx.query(`SELECT pg_advisory_xact_lock${write ? "" : "_shared"}(18765,12)`);
      return body(tx);
    });
  }
  const repository = {
    getApiKeyContextSource: (
      id: string | null | undefined,
      type: string
    ): Promise<(ApiKeyContextSource & { enabled: true }) | null> =>
      !id
        ? Promise.resolve(null)
        : operation(false, async (tx) => {
            const row = (
              await tx.query(
                "SELECT * FROM omniroute.api_key_context_sources WHERE api_key_id=$1 AND source_type=$2 AND enabled=1",
                [id, type]
              )
            ).rows[0];
            return row ? (context(row) as ApiKeyContextSource & { enabled: true }) : null;
          }),
    listApiKeyContextSources: (id: string) =>
      operation(false, async (tx) =>
        (
          await tx.query(
            "SELECT * FROM omniroute.api_key_context_sources WHERE api_key_id=$1 ORDER BY insertion_order",
            [id]
          )
        ).rows.map(context)
      ),
    setApiKeyContextSource: (
      id: string,
      type: string,
      config: { token?: string; baseUrl?: string; vaultPath?: string; enabled?: boolean }
    ) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        await tx.query(
          `INSERT INTO omniroute.api_key_context_sources(api_key_id,source_type,token,base_url,vault_path,enabled,created_at,updated_at)
        VALUES($1,$2,$3,$4,$5,COALESCE($6::integer,1),$7,$7) ON CONFLICT(api_key_id,source_type) DO UPDATE SET
        token=COALESCE($3,api_key_context_sources.token),base_url=COALESCE($4,api_key_context_sources.base_url),vault_path=COALESCE($5,api_key_context_sources.vault_path),enabled=COALESCE($6,api_key_context_sources.enabled),updated_at=$7`,
          [
            id,
            type,
            config.token == null ? null : encodeContextToken(config.token),
            config.baseUrl ?? null,
            config.vaultPath ?? null,
            config.enabled === undefined ? null : config.enabled ? 1 : 0,
            now,
          ]
        );
      }),
    deleteApiKeyContextSource: (id: string, type: string) =>
      operation(true, async (tx) => {
        await tx.query(
          "DELETE FROM omniroute.api_key_context_sources WHERE api_key_id=$1 AND source_type=$2",
          [id, type]
        );
      }),
    getAllAgentBridgeStates: () =>
      operation(false, async (tx) =>
        (
          await tx.query('SELECT * FROM omniroute.agent_bridge_state ORDER BY agent_id COLLATE "C"')
        ).rows.map(state)
      ),
    getAgentBridgeState: (id: string) =>
      operation(false, async (tx) => {
        const row = (
          await tx.query("SELECT * FROM omniroute.agent_bridge_state WHERE agent_id=$1", [id])
        ).rows[0];
        return row ? state(row) : null;
      }),
    upsertAgentBridgeState: (input: Partial<AgentBridgeStateRow> & { agent_id: string }) =>
      operation(true, async (tx) => {
        const columns = [
          "dns_enabled",
          "cert_trusted",
          "setup_completed",
          "last_started_at",
          "last_error",
        ] as const;
        const values: unknown[] = [input.agent_id];
        const updates: string[] = [];
        for (const column of columns) {
          const value = input[column];
          values.push(
            column.endsWith("enabled") || column === "cert_trusted" || column === "setup_completed"
              ? value
                ? 1
                : 0
              : (value ?? null)
          );
          if (value !== undefined) updates.push(`${column}=excluded.${column}`);
        }
        await tx.query(
          `INSERT INTO omniroute.agent_bridge_state(agent_id,${columns.join(",")}) VALUES($1,$2,$3,$4,$5,$6)
        ON CONFLICT(agent_id) ${updates.length ? `DO UPDATE SET ${updates.join(",")}` : "DO NOTHING"}`,
          values
        );
      }),
    setLastStarted: (id: string, time: string) =>
      operation(true, async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.agent_bridge_state(agent_id,last_started_at) VALUES($1,$2) ON CONFLICT(agent_id) DO UPDATE SET last_started_at=excluded.last_started_at",
          [id, time]
        );
      }),
    setLastError: (id: string, error: string | null) =>
      operation(true, async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.agent_bridge_state(agent_id,last_error) VALUES($1,$2) ON CONFLICT(agent_id) DO UPDATE SET last_error=excluded.last_error",
          [id, error]
        );
      }),
    getMappingsForAgent: (id: string) =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<AgentBridgeMappingRow>(
              'SELECT agent_id,source_model,target_model,updated_at FROM omniroute.agent_bridge_mappings WHERE agent_id=$1 ORDER BY source_model COLLATE "C"',
              [id]
            )
          ).rows
      ),
    setMappings: (id: string, mappings: Array<{ source: string; target: string }>) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        await tx.query("DELETE FROM omniroute.agent_bridge_mappings WHERE agent_id=$1", [id]);
        for (const mapping of mappings)
          await tx.query(
            "INSERT INTO omniroute.agent_bridge_mappings(agent_id,source_model,target_model,updated_at) VALUES($1,$2,$3,$4)",
            [id, mapping.source, mapping.target, now]
          );
      }),
    deleteMapping: (id: string, model: string) =>
      operation(true, async (tx) => {
        await tx.query(
          "DELETE FROM omniroute.agent_bridge_mappings WHERE agent_id=$1 AND source_model=$2",
          [id, model]
        );
      }),
    syncAgentBridgeMappingsToMitmAlias: (id: string) =>
      operation(true, async (tx) => {
        if (!["antigravity", "claude-code", "kiro"].includes(id)) return;
        const rows = (
          await tx.query<AgentBridgeMappingRow>(
            'SELECT agent_id,source_model,target_model,updated_at FROM omniroute.agent_bridge_mappings WHERE agent_id=$1 ORDER BY source_model COLLATE "C"',
            [id]
          )
        ).rows;
        const mappings = Object.fromEntries(rows.map((r) => [r.source_model, r.target_model]));
        await tx.query(
          "INSERT INTO omniroute.key_value(namespace,key,value) VALUES('mitmAlias',$1,$2) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value",
          [id, JSON.stringify(mappings)]
        );
      }),
    getAllBypassPatterns: () =>
      operation(
        false,
        async (tx) =>
          (
            await tx.query<AgentBridgeBypassRow>(
              'SELECT pattern,source,created_at FROM omniroute.agent_bridge_bypass ORDER BY source COLLATE "C",pattern COLLATE "C"'
            )
          ).rows
      ),
    getUserBypassPatterns: () =>
      operation(false, async (tx) =>
        (
          await tx.query<{ pattern: string }>(
            "SELECT pattern FROM omniroute.agent_bridge_bypass WHERE source='user' ORDER BY pattern COLLATE \"C\""
          )
        ).rows.map((r) => r.pattern)
      ),
    replaceUserBypassPatterns: (patterns: string[]) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        await tx.query("DELETE FROM omniroute.agent_bridge_bypass WHERE source='user'");
        for (const pattern of patterns)
          await tx.query(
            "INSERT INTO omniroute.agent_bridge_bypass(pattern,source,created_at) VALUES($1,'user',$2)",
            [pattern, now]
          );
      }),
    seedDefaultBypassPatterns: (patterns: string[]) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        for (const pattern of patterns)
          await tx.query(
            "INSERT INTO omniroute.agent_bridge_bypass(pattern,source,created_at) VALUES($1,'default',$2) ON CONFLICT(pattern) DO NOTHING",
            [pattern, now]
          );
      }),
    insertPlugin: (input: PluginCreateInput) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        const rows = (
          await tx.query(
            `INSERT INTO omniroute.plugins(id,name,version,description,author,license,main,source,tags,status,enabled,manifest,config,config_schema,hooks,permissions,plugin_dir,installed_at,updated_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$18) RETURNING *`,
            [
              input.id,
              input.name,
              input.version,
              input.description ?? null,
              input.author ?? null,
              input.license ?? "MIT",
              input.main,
              input.source ?? "local",
              JSON.stringify(input.tags ?? []),
              input.status ?? "installed",
              input.enabled ? 1 : 0,
              JSON.stringify(input.manifest),
              JSON.stringify(input.config ?? {}),
              JSON.stringify(input.configSchema ?? {}),
              JSON.stringify(input.hooks ?? []),
              JSON.stringify(input.permissions ?? []),
              input.pluginDir,
              now,
            ]
          )
        ).rows;
        return plugin(rows[0]);
      }),
    getPluginById: (id: string) =>
      operation(false, async (tx) => {
        const row = (await tx.query("SELECT * FROM omniroute.plugins WHERE id=$1", [id])).rows[0];
        return row ? plugin(row) : null;
      }),
    getPluginByName: (name: string) =>
      operation(false, async (tx) => {
        const row = (await tx.query("SELECT * FROM omniroute.plugins WHERE name=$1", [name]))
          .rows[0];
        return row ? plugin(row) : null;
      }),
    listPlugins: (status?: PluginRow["status"]) =>
      operation(false, async (tx) =>
        (
          await tx.query(
            `SELECT * FROM omniroute.plugins ${status ? "WHERE status=$1" : ""} ORDER BY name COLLATE "C",insertion_order`,
            status ? [status] : []
          )
        ).rows.map(plugin)
      ),
    updatePluginStatus: (name: string, status: PluginRow["status"], error?: string) =>
      operation(true, async (tx) => {
        const now = new Date().toISOString();
        return (
          ((
            await tx.query(
              "UPDATE omniroute.plugins SET status=$1,enabled=$2,error_message=$3,updated_at=$4,activated_at=COALESCE($5,activated_at) WHERE name=$6",
              [
                status,
                status === "active" ? 1 : 0,
                error ?? null,
                now,
                status === "active" ? now : null,
                name,
              ]
            )
          ).rowCount ?? 0) > 0
        );
      }),
    updatePluginManifest: (name: string, manifest: Record<string, unknown>, hooks: string[]) =>
      operation(
        true,
        async (tx) =>
          ((
            await tx.query(
              "UPDATE omniroute.plugins SET manifest=$1,hooks=$2,updated_at=$3 WHERE name=$4",
              [JSON.stringify(manifest), JSON.stringify(hooks), new Date().toISOString(), name]
            )
          ).rowCount ?? 0) > 0
      ),
    updatePluginConfig: (name: string, config: Record<string, unknown>) =>
      operation(
        true,
        async (tx) =>
          ((
            await tx.query("UPDATE omniroute.plugins SET config=$1,updated_at=$2 WHERE name=$3", [
              JSON.stringify(config),
              new Date().toISOString(),
              name,
            ])
          ).rowCount ?? 0) > 0
      ),
    deletePlugin: (name: string) =>
      operation(
        true,
        async (tx) =>
          ((await tx.query("DELETE FROM omniroute.plugins WHERE name=$1", [name])).rowCount ?? 0) >
          0
      ),
    pluginExists: (name: string) =>
      operation(false, async (tx) =>
        Boolean((await tx.query("SELECT 1 FROM omniroute.plugins WHERE name=$1", [name])).rows[0])
      ),
    recordPluginExecution: (
      name: string,
      hook: string,
      duration: number,
      success: boolean,
      error?: string
    ) =>
      operation(true, async (tx) => {
        await tx.query(
          "INSERT INTO omniroute.plugin_analytics(plugin_name,hook,duration_ms,success,error_message) VALUES($1,$2,$3,$4,$5)",
          [name, hook, duration, success ? 1 : 0, error ?? null]
        );
      }),
    getPluginAnalytics: (name: string): Promise<PluginExecutionRow[]> =>
      operation(false, async (tx) =>
        (
          await tx.query(
            "SELECT plugin_name,hook,duration_ms,success,error_message,created_at FROM omniroute.plugin_analytics WHERE plugin_name=$1 ORDER BY created_at DESC,id",
            [name]
          )
        ).rows.map((r) => ({
          pluginName: String(r.plugin_name),
          hook: String(r.hook),
          durationMs: Number(r.duration_ms),
          success: r.success === 1,
          errorMessage: r.error_message as string | null,
          createdAt: String(r.created_at),
        }))
      ),
    getPluginAnalyticsSummary: (name: string): Promise<PluginAnalyticsSummary> =>
      operation(false, async (tx) => {
        const row = (
          await tx.query(
            "SELECT count(*) AS total,SUM(CASE WHEN success=1 THEN 1 ELSE 0 END) AS successes,SUM(CASE WHEN success=0 THEN 1 ELSE 0 END) AS failures,AVG(duration_ms) AS avg_duration FROM omniroute.plugin_analytics WHERE plugin_name=$1",
            [name]
          )
        ).rows[0];
        return {
          totalCalls: Number(row.total ?? 0),
          successCount: Number(row.successes ?? 0),
          failureCount: Number(row.failures ?? 0),
          avgDurationMs: Number(row.avg_duration ?? 0),
        };
      }),
    recordPluginMetric: (name: string, event: string, duration: number, error: boolean) =>
      operation(true, async (tx) => {
        await tx.query(
          `INSERT INTO omniroute.plugin_metrics(plugin_name,event,calls,errors,total_duration_ms,last_called_at) VALUES($1,$2,1,$3,$4,$5)
      ON CONFLICT(plugin_name,event) DO UPDATE SET calls=plugin_metrics.calls+1,errors=plugin_metrics.errors+excluded.errors,total_duration_ms=plugin_metrics.total_duration_ms+excluded.total_duration_ms,last_called_at=excluded.last_called_at`,
          [name, event, error ? 1 : 0, duration, new Date().toISOString()]
        );
      }),
    getPluginMetrics: (name?: string): Promise<PluginMetricRow[]> =>
      operation(false, async (tx) =>
        (
          await tx.query(
            `SELECT plugin_name,event,calls,errors,total_duration_ms,last_called_at FROM omniroute.plugin_metrics ${name ? "WHERE plugin_name=$1" : ""} ORDER BY plugin_name COLLATE "C",event COLLATE "C"`,
            name ? [name] : []
          )
        ).rows.map((r) => ({
          pluginName: String(r.plugin_name),
          event: String(r.event),
          calls: Number(r.calls),
          errors: Number(r.errors),
          totalDurationMs: Number(r.total_duration_ms),
          lastCalledAt: r.last_called_at as string | null,
        }))
      ),
    clearPluginMetrics: (name?: string) =>
      operation(
        true,
        async (tx) =>
          (
            await tx.query(
              `DELETE FROM omniroute.plugin_metrics ${name ? "WHERE plugin_name=$1" : ""}`,
              name ? [name] : []
            )
          ).rowCount ?? 0
      ),
    updateSkill: (id: string, patch: SkillPatch) =>
      operation(true, async (tx) => {
        const values: unknown[] = [id],
          sets: string[] = [];
        let timestampProvided = false;
        for (const [field, value] of Object.entries(patch)) {
          if (!["enabled", "mode", "updated_at"].includes(field)) continue;
          if (field === "updated_at") {
            timestampProvided = true;
            continue;
          }
          values.push(field === "enabled" && typeof value === "boolean" ? (value ? 1 : 0) : value);
          sets.push(`${field}=$${values.length}`);
        }
        if (!sets.length && !timestampProvided) return 0;
        sets.push(
          "updated_at=to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI:SS')"
        );
        return (
          (await tx.query(`UPDATE omniroute.skills SET ${sets.join(",")} WHERE id=$1`, values))
            .rowCount ?? 0
        );
      }),
  };
  return repository;
}
