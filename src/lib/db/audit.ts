import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import type { AuditLogFilter, AuditLogWriteEntry } from "./auditSemantics";
import {
  countPostgresAuditLog,
  getPostgresAuditLog,
  writePostgresAuditEvent,
  writePostgresMcpAudit,
  queryPostgresMcpAudit,
  getPostgresMcpAuditStats,
  readPostgresNoLog,
  readPostgresMcpConfiguration,
  type McpAuditWrite,
  type McpAuditFilters,
  deletePostgresAuditRowsBefore,
  trimPostgresProxyAuditRows,
  type AuditRetentionTable,
} from "./postgres/auditRepository";

/** Selected audit owner. Consumers never receive a SQL handle or select a driver. */
export function getPostgresAuditRepository() {
  if (!getSelectedPostgresDatabase()) return undefined;
  const database = () => {
    const selected = getSelectedPostgresDatabase();
    if (!selected) throw new Error("PostgreSQL audit operation is outside its selected scope");
    return selected;
  };
  return {
    write: (entry: AuditLogWriteEntry) => writePostgresAuditEvent(database(), entry),
    list: (filter?: AuditLogFilter) => getPostgresAuditLog(database(), filter),
    count: (filter?: AuditLogFilter) => countPostgresAuditLog(database(), filter),
    writeMcp: (entry: McpAuditWrite) => writePostgresMcpAudit(database(), entry),
    queryMcp: (filters?: McpAuditFilters) => queryPostgresMcpAudit(database(), filters),
    mcpStats: () => getPostgresMcpAuditStats(database()),
    noLog: (apiKeyId: string) => readPostgresNoLog(database(), apiKeyId),
    deleteBefore: (table: AuditRetentionTable, cutoff: string) =>
      deletePostgresAuditRowsBefore(database(), table, cutoff),
    trimProxy: (maxRows: number) => trimPostgresProxyAuditRows(database(), maxRows),
    mcpConfiguration: () => readPostgresMcpConfiguration(database()),
  };
}

export async function readMcpServerConfigurationAsync(): Promise<
  Record<string, unknown> | undefined
> {
  const repository = getPostgresAuditRepository();
  return repository?.mcpConfiguration();
}
