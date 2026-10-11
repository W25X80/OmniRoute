/** Source migration 161 indexes; the table is owned by the application-import schema. */
export const POSTGRES_CONFIG_AUDIT_INDEX_STATEMENTS: readonly string[] = [
  "CREATE INDEX idx_config_audit_log_target_created ON omniroute.config_audit_log(target,timestamp)",
  "CREATE INDEX idx_config_audit_log_created ON omniroute.config_audit_log(timestamp)",
];
