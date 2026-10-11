export const HEALTH_STATS_CUTOFF = "2026-09-10T00:00:00.000Z";
export interface HealthStatsFixtureRow {
  id: string;
  timestamp: string;
  status: number | null;
  model: string | null;
  requestedModel: string | null;
  provider: string | null;
  connectionId: string | null;
  duration: number | null;
  errorSummary: string | null;
}
export function healthStatsFixtureRows(prefix: string): HealthStatsFixtureRow[] {
  const row = (
    id: string,
    overrides: Partial<HealthStatsFixtureRow> = {}
  ): HealthStatsFixtureRow => ({
    id: `${prefix}-${id}`,
    timestamp: HEALTH_STATS_CUTOFF,
    status: 200,
    model: "fallback",
    requestedModel: null,
    provider: `${prefix}-primary`,
    connectionId: null,
    duration: 1,
    errorSummary: null,
    ...overrides,
  });
  return [
    row("fallback-Z", {
      status: 503,
      model: null,
      requestedModel: "fallback",
      duration: 1,
      errorSummary: "upstream",
    }),
    row("fallback-a", {
      status: 200,
      model: null,
      requestedModel: "fallback",
      connectionId: "",
      duration: 2,
    }),
    row("fallback-null", { timestamp: "2026-09-10T00:00:01.000Z", status: null, duration: null }),
    row("old", { timestamp: "2026-09-09T23:59:59.999Z", duration: 9000 }),
    row("negative-Z", { model: "negative", status: 199, duration: -1 }),
    row("negative-a", { model: "negative", status: 399, duration: -2 }),
    row("null-duration", { model: "null-duration", status: 399, duration: null, errorSummary: "" }),
    row("unknown-model", {
      connectionId: "unknown-model",
      model: null,
      requestedModel: null,
      status: 400,
      duration: 0.5,
    }),
    row("empty-model", {
      connectionId: "empty-model",
      model: "",
      requestedModel: "unused",
      status: 204,
      duration: 0.5,
    }),
    row("secondary-a", { provider: `${prefix}-secondary`, status: 299, duration: 10 }),
    row("secondary-z", { provider: `${prefix}-secondary`, status: 400, duration: 10 }),
    row("injection", { provider: `${prefix}' OR TRUE --`, status: 200, duration: 90 }),
    row("empty-provider", { provider: "", connectionId: `${prefix}-empty-provider`, duration: 0 }),
    row("null-provider", { provider: null, duration: 9999 }),
    row("sentinel", { provider: "-", duration: 9999 }),
  ];
}
export function insertHealthStatsSqliteRows(
  db: { prepare(sql: string): { run(...values: unknown[]): unknown } },
  rows: readonly HealthStatsFixtureRow[]
): void {
  const insert = db.prepare(`INSERT INTO call_logs
    (id,timestamp,status,model,requested_model,provider,connection_id,duration,error_summary)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  for (const row of rows)
    insert.run(
      row.id,
      row.timestamp,
      row.status,
      row.model,
      row.requestedModel,
      row.provider,
      row.connectionId,
      row.duration,
      row.errorSummary
    );
}
