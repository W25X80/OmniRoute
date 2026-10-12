import type { PostgresDatabase } from "./database";

export interface NativeProviderQuotaRow {
  connection_id: string;
  model: string;
  tokens_used: number;
  token_limit: number;
  window_start: number;
  window_reset: number;
  updated_at: string;
}

export async function readProviderQuotaRows(
  database: PostgresDatabase,
  id?: string,
  model?: string
): Promise<NativeProviderQuotaRow[]> {
  const params: unknown[] = [];
  let where = "";
  if (id !== undefined) {
    params.push(id);
    where = " WHERE connection_id=$1";
  }
  if (model !== undefined) {
    params.push(model);
    where += " AND model=$2";
  }
  return database.transaction(async (tx) =>
    (await tx.query(`SELECT * FROM omniroute.provider_quota_state${where}`, params)).rows.map(
      (r) => ({
        connection_id: String(r.connection_id),
        model: String(r.model),
        tokens_used: Number(r.tokens_used),
        token_limit: Number(r.token_limit),
        window_start: Number(r.window_start),
        window_reset: Number(r.window_reset),
        updated_at: String(r.updated_at),
      })
    )
  );
}

export async function writeProviderQuotaUsage(
  database: PostgresDatabase,
  id: string,
  model: string,
  delta: number,
  opts: { tokenLimit?: number; windowMs?: number } = {}
): Promise<void> {
  const window = opts.windowMs ?? 60_000;
  if (!Number.isFinite(window) || window <= 0 || !Number.isFinite(delta))
    throw new TypeError("Invalid quota window or token delta");
  await database.transaction(async (tx) => {
    // Serialize this counter even before its first row exists. Capture its window only
    // after the lock acknowledgement, so a queued write cannot publish an older clock.
    // The namespace is distinct from quota-consumption locks; hash collisions can only
    // reduce parallelism, while the JSON tuple keeps entity keys unambiguous.
    await tx.query("SELECT pg_advisory_xact_lock(18767,hashtext($1))", [
      JSON.stringify([id, model]),
    ]);
    const now = Date.now(),
      start = Math.floor(now / window) * window;
    await tx.query(
      `INSERT INTO omniroute.provider_quota_state
    (connection_id,model,tokens_used,token_limit,window_start,window_reset,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT(connection_id,model) DO UPDATE SET tokens_used=CASE WHEN provider_quota_state.window_reset>0
      AND $8>provider_quota_state.window_reset THEN excluded.tokens_used ELSE provider_quota_state.tokens_used+excluded.tokens_used END,
      token_limit=CASE WHEN excluded.token_limit>0 THEN excluded.token_limit ELSE provider_quota_state.token_limit END,
      window_start=excluded.window_start,window_reset=excluded.window_reset,updated_at=excluded.updated_at`,
      [
        id,
        model,
        delta,
        opts.tokenLimit ?? 0,
        start,
        start + window,
        new Date(now).toISOString(),
        now,
      ]
    );
  });
}

export async function deleteProviderQuotaRows(
  database: PostgresDatabase,
  id: string
): Promise<void> {
  await database.transaction(async (tx) => {
    await tx.query("DELETE FROM omniroute.provider_quota_state WHERE connection_id=$1", [id]);
  });
}

export async function deleteExpiredProviderQuotaRows(
  database: PostgresDatabase,
  now = Date.now()
): Promise<number> {
  return database.transaction(
    async (tx) =>
      (
        await tx.query(
          "DELETE FROM omniroute.provider_quota_state WHERE window_reset>0 AND window_reset <= $1",
          [now]
        )
      ).rowCount || 0
  );
}
