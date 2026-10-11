import { getDbInstance } from "./core";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";

interface StatementLike<TRow = unknown> {
  all: (...params: unknown[]) => TRow[];
  get: (...params: unknown[]) => TRow | undefined;
  run: (...params: unknown[]) => { changes?: number };
}

interface DbLike {
  prepare: <TRow = unknown>(sql: string) => StatementLike<TRow>;
}

interface QuotaObservation {
  resetAt: string | null;
  remainingPercentage: number | null;
}

interface ResetEventInput {
  provider: string;
  connectionId: string;
  windowKey: string;
  currentResetAt: string | null;
  currentRemainingPercentage: number | null;
  previousObservation?: QuotaObservation | null;
  observedAt?: string;
}

interface ResetEventWindowRow {
  windowStartedAt: string;
  windowResetsAt: string;
  observedAt: string;
}

interface QuotaSnapshotObservationRow {
  nextResetAt: string | null;
  remainingPercentage: number | null;
}

interface QuotaSnapshotWindowRow {
  nextResetAt: string | null;
  remainingPercentage: number | null;
  createdAt: string | null;
}

export interface ProviderQuotaWindowStart {
  windowStartIso: string;
  source: "recorded_reset_event" | "observed_snapshot_reset";
}

function toNumberOrNull(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function clampPercent(value: number | null): number | null {
  if (value === null || !Number.isFinite(value)) return null;
  return Math.max(0, Math.min(100, value));
}

function usedPercent(remainingPercentage: number | null): number | null {
  const remaining = clampPercent(remainingPercentage);
  return remaining === null ? null : Math.max(0, Math.min(100, 100 - remaining));
}

function isResetDrop(
  previousUsedPercentage: number | null,
  currentUsedPercentage: number
): boolean {
  if (previousUsedPercentage === null) return false;
  const droppedToResetFloor =
    currentUsedPercentage <= 1 && previousUsedPercentage > currentUsedPercentage;
  const significantDrop = previousUsedPercentage - currentUsedPercentage >= 5;
  return droppedToResetFloor || significantDrop;
}

function parseResetIso(value: string | null): string | null {
  if (!value) return null;
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) return null;
  return new Date(parsed).toISOString();
}

function resetDay(value: string | null): string | null {
  const iso = parseResetIso(value);
  return iso ? iso.slice(0, 10) : null;
}

function normalizeWindowKey(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function isPrimaryWeeklyWindow(windowKey: string): boolean {
  const normalized = normalizeWindowKey(windowKey);
  return (
    (normalized.includes("weekly") || normalized.includes("7d")) && !normalized.includes("sonnet")
  );
}

function getLatestSnapshotObservation(
  connectionId: string,
  windowKey: string
): QuotaObservation | null {
  const db = getDbInstance() as unknown as DbLike;
  try {
    const row = db
      .prepare<QuotaSnapshotObservationRow>(
        `
        SELECT
          next_reset_at as nextResetAt,
          remaining_percentage as remainingPercentage
        FROM quota_snapshots
        WHERE connection_id = ?
          AND LOWER(window_key) = LOWER(?)
          AND next_reset_at IS NOT NULL
        ORDER BY created_at DESC, id DESC
        LIMIT 1
      `
      )
      .get(connectionId, windowKey);
    if (!row) return null;
    return {
      resetAt: row.nextResetAt,
      remainingPercentage: toNumberOrNull(row.remainingPercentage),
    };
  } catch (error: unknown) {
    if (error instanceof Error && error.message.includes("no such table")) return null;
    throw error;
  }
}

export function recordProviderQuotaResetEventIfChanged(input: ResetEventInput): void {
  if (!input.connectionId || !input.windowKey || !isPrimaryWeeklyWindow(input.windowKey)) return;

  const currentResetIso = parseResetIso(input.currentResetAt);
  if (!currentResetIso) return;

  const previous =
    input.previousObservation ?? getLatestSnapshotObservation(input.connectionId, input.windowKey);
  const previousResetIso = parseResetIso(previous?.resetAt ?? null);
  if (!previousResetIso) return;

  const previousResetMs = Date.parse(previousResetIso);
  const currentResetMs = Date.parse(currentResetIso);
  if (!Number.isFinite(previousResetMs) || !Number.isFinite(currentResetMs)) return;

  const previousRemaining = clampPercent(toNumberOrNull(previous?.remainingPercentage));
  const currentRemaining = clampPercent(toNumberOrNull(input.currentRemainingPercentage));
  const observedAt = parseResetIso(input.observedAt ?? null) ?? new Date().toISOString();
  const previousUsed = usedPercent(previousRemaining);
  const currentUsed = usedPercent(currentRemaining);
  const resetMovedForward =
    currentResetMs > previousResetMs && resetDay(previousResetIso) !== resetDay(currentResetIso);
  const resetObservedWithinSameResetAt =
    resetDay(previousResetIso) === resetDay(currentResetIso) &&
    currentUsed !== null &&
    isResetDrop(previousUsed, currentUsed);

  if (!resetMovedForward && !resetObservedWithinSameResetAt) return;

  const windowStartedAt = resetMovedForward ? previousResetIso : observedAt;

  try {
    const db = getDbInstance() as unknown as DbLike;
    db.prepare(
      `
      INSERT OR IGNORE INTO provider_quota_reset_events
        (provider, connection_id, window_key, window_started_at, window_resets_at,
         observed_at, previous_remaining_percentage, new_remaining_percentage,
         previous_used_percentage, new_used_percentage, raw_data)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
    ).run(
      input.provider,
      input.connectionId,
      input.windowKey,
      windowStartedAt,
      currentResetIso,
      observedAt,
      previousRemaining,
      currentRemaining,
      previousUsed,
      currentUsed,
      null
    );
  } catch (error: unknown) {
    if (error instanceof Error && error.message.includes("no such table")) return;
    throw error;
  }
}

function getRecordedQuotaWindowStartIso(
  connectionId: string,
  targetResetAtIso: string,
  nowMs = Date.now()
): string | null {
  if (!connectionId || !targetResetAtIso) return null;
  const targetDay = resetDay(targetResetAtIso);
  if (!targetDay) return null;

  const db = getDbInstance() as unknown as DbLike;
  const nowIso = new Date(nowMs).toISOString();

  try {
    const rows = db
      .prepare<ResetEventWindowRow>(
        `
        SELECT
          window_started_at as windowStartedAt,
          window_resets_at as windowResetsAt,
          observed_at as observedAt
        FROM provider_quota_reset_events
        WHERE connection_id = @connectionId
          AND LOWER(window_key) LIKE '%weekly%'
          AND LOWER(window_key) NOT LIKE '%sonnet%'
          AND observed_at <= @nowIso
        ORDER BY observed_at DESC, id DESC
      `
      )
      .all({ connectionId, nowIso });

    for (const row of rows) {
      if (resetDay(row.windowResetsAt) === targetDay) {
        return parseResetIso(row.windowStartedAt);
      }
    }
    return null;
  } catch (error: unknown) {
    if (error instanceof Error && error.message.includes("no such table")) return null;
    throw error;
  }
}

function getObservedQuotaWindowStartIso(
  connectionId: string,
  targetResetAtIso: string,
  nowMs = Date.now()
): { windowStartIso: string; resetDrop: boolean } | null {
  if (!connectionId || !targetResetAtIso) return null;
  const targetDay = resetDay(targetResetAtIso);
  if (!targetDay) return null;

  const db = getDbInstance() as unknown as DbLike;
  const nowIso = new Date(nowMs).toISOString();

  try {
    const rows = db
      .prepare<QuotaSnapshotWindowRow>(
        `
        SELECT
          next_reset_at as nextResetAt,
          remaining_percentage as remainingPercentage,
          created_at as createdAt
        FROM quota_snapshots
        WHERE connection_id = @connectionId
          AND LOWER(window_key) LIKE '%weekly%'
          AND LOWER(window_key) NOT LIKE '%sonnet%'
          AND created_at <= @nowIso
        ORDER BY created_at ASC, id ASC
      `
      )
      .all({ connectionId, nowIso });

    let firstObservedIso: string | null = null;
    let resetDropIso: string | null = null;
    let previousUsedPercentage: number | null = null;

    for (const row of rows) {
      const createdIso = parseResetIso(row.createdAt);
      if (!createdIso || resetDay(row.nextResetAt) !== targetDay) continue;

      if (!firstObservedIso) firstObservedIso = createdIso;

      const currentUsedPercentage = usedPercent(clampPercent(row.remainingPercentage));
      if (currentUsedPercentage !== null) {
        if (isResetDrop(previousUsedPercentage, currentUsedPercentage)) {
          resetDropIso = createdIso;
        }
        previousUsedPercentage = currentUsedPercentage;
      }
    }

    if (resetDropIso) return { windowStartIso: resetDropIso, resetDrop: true };
    if (firstObservedIso) return { windowStartIso: firstObservedIso, resetDrop: false };
    return null;
  } catch (error: unknown) {
    if (error instanceof Error && error.message.includes("no such table")) return null;
    throw error;
  }
}

export function getProviderQuotaWindowStart(
  connectionId: string,
  targetResetAtIso: string,
  nowMs = Date.now()
): ProviderQuotaWindowStart | null {
  const recordedIso = getRecordedQuotaWindowStartIso(connectionId, targetResetAtIso, nowMs);
  const observed = getObservedQuotaWindowStartIso(connectionId, targetResetAtIso, nowMs);

  if (!recordedIso && !observed) return null;
  if (!recordedIso && observed) {
    return { windowStartIso: observed.windowStartIso, source: "observed_snapshot_reset" };
  }
  if (recordedIso && !observed) {
    return { windowStartIso: recordedIso, source: "recorded_reset_event" };
  }

  const recordedMs = Date.parse(recordedIso!);
  const observedMs = Date.parse(observed!.windowStartIso);
  if (
    observed!.resetDrop &&
    Number.isFinite(recordedMs) &&
    Number.isFinite(observedMs) &&
    observedMs > recordedMs
  ) {
    return { windowStartIso: observed!.windowStartIso, source: "observed_snapshot_reset" };
  }

  return { windowStartIso: recordedIso!, source: "recorded_reset_event" };
}

export function getProviderQuotaWindowStartIso(
  connectionId: string,
  targetResetAtIso: string,
  nowMs = Date.now()
): string | null {
  return getProviderQuotaWindowStart(connectionId, targetResetAtIso, nowMs)?.windowStartIso ?? null;
}

export async function recordProviderQuotaResetEventIfChangedAsync(
  input: ResetEventInput
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) return recordProviderQuotaResetEventIfChanged(input);
  if (!input.connectionId || !input.windowKey || !isPrimaryWeeklyWindow(input.windowKey)) return;
  const currentResetIso = parseResetIso(input.currentResetAt);
  if (!currentResetIso) return;
  await database.transaction(async (tx) => {
    await tx.query("SELECT pg_advisory_xact_lock(18765,8)");
    const row = input.previousObservation
      ? null
      : (
          await tx.query(
            `SELECT next_reset_at,remaining_percentage FROM omniroute.quota_snapshots
      WHERE connection_id=$1 AND LOWER(window_key)=LOWER($2) AND next_reset_at IS NOT NULL
      ORDER BY created_at DESC,id DESC LIMIT 1`,
            [input.connectionId, input.windowKey]
          )
        ).rows[0];
    const previous =
      input.previousObservation ??
      (row
        ? {
            resetAt: String(row.next_reset_at),
            remainingPercentage: toNumberOrNull(row.remaining_percentage),
          }
        : null);
    const previousResetIso = parseResetIso(previous?.resetAt ?? null);
    if (!previousResetIso) return;
    const previousRemaining = clampPercent(toNumberOrNull(previous?.remainingPercentage)),
      currentRemaining = clampPercent(toNumberOrNull(input.currentRemainingPercentage));
    const previousUsed = usedPercent(previousRemaining),
      currentUsed = usedPercent(currentRemaining);
    const observedAt = parseResetIso(input.observedAt ?? null) ?? new Date().toISOString();
    const forward =
      Date.parse(currentResetIso) > Date.parse(previousResetIso) &&
      resetDay(previousResetIso) !== resetDay(currentResetIso);
    const drop =
      resetDay(previousResetIso) === resetDay(currentResetIso) &&
      currentUsed !== null &&
      isResetDrop(previousUsed, currentUsed);
    if (!forward && !drop) return;
    await tx.query(
      `INSERT INTO omniroute.provider_quota_reset_events
      (provider,connection_id,window_key,window_started_at,window_resets_at,observed_at,previous_remaining_percentage,
       new_remaining_percentage,previous_used_percentage,new_used_percentage,raw_data,created_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULL,$6)
      ON CONFLICT(connection_id,window_key,window_started_at,window_resets_at) DO NOTHING`,
      [
        input.provider,
        input.connectionId,
        input.windowKey,
        forward ? previousResetIso : observedAt,
        currentResetIso,
        observedAt,
        previousRemaining,
        currentRemaining,
        previousUsed,
        currentUsed,
      ]
    );
  });
}

export async function getProviderQuotaWindowStartAsync(
  connectionId: string,
  targetResetAtIso: string,
  nowMs = Date.now()
): Promise<ProviderQuotaWindowStart | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return getProviderQuotaWindowStart(connectionId, targetResetAtIso, nowMs);
  const target = resetDay(targetResetAtIso);
  if (!connectionId || !target) return null;
  return database.transaction(async (tx) => {
    const nowIso = new Date(nowMs).toISOString();
    const recordedRows = (
      await tx.query(
        `SELECT window_started_at,window_resets_at FROM omniroute.provider_quota_reset_events
      WHERE connection_id=$1 AND LOWER(window_key) LIKE '%weekly%' AND LOWER(window_key) NOT LIKE '%sonnet%'
      AND observed_at <= $2 ORDER BY observed_at DESC,id DESC`,
        [connectionId, nowIso]
      )
    ).rows;
    const recorded = recordedRows.find((r) => resetDay(String(r.window_resets_at)) === target);
    const recordedIso = recorded ? parseResetIso(String(recorded.window_started_at)) : null;
    const rows = (
      await tx.query(
        `SELECT next_reset_at,remaining_percentage,created_at FROM omniroute.quota_snapshots
      WHERE connection_id=$1 AND LOWER(window_key) LIKE '%weekly%' AND LOWER(window_key) NOT LIKE '%sonnet%'
      AND created_at <= $2 ORDER BY created_at ASC,id ASC`,
        [connectionId, nowIso]
      )
    ).rows;
    let first: string | null = null,
      drop: string | null = null,
      prior: number | null = null;
    for (const row of rows) {
      const created = parseResetIso(String(row.created_at));
      if (
        !created ||
        resetDay(row.next_reset_at === null ? null : String(row.next_reset_at)) !== target
      )
        continue;
      first ??= created;
      const current = usedPercent(clampPercent(toNumberOrNull(row.remaining_percentage)));
      if (current !== null) {
        if (isResetDrop(prior, current)) drop = created;
        prior = current;
      }
    }
    const observed = drop || first;
    if (!recordedIso && !observed) return null;
    if (observed && (!recordedIso || (drop && Date.parse(drop) > Date.parse(recordedIso))))
      return { windowStartIso: observed, source: "observed_snapshot_reset" };
    return { windowStartIso: recordedIso!, source: "recorded_reset_event" };
  });
}

export async function getProviderQuotaWindowStartIsoAsync(
  connectionId: string,
  targetResetAtIso: string,
  nowMs = Date.now()
): Promise<string | null> {
  return (
    (await getProviderQuotaWindowStartAsync(connectionId, targetResetAtIso, nowMs))
      ?.windowStartIso ?? null
  );
}
