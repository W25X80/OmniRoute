import { getSelectedPersistenceIdentity, getSelectedPostgresDatabase } from "./controlPlaneContext";

export function isPostgresSessionModelHistorySelected(): boolean {
  return getSelectedPostgresDatabase() !== undefined;
}

export interface SessionModelHistoryFence {
  readonly authority: object;
  readonly sessionId: string;
  readonly comboName: string;
  readonly highWater: string | null;
}

/** Observe the pin before dispatch; later failure cleanup cannot remove a newer success. */
export async function observeSessionModelHistoryFence(
  sessionId: string,
  comboName: string
): Promise<SessionModelHistoryFence | null> {
  const database = getSelectedPostgresDatabase();
  if (!database) return null;
  const authority = getSelectedPersistenceIdentity();
  if (!authority) throw new Error("Session history authority is unavailable");
  return database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765,13)");
    const { rows } = await transaction.query<{ high_water: string | null }>(
      `SELECT max(id)::text AS high_water FROM omniroute.session_model_history
       WHERE session_id=$1 AND combo_name=$2`,
      [sessionId, comboName]
    );
    return { authority, sessionId, comboName, highWater: rows[0].high_water };
  });
}

/**
 * The shared session lock serializes native inserts, observation and deletion. IDs are
 * compared as database bigints, without reducing their range to JavaScript numbers.
 */
export async function deleteSessionModelHistoryWithFence(
  fence: SessionModelHistoryFence,
  stillCurrent: () => boolean
): Promise<boolean> {
  const database = getSelectedPostgresDatabase();
  if (!database || fence.authority !== getSelectedPersistenceIdentity()) return false;
  return database.transaction(async (transaction) => {
    await transaction.query("SELECT pg_advisory_xact_lock(18765,13)");
    if (!stillCurrent()) return false;
    const { rows } = await transaction.query<{ high_water: string | null }>(
      `SELECT max(id)::text AS high_water FROM omniroute.session_model_history
       WHERE session_id=$1 AND combo_name=$2`,
      [fence.sessionId, fence.comboName]
    );
    if (rows[0].high_water !== fence.highWater) return false;
    await transaction.query(
      `DELETE FROM omniroute.session_model_history WHERE session_id=$1 AND combo_name=$2`,
      [fence.sessionId, fence.comboName]
    );
    return true;
  });
}
