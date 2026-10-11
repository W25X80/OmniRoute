import { parseDatabaseBackend } from "./backendSelection";
import { getSelectedPostgresDatabase } from "./controlPlaneContext";
import { createPostgresCloudAgentRepository } from "./postgres/cloudAgentRepository";
import { getDbInstance } from "./core.ts";
import { emit } from "@/lib/events/eventBus";

/**
 * Publish an `agent.task.updated` transition for the orchestration canvas (Fase 2, Task B2).
 * Best-effort: a listener throwing must never break the DB write path that triggered it.
 */
function emitAgentTaskUpdated(source: "cloud-agent" | "a2a", taskId: string, state: string): void {
  try {
    emit("agent.task.updated", { source, taskId, state, timestamp: Date.now() });
  } catch {
    /* listeners never derail the write path */
  }
}

export interface CloudAgentTaskRow {
  id: string;
  provider_id: string;
  external_id: string | null;
  status: string;
  prompt: string;
  source: string;
  options: string;
  result: string | null;
  activities: string;
  error: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

const initializedCloudTaskHandles = new WeakSet<object>();
function getCloudAgentDb() {
  const database = getDbInstance();
  if (!initializedCloudTaskHandles.has(database)) createCloudAgentTaskTable();
  return database;
}

export function createCloudAgentTaskTable(): void {
  const db = getDbInstance();

  db.exec(`
    CREATE TABLE IF NOT EXISTS cloud_agent_tasks (
      id TEXT PRIMARY KEY,
      provider_id TEXT NOT NULL,
      external_id TEXT,
      status TEXT NOT NULL DEFAULT 'queued',
      prompt TEXT NOT NULL,
      source TEXT NOT NULL,
      options TEXT DEFAULT '{}',
      result TEXT,
      activities TEXT DEFAULT '[]',
      error TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      completed_at TEXT
    )
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_cloud_agent_tasks_provider
    ON cloud_agent_tasks(provider_id)
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_cloud_agent_tasks_status
    ON cloud_agent_tasks(status)
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_cloud_agent_tasks_created
    ON cloud_agent_tasks(created_at DESC)
  `);
  initializedCloudTaskHandles.add(db);
}

export function insertCloudAgentTask(task: CloudAgentTaskRow): void {
  const db = getCloudAgentDb();
  db.prepare(
    `
    INSERT INTO cloud_agent_tasks (
      id, provider_id, external_id, status, prompt, source,
      options, result, activities, error, created_at, updated_at, completed_at
    ) VALUES (
      @id, @provider_id, @external_id, @status, @prompt, @source,
      @options, @result, @activities, @error, @created_at, @updated_at, @completed_at
    )
  `
  ).run(task);
  emitAgentTaskUpdated("cloud-agent", task.id, task.status);
}

// Whitelist of allowed columns for update operations
const ALLOWED_UPDATE_COLUMNS = new Set([
  "status",
  "prompt",
  "source",
  "options",
  "result",
  "activities",
  "error",
  "completed_at",
]);

export function updateCloudAgentTask(
  id: string,
  updates: Partial<Omit<CloudAgentTaskRow, "id">>
): void {
  const db = getCloudAgentDb();

  // Validate keys against whitelist to prevent SQL injection
  const validUpdates: Partial<Omit<CloudAgentTaskRow, "id">> = {};
  for (const [key, value] of Object.entries(updates)) {
    if (ALLOWED_UPDATE_COLUMNS.has(key)) {
      (validUpdates as Record<string, unknown>)[key] = value;
    }
  }

  const fields = Object.keys(validUpdates)
    .map((key) => `${key} = @${key}`)
    .join(", ");

  if (!fields) return; // No valid updates

  db.prepare(
    `
    UPDATE cloud_agent_tasks
    SET ${fields}, updated_at = datetime('now')
    WHERE id = @id
  `
  ).run({ id, ...validUpdates });
  // Publish the row's real status: an update that only touches result/activities/error must
  // not fabricate a state the canvas has never heard of. No row means nothing was written.
  const state = (validUpdates.status as string | undefined) ?? getCloudAgentTaskById(id)?.status;
  if (state) emitAgentTaskUpdated("cloud-agent", id, state);
}

export function getCloudAgentTaskById(id: string): CloudAgentTaskRow | null {
  const db = getCloudAgentDb();
  return db
    .prepare("SELECT * FROM cloud_agent_tasks WHERE id = ?")
    .get(id) as CloudAgentTaskRow | null;
}

export function getCloudAgentTasksByProvider(providerId: string, limit = 50): CloudAgentTaskRow[] {
  const db = getCloudAgentDb();
  return db
    .prepare(
      "SELECT * FROM cloud_agent_tasks WHERE provider_id = ? ORDER BY created_at DESC LIMIT ?"
    )
    .all(providerId, limit) as CloudAgentTaskRow[];
}

export function getCloudAgentTasksByStatus(status: string, limit = 50): CloudAgentTaskRow[] {
  const db = getCloudAgentDb();
  return db
    .prepare("SELECT * FROM cloud_agent_tasks WHERE status = ? ORDER BY created_at DESC LIMIT ?")
    .all(status, limit) as CloudAgentTaskRow[];
}

export function getAllCloudAgentTasks(limit = 100): CloudAgentTaskRow[] {
  const db = getCloudAgentDb();
  return db
    .prepare("SELECT * FROM cloud_agent_tasks ORDER BY created_at DESC LIMIT ?")
    .all(limit) as CloudAgentTaskRow[];
}

export function deleteCloudAgentTask(id: string): void {
  const db = getCloudAgentDb();
  db.prepare("DELETE FROM cloud_agent_tasks WHERE id = ?").run(id);
}

export async function createCloudAgentTaskTableAsync(): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (database) {
    await createPostgresCloudAgentRepository(database).assertCloudAgentTaskTable();
    return;
  }
  createCloudAgentTaskTable();
}
export async function insertCloudAgentTaskAsync(task: CloudAgentTaskRow): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    insertCloudAgentTask(task);
    return;
  }
  await createPostgresCloudAgentRepository(database).insertCloudAgentTask(task);
  emitAgentTaskUpdated("cloud-agent", task.id, task.status);
}
export async function updateCloudAgentTaskAsync(
  id: string,
  updates: Partial<Omit<CloudAgentTaskRow, "id">>
): Promise<void> {
  const database = getSelectedPostgresDatabase();
  if (!database) {
    updateCloudAgentTask(id, updates);
    return;
  }
  const state = await createPostgresCloudAgentRepository(database).updateCloudAgentTask(
    id,
    updates
  );
  if (state) emitAgentTaskUpdated("cloud-agent", id, state);
}

export async function getCloudAgentTaskByIdAsync(
  ...args: Parameters<typeof getCloudAgentTaskById>
): Promise<Awaited<ReturnType<typeof getCloudAgentTaskById>>> {
  const database = getSelectedPostgresDatabase();
  return database
    ? createPostgresCloudAgentRepository(database).getCloudAgentTaskById(...args)
    : getCloudAgentTaskById(...args);
}

export async function getCloudAgentTasksByProviderAsync(
  ...args: Parameters<typeof getCloudAgentTasksByProvider>
): Promise<Awaited<ReturnType<typeof getCloudAgentTasksByProvider>>> {
  const database = getSelectedPostgresDatabase();
  return database
    ? createPostgresCloudAgentRepository(database).getCloudAgentTasksByProvider(...args)
    : getCloudAgentTasksByProvider(...args);
}

export async function getCloudAgentTasksByStatusAsync(
  ...args: Parameters<typeof getCloudAgentTasksByStatus>
): Promise<Awaited<ReturnType<typeof getCloudAgentTasksByStatus>>> {
  const database = getSelectedPostgresDatabase();
  return database
    ? createPostgresCloudAgentRepository(database).getCloudAgentTasksByStatus(...args)
    : getCloudAgentTasksByStatus(...args);
}

export async function getAllCloudAgentTasksAsync(
  ...args: Parameters<typeof getAllCloudAgentTasks>
): Promise<Awaited<ReturnType<typeof getAllCloudAgentTasks>>> {
  const database = getSelectedPostgresDatabase();
  return database
    ? createPostgresCloudAgentRepository(database).getAllCloudAgentTasks(...args)
    : getAllCloudAgentTasks(...args);
}

export async function deleteCloudAgentTaskAsync(
  ...args: Parameters<typeof deleteCloudAgentTask>
): Promise<Awaited<ReturnType<typeof deleteCloudAgentTask>>> {
  const database = getSelectedPostgresDatabase();
  return database
    ? createPostgresCloudAgentRepository(database).deleteCloudAgentTask(...args)
    : deleteCloudAgentTask(...args);
}

/** Preserve SQLite import initialization; native schema belongs to awaited bootstrap. */
export function initializeLegacyCloudAgentTaskTableOnImport(): void {
  if (parseDatabaseBackend() === "postgres") return;
  if (!getSelectedPostgresDatabase()) createCloudAgentTaskTable();
}
