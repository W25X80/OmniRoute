import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import type { BlockedHistoryEntry } from "./blockedHistory";
import type { ProbeCause } from "./decision";

interface HistoryFileIO {
  read(filename: string): Promise<string>;
  mkdir(directory: string): Promise<unknown>;
  write(filename: string, content: string): Promise<unknown>;
  rename(from: string, to: string): Promise<unknown>;
  remove(filename: string): Promise<unknown>;
}
const fileIO: HistoryFileIO = {
  read: (filename) => fs.readFile(filename, "utf8"),
  mkdir: (directory) => fs.mkdir(directory, { recursive: true }),
  write: (filename, content) => fs.writeFile(filename, content, "utf8"),
  rename: (from, to) => fs.rename(from, to),
  remove: (filename) => fs.rm(filename, { force: true }),
};

function validEntry(value: unknown): value is BlockedHistoryEntry {
  if (!value || typeof value !== "object") return false;
  const entry = value as Record<string, unknown>;
  return (
    typeof entry.count === "number" &&
    Number.isFinite(entry.count) &&
    entry.count > 0 &&
    typeof entry.firstSeen === "number" &&
    typeof entry.lastSeen === "number" &&
    (entry.lastCause === "unclassified" ||
      entry.lastCause === "target_refused" ||
      entry.lastCause === "unproven") &&
    (typeof entry.lastStatus === "number" || entry.lastStatus === null)
  );
}

/** One application's refusal-history file. Mutations never wait for a disk write. */
export class NativeBlockedHistoryStore {
  private readonly entries = new Map<string, BlockedHistoryEntry>();
  private loading?: Promise<void>;
  private writing?: Promise<void>;
  private cancelScheduled?: () => void;
  private revision = 0;
  private persistedRevision = 0;
  private closed = false;
  private closing?: Promise<void>;

  constructor(
    private readonly filename: string,
    private readonly schedule: (flush: () => Promise<void>, dispatched: () => void) => () => void,
    private readonly io: HistoryFileIO = fileIO,
    private readonly warn: () => void = () =>
      console.warn("[ProxyHealth] Blocked history unreadable, starting empty.")
  ) {}

  private load(): Promise<void> {
    return (this.loading ??= (async () => {
      let text: string;
      try {
        text = await this.io.read(this.filename);
      } catch {
        return;
      }
      try {
        const parsed: unknown = JSON.parse(text);
        if (!parsed || typeof parsed !== "object") return;
        for (const [id, value] of Object.entries(parsed)) {
          if (!validEntry(value)) continue;
          if (this.entries.size >= 5000) break;
          this.entries.set(id, value);
        }
      } catch {
        this.warn();
      }
    })());
  }

  private changed(): void {
    this.revision++;
    if (!this.cancelScheduled && !this.closed) {
      this.cancelScheduled = this.schedule(
        async () => {
          await this.flush();
        },
        () => {
          this.cancelScheduled = undefined;
        }
      );
    }
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("Proxy health history is closed");
  }

  async record(id: string, cause: ProbeCause, status: number | null, at: number): Promise<void> {
    this.assertOpen();
    if (!id) return;
    await this.load();
    this.assertOpen();
    const prior = this.entries.get(id);
    if (!prior && this.entries.size >= 5000) {
      const oldest = this.entries.keys().next();
      if (!oldest.done) this.entries.delete(oldest.value);
    }
    this.entries.delete(id);
    this.entries.set(id, {
      count: (prior?.count ?? 0) + 1,
      firstSeen: prior?.firstSeen ?? at,
      lastSeen: at,
      lastCause: cause,
      lastStatus: status,
    });
    this.changed();
  }

  async read(ids: Iterable<string>): Promise<Record<string, BlockedHistoryEntry>> {
    this.assertOpen();
    await this.load();
    this.assertOpen();
    const result: Record<string, BlockedHistoryEntry> = Object.create(null);
    for (const id of ids) {
      const entry = this.entries.get(id);
      if (entry) result[id] = { ...entry };
    }
    return result;
  }

  async remove(id: string): Promise<void> {
    this.assertOpen();
    await this.load();
    this.assertOpen();
    if (this.entries.delete(id)) this.changed();
  }

  async prune(liveIds: Iterable<string>): Promise<number> {
    this.assertOpen();
    await this.load();
    this.assertOpen();
    const live = new Set(liveIds);
    let pruned = 0;
    for (const id of this.entries.keys()) {
      if (!live.has(id)) {
        this.entries.delete(id);
        pruned++;
      }
    }
    if (pruned) this.changed();
    return pruned;
  }

  flush(): Promise<void> {
    this.cancelScheduled?.();
    this.cancelScheduled = undefined;
    if (this.writing) return this.writing;
    const writing = (async () => {
      await this.loading;
      while (this.persistedRevision < this.revision) {
        const revision = this.revision;
        const content = JSON.stringify(Object.fromEntries(this.entries));
        const temporary = `${this.filename}.${process.pid}.${randomUUID()}.tmp`;
        await this.io.mkdir(path.dirname(this.filename));
        try {
          await this.io.write(temporary, content);
          await this.io.rename(temporary, this.filename);
          this.persistedRevision = revision;
        } finally {
          await this.io.remove(temporary);
        }
      }
    })();
    this.writing = writing;
    void writing
      .finally(() => {
        if (this.writing === writing) this.writing = undefined;
      })
      .catch(() => {});
    return writing;
  }

  /** Called only after accepted application requests/background work have drained. */
  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.closed = true;
    this.cancelScheduled?.();
    this.cancelScheduled = undefined;
    return (this.closing = this.flush());
  }
}
