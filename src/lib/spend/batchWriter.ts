import { randomUUID } from "node:crypto";
import { captureSpendEntries, isPostgresSpendReceiptError } from "@/lib/db/postgres/spendReceipts";
import { batchSaveCostEntriesAsync } from "@/lib/db/domainStateAsync";
import { getSelectedPersistenceIdentity } from "@/lib/db/controlPlaneContext";
import { runPersistenceBackgroundWork } from "@/lib/db/persistenceRuntime";
import { isPostgresErrorKind } from "@/lib/db/postgres/database";

export interface BufferedCostEntry {
  readonly receiptId?: string;
  apiKeyId: string;
  cost: number;
  timestamp: number;
}

interface SpendBatchWriterOptions {
  createReceiptId?: () => string;
  flushIntervalMs?: number;
  maxBufferSize?: number;
  persistEntries?: (entries: BufferedCostEntry[]) => Promise<void> | void;
  logger?: Pick<Console, "log" | "error">;
}

type FlushResult = {
  flushedEntries: number;
  uniqueKeys: number;
  requeued: boolean;
};

const DEFAULT_FLUSH_INTERVAL_MS = 60_000;
const DEFAULT_MAX_BUFFER_SIZE = 1_000;

function getFlushIntervalMs() {
  const parsed = Number.parseInt(process.env.OMNIROUTE_SPEND_FLUSH_INTERVAL_MS || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_FLUSH_INTERVAL_MS;
}

function getMaxBufferSize() {
  const parsed = Number.parseInt(process.env.OMNIROUTE_SPEND_MAX_BUFFER_SIZE || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_MAX_BUFFER_SIZE;
}

function normalizeEntry(entry: BufferedCostEntry): BufferedCostEntry | null {
  if (!entry?.apiKeyId || !Number.isFinite(entry.cost) || entry.cost <= 0) return null;
  return {
    apiKeyId: entry.apiKeyId,
    cost: entry.cost,
    timestamp: Number.isFinite(entry.timestamp) ? entry.timestamp : Date.now(),
  };
}

export class SpendBatchWriter {
  private buffer: BufferedCostEntry[] = [];
  private inFlightEntries: BufferedCostEntry[] = [];
  private uncertainEntries: BufferedCostEntry[] = [];
  private discardedApiKeyIds = new Set<string>();
  private timer: NodeJS.Timeout | null = null;
  private started = false;
  private nativeClosing = false;
  private nativeSealed = false;
  private nativeClosePromise: Promise<void> | null = null;
  private flushPromise: Promise<FlushResult> | null = null;
  private persistEntries: (entries: BufferedCostEntry[]) => Promise<void> | void;
  private logger: Pick<Console, "log" | "error">;
  private createReceiptId?: () => string;
  private flushIntervalMs: number;
  private maxBufferSize: number;

  constructor(options: SpendBatchWriterOptions = {}) {
    this.createReceiptId = options.createReceiptId;
    this.persistEntries =
      options.persistEntries ||
      ((entries) => runPersistenceBackgroundWork(() => batchSaveCostEntriesAsync(entries)));
    this.logger = options.logger || console;
    this.flushIntervalMs = options.flushIntervalMs ?? getFlushIntervalMs();
    this.maxBufferSize = options.maxBufferSize ?? getMaxBufferSize();
  }

  start() {
    if (this.started || this.nativeClosing) return;
    this.started = true;
    this.timer = setInterval(() => {
      void this.flush();
    }, this.flushIntervalMs);
    this.timer.unref?.();
  }

  increment(apiKeyId: string, cost: number, timestamp = Date.now()) {
    const entry = normalizeEntry({ apiKeyId, cost, timestamp });
    if (!entry) return;
    if (this.nativeSealed) throw new Error("Native spend writer is closed");

    this.start();
    this.discardedApiKeyIds.delete(entry.apiKeyId);
    const accepted = this.createReceiptId
      ? captureSpendEntries([{ ...entry, receiptId: this.createReceiptId() }])[0]
      : entry;
    this.buffer.push(accepted);

    if (!this.nativeClosing && this.buffer.length >= this.maxBufferSize) {
      void this.flush();
    }
  }

  getBufferedEntries(
    apiKeyId: string,
    sinceTimestamp = 0,
    untilTimestamp = Number.POSITIVE_INFINITY
  ) {
    const matchesWindow = (entry: BufferedCostEntry) =>
      entry.apiKeyId === apiKeyId &&
      entry.timestamp >= sinceTimestamp &&
      entry.timestamp < untilTimestamp;

    return [...this.uncertainEntries, ...this.inFlightEntries, ...this.buffer].filter(
      matchesWindow
    );
  }

  getPendingCostTotal(
    apiKeyId: string,
    sinceTimestamp = 0,
    untilTimestamp = Number.POSITIVE_INFINITY
  ) {
    return this.getBufferedEntries(apiKeyId, sinceTimestamp, untilTimestamp).reduce(
      (sum, entry) => sum + entry.cost,
      0
    );
  }

  discardEntries(apiKeyId: string) {
    this.discardedApiKeyIds.add(apiKeyId);
    this.buffer = this.buffer.filter((entry) => entry.apiKeyId !== apiKeyId);
    this.uncertainEntries = this.uncertainEntries.filter((entry) => entry.apiKeyId !== apiKeyId);
    this.inFlightEntries = this.inFlightEntries.filter((entry) => entry.apiKeyId !== apiKeyId);
  }

  async flush(): Promise<FlushResult> {
    if (this.flushPromise) {
      return this.flushPromise;
    }

    if (this.buffer.length === 0) {
      return { flushedEntries: 0, uniqueKeys: 0, requeued: false };
    }

    const entriesToFlush = [...this.buffer];
    this.buffer = [];
    this.inFlightEntries = entriesToFlush;

    this.flushPromise = (async () => {
      const entriesToPersist = entriesToFlush.filter(
        (entry) => !this.discardedApiKeyIds.has(entry.apiKeyId)
      );
      const uniqueKeys = new Set(entriesToPersist.map((entry) => entry.apiKeyId)).size;

      try {
        if (entriesToPersist.length > 0) {
          await this.persistEntries(entriesToPersist);
        }
        this.logger.log(
          `[SpendWriter] Flushed ${entriesToPersist.length} cost entr${
            entriesToPersist.length === 1 ? "y" : "ies"
          } across ${uniqueKeys} key(s)`
        );
        return {
          flushedEntries: entriesToPersist.length,
          uniqueKeys,
          requeued: false,
        };
      } catch (error) {
        if (isPostgresErrorKind(error, "commit_unknown") || isPostgresSpendReceiptError(error)) {
          // Replaying an unacknowledged financial COMMIT can charge twice. Keep
          // uncertainty visible to diagnostics and conservatively to budget reads.
          this.uncertainEntries.push(...entriesToPersist);
          this.logger.error(
            `[SpendWriter] Quarantined ${entriesToPersist.length} entries after uncertain or conflicting persistence outcome; reconciliation required`
          );
          return { flushedEntries: 0, uniqueKeys, requeued: false };
        }
        this.buffer = [...entriesToPersist, ...this.buffer];
        const message = error instanceof Error ? error.message : String(error);
        this.logger.error(`[SpendWriter] Flush error: ${message}`);
        return {
          flushedEntries: 0,
          uniqueKeys,
          requeued: true,
        };
      } finally {
        this.inFlightEntries = [];
        this.flushPromise = null;
      }
    })();

    return this.flushPromise;
  }

  async stop() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.started = false;
    return this.flush();
  }

  /** Stop producers now; already admitted request/stream owners may still account. */
  beginNativeClose(): void {
    this.nativeClosing = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.started = false;
  }

  private incompleteNativeDrain(): Error {
    return new Error(
      "Native spend writer has unsettled cost entries " +
        `(buffered=${this.buffer.length}, inFlight=${this.inFlightEntries.length}, uncertain=${this.uncertainEntries.length})`
    );
  }

  /** Call after accepted work drains. Never replay a failed/uncertain final flush. */
  closeNative(): Promise<void> {
    if (this.nativeClosePromise) return this.nativeClosePromise;
    this.beginNativeClose();
    this.nativeSealed = true;
    this.nativeClosePromise = (async () => {
      // A prior successful batch can finish while later admitted increments wait
      // in the buffer. Join it first, then flush that distinct final batch once.
      if (this.flushPromise && (await this.flushPromise).requeued) {
        throw this.incompleteNativeDrain();
      }
      const result = await this.flush();
      if (
        result.requeued ||
        this.buffer.length ||
        this.inFlightEntries.length ||
        this.uncertainEntries.length
      ) {
        throw this.incompleteNativeDrain();
      }
    })();
    return this.nativeClosePromise;
  }

  getUncertainEntries(): readonly BufferedCostEntry[] {
    return this.uncertainEntries.map((entry) => ({ ...entry }));
  }

  /** Remove only quarantined events whose canonical receipt was read after commit. */
  resolveCommittedReceipts(receiptIds: readonly string[]): number {
    const committed = new Set(receiptIds);
    const before = this.uncertainEntries.length;
    this.uncertainEntries = this.uncertainEntries.filter(
      (entry) => !entry.receiptId || !committed.has(entry.receiptId)
    );
    return before - this.uncertainEntries.length;
  }

  resetForTests() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.started = false;
    this.nativeClosing = false;
    this.nativeSealed = false;
    this.nativeClosePromise = null;
    this.buffer = [];
    this.inFlightEntries = [];
    this.uncertainEntries = [];
    this.discardedApiKeyIds.clear();
    this.flushPromise = null;
  }
}

const SPEND_WRITER_STATE = Symbol.for("omniroute.spendBatchWriters");
interface SpendWriterState {
  sqlite: SpendBatchWriter;
  postgres: WeakMap<object, SpendBatchWriter>;
}
const sharedWriters = globalThis as typeof globalThis & { [SPEND_WRITER_STATE]?: SpendWriterState };
const writerState = (sharedWriters[SPEND_WRITER_STATE] ??= {
  sqlite: new SpendBatchWriter(),
  postgres: new WeakMap(),
});

function selectedSpendWriter(): SpendBatchWriter {
  const identity = getSelectedPersistenceIdentity();
  if (!identity) return writerState.sqlite;
  let writer = writerState.postgres.get(identity);
  if (!writer) {
    writer = new SpendBatchWriter({
      createReceiptId: randomUUID,
      persistEntries: (entries) =>
        runPersistenceBackgroundWork(async () => {
          if (getSelectedPersistenceIdentity() !== identity)
            throw new Error("Spend persistence changed during background work");
          await batchSaveCostEntriesAsync(entries);
        }),
    });
    writerState.postgres.set(identity, writer);
  }
  return writer;
}

/** Share pending spend across Next bundles, while keeping SQLite and each native database separate. */
export const spendBatchWriter = new Proxy(writerState.sqlite, {
  get(_target, property) {
    const writer = selectedSpendWriter();
    const value = Reflect.get(writer, property);
    return typeof value === "function"
      ? (...args: unknown[]) => {
          if (selectedSpendWriter() !== writer)
            throw new Error("Spend writer belongs to another persistence scope");
          return Reflect.apply(value, writer, args);
        }
      : value;
  },
});

export function startSpendBatchWriter() {
  spendBatchWriter.start();
}

export async function flushSpendBatchWriter() {
  return spendBatchWriter.flush();
}

export async function stopSpendBatchWriter() {
  return spendBatchWriter.stop();
}

export function beginNativeSpendBatchWriterClose(): void {
  if (getSelectedPersistenceIdentity()) spendBatchWriter.beginNativeClose();
}

export async function closeNativeSpendBatchWriter(): Promise<void> {
  if (getSelectedPersistenceIdentity()) await spendBatchWriter.closeNative();
}

export function resetSpendBatchWriterForTests() {
  spendBatchWriter.resetForTests();
}

export function discardSpendBatchEntries(apiKeyId: string) {
  spendBatchWriter.discardEntries(apiKeyId);
}
