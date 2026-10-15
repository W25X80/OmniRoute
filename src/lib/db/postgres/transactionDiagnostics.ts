import { createHash } from "node:crypto";
import { performance } from "node:perf_hooks";

type Phase = "admission" | "connect" | "begin" | "body" | "query" | "commit" | "rollback";

/** Timings contain no SQL text, bound values, driver messages or caller payloads. */
export class TransactionDiagnostics {
  readonly started = performance.now();
  readonly operation: string;
  phase: Phase = "admission";
  failedPhase: Phase | undefined;
  queueMs = 0;
  connectionMs = 0;
  queryCount = 0;
  queryMs = 0;
  lockMs = 0;
  commitMs = 0;
  callbackGapMs = 0;
  statementId: string | undefined;
  lockOwner: string | undefined;
  lockMode: "shared" | "exclusive" | undefined;
  private settledAt: number | undefined;
  private nextQueryIsLock = false;
  private queryingLock = false;

  noteLock(): void {
    this.nextQueryIsLock = true;
  }

  constructor(operation?: string) {
    this.operation =
      operation && /^[a-zA-Z][a-zA-Z0-9_.-]{0,63}$/.test(operation) ? operation : "transaction";
  }

  beginBody(): void {
    this.phase = "body";
    this.settledAt = performance.now();
  }

  noteGap(): void {
    if (this.settledAt !== undefined)
      this.callbackGapMs = Math.max(this.callbackGapMs, performance.now() - this.settledAt);
  }

  startQuery(text: string): number {
    this.noteGap();
    this.phase = "query";
    this.queryCount++;
    this.queryingLock = this.nextQueryIsLock;
    this.nextQueryIsLock = false;
    this.statementId = createHash("sha256").update(text).digest("hex");
    return performance.now();
  }

  finishQuery(started: number): void {
    const elapsed = performance.now() - started;
    this.queryMs += elapsed;
    if (this.queryingLock) this.lockMs += elapsed;
    this.queryingLock = false;
    this.beginBody();
  }

  failed(): void {
    this.failedPhase ??= this.phase;
    if (this.phase === "body") this.noteGap();
  }

  snapshot(outcome: string, sqlState?: string) {
    return {
      module: "postgres",
      operation: this.operation,
      phase: this.failedPhase ?? this.phase,
      outcome,
      sqlState,
      statementId: this.statementId,
      lockOwner: this.lockOwner,
      lockMode: this.lockMode,
      queryCount: this.queryCount,
      queueMs: Math.round(this.queueMs),
      connectionMs: Math.round(this.connectionMs),
      queryMs: Math.round(this.queryMs),
      lockMs: Math.round(this.lockMs),
      commitMs: Math.round(this.commitMs),
      callbackGapMs: Math.round(this.callbackGapMs),
      durationMs: Math.round(performance.now() - this.started),
    };
  }
}
