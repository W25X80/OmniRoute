export interface ProxyLogBufferStatus {
  bufferedRows: number;
  bufferedBytes: number;
  rowLimit: number;
  byteLimit: number;
  eventByteLimit: number;
  droppedEvents: number;
  oversizedEvents: number;
  consecutiveFlushFailures: number;
  nextFlushAt: number;
}

export const NATIVE_PROXY_LOG_ROW_LIMIT = 1000;
export const NATIVE_PROXY_LOG_BYTE_LIMIT = 2 * 1024 * 1024;
export const NATIVE_PROXY_LOG_EVENT_BYTE_LIMIT = 16 * 1024;

/** Reservations cover both pending and in-flight rows, including failed batches. */
export class NativeProxyLogBuffer {
  private readonly reserved = new Map<string, number>();
  private bytes = 0;
  private dropped = 0;
  private oversized = 0;
  private nextWarningAt = 0;
  private flushFailures = 0;
  private retryAt = 0;

  constructor(
    private readonly rowLimit = NATIVE_PROXY_LOG_ROW_LIMIT,
    private readonly byteLimit = NATIVE_PROXY_LOG_BYTE_LIMIT,
    private readonly eventByteLimit = NATIVE_PROXY_LOG_EVENT_BYTE_LIMIT
  ) {
    if (
      ![rowLimit, byteLimit, eventByteLimit].every(
        (value) => Number.isSafeInteger(value) && value > 0
      )
    ) {
      throw new Error("Invalid proxy telemetry buffer limits");
    }
  }

  reserve(id: string, bytes: number): boolean {
    const duplicate = this.reserved.has(id);
    const oversized = !Number.isSafeInteger(bytes) || bytes < 0 || bytes > this.eventByteLimit;
    if (
      duplicate ||
      oversized ||
      this.reserved.size >= this.rowLimit ||
      this.bytes + bytes > this.byteLimit
    ) {
      this.dropped = Math.min(Number.MAX_SAFE_INTEGER, this.dropped + 1);
      if (oversized) this.oversized = Math.min(Number.MAX_SAFE_INTEGER, this.oversized + 1);
      return false;
    }
    this.reserved.set(id, bytes);
    this.bytes += bytes;
    return true;
  }

  has(id: string): boolean {
    return this.reserved.has(id);
  }

  release(id: string): void {
    const bytes = this.reserved.get(id);
    if (bytes === undefined) return;
    this.reserved.delete(id);
    this.bytes -= bytes;
  }

  warningDue(now: number): boolean {
    if (now < this.nextWarningAt) return false;
    this.nextWarningAt = now + 60_000;
    return true;
  }

  canScheduleFlush(now: number): boolean {
    return now >= this.retryAt;
  }

  noteFlushFailure(now: number): void {
    this.flushFailures = Math.min(Number.MAX_SAFE_INTEGER, this.flushFailures + 1);
    const delay = Math.min(30_000, 1000 * 2 ** Math.min(this.flushFailures - 1, 5));
    this.retryAt = now + delay;
  }

  noteFlushSuccess(): void {
    this.flushFailures = 0;
    this.retryAt = 0;
  }

  status(): ProxyLogBufferStatus {
    return {
      bufferedRows: this.reserved.size,
      bufferedBytes: this.bytes,
      rowLimit: this.rowLimit,
      byteLimit: this.byteLimit,
      eventByteLimit: this.eventByteLimit,
      droppedEvents: this.dropped,
      oversizedEvents: this.oversized,
      consecutiveFlushFailures: this.flushFailures,
      nextFlushAt: this.retryAt,
    };
  }
}
