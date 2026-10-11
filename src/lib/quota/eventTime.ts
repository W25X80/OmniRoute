/** Validate the captured native completion instant without consulting another clock. */
export function validateQuotaEventTimeMs(eventTimeMs: number): number {
  if (
    !Number.isSafeInteger(eventTimeMs) ||
    eventTimeMs < 0 ||
    eventTimeMs > 8_640_000_000_000_000
  ) {
    throw new RangeError("Quota completion time is invalid");
  }
  return eventTimeMs;
}
