/** Source datetime projection for intelligence expiry, including accepted legacy date forms. */
export function intelligenceExpiryTimestamp(value: string, now = Date.now()): string | null {
  const format = (date: Date, originalHour?: number): string | null => {
    const instant = date.getTime();
    if (!Number.isFinite(instant) || instant < -210866760000000 || instant >= 253402300800000)
      return null;
    const year = date.getUTCFullYear();
    const pad = (number: number) => String(number).padStart(2, "0");
    const yearText = `${year < 0 ? "-" : ""}${String(Math.abs(year)).padStart(4, "0")}`;
    return `${yearText}-${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())} ${pad(originalHour ?? date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
  };
  if (/^now$/i.test(value)) return format(new Date(now));
  if (/^[\x09-\x0d ]*[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?[\x09-\x0d ]*$/.test(value)) {
    const julian = Number(value);
    if (!Number.isFinite(julian) || julian < 0 || julian >= 5373484.5) return null;
    return format(new Date(Math.round(julian * 86400000) - 210866760000000));
  }
  const dated = /^(-?\d{4})-(\d{2})-(\d{2})(?:[T\x09-\x0d ]+(.*))?$/.exec(value);
  const time =
    /^(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d+))?)?[\x09-\x0d ]*(Z|[+-]\d{2}:\d{2})?[\x09-\x0d ]*$/i.exec(
      dated ? dated[4] || "00:00" : value
    );
  if (!time) return null;
  const year = dated ? Number(dated[1]) : 2000;
  const month = dated ? Number(dated[2]) : 1;
  const day = dated ? Number(dated[3]) : 1;
  const hour = Number(time[1]),
    minute = Number(time[2]),
    second = Number(time[3] ?? 0);
  if (month < 1 || month > 12 || day < 1 || day > 31 || hour > 24 || minute > 59 || second > 59)
    return null;
  const timezone = time[5] ?? "Z";
  const zoneHour = timezone.toUpperCase() === "Z" ? 0 : Number(timezone.slice(1, 3));
  const zoneMinute = timezone.toUpperCase() === "Z" ? 0 : Number(timezone.slice(4, 6));
  if (zoneHour > 14 || zoneMinute > 59) return null;
  const offset = (timezone.startsWith("-") ? -1 : 1) * (zoneHour * 60 + zoneMinute);
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(
    hour,
    minute - offset,
    second,
    Math.min(999, Math.round(Number(`0.${time[4] ?? 0}`) * 1000))
  );
  if (offset !== 0) return format(date);
  // Source keeps HMS verbatim for zero-offset dates, while normalizing overflowing day values.
  if (hour === 24) {
    if (!format(date)) return null;
    date.setUTCHours(0, minute, second, 0);
    date.setUTCDate(date.getUTCDate() - 1);
    return format(date, hour);
  }
  return format(date);
}
