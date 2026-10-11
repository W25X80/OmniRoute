import assert from "node:assert/strict";
import test from "node:test";
import Database from "better-sqlite3";
import { intelligenceExpiryTimestamp } from "../../src/lib/db/postgres/intelligenceExpiry.ts";

test("native intelligence expiry matches the SQLite datetime domain", (context) => {
  const database = new Database(":memory:");
  try {
    const query = database.prepare("SELECT datetime(?) AS value");
    const values = [
      "2026-01-01",
      "2026-02-30 00:00",
      "2026-02-31 23:00:00",
      "2026-01-01T24:00:00",
      "2026-01-01 24:01:01",
      "2026-01-01T12:00:00.9999Z",
      "2026-01-01 12:00:59.9999",
      " 2026-01-01 ",
      "2026-01-01\t12:00:00",
      "2026-01-01T12:00:00z",
      "2026-01-01T12:00:00+14:59",
      "2461041.5",
      "0",
      "12:34:56",
      "2026-01-01\u00a0",
      "-0001-01-01",
      "9999-12-31 24:00:00",
      "not-a-date",
      "2026-01-01junk",
      "2026-01-00",
      "2026-13-01",
      "2026-01-01 25:00",
      "2026-01-01 00:60",
      "2026-01-01 00:00:60",
      "2026-01-01T12:00:00+15:00",
      "2026-01-01T12:00:00+00:60",
      "2026-01-01 ",
      "2026-01-01TT12:00",
      "2026-01-01T00:00:59.9999+00:01",
      "5373484.4999999",
      "+2461041.5",
      " 2461041.5 ",
      "2.4610415e6",
      "12:34:56.123",
      "12:34+00:00",
    ];
    for (const year of [0, 1, 1900, 2000, 2026, 9999])
      for (const month of [1, 2, 12])
        for (const day of [1, 28, 29, 30, 31])
          for (const zone of ["Z", "+00:00", "-03:30", "+14:59"])
            values.push(
              `${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T23:59:59.123${zone}`
            );
    context.diagnostic(`Compared ${values.length} expiry forms with SQLite datetime`);
    for (const value of values) {
      const expected = (query.get(value) as { value: string | null }).value;
      assert.equal(intelligenceExpiryTimestamp(value), expected, JSON.stringify(value));
    }
  } finally {
    database.close();
  }
});
