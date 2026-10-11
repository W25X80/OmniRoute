import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";
import { applicationImportDigest } from "../../../src/lib/db/postgres/applicationImportDigest.ts";

// Independent oracle bytes, ordered manually rather than by the implementation.
const canonicalAuthority =
  '{"sourceAuthority":"947b9b14-7366-47d3-ac5d-0450a5d83abd","sourceGeneration":"37","targetAuthority":"91ae17e6-f411-4c61-8a17-7538d11fb860","targetGeneration":"0"}';
const original = {
  sourceAuthority: "947b9b14-7366-47d3-ac5d-0450a5d83abd",
  sourceGeneration: "37",
  targetAuthority: "91ae17e6-f411-4c61-8a17-7538d11fb860",
  targetGeneration: "0",
};
const jsonbOrder = {
  sourceAuthority: original.sourceAuthority,
  targetAuthority: original.targetAuthority,
  sourceGeneration: original.sourceGeneration,
  targetGeneration: original.targetGeneration,
};

test("restore authority digest agrees with independently specified canonical bytes after JSONB ordering", () => {
  assert.notEqual(JSON.stringify(original), JSON.stringify(jsonbOrder));
  const oracle = createHash("sha256").update(canonicalAuthority).digest("hex");
  assert.equal(applicationImportDigest(original), oracle);
  assert.equal(applicationImportDigest(jsonbOrder), oracle);
});

test("nested receipt artifacts and cells ignore object key order while preserving every value and array order", () => {
  const first = {
    restoreAuthority: original,
    artifacts: [{ sourceReference: "old", targetReference: "new", sizeBytes: 3, checksum: "abc" }],
    rows: [
      [{ kind: "integer", value: "2" }, "first"],
      [null, "second"],
    ],
  };
  const second = {
    rows: [
      [{ value: "2", kind: "integer" }, "first"],
      [null, "second"],
    ],
    artifacts: [{ checksum: "abc", sizeBytes: 3, targetReference: "new", sourceReference: "old" }],
    restoreAuthority: jsonbOrder,
  };
  assert.equal(applicationImportDigest(first), applicationImportDigest(second));
  assert.notEqual(
    applicationImportDigest(first),
    applicationImportDigest({ ...second, rows: [...second.rows].reverse() })
  );
  assert.notEqual(
    applicationImportDigest(first),
    applicationImportDigest({
      ...second,
      restoreAuthority: { ...jsonbOrder, targetGeneration: "1" },
    })
  );
  assert.equal(
    applicationImportDigest({ optional: undefined, value: [undefined, null] }),
    applicationImportDigest({ value: [null, null] })
  );
});

test("digest rejects non-JSON and cyclic input without ambiguous coercion", () => {
  assert.throws(() => applicationImportDigest({ value: Infinity }), TypeError);
  assert.throws(() => applicationImportDigest({ value: 1n }), TypeError);
  assert.throws(() => applicationImportDigest(new Date()), TypeError);
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  assert.throws(() => applicationImportDigest(cycle), TypeError);
});
