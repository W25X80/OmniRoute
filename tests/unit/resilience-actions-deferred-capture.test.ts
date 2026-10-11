import assert from "node:assert/strict";
import test from "node:test";
import {
  captureResilienceActions,
  noteResilienceAction,
  readResilienceActions,
  runWithResilienceActionsContext,
  RESILIENCE_NOTE_CAP,
} from "../../src/lib/usage/resilienceActionsContext.ts";

test("capture is immutable and a failed sink leaves its retry summary", () => {
  runWithResilienceActionsContext(() => {
    noteResilienceAction({ rotations: 2 });
    const captured = captureResilienceActions();
    noteResilienceAction({ continued: 1 });
    assert.equal(captured.serialized, JSON.stringify({ rotations: 2 }));
    assert.deepEqual(readResilienceActions(), { rotations: 2, continued: 1 });
  });
});

test("successful deferred consumption clears the actual unchanged caller store once", () => {
  runWithResilienceActionsContext(() => {
    noteResilienceAction({ resumed: true });
    const first = captureResilienceActions(),
      second = captureResilienceActions();
    first.consume();
    assert.equal(readResilienceActions(), null);
    noteResilienceAction({ resumed: true });
    second.consume();
    assert.deepEqual(readResilienceActions(), { resumed: true });
  });
});

test("notes after capture are retained conservatively rather than erased on success", () => {
  runWithResilienceActionsContext(() => {
    noteResilienceAction({ buffered: "retry", rotations: 1 });
    const captured = captureResilienceActions();
    noteResilienceAction({ buffered: "pass", rotations: 1 });
    captured.consume();
    assert.deepEqual(readResilienceActions(), { rotations: 2, buffered: "pass" });
    const latest = captureResilienceActions();
    latest.consume();
    assert.equal(readResilienceActions(), null);
  });
});

test("dropped notes also revoke consumption and the unchanged consumed cap restarts", () => {
  runWithResilienceActionsContext(() => {
    for (let i = 0; i < RESILIENCE_NOTE_CAP; i++) noteResilienceAction({ rotations: 1 });
    const captured = captureResilienceActions();
    noteResilienceAction({ rotations: 1 });
    captured.consume();
    assert.equal(readResilienceActions()?.dropped_notes, 1);
    captureResilienceActions().consume();
    noteResilienceAction({ rotations: 1 });
    assert.deepEqual(readResilienceActions(), { rotations: 1 });
  });
});

test("empty capture outside an attempt has no effect on another attempt", () => {
  const outside = captureResilienceActions();
  assert.equal(outside.serialized, null);
  runWithResilienceActionsContext(() => {
    noteResilienceAction({ resumed: true });
    outside.consume();
    assert.deepEqual(readResilienceActions(), { resumed: true });
  });
});
