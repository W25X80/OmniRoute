import {
  getSelectedControlPlane,
  getSelectedPersistenceIdentity,
} from "../../../src/lib/db/controlPlaneContext.ts";
import { fixture } from "./runtimeOwnedWorkFixture.ts";
import { hookFixture } from "./reasoningHookFixture.ts";
export * from "./reasoningHookFixture.ts";

export const sequentialFixture = {
  active: 0,
  peak: 0,
  entered: new Map<string, () => void>(),
  pending: new Map<string, Promise<void>>(),
  failures: new Set<string>(),
  warnings: [] as string[],
  reset() {
    this.active = 0;
    this.peak = 0;
    this.entered.clear();
    this.pending.clear();
    this.failures.clear();
    this.warnings = [];
  },
};
export async function setReasoningCacheAsync(
  key: string,
  _provider: string,
  _model: string,
  reasoning: string
) {
  const authority = getSelectedPersistenceIdentity();
  hookFixture.writes.push({ key, reasoning, authority, generation: fixture.selectedGeneration() });
  sequentialFixture.active++;
  sequentialFixture.peak = Math.max(sequentialFixture.peak, sequentialFixture.active);
  try {
    sequentialFixture.entered.get(key)?.();
    if (sequentialFixture.pending.has(key)) await sequentialFixture.pending.get(key);
    getSelectedControlPlane()!.assertReady();
    if (sequentialFixture.failures.has(key)) throw new Error("controlled optional write failure");
  } finally {
    sequentialFixture.active--;
  }
}

export function logger(_tag: string) {
  return {
    warn(message: string) {
      sequentialFixture.warnings.push(message);
    },
  };
}
