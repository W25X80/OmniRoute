import { getSelectedControlPlane } from "../../../src/lib/db/controlPlaneContext.ts";
import type { translateRequest as sourceTranslateRequest } from "../../../open-sse/translator/index.ts";

type TranslationArgs = Parameters<typeof sourceTranslateRequest>;
export const translationFixture = {
  syncCalls: [] as TranslationArgs[],
  asyncCalls: [] as TranslationArgs[],
  entered: undefined as (() => void) | undefined,
  pending: undefined as Promise<void> | undefined,
  failure: undefined as Error | undefined,
  reset() {
    this.syncCalls = [];
    this.asyncCalls = [];
    this.entered = undefined;
    this.pending = undefined;
    this.failure = undefined;
  },
};
function translated(args: TranslationArgs): Record<string, unknown> {
  return {
    ...(args[3] as Record<string, unknown>),
    translated: true,
    thinking: { type: "adaptive", budget_tokens: 1234 },
  };
}
export function translateRequest(...args: TranslationArgs) {
  translationFixture.syncCalls.push(args);
  return translated(args);
}
export async function translateRequestAsync(...args: TranslationArgs) {
  translationFixture.asyncCalls.push(args);
  getSelectedControlPlane()?.assertReady();
  translationFixture.entered?.();
  if (translationFixture.pending) await translationFixture.pending;
  getSelectedControlPlane()?.assertReady();
  if (translationFixture.failure) throw translationFixture.failure;
  return translated(args);
}
