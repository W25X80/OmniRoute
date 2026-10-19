import * as fallback from "../../open-sse/services/accountFallback.ts";
export * from "../../open-sse/services/accountFallback.ts";

export const concurrencyFallbackCalls: ReturnType<typeof fallback.checkFallbackError>[] = [];
export const checkFallbackError: typeof fallback.checkFallbackError = (...args) => {
  const result = fallback.checkFallbackError(...args);
  concurrencyFallbackCalls.push(result);
  return result;
};
