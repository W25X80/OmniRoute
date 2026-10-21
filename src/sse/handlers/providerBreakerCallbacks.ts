import { isIngressRequestDeadline } from "@omniroute/open-sse/utils/requestDeadline";

import { isLocalStreamLifecycleError } from "../../shared/utils/circuitBreaker";
import * as log from "../utils/logger";

// Provider breakers are process-owned. Their callbacks must never close over a
// request handler's scope, including its body and persistence authority.
export function breakerCountsAsFailure(error: unknown): boolean {
  return !isLocalStreamLifecycleError(error) && !isIngressRequestDeadline(error);
}

export function logBreakerStateChange(name: string, from: string, to: string): void {
  log.info("CIRCUIT", `${name}: ${from} → ${to}`);
}
