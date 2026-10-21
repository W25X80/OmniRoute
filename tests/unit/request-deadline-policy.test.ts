import assert from "node:assert/strict";
import test from "node:test";
import { checkFallbackError } from "../../open-sse/services/accountFallback.ts";
import { buildErrorBody } from "../../open-sse/utils/error.ts";
import {
  isRequestScopedUpstreamFailure,
  shouldRecordModelLockoutForComboFailure,
} from "../../open-sse/services/combo/comboPredicates.ts";
import { shouldTripProviderBreakerForResult } from "../../src/sse/handlers/chatPredicates.ts";
import {
  INGRESS_REQUEST_DEADLINE_CODE,
  isIngressRequestDeadline,
} from "../../open-sse/utils/requestDeadline.ts";

test("a provider-supplied ingress error spelling cannot bypass normal upstream failure policy", () => {
  const structured = { code: INGRESS_REQUEST_DEADLINE_CODE, type: "request_deadline" };
  const fallback = checkFallbackError(
    504,
    "configured ingress deadline",
    0,
    "model",
    "claude",
    null,
    null,
    structured
  );
  assert.equal(fallback.shouldFallback, true);
  assert.ok(fallback.cooldownMs > 0);
  assert.notEqual(fallback.skipProviderBreaker, true);
  const publicError = buildErrorBody(504, "Configured ingress deadline", undefined, structured);
  assert.equal(publicError.error.code, structured.code);
  assert.equal(publicError.error.type, structured.type);
  assert.equal(isRequestScopedUpstreamFailure(structured), false);
  assert.equal(
    shouldRecordModelLockoutForComboFailure(isRequestScopedUpstreamFailure(structured), structured),
    true
  );
  assert.equal(
    shouldTripProviderBreakerForResult(
      {
        status: 504,
        errorCode: structured.code,
        errorType: structured.type,
        error: "configured ingress deadline",
      },
      false,
      false
    ),
    true
  );
});

test("a matching upstream error spelling cannot manufacture trusted local provenance", () => {
  const error = Object.assign(new Error("configured ingress deadline"), {
    name: "TimeoutError",
    code: INGRESS_REQUEST_DEADLINE_CODE,
  });
  assert.equal(isIngressRequestDeadline(error), false);
});

test("a genuine upstream timeout keeps the existing provider failure classification", () => {
  assert.equal(
    shouldTripProviderBreakerForResult(
      {
        status: 504,
        errorCode: "upstream_timeout",
        errorType: "upstream_timeout",
        error: "Upstream response timed out",
      },
      false,
      false
    ),
    true
  );
});
