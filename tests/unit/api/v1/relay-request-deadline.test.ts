import assert from "node:assert/strict";
import test from "node:test";
import * as zod from "zod";
import { finalizeReadableStream } from "../../../../src/app/api/v1/relay/chat/completions/streamFinalizer.ts";
import { controlledOwnerModule } from "../../../helpers/persistence/controlledOwnerModule.ts";

const DEADLINE = Symbol.for("omniroute.request-ingress-deadline.v1");

function deadlineError(): Error {
  const error = new Error("Gateway request exceeded its configured ingress deadline");
  Object.defineProperty(error, DEADLINE, { value: true });
  return error;
}

function deadlinePorts(signal: AbortSignal, promote = () => {}) {
  return {
    getRequestLifecycleSignal: () => signal,
    isIngressRequestDeadline: (error: unknown) =>
      error instanceof Error && (error as unknown as Record<symbol, unknown>)[DEADLINE] === true,
    promoteRequestToStreaming: promote,
    throwIfRequestCancelled: (activeSignal?: AbortSignal) => activeSignal?.throwIfAborted(),
  };
}

function makeRequest(path: string) {
  return new Request(`http://localhost${path}`, {
    method: "POST",
    headers: {
      authorization: "Bearer relay-token",
      "content-type": "application/json",
      "x-request-id": "relay-deadline-test",
    },
    body: JSON.stringify({
      model: "openai/gpt-4.1",
      messages: [{ role: "user", content: "hello" }],
    }),
  });
}

async function makeMainRelayRoute(
  signal: AbortSignal,
  fetchImpl: typeof fetch,
  beforeTokenLookup: () => Promise<void> = async () => {}
) {
  const counts = { fetch: 0, fallback: 0, failures: 0, cleared: 0, usage: 0, promotions: 0 };
  const ports = {
    "@/shared/utils/cors": {
      CORS_HEADERS: { "Access-Control-Allow-Origin": "*" },
      handleCorsOptions: async () => new Response(null, { status: 204 }),
    },
    "@omniroute/open-sse/utils/upstreamResponseHeaders": {
      stripSensitiveResponseHeaders: (headers: Headers) => new Headers(headers),
    },
    "@/lib/requestDeadline": deadlinePorts(signal, () => counts.promotions++),
    "@/sse/handlers/chat": {
      handleChat: async () => {
        counts.fallback++;
        return new Response("fallback", { status: 200 });
      },
    },
    "@/shared/middleware/withChatAdmission": { withChatAdmission: (handler: unknown) => handler },
    "@/middleware/promptInjectionGuard": {
      createInjectionGuard: () => () => ({ blocked: false, result: { detections: [] } }),
    },
    "@/lib/db/relayProxies": {
      getRelayTokenByHashAsync: async () => {
        await beforeTokenLookup();
        return {
          id: "relay-id",
          tokenPrefix: "rl_test",
          allowedModels: '["*"]',
          expiresAt: null,
        };
      },
      checkRateLimitAsync: async () => ({ allowed: true, resetIn: 0 }),
      recordRelayUsageAsync: async () => {
        counts.usage++;
      },
    },
    "@omniroute/open-sse/utils/error": {
      buildErrorBody: (status: number, message: string) => ({ error: { code: status, message } }),
      parseUpstreamError: async () => ({ statusCode: 502, message: "upstream error" }),
      sanitizeErrorMessage: (message: string) => message,
    },
    "@omniroute/open-sse/config/providerPluginManifestRegistry.ts": {
      getProviderPluginManifestEntryForModel: () => null,
    },
    "@omniroute/open-sse/config/providerPluginManifestUrl.ts": {
      getProviderPluginManifestHeader: () => ({}),
    },
    "@omniroute/open-sse/utils/upstreamResponseHeaders.ts": {
      stripStaleEncodingHeaders: (headers: Headers) => new Headers(headers),
    },
    "./relaySecurity": {
      checkIpRateLimit: () => ({ allowed: true, resetIn: 0 }),
      extractToken: (request: Request) =>
        request.headers.get("authorization")?.replace(/^Bearer /, "") ?? null,
      getClientIp: () => "127.0.0.1",
      hashToken: (token: string) => token,
      sanitizeForensicHeader: (value: string | null) => value,
    },
    "./routingBackend": {
      getBifrostRoutingConfig: () => ({
        baseUrl: "http://bifrost.test",
        timeoutMs: 1000,
        streamingEnabled: true,
        enabled: true,
      }),
      getRoutingFallbackHeader: () => "bifrost",
      getRoutingFallbackReasonHeader: (reason: string | null) => reason ?? undefined,
      resolveRelayRoutingBackend: () => "auto",
      shouldTryBifrostForRequest: () => ({ tryBifrost: true }),
    },
    "./streamFinalizer": { finalizeReadableStream },
    "./bifrostCooldown": {
      clearBifrostFailure: () => {
        counts.cleared++;
      },
      getActiveBifrostCooldown: () => null,
      recordBifrostFailure: () => {
        counts.failures++;
      },
    },
  };
  const route = await controlledOwnerModule<{ POST(request: Request): Promise<Response> }>(
    new URL("../../../../src/app/api/v1/relay/chat/completions/route.ts", import.meta.url),
    ports,
    {
      fetch: async (...args: Parameters<typeof fetch>) => {
        counts.fetch++;
        return fetchImpl(...args);
      },
      Request,
      Response,
      Headers,
      AbortController,
      AbortSignal,
      URL,
      setTimeout,
      clearTimeout,
    }
  );
  return { route, counts };
}

async function makeDirectBifrostRoute(signal: AbortSignal, fetchImpl: typeof fetch) {
  const counts = { fetch: 0, usage: 0, promotions: 0 };
  const ports = {
    zod: { z: zod.z },
    "@/shared/utils/cors": {
      CORS_HEADERS: { "Access-Control-Allow-Origin": "*" },
      handleCorsOptions: async () => new Response(null, { status: 204 }),
    },
    "@omniroute/open-sse/utils/upstreamResponseHeaders": {
      stripSensitiveResponseHeaders: (headers: Headers) => new Headers(headers),
    },
    "@/lib/requestDeadline": deadlinePorts(signal, () => counts.promotions++),
    "@/middleware/promptInjectionGuard": {
      createInjectionGuard: () => () => ({ blocked: false, result: { detections: [] } }),
    },
    "@/lib/db/relayProxies": {
      getRelayTokenByHashAsync: async () => ({
        id: "relay-id",
        tokenPrefix: "rl_test",
        allowedModels: '["*"]',
        expiresAt: null,
      }),
      checkRateLimitAsync: async () => ({ allowed: true, resetIn: 0 }),
      recordRelayUsageAsync: async () => {
        counts.usage++;
      },
    },
    "@omniroute/open-sse/utils/error": {
      buildErrorBody: (status: number, message: string) => ({ error: { code: status, message } }),
      parseUpstreamError: async () => ({ statusCode: 502, message: "upstream error" }),
      sanitizeErrorMessage: (message: string) => message,
    },
    "@omniroute/open-sse/config/providerPluginManifestUrl.ts": {
      getProviderPluginManifestHeader: () => ({}),
    },
    "../relaySecurity": {
      checkIpRateLimit: () => ({ allowed: true, resetIn: 0 }),
      extractToken: (request: Request) =>
        request.headers.get("authorization")?.replace(/^Bearer /, "") ?? null,
      getClientIp: () => "127.0.0.1",
      hashToken: (token: string) => token,
      sanitizeForensicHeader: (value: string | null) => value,
    },
    "../streamFinalizer": {
      finalizeReadableStream,
    },
  };
  const route = await controlledOwnerModule<{ POST(request: Request): Promise<Response> }>(
    new URL("../../../../src/app/api/v1/relay/chat/completions/bifrost/route.ts", import.meta.url),
    ports,
    {
      process: {
        env: {
          BIFROST_BASE_URL: "http://bifrost.test",
          BIFROST_TIMEOUT_MS: "1000",
          BIFROST_STREAMING_ENABLED: "1",
          BIFROST_ENABLED: "1",
        },
      },
      fetch: async (...args: Parameters<typeof fetch>) => {
        counts.fetch++;
        return fetchImpl(...args);
      },
      Request,
      Response,
      Headers,
      AbortController,
      AbortSignal,
      URL,
      setTimeout,
      clearTimeout,
    }
  );
  return { route, counts };
}

function requestWithBifrostBody(stream: boolean) {
  return new Request("http://localhost/api/v1/relay/chat/completions/bifrost", {
    method: "POST",
    headers: {
      authorization: "Bearer relay-token",
      "content-type": "application/json",
      "x-request-id": "relay-deadline-test",
    },
    body: JSON.stringify({
      model: "openai/gpt-4.1",
      stream,
      messages: [{ role: "user", content: "hello" }],
    }),
  });
}

test(
  "deadline expiring during relay token lookup prevents Bifrost and internal fallback",
  { timeout: 5000 },
  async () => {
    const lifecycle = new AbortController();
    const { route, counts } = await makeMainRelayRoute(
      lifecycle.signal,
      async () => {
        throw new Error("fetch must not run after deadline");
      },
      () => new Promise<void>((resolve) => setTimeout(resolve, 25))
    );

    const timer = setTimeout(() => lifecycle.abort(deadlineError()), 5);
    const response = await route.POST(makeRequest("/api/v1/relay/chat/completions"));
    clearTimeout(timer);
    assert.equal(response.status, 504);
    assert.equal(counts.fetch, 0);
    assert.equal(counts.fallback, 0);
    assert.equal(counts.failures, 0);
    assert.equal(counts.promotions, 0);
  }
);

test(
  "local deadline cancels an active Bifrost fetch without fallback or failure mutation",
  { timeout: 5000 },
  async () => {
    const lifecycle = new AbortController();
    let fetchStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      fetchStarted = resolve;
    });
    let observedSignal: AbortSignal | undefined;
    const { route, counts } = await makeMainRelayRoute(lifecycle.signal, async (_input, init) => {
      observedSignal = init?.signal as AbortSignal;
      fetchStarted();
      return new Promise<Response>((_resolve, reject) => {
        observedSignal?.addEventListener("abort", () => reject(observedSignal?.reason), {
          once: true,
        });
      });
    });

    const request = route.POST(makeRequest("/api/v1/relay/chat/completions"));
    await started;
    lifecycle.abort(deadlineError());
    const response = await request;

    assert.equal(response.status, 504);
    assert.equal(observedSignal?.aborted, true);
    assert.equal(counts.fetch, 1);
    assert.equal(counts.fallback, 0);
    assert.equal(counts.failures, 0);
  }
);

test(
  "a genuine Bifrost network error keeps the existing automatic fallback",
  { timeout: 5000 },
  async () => {
    const lifecycle = new AbortController();
    const { route, counts } = await makeMainRelayRoute(lifecycle.signal, async () => {
      throw new Error("socket reset");
    });

    const response = await route.POST(makeRequest("/api/v1/relay/chat/completions"));
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("X-Routing-Fallback-Reason"), "bifrost-error");
    assert.equal(counts.fetch, 1);
    assert.equal(counts.fallback, 1);
    assert.equal(counts.failures, 1);
  }
);

test(
  "the direct Bifrost route returns local 504 without advertising a TS retry",
  { timeout: 5000 },
  async () => {
    const lifecycle = new AbortController();
    let fetchStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      fetchStarted = resolve;
    });
    let observedSignal: AbortSignal | undefined;
    const { route, counts } = await makeDirectBifrostRoute(
      lifecycle.signal,
      async (_input, init) => {
        observedSignal = init?.signal as AbortSignal;
        fetchStarted();
        return new Promise<Response>((_resolve, reject) => {
          observedSignal?.addEventListener("abort", () => reject(observedSignal?.reason), {
            once: true,
          });
        });
      }
    );

    const request = route.POST(requestWithBifrostBody(false));
    await started;
    lifecycle.abort(deadlineError());
    const response = await request;

    assert.equal(response.status, 504);
    assert.equal(observedSignal?.aborted, true);
    assert.equal(counts.promotions, 0);
    assert.equal(response.headers.get("X-Bifrost-Fallback"), null);
    assert.equal(counts.fetch, 1);
    assert.equal(counts.usage, 0);
  }
);

test(
  "the direct Bifrost route does not dispatch after its preflight deadline",
  { timeout: 5000 },
  async () => {
    const lifecycle = new AbortController();
    lifecycle.abort(deadlineError());
    const { route, counts } = await makeDirectBifrostRoute(lifecycle.signal, async () => {
      throw new Error("fetch must not run after deadline");
    });

    const response = await route.POST(requestWithBifrostBody(false));
    assert.equal(response.status, 504);
    assert.equal(response.headers.get("X-Bifrost-Fallback"), null);
    assert.equal(counts.fetch, 0);
    assert.equal(counts.usage, 0);
    assert.equal(counts.promotions, 0);
  }
);

test(
  "the direct Bifrost route promotes only explicit client streaming requests",
  { timeout: 5000 },
  async () => {
    const lifecycle = new AbortController();
    const { route, counts } = await makeDirectBifrostRoute(
      lifecycle.signal,
      async () => new Response('{"ok":true}', { status: 200 })
    );

    const response = await route.POST(requestWithBifrostBody(true));
    assert.equal(response.status, 200);
    assert.equal(await response.text(), '{"ok":true}');
    assert.equal(counts.promotions, 1);
  }
);
