export type ProviderCredentials = {
  accessToken?: string;
  refreshToken?: string;
  apiKey?: string;
  email?: string | null;
  projectId?: string | null;
  expiresAt?: string;
  connectionId?: string; // T07: used for API key rotation index
  maxConcurrent?: number | null;
  /**
   * Optional per-model concurrency ceilings for this connection (see
   * ProviderCredentials in open-sse/types.d.ts). Normalized at credential
   * selection; the chat core resolves the exact-model cap fail-open.
   */
  modelConcurrency?: Record<string, number> | null;
  rateLimitMaxConcurrent?: number | null;
  providerSpecificData?: Record<string, unknown>;
  requestEndpointPath?: string;
};

/** Trusted request-local observer, supplied only by strict model validation. */
export type StrictValidationDispatch = {
  beforeFetch(details: {
    provider: string;
    model: string;
    credentials: ProviderCredentials;
    url: string;
    headers: HeadersInit | undefined;
    body: BodyInit | null | undefined;
  }): void | Promise<void>;
  reject(): never;
};

export function assertValidationCredentials(
  observer: StrictValidationDispatch | undefined,
  credentials: ProviderCredentials
): void {
  if (!observer) return;
  const extraKeys = credentials.providerSpecificData?.extraApiKeys;
  if (extraKeys == null) return;
  // Reject rotation before resolveEffectiveKey mutates selectedKeyId or
  // consults the shared key-health state. Malformed key lists fail closed.
  if (!Array.isArray(extraKeys) || extraKeys.length > 0) observer.reject();
}

async function prepareValidationFetch(
  observer: StrictValidationDispatch | undefined,
  details: Pick<
    Parameters<StrictValidationDispatch["beforeFetch"]>[0],
    "provider" | "model" | "credentials" | "url"
  >,
  options: RequestInit
): Promise<RequestInit> {
  if (!observer) return options;
  options.signal?.throwIfAborted();
  await observer.beforeFetch({ ...details, headers: options.headers, body: options.body });
  // After the awaited native freshness read, cancellation is checked again before transport.
  options.signal?.throwIfAborted();
  // Automatic redirects would perform an unobserved second HTTP dispatch,
  // possibly to a different provider or credential scope.
  return { ...options, redirect: "error" };
}

type FetchTransport = (url: string, options: RequestInit) => Promise<Response>;

/**
 * `fetch` that runs the strict-validation fence first; plain `fetch` when there is no observer.
 * `transport` replaces the global `fetch` for the physical request (e.g. the connect-time
 * guarded dispatch for operator-supplied base URLs, #13330); it must start the request
 * synchronously so nothing awaited separates the fence from the transport.
 */
export function validationFetch(
  observer: StrictValidationDispatch | undefined,
  provider: string,
  model: string,
  credentials: ProviderCredentials,
  transport: FetchTransport = (url, options) => fetch(url, options)
): FetchTransport {
  return async (url, options) =>
    transport(
      url,
      await prepareValidationFetch(observer, { provider, model, credentials, url }, options)
    );
}
