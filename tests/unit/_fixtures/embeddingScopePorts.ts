import { getProviderCredentials as selectCredentials } from "../../../src/sse/services/scopedCredentials.ts";

export const embeddingScopePorts = {
  diagnostic: null as Record<string, unknown> | null,
  selected: [] as Array<string | null>,
  calls: [] as Array<{ url: string; headers: Headers; body: Record<string, unknown> }>,
};

/** Only diagnostic cases replace this port; ordinary cases use the real selector/repository. */
export async function getProviderCredentials(...args: Parameters<typeof selectCredentials>) {
  if (embeddingScopePorts.diagnostic) {
    return embeddingScopePorts.diagnostic as Awaited<ReturnType<typeof selectCredentials>>;
  }
  const result = await selectCredentials(...args);
  embeddingScopePorts.selected.push(
    result && "connectionId" in result ? result.connectionId : null
  );
  return result;
}

/** Lexical binding in the actual handler; never changes globalThis.fetch or uses the network. */
export async function embeddingFetch(input: string | URL | Request, init: RequestInit = {}) {
  embeddingScopePorts.calls.push({
    url: String(input),
    headers: new Headers(init.headers),
    body: JSON.parse(String(init.body)) as Record<string, unknown>,
  });
  return new Response(
    JSON.stringify({
      object: "list",
      data: [{ object: "embedding", embedding: [0.1, 0.2], index: 0 }],
      usage: { prompt_tokens: 2, total_tokens: 2 },
    }),
    { status: 200, headers: { "content-type": "application/json" } }
  );
}
