/**
 * MCP HTTP Transport Layer — session-aware handlers for SSE and Streamable HTTP.
 *
 * Runs the MCP server **inside** the Next.js process so it can be toggled
 * from the dashboard without requiring `omniroute --mcp`.
 *
 * Transport modes:
 *   - SSE:             GET /api/mcp/sse (event stream)  +  POST /api/mcp/sse (messages)
 *   - Streamable HTTP: POST /api/mcp/stream (messages)  +  GET /api/mcp/stream (SSE stream)  +  DELETE /api/mcp/stream (session end)
 */

import { randomUUID } from "node:crypto";
import { createMcpServerAsync } from "./server.ts";
import { resolveMcpCallerAuthInfo, withMcpHttpAuthContext } from "./httpAuthContext.ts";
import { sanitizeErrorMessage } from "../utils/error.ts";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/webStandardStreamableHttp.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

let _sseServer: McpServer | null = null;
let _sseTransport: WebStandardStreamableHTTPServerTransport | null = null;
let _sseStartedAt: number | null = null;
type SseServer = {
  server: McpServer;
  transport: WebStandardStreamableHTTPServerTransport;
};
let _sseInitialization: Promise<SseServer> | null = null;
let _sseGeneration = 0;

type StreamableSession = {
  sessionId: string;
  server: McpServer;
  transport: WebStandardStreamableHTTPServerTransport;
  startedAt: number;
  lastActivityAt: number;
};

const _streamableSessions = new Map<string, StreamableSession>();

const MCP_SESSION_IDLE_MS = 5 * 60 * 1000;

const _mcpSessionSweep = setInterval(() => {
  const now = Date.now();
  for (const [sessionId, session] of _streamableSessions) {
    if (now - session.lastActivityAt > MCP_SESSION_IDLE_MS) {
      try {
        closeStreamableSession(sessionId);
      } catch {}
    }
  }
}, 60_000);
if (typeof _mcpSessionSweep === "object" && "unref" in _mcpSessionSweep) {
  (_mcpSessionSweep as { unref?: () => void }).unref?.();
}

function closeSseTransport(): void {
  _sseGeneration++;
  _sseInitialization = null;
  if (_sseTransport) {
    try {
      _sseTransport.close();
    } catch {
      // ignore shutdown errors
    }
  }
  _sseServer = null;
  _sseTransport = null;
  _sseStartedAt = null;
}

function closeStreamableSession(sessionId: string): void {
  const session = _streamableSessions.get(sessionId);
  if (!session) {
    return;
  }

  try {
    session.transport.close();
  } catch {
    // ignore shutdown errors
  }
  _streamableSessions.delete(sessionId);
}

function closeAllStreamableSessions(): void {
  for (const sessionId of _streamableSessions.keys()) {
    closeStreamableSession(sessionId);
  }
}

async function ensureSseServer(): Promise<SseServer> {
  if (_sseServer && _sseTransport) return { server: _sseServer, transport: _sseTransport };
  if (_sseInitialization) return _sseInitialization;
  closeAllStreamableSessions();
  const generation = _sseGeneration;
  const initialization = (async () => {
    const server = await createMcpServerAsync();
    const transport = new WebStandardStreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
    });
    try {
      await server.connect(transport);
      if (generation !== _sseGeneration) {
        throw new Error("MCP transport initialization was superseded");
      }
      _sseServer = server;
      _sseTransport = transport;
      _sseStartedAt = Date.now();
      console.log("[MCP] HTTP transport started (sse)");
      return { server, transport };
    } catch (error) {
      await server.close().catch(() => {});
      throw error;
    }
  })();
  _sseInitialization = initialization;
  try {
    return await initialization;
  } finally {
    if (_sseInitialization === initialization) _sseInitialization = null;
  }
}

async function createStreamableSession(): Promise<StreamableSession> {
  closeSseTransport();

  const sessionId = randomUUID();
  const server = await createMcpServerAsync();
  const transport = new WebStandardStreamableHTTPServerTransport({
    sessionIdGenerator: () => sessionId,
  });
  const session = {
    sessionId,
    server,
    transport,
    startedAt: Date.now(),
    lastActivityAt: Date.now(),
  };

  await server.connect(transport);
  _streamableSessions.set(sessionId, session);
  console.log(`[MCP] HTTP transport started (streamable-http:${sessionId})`);
  return session;
}

async function isInitializeRequest(request: Request): Promise<boolean> {
  if (request.method !== "POST") {
    return false;
  }

  try {
    const body = (await request.clone().json()) as { method?: unknown };
    return body?.method === "initialize";
  } catch {
    return false;
  }
}

/**
 * Resolve the caller's per-key scopes (#7895) and hand the request to the
 * transport with `authInfo` populated, so `extra.authInfo.scopes` reaching
 * tool handlers reflects the real `api_keys.scopes` row instead of the
 * `OMNIROUTE_MCP_SCOPES` env fallback. When no per-key auth can be resolved
 * (no key, invalid key, stdio has no `Request` at all), `authInfo` stays
 * `undefined` and `scopeEnforcement.ts` falls through to its existing
 * meta/env chain unchanged.
 */
async function handleRequestWithAuthInfo(
  transport: WebStandardStreamableHTTPServerTransport,
  request: Request
): Promise<Response> {
  const authInfo = await resolveMcpCallerAuthInfo(request);
  return transport.handleRequest(request, { authInfo });
}

/**
 * JSON-RPC 2.0 transport-level error envelope.
 *
 * S-05 (#15159): this is NOT the canonical `errorResponse` from
 * `open-sse/utils/error.ts` — it is a local builder for the JSON-RPC shape that MCP
 * clients parse, so it cannot be replaced by the canonical helper without breaking
 * the protocol. It did, however, interpolate `message` with no sanitization.
 *
 * Both current call sites pass static literals, so nothing leaks today — but the
 * next caller forwarding an `err.message` would put it on the wire unredacted,
 * reintroducing S-02's leak on the JSON-RPC surface. Sanitizing here makes the
 * builder safe by default rather than safe by caller discipline.
 */
function errorResponse(message: string, code: number, status = 400): Response {
  return new Response(
    JSON.stringify({
      jsonrpc: "2.0",
      error: { code, message: sanitizeErrorMessage(message) },
      id: null,
    }),
    {
      status,
      headers: { "Content-Type": "application/json" },
    }
  );
}

export function protectMcpSseResponse(request: Request, response: Response): Response {
  if (
    request.method !== "POST" ||
    !response.headers.get("content-type")?.toLowerCase().includes("text/event-stream")
  ) {
    return response;
  }

  const headers = new Headers(response.headers);
  const cacheControl = headers.get("cache-control");
  if (!/(?:^|,)\s*no-transform(?:\s*(?:,|$))/i.test(cacheControl ?? "")) {
    headers.set("cache-control", [cacheControl, "no-transform"].filter(Boolean).join(", "));
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function withSessionHeader(response: Response, sessionId: string): Response {
  if (response.headers.get("mcp-session-id")) {
    return response;
  }

  const headers = new Headers(response.headers);
  headers.set("mcp-session-id", sessionId);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

async function handleStreamableRequest(request: Request): Promise<Response> {
  const sessionId = request.headers.get("mcp-session-id");

  if (sessionId) {
    const session = _streamableSessions.get(sessionId);
    if (!session) {
      // MCP spec (2025-03-26 / 2025-11-25, Session Management): once a session is
      // terminated/unknown, the server MUST respond with HTTP 404 Not Found so the
      // client re-initializes. A 400 here is non-recoverable for spec-compliant
      // clients (they only re-init on 404). See issue #5169.
      //
      // Auto-recovery: if the client sends an initialize request with a stale session
      // id (e.g. after a server restart or idle eviction), treat it as a fresh
      // initialization rather than hard-failing with 404. This avoids requiring users
      // to manually restart their MCP client after every server restart.
      if (await isInitializeRequest(request)) {
        let newSession: StreamableSession | undefined;
        try {
          const activeSession = await createStreamableSession();
          newSession = activeSession;
          const response = await withMcpHttpAuthContext(request, () =>
            handleRequestWithAuthInfo(activeSession.transport, request)
          );
          return withSessionHeader(response, newSession.sessionId);
        } catch (err) {
          if (newSession) closeStreamableSession(newSession.sessionId);
          console.error("[MCP] Streamable HTTP error during stale-session recovery:", err);
          return new Response(JSON.stringify({ error: "MCP transport error" }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
          });
        }
      }
      return errorResponse("Not Found: Unknown Mcp-Session-Id header", -32000, 404);
    }

    try {
      session.lastActivityAt = Date.now();
      const response = await withMcpHttpAuthContext(request, () =>
        handleRequestWithAuthInfo(session.transport, request)
      );
      if (request.method === "DELETE") {
        closeStreamableSession(sessionId);
      }
      return withSessionHeader(response, sessionId);
    } catch (err) {
      console.error("[MCP] Streamable HTTP error:", err);
      if (request.method === "DELETE") {
        closeStreamableSession(sessionId);
      }
      return new Response(JSON.stringify({ error: "MCP transport error" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  }

  if (!(await isInitializeRequest(request))) {
    return errorResponse("Bad Request: Mcp-Session-Id header is required", -32000);
  }

  let session: StreamableSession | undefined;
  try {
    const activeSession = await createStreamableSession();
    session = activeSession;
    const response = await withMcpHttpAuthContext(request, () =>
      handleRequestWithAuthInfo(activeSession.transport, request)
    );
    return withSessionHeader(response, session.sessionId);
  } catch (err) {
    if (session) closeStreamableSession(session.sessionId);
    console.error("[MCP] Streamable HTTP error:", err);
    return new Response(JSON.stringify({ error: "MCP transport error" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}

/**
 * Handle Streamable HTTP requests (POST / GET / DELETE).
 * Used by the Next.js route at /api/mcp/stream.
 */
export async function handleMcpStreamableHTTP(request: Request): Promise<Response> {
  return protectMcpSseResponse(request, await handleStreamableRequest(request));
}

interface RpcRequest {
  method?: string;
  [key: string]: unknown;
}

/**
 * Handle SSE requests.
 * SSE transport is implemented via Streamable HTTP transport with GET for SSE stream
 * and POST for messages (the Streamable HTTP transport supports both patterns).
 */
export async function handleMcpSSE(request: Request): Promise<Response> {
  if (request.method === "POST") {
    try {
      const body = await request.clone().json();
      const isInitialize = Array.isArray(body)
        ? body.some((req: RpcRequest) => req?.method === "initialize")
        : (body as RpcRequest)?.method === "initialize";

      if (isInitialize) {
        console.log("[MCP] New client initialize detected, resetting SSE singleton...");
        closeSseTransport();
      }
    } catch (err) {}
  }
  try {
    const { transport } = await ensureSseServer();
    const response = await withMcpHttpAuthContext(request, () =>
      handleRequestWithAuthInfo(transport, request)
    );
    return protectMcpSseResponse(request, response);
  } catch (err) {
    console.error("[MCP] SSE error:", err);
    return new Response(JSON.stringify({ error: "MCP SSE transport error" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}

export function getMcpHttpStatus(): {
  online: boolean;
  transport: string | null;
  startedAt: number | null;
  uptime: string | null;
} {
  const streamableStartedAt =
    _streamableSessions.size > 0
      ? Math.min(...Array.from(_streamableSessions.values(), (session) => session.startedAt))
      : null;
  const startedAt = streamableStartedAt ?? _sseStartedAt;
  const transport = _streamableSessions.size > 0 ? "streamable-http" : _sseTransport ? "sse" : null;
  const online = transport !== null;

  return {
    online,
    transport,
    startedAt,
    uptime: startedAt ? `${Math.floor((Date.now() - startedAt) / 1000)}s` : null,
  };
}

export function isMcpHttpTransportReady(
  enabled: boolean,
  transport: string | null | undefined
): boolean {
  return enabled && (transport === "sse" || transport === "streamable-http");
}

export function shutdownMcpHttp(): void {
  closeSseTransport();
  closeAllStreamableSessions();
  console.log("[MCP] HTTP transport shutdown");
}

export function isMcpHttpActive(): boolean {
  return _sseTransport !== null || _streamableSessions.size > 0;
}
