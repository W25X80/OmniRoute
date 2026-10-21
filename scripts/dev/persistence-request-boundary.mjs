import { AsyncResource } from "node:async_hooks";
import { wrapRequestListenerWithDeadline } from "./request-deadline.mjs";

export function isSqlitePersistenceRequested(env = process.env) {
  const backend = env.OMNIROUTE_DATABASE_BACKEND ?? "postgres";
  if (backend !== "sqlite" && backend !== "postgres") {
    throw new Error("OMNIROUTE_DATABASE_BACKEND must be sqlite or postgres");
  }
  return backend === "sqlite";
}

function isPureHealthRequest(req) {
  const pathname = (req.url || "").split("?")[0];
  if (pathname === "/healthz") return true;
  // Keep this self-contained in the assembled server, using the existing basePath shape.
  const raw =
    typeof process.env.OMNIROUTE_BASE_PATH === "string"
      ? process.env.OMNIROUTE_BASE_PATH.trim()
      : "";
  if (!raw.startsWith("/") || /[?#\\]/.test(raw)) return false;
  const segments = raw.split("/").filter(Boolean);
  if (segments.some((part) => part === "." || part === "..")) return false;
  return pathname === `/${segments.join("/")}/healthz`;
}

/** EventEmitter invokes callbacks in the emitter's context, not their registration context. */
export function bindPersistenceRequestCallback(callback) {
  const bound = AsyncResource.bind(callback);
  // Keep the returned callback identity when using once() or explicit removal.
  bound.listener = callback;
  return bound;
}

function failUnavailable(res, error) {
  if (res.destroyed || res.writableFinished || res.writableEnded) return;
  if (res.headersSent) {
    res.destroy();
    return;
  }
  const pressure = error?.[Symbol.for("omniroute.persistence.request-capacity")] === true;
  res.writeHead(503, {
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    "Retry-After": pressure ? "5" : "1",
  });
  res.end(
    pressure
      ? '{"error":{"message":"Service temporarily unavailable due to resource pressure. Retry shortly.","type":"server_error","code":"resource_pressure"}}'
      : '{"error":{"message":"Persistence is not ready","type":"server_error","code":"persistence_not_ready"}}'
  );
}

/** Pause only this incoming message; restore normal methods after its first data consumer. */
function holdIncomingBody(req) {
  if (typeof req.pause !== "function" || typeof req.resume !== "function") {
    return { handoff() {}, restore() {} };
  }
  const hadDataConsumer = req.listenerCount("data") > 0;
  const originalOn = req.on;
  const originalAdd = req.addListener;
  const ownOn = Object.hasOwn(req, "on");
  const ownAdd = Object.hasOwn(req, "addListener");
  let restored = false;

  const restore = () => {
    if (restored) return;
    restored = true;
    if (req.on === on) {
      if (ownOn) req.on = originalOn;
      else delete req.on;
    }
    if (req.addListener === add) {
      if (ownAdd) req.addListener = originalAdd;
      else delete req.addListener;
    }
  };

  const register = (original, self, event, callback, rest) => {
    const result = original.call(
      self,
      event,
      event === "data" && typeof callback === "function" && typeof callback.listener !== "function"
        ? bindPersistenceRequestCallback(callback)
        : callback,
      ...rest
    );
    if (event === "data") {
      // Native once wrappers retain their identity and removal semantics.
      // Install before resuming: Next's stream adapter must receive the buffered prefix.
      restore();
      req.resume();
    }
    return result;
  };
  const on = function (event, callback, ...rest) {
    return register(originalOn, this, event, callback, rest);
  };
  const add = function (event, callback, ...rest) {
    return register(originalAdd, this, event, callback, rest);
  };
  req.pause();
  req.on = on;
  req.addListener = add;
  return {
    restore,
    handoff() {
      if (hadDataConsumer) {
        restore();
        req.resume();
      }
    },
  };
}

function httpLifetime(req, res, onSettled) {
  let settled = false;
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  const settle = () => {
    if (settled) return;
    settled = true;
    onSettled();
    resolve();
  };
  // These callbacks perform no persistence work. Register before snapshot admission can await.
  res.once("finish", settle);
  res.once("close", settle);
  res.once("error", settle);
  req.once("aborted", settle);
  req.once("error", settle);
  if (res.destroyed || res.writableFinished || res.writableEnded || req.aborted) settle();
  return {
    promise,
    isSettled: () => settled,
    dispose() {
      res.removeListener("finish", settle);
      res.removeListener("close", settle);
      res.removeListener("error", settle);
      req.removeListener("aborted", settle);
      req.removeListener("error", settle);
    },
  };
}

/** The hook owns handler Promise + HTTP lifetime; detached SSE tails need a separate permit. */
function wrapPersistenceListener(listener, loadedNextConfiguration) {
  const routing = isSqlitePersistenceRequested()
    ? undefined
    : captureNativeFileRouting(loadedNextConfiguration);
  return function persistenceAwareRequestHandler(req, res) {
    const hook = globalThis.__omniroutePersistenceRequest;
    if (isPureHealthRequest(req)) return listener.call(this, req, res);
    if (typeof hook !== "function") {
      if (isSqlitePersistenceRequested()) return listener.call(this, req, res);
      failUnavailable(res);
      return;
    }
    let bodyGate;
    const lifetime = httpLifetime(req, res, () => bodyGate?.restore());
    bodyGate = holdIncomingBody(req);
    if (lifetime.isSettled()) bodyGate.restore();
    const self = this;
    const run = () => {
      if (lifetime.isSettled()) return;
      try {
        return listener.call(self, req, res);
      } finally {
        bodyGate.handoff();
      }
    };
    let owned;
    try {
      if (routing) {
        const file = classifyNativeFileRequest(req.url || "", req.method || "GET", routing);
        owned = hook(
          run,
          lifetime.promise,
          file ? { req, res, ...file, onFailure: () => failUnavailable(res) } : undefined
        );
      } else owned = hook(run, lifetime.promise);
    } catch (error) {
      lifetime.dispose();
      bodyGate.restore();
      failUnavailable(res, error);
      if (error?.[Symbol.for("omniroute.persistence.request-capacity")] !== true)
        console.error("[Persistence] Request boundary failed");
      return;
    }
    return Promise.resolve(owned)
      .catch((error) => {
        failUnavailable(res, error);
        if (error?.[Symbol.for("omniroute.persistence.request-capacity")] !== true)
          console.error("[Persistence] Request boundary failed");
      })
      .finally(() => {
        lifetime.dispose();
        bodyGate.restore();
      });
  };
}

export function wrapRequestListenerWithPersistence(listener, loadedNextConfiguration) {
  return wrapRequestListenerWithDeadline(
    wrapPersistenceListener(listener, loadedNextConfiguration),
    loadedNextConfiguration
  );
}

/** Capture the already loaded Next config; never infer routing from a mutable env alias. */
export function captureNativeFileRouting(config) {
  const basePath = config?.basePath;
  const caseSensitiveRoutes = config?.experimental?.caseSensitiveRoutes;
  if (
    typeof basePath !== "string" ||
    typeof caseSensitiveRoutes !== "boolean" ||
    (basePath !== "" && (!basePath.startsWith("/") || basePath.endsWith("/"))) ||
    /[?#\\]/.test(basePath)
  )
    throw new TypeError("Native file routing configuration unavailable");
  return Object.freeze({ basePath, caseSensitiveRoutes });
}
/** Generated server.js sets this exact config before its serving createServer call. */
export function loadedStandaloneNextConfiguration() {
  try {
    return JSON.parse(process.env.__NEXT_PRIVATE_STANDALONE_CONFIG);
  } catch {
    throw new TypeError("Native file routing configuration unavailable");
  }
}
export function classifyNativeFileRequest(url, method, routing) {
  // Match Next's parseUrl before rewrites, including fragments and dot segments.
  const pathname = new URL(url.startsWith("/") ? "http://n" + url : url).pathname;
  // Next redirects these raw paths before installing the clone/body consumer.
  if (/(\\|\/\/)/.test(url.split("?", 1)[0])) return;
  if (pathname.length > 1 && pathname.endsWith("/")) return;
  const base = routing.basePath;
  let canonical;
  if (!base || pathname === base || pathname.startsWith(base + "/")) {
    canonical = pathname.slice(base.length) || "/";
  }
  const escaped = base.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const alias = pathname.match(
    new RegExp("^" + escaped + "/v1(?:/v1)?(?=/|$)", routing.caseSensitiveRoutes ? "" : "i")
  );
  if (alias) canonical = "/api/v1" + pathname.slice(alias[0].length);
  if (method === "POST" && canonical === "/api/v1/files") return { kind: "upload", api: true };
  if (method === "GET" || method === "HEAD") {
    if (/^\/api\/v1\/files\/[^/]+\/content$/.test(canonical || ""))
      return { kind: "download", api: true };
    if (/^\/api\/files\/[^/]+\/content$/.test(canonical || ""))
      return { kind: "download", api: false };
  }
}
