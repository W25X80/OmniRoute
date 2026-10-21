import { AsyncLocalStorage } from "node:async_hooks";
import { performance } from "node:perf_hooks";

const CONTEXT = Symbol.for("omniroute.request-deadline-context.v1");
const DEADLINE = Symbol.for("omniroute.request-ingress-deadline.v1");
const CODE = "request_ingress_deadline_exceeded";
const MAX_TIMEOUT_MS = 2_147_483_647;
const contexts = (globalThis[CONTEXT] ??= new AsyncLocalStorage());
const INFERENCE_REQUEST_PATH_PATTERNS = [
  "^/(?:api/)?v1/(?:v1/)?chat/completions/?$",
  "^/(?:api/)?v1/(?:v1/)?messages/?$",
  "^/(?:api/)?v1/(?:v1/)?responses(?:/(?!input_tokens(?:/|$))[^?#]+)?/?$",
  "^/(?:api/)?v1/(?:v1/)?providers/[^/]+/chat/completions/?$",
  "^/(?:api/)?v1/(?:v1/)?relay/chat/completions(?:/bifrost)?/?$",
  "^/(?:api/)?v1/(?:v1/)?vscode/(?:raw/)?[^/]+/(?:v1/)?(?:chat/completions|responses)/?$",
  // These root paths are explicit rewrites in next.config.mjs.
  "^/chat/completions/?$",
  "^/responses(?:/(?!input_tokens(?:/|$))[^?#]+)?/?$",
  "^/codex(?:/[^?#]*)?/?$",
];
const CASE_SENSITIVE_INFERENCE_PATHS = INFERENCE_REQUEST_PATH_PATTERNS.map(
  (pattern) => new RegExp(pattern)
);
const CASE_INSENSITIVE_INFERENCE_PATHS = INFERENCE_REQUEST_PATH_PATTERNS.map(
  (pattern) => new RegExp(pattern, "i")
);
const GEMINI_MODELS_PREFIX = /^\/(?:api\/)?v1beta\/models\//;
const CASE_INSENSITIVE_GEMINI_MODELS_PREFIX = /^\/(?:api\/)?v1beta\/models\//i;

export function resolveIngressRequestTimeoutMs(env = process.env) {
  const raw = env.OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS?.trim();
  if (!raw) return 0;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0 || value > MAX_TIMEOUT_MS) {
    throw new TypeError(
      "OMNIROUTE_INGRESS_REQUEST_TIMEOUT_MS must be an integer from 0 to 2147483647"
    );
  }
  return value;
}

function isGeminiInferenceRequest(pathname, caseSensitive) {
  // The actual catch-all route treats every non-stream action as generateContent,
  // so any nonempty POST descendant can reach handleChat. The model catalog is
  // the exact /models root and remains outside the request budget.
  const prefix = (
    caseSensitive ? GEMINI_MODELS_PREFIX : CASE_INSENSITIVE_GEMINI_MODELS_PREFIX
  ).exec(pathname);
  if (!prefix) return false;
  return pathname.slice(prefix[0].length).replace(/\/+$/, "").length > 0;
}

function isInferenceRequest(req, routing) {
  if (req.method !== "POST") return false;
  let pathname;
  try {
    pathname = new URL(req.url || "/", "http://localhost").pathname;
  } catch {
    return false;
  }
  const base = routing.basePath;
  if (base && pathname !== base && !pathname.startsWith(base + "/")) return false;
  pathname = pathname.slice(base.length);
  const patterns = routing?.experimental?.caseSensitiveRoutes
    ? CASE_SENSITIVE_INFERENCE_PATHS
    : CASE_INSENSITIVE_INFERENCE_PATHS;
  const caseSensitive = Boolean(routing?.experimental?.caseSensitiveRoutes);
  return (
    patterns.some((pattern) => pattern.test(pathname)) ||
    isGeminiInferenceRequest(pathname, caseSensitive)
  );
}

function deadlineError() {
  const error = new Error("Gateway request exceeded its configured ingress deadline");
  error.name = "TimeoutError";
  error.code = CODE;
  error.statusCode = 504;
  error[DEADLINE] = true;
  return error;
}

/** Own the pre-route silent phase and JSON total budget without committing success early. */
export function wrapRequestListenerWithDeadline(listener, routing, env = process.env) {
  const timeoutMs = resolveIngressRequestTimeoutMs(env);
  if (timeoutMs === 0) return listener;
  // SQLite's listener does not require the loaded Next configuration. Capture
  // the same normalized base path once rather than consulting mutable env later.
  const rawBase = env.OMNIROUTE_BASE_PATH?.trim() ?? "";
  const segments = rawBase.split("/").filter(Boolean);
  const basePath =
    rawBase.startsWith("/") &&
    !/[?#\\]/.test(rawBase) &&
    !segments.some((part) => part === "." || part === "..")
      ? segments.length
        ? "/" + segments.join("/")
        : ""
      : "";
  const selectedRouting = routing ?? { basePath };
  return function ingressDeadlineRequest(req, res) {
    if (!isInferenceRequest(req, selectedRouting)) return listener.call(this, req, res);
    const cancellation = new AbortController();
    const deadlineAt = performance.now() + timeoutMs;
    let mode = "preparing";
    let timer;
    let ownResponse = false;
    let writingDeadline = false;
    let httpSettled = false;
    let workSettled = false;
    let terminalReason;
    let disposed = false;
    const originals = new Map();
    const state = {
      signal: cancellation.signal,
      deadlineAt,
      get mode() {
        return mode;
      },
      promoteStreaming() {
        state.assertLive();
        mode = "streaming";
        clearTimeout(timer);
      },
      assertLive() {
        if (mode !== "streaming" && mode !== "settled" && performance.now() >= deadlineAt) {
          expire();
          throw terminalReason ?? deadlineError();
        }
        cancellation.signal.throwIfAborted();
      },
      remainingMs() {
        return Math.max(0, deadlineAt - performance.now());
      },
    };

    const dispose = () => {
      if (disposed || !httpSettled || !workSettled) return;
      disposed = true;
      clearTimeout(timer);
      timer = undefined;
      res.removeListener("finish", onFinish);
      res.removeListener("close", onClose);
      req.removeListener("aborted", onClose);
      for (const [name, original] of originals) {
        if (original.own) res[name] = original.method;
        else delete res[name];
      }
      originals.clear();
      if (mode !== "deadline") mode = "settled";
      // Completed async resources may retain the metadata store. Release the
      // native request/body even while that lightweight ALS store survives.
      req = undefined;
      res = undefined;
    };
    const onFinish = () => {
      if (disposed) return;
      httpSettled = true;
      clearTimeout(timer);
      dispose();
    };
    const onClose = () => {
      if (disposed) return;
      httpSettled = true;
      clearTimeout(timer);
      if (!res.writableFinished && !cancellation.signal.aborted) {
        cancellation.abort(terminalReason ?? new DOMException("Client disconnected", "AbortError"));
      }
      dispose();
    };
    const expire = () => {
      if (
        disposed ||
        httpSettled ||
        mode === "streaming" ||
        mode === "deadline" ||
        cancellation.signal.aborted
      )
        return;
      mode = "deadline";
      const reason = deadlineError();
      terminalReason = reason;
      ownResponse = !res.headersSent && !res.destroyed && !res.writableEnded;
      if (res.destroyed || res.writableEnded || res.writableFinished) {
        cancellation.abort(reason);
        return;
      }
      if (res.headersSent) {
        cancellation.abort(reason);
        res.destroy();
        return;
      }
      // No upstream text is interpolated into this transport-owned failure.
      // Block a late Next response while retaining its persistence cleanup owner.
      const body = JSON.stringify({
        error: {
          message: "Gateway request exceeded its configured ingress deadline",
          type: "request_deadline",
          code: CODE,
        },
      });
      const writeHead = originals.get("writeHead").method;
      const end = originals.get("end").method;
      const response = res;
      const incompleteInput = req.complete !== true;
      // Cancelling an incoming body may destroy its native socket. Deliver the
      // bounded error first; the deadline state already forbids new dispatch.
      const cancel = () => cancellation.abort(reason);
      res.once("finish", cancel);
      writingDeadline = true;
      try {
        writeHead.call(res, 504, {
          "Content-Type": "application/json; charset=utf-8",
          "Cache-Control": "no-store",
          "Content-Length": Buffer.byteLength(body),
          ...(incompleteInput ? { Connection: "close" } : {}),
        });
        end.call(res, body);
      } finally {
        writingDeadline = false;
      }
      if (response.writableFinished) cancel();
    };
    for (const name of ["setHeader", "appendHeader", "writeHead", "flushHeaders", "write", "end"]) {
      if (typeof res[name] !== "function") continue;
      const method = res[name];
      originals.set(name, { method, own: Object.hasOwn(res, name) });
      res[name] = function guardedResponseWrite(...args) {
        if (ownResponse && !writingDeadline) {
          const callback = args.at(-1);
          if (typeof callback === "function") queueMicrotask(callback);
          return name === "write" ? true : this;
        }
        return method.apply(this, args);
      };
    }
    res.once("finish", onFinish);
    res.once("close", onClose);
    req.once("aborted", onClose);
    timer = setTimeout(expire, timeoutMs);
    timer.unref?.();
    const self = this;
    let work;
    try {
      work = contexts.run(state, () => listener.call(self, req, res));
    } catch (error) {
      workSettled = true;
      dispose();
      throw error;
    }
    return Promise.resolve(work)
      .catch((error) => {
        if (!cancellation.signal.aborted) throw error;
      })
      .finally(() => {
        workSettled = true;
        dispose();
      });
  };
}
