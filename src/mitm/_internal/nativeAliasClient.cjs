"use strict";

const { normalizeAliasEntry } = require("./aliasConfig.cjs");

const CHANNEL = "omniroute-mitm-alias.v1";
const UNAVAILABLE = "MITM alias lookup is unavailable";

/** The inherited child IPC channel is the capability; no network listener or credentials. */
function createNativeAliasClient(transport = process, { timeoutMs = 15000, limit = 32 } = {}) {
  const pending = new Map();
  let sequence = 0;
  let closed = false;

  function finish(id, error, value) {
    const entry = pending.get(id);
    if (!entry) return;
    pending.delete(id);
    clearTimeout(entry.timer);
    if (error) entry.reject(new Error(UNAVAILABLE));
    else entry.resolve(value);
  }

  function onMessage(message) {
    if (
      !message ||
      message.channel !== CHANNEL ||
      message.kind !== "result" ||
      !Number.isSafeInteger(message.id) ||
      !pending.has(message.id)
    )
      return;
    if (message.ok !== true) return finish(message.id, true);
    if (message.override === null) return finish(message.id, false, null);
    const entry = normalizeAliasEntry(message.override);
    if (!entry || JSON.stringify(entry).length > 8192) return finish(message.id, true);
    finish(message.id, false, entry);
  }

  function close() {
    if (closed) return;
    closed = true;
    transport.removeListener("message", onMessage);
    transport.removeListener("disconnect", close);
    for (const id of pending.keys()) finish(id, true);
  }

  transport.on("message", onMessage);
  transport.once("disconnect", close);

  return {
    lookup(model, aliasKey) {
      if (!model) return Promise.resolve(null);
      if (
        closed ||
        transport.connected !== true ||
        typeof transport.send !== "function" ||
        pending.size >= limit ||
        sequence === Number.MAX_SAFE_INTEGER ||
        typeof model !== "string" ||
        model.length > 2048 ||
        typeof aliasKey !== "string" ||
        aliasKey.length > 64
      )
        return Promise.reject(new Error(UNAVAILABLE));
      const id = ++sequence;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => finish(id, true), timeoutMs);
        pending.set(id, { resolve, reject, timer });
        try {
          transport.send({ channel: CHANNEL, kind: "lookup", id, model, aliasKey }, (error) => {
            if (error) finish(id, true);
          });
        } catch {
          finish(id, true);
        }
      });
    },
    close,
  };
}

module.exports = { createNativeAliasClient };
