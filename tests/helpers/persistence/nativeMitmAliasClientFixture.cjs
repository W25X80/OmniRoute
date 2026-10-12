"use strict";

const { createNativeAliasClient } = require("../../../src/mitm/_internal/nativeAliasClient.cjs");
const client = createNativeAliasClient(process, { timeoutMs: 100, limit: 1 });
const results = [];
function report(result) {
  results.push(result);
  if (process.connected) process.send?.(result);
}
process.on("message", (message) => {
  if (message?.fixture !== "lookup") return;
  void client.lookup(message.model, "antigravity").then(
    (override) => report({ fixture: "result", label: message.label, override }),
    (error) => report({ fixture: "result", label: message.label, error: error.message })
  );
});
process.once("disconnect", () =>
  queueMicrotask(() => process.stdout.write(JSON.stringify(results)))
);
