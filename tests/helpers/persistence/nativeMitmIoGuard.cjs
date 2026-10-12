"use strict";

// Module loading and startup-only certificate/target reads remain permitted.
// Once listening, serving code must not consult an application SQLite file or sync stats I/O.
const fs = require("node:fs");
const originalWrite = process.stdout.write;
let serving = false;
for (const name of ["existsSync", "readFileSync", "writeFileSync"]) {
  const original = fs[name];
  fs[name] = (...args) => {
    if (serving || name === "writeFileSync") {
      process.send?.({ fixture: "sync-io", name });
      throw new Error("Synchronous serving filesystem I/O is forbidden");
    }
    return original(...args);
  };
}
process.stdout.write = function (chunk, ...args) {
  if (String(chunk).includes("MITM ready")) serving = true;
  return originalWrite.call(this, chunk, ...args);
};

if (process.env.OMNIROUTE_TEST_MITM_LIFETIME === "1") {
  const https = require("node:https");
  const dns = require("node:dns");
  const originalCreateServer = https.createServer;
  https.createServer = function (options, listener) {
    return originalCreateServer.call(this, options, (req, res) => {
      const label = req.headers["x-fixture-request"];
      res.once("close", () => process.send?.({ fixture: "downstream-close", label }));
      void Promise.resolve(listener(req, res)).then(
        () => process.send?.({ fixture: "handler-complete", label }),
        () => process.send?.({ fixture: "handler-error", label })
      );
    });
  };
  // Missing mappings normally dispatch to a public provider. Keep the actual
  // passthrough HTTPS request, but constrain its DNS and transport to loopback.
  dns.Resolver.prototype.resolve4 = function (_host, callback) {
    callback(null, ["127.0.0.1"]);
  };
  const originalRequest = https.request;
  https.request = function (options, ...args) {
    process.send?.({ fixture: "upstream-dispatch" });
    return originalRequest.call(
      this,
      {
        ...options,
        hostname: "127.0.0.1",
        port: Number(process.env.OMNIROUTE_TEST_MITM_UPSTREAM_PORT),
      },
      ...args
    );
  };
}
