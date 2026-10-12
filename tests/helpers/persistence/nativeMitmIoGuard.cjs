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
