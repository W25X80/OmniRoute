"use strict";

const fs = require("node:fs/promises");

/** One physical write at a time; a burst retains only its newest immutable snapshot. */
function createAsyncStatsWriter(file, { writeSnapshot } = {}) {
  const temporary = `${file}.${process.pid}.tmp`;
  const write =
    writeSnapshot ||
    (async (snapshot) => {
      try {
        await fs.writeFile(temporary, snapshot, { mode: 0o600 });
        await fs.rename(temporary, file);
      } catch (error) {
        await fs.unlink(temporary).catch(() => {});
        throw error;
      }
    });
  let pending;
  let active;
  let closing;

  function start() {
    if (active) return;
    active = Promise.resolve()
      .then(async () => {
        while (pending !== undefined) {
          const snapshot = pending;
          pending = undefined;
          try {
            await write(snapshot);
          } catch {
            // Best-effort stats never fail proxy traffic and are not replayed.
          }
        }
      })
      .finally(() => {
        active = undefined;
        if (pending !== undefined) start();
      });
  }

  function publish(snapshot) {
    if (closing) return;
    pending = JSON.stringify(snapshot, null, 2);
    start();
  }

  return {
    publish,
    close(snapshot) {
      if (closing) return closing;
      if (snapshot !== undefined) publish(snapshot);
      closing = (async () => {
        while (active) await active;
      })();
      return closing;
    },
  };
}

module.exports = { createAsyncStatsWriter };
