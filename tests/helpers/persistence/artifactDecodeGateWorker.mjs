import { parentPort } from "node:worker_threads";
let pending;
parentPort.on("message", (request) => {
  if (request.release && pending) {
    const bytes = Buffer.from(pending.bytes).toString("utf8");
    parentPort.postMessage({
      id: pending.id,
      kind: "deserialize",
      result: { artifact: JSON.parse(bytes), state: "ready" },
    });
    pending = undefined;
  } else if (request.fail) {
    throw new Error("Private fixture detail must not reach an artifact error");
  } else {
    pending = request;
    parentPort.postMessage({ id: -1, marker: "entered" });
  }
});
