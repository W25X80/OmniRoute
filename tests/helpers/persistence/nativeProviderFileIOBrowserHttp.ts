import { EventEmitter } from "node:events";
import type { ClientRequest, IncomingMessage } from "node:http";
function get(_options: unknown, listener: (response: IncomingMessage) => void): ClientRequest {
  const response = Object.assign(new EventEmitter(), { statusCode: 200 });
  const request = Object.assign(new EventEmitter(), { destroy() {} });
  queueMicrotask(() => {
    listener(response as IncomingMessage);
    response.emit(
      "data",
      Buffer.from(JSON.stringify({ webSocketDebuggerUrl: "ws://controlled-browser" }))
    );
    response.emit("end");
  });
  return request as ClientRequest;
}
const http = { get };
export default http;
