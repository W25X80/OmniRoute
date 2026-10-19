import { isResponsesTerminalEventType } from "../../utils/responsesTerminalEvent.ts";
import { parseSSEDataLines } from "../../utils/streamHelpers.ts";

/** Observe complete SSE frames without re-encoding their bytes or reading ahead. */
export function createCodexTerminalTransform(): TransformStream<Uint8Array, Uint8Array> {
  const decoder = new TextDecoder("utf-8", { ignoreBOM: true });
  let line = "";
  let data: string[] = [];
  let skipLf = false;
  let firstLine = true;

  function endLine(): boolean {
    if (firstLine) {
      line = line.replace(/^\uFEFF/, "");
      firstLine = false;
    }
    const value = line;
    line = "";
    if (value === "") {
      const event = parseSSEDataLines(data, { logWarning: false });
      data = [];
      return !!event && isResponsesTerminalEventType(event.type);
    }
    if (value.startsWith("data:")) {
      const payload = value.slice(5);
      data.push(payload.startsWith(" ") ? payload.slice(1) : payload);
    }
    return false;
  }

  return new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      let start = 0;
      for (let i = 0; i < chunk.length; i++) {
        const byte = chunk[i];
        if (skipLf) {
          skipLf = false;
          if (byte === 10) {
            start = i + 1;
            continue;
          }
        }
        if (byte !== 10 && byte !== 13) continue;
        line += decoder.decode(chunk.subarray(start, i), { stream: true });
        line += decoder.decode();
        if (endLine()) {
          // CR alone is a valid SSE newline. Include its optional LF when it is
          // already in this chunk; a later LF belongs to the discarded suffix.
          const end = i + 1 + Number(byte === 13 && chunk[i + 1] === 10);
          controller.enqueue(chunk.subarray(0, end));
          controller.terminate();
          return;
        }
        skipLf = byte === 13;
        start = i + 1;
      }
      line += decoder.decode(chunk.subarray(start), { stream: true });
      controller.enqueue(chunk);
    },
  });
}

export function closeCodexSseOnTerminal(response: Response): Response {
  if (!response.ok || !response.headers.get("content-type")?.includes("text/event-stream"))
    return response;
  const body = response.body;
  if (!body) return response;
  const headers = new Headers(response.headers);
  headers.delete("content-length");
  return new Response(body.pipeThrough(createCodexTerminalTransform()), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
