/** Terminal event types already understood by Responses translation and recovery. */
export function isResponsesTerminalEventType(type: unknown): boolean {
  return (
    type === "response.completed" || type === "response.failed" || type === "response.incomplete"
  );
}
