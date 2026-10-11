export interface StreamBeforeTerminalPayload {
  kind: "response.completed" | "response.incomplete" | "done";
  clientResponse: unknown;
}

export type StreamBeforeTerminal = (payload: StreamBeforeTerminalPayload) => void | Promise<void>;

/** Orders terminal publication without changing the default synchronous decoder. */
export function createStreamTerminalPublication(beforeTerminal: StreamBeforeTerminal) {
  let pending: Promise<void> | undefined;
  return {
    prepare(payload: StreamBeforeTerminalPayload): void {
      const owner = () => beforeTerminal(payload);
      pending = pending ? pending.then(owner) : Promise.resolve().then(owner);
    },
    publish(body: () => void): void {
      if (pending) pending = pending.then(body);
      else body();
    },
    settle(): Promise<void> | undefined {
      const work = pending;
      if (!work) return undefined;
      return work.finally(() => {
        if (pending === work) pending = undefined;
      });
    },
  };
}
