export function finalizeReadableStream(
  body: ReadableStream<Uint8Array>,
  onFinalize: (error?: unknown) => void | Promise<void>
): ReadableStream<Uint8Array> {
  const reader = body.getReader();
  let finalized = false;

  const finalizeOnce = async (error?: unknown) => {
    if (finalized) return;
    finalized = true;
    await onFinalize(error);
  };

  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          await finalizeOnce();
          controller.close();
          return;
        }
        controller.enqueue(value);
      } catch (error) {
        await finalizeOnce(error);
        controller.error(error);
      }
    },
    async cancel(reason) {
      try {
        await reader.cancel(reason);
      } finally {
        await finalizeOnce(reason);
      }
    },
  });
}
