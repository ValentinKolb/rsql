import type { SSEEvent, SSESubscription } from "../types";

export const createSSESubscription = (
  response: Response,
  controller: AbortController,
): SSESubscription => {
  if (!response.body) {
    throw new Error("SSE response body is not readable");
  }

  return {
    response,
    close: () => {
      controller.abort();
    },
    stream: parseEventStream(response.body, controller.signal),
  };
};

async function* parseEventStream(
  stream: ReadableStream<Uint8Array>,
  signal: AbortSignal,
): AsyncGenerator<SSEEvent> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();

  let buffer = "";
  let eventName = "message";
  let dataLines: string[] = [];

  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      while (true) {
        const idx = buffer.indexOf("\n");
        if (idx === -1) {
          break;
        }

        const rawLine = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 1);
        const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;

        if (line === "") {
          if (dataLines.length > 0) {
            const rawData = dataLines.join("\n");
            dataLines = [];
            try {
              const parsed = JSON.parse(rawData) as SSEEvent;
              if (!parsed.action && eventName !== "message") {
                parsed.action = eventName;
              }
              yield parsed;
            } catch {
              // Ignore malformed event payloads.
            }
          }
          eventName = "message";
          continue;
        }

        if (line.startsWith(":")) {
          continue;
        }

        if (line.startsWith("event:")) {
          eventName = line.slice("event:".length).trim();
          continue;
        }

        if (line.startsWith("data:")) {
          dataLines.push(line.slice("data:".length).trim());
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}
