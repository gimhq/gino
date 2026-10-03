import type { HttpResult, WireContext } from "./index";
import { ingestHttpFrame as ingestFrame, MalformedFrameError } from "../../protocol/inbound";

/** `POST /agent/output/:agentId` — pre-WS thread_result fallback (protocol §2) → M5 store. */
export function ingestHttpFrame(ctx: WireContext, agentId: string, raw: string | Uint8Array): HttpResult {
  // Only an undecodable frame is 400 (permanently bad — the
  // agent must drop it); a storage failure propagates to the error handler
  // as a 5xx so the agent retries the frame.
  try {
    // Host events (M6: a stored thread_message triggers the webhook pump via `transcript.message`).
    const body = ingestFrame(
      { storage: ctx.storage, agentId, logger: ctx.logger, metrics: ctx.metrics, events: ctx.events },
      raw,
    );
    return { status: 200, body };
  }
  catch (err) {
    if (err instanceof MalformedFrameError) {
      ctx.logger.warn({ err, agentId }, "agent output fallback: malformed frame rejected");
      return { status: 400, body: { success: false, error: { code: "BAD_FRAME", message: "malformed frame" } } };
    }
    ctx.logger.error({ err, agentId }, "agent output fallback: storage failure (agent should retry)");
    throw err;
  }
}
