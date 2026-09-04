// M6 — `/messages/*` (mounted at `/messages`, behind the channel token). The
// user-facing I/O surface for channels (§7):
//   POST /:channel/send       ingress — route an inbound user message → M4
//   POST /:channel/webhook    ingress — inbound webhook (same routing, adapter shape)
//   GET  /:channel/poll       egress  — long-poll new messages (cursor-driven)
//   POST /:channel/poll       egress  — poll with body options (after/ack/limit)
//
// Egress TAILS the M5 store by a per-(channel, task) cursor and NEVER writes the
// transcript (storage ⟂ distribution, §9). Ingress hands the message to M4 via
// the orchestrator (it writes only `channel_bindings`). Explicit routes only —
// no catch-all.
import type { AppDatabase } from "@/db";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { z } from "zod";
import { MAX_POLL_LIMIT, pollChannel, requireChannel, routeInbound } from "@/channels";
import { hashToken, safeEqual } from "@/platform";
import { AttachmentDescriptor } from "@/protocol/frames";
import { ok } from "@/shared/lib/api-response";
import { describeRoute, errors, jsonOk, TAGS, validator } from "@/shared/lib/openapi";
import { bearerToken, channelTokenAuth } from "@/shared/middleware/auth";
import { rateLimit } from "@/shared/middleware/rate-limit";

/** Long-poll bounds: a poll waits at most this long for new messages, checking every interval. */
const LONG_POLL_MAX_MS = 30_000;
const LONG_POLL_INTERVAL_MS = 250;

const sendBody = z.object({
  conversation_ref: z.string().min(1),
  text: z.string().min(1),
  // HOST-M1M2-12: descriptors are validated against the wire schema
  // (protocol §5) BEFORE they are forwarded into `message.data.attachments` —
  // the host never emits a frame violating its own published contract.
  attachments: z.array(AttachmentDescriptor).optional(),
  engine: z.string().optional(),
  model: z.string().optional(),
  workdir: z.string().optional(),
  meta: z.record(z.string(), z.unknown()).optional(),
});

const pollBody = z.object({
  conversation_ref: z.string().optional(),
  task: z.string().optional(),
  after: z.number().int().nonnegative().optional(),
  limit: z.number().int().positive().max(MAX_POLL_LIMIT).optional(),
  ack: z.number().int().nonnegative().optional(),
  wait: z.number().int().nonnegative().optional(),
});

// HOST-M1M2-17: the GET variant enforces the SAME numeric contract as the POST
// body (coerced from query strings) — garbage like `?after=latest` is a 422,
// never a silently degraded poll (`after: NaN` / a collapsed long-poll wait).
const pollQuery = z.object({
  conversation_ref: z.string().optional(),
  task: z.string().optional(),
  after: z.coerce.number().int().nonnegative().optional(),
  limit: z.coerce.number().int().positive().max(MAX_POLL_LIMIT).optional(),
  ack: z.coerce.number().int().nonnegative().optional(),
  wait: z.coerce.number().int().nonnegative().optional(),
});

interface PollParams {
  conversationRef?: string;
  taskId?: string;
  after?: number;
  limit?: number;
  ack?: number;
  waitMs: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Run a (possibly long-)poll: tail immediately, then re-tail until messages
 * appear or `waitMs` elapses.
 *
 * Delivery semantics — AT-LEAST-ONCE (HOST-M5M6-01). A poll read NEVER
 * advances the stored per-(channel, task) cursor, so a batch the client never
 * received (dropped response, crash mid-flush) is simply re-delivered on the
 * next poll. The client confirms receipt by passing `ack` = the `cursor` of a
 * delivery it has durably received; the cursor then advances forward-only for
 * every task the poll targets. Contract for unscoped polls (no
 * `task`/`conversation_ref`) whose batches may be `limit`-truncated: ack the
 * SMALLEST delivery cursor received, or ack per task with scoped polls.
 * `after` reads from an explicit position without confirming anything.
 */
async function runPoll(db: AppDatabase, channelRef: string, params: PollParams) {
  const channel = requireChannel(db, channelRef);
  const base = {
    ...(params.conversationRef != null ? { conversationRef: params.conversationRef } : {}),
    ...(params.taskId != null ? { taskId: params.taskId } : {}),
    ...(params.after != null ? { after: params.after } : {}),
    ...(params.limit != null ? { limit: params.limit } : {}),
    ...(params.ack != null ? { ack: params.ack } : {}),
  };

  const deadline = Date.now() + Math.min(Math.max(params.waitMs, 0), LONG_POLL_MAX_MS);
  // Re-polling with the same options is idempotent: reads don't move the
  // cursor and the forward-only `ack` is a no-op after the first iteration.
  let deliveries = pollChannel(db, channel, base);
  while (deliveries.length === 0 && Date.now() < deadline) {
    await sleep(LONG_POLL_INTERVAL_MS);
    deliveries = pollChannel(db, channel, base);
  }
  return { channel: channel.id, deliveries };
}

export function messagesRoutes() {
  const router = new Hono<AppEnv>();

  // HOST-M9-02: throttle the channel I/O surface ahead of auth. A VALID
  // channel bearer drains the principal budget (keyed by token hash, never
  // the raw secret); an absent or invalid bearer falls back to the client IP,
  // so brute-force guessing of the shared token cannot rotate keys to evade
  // the limit.
  router.use("*", rateLimit({
    bucket: "messages",
    limits: config => ({ windowMs: config.RATE_LIMIT_MESSAGES_WINDOW_MS, max: config.RATE_LIMIT_MESSAGES_MAX }),
    key: (c) => {
      const expected = c.var.config.HTTP_CHANNEL_TOKEN;
      const bearer = bearerToken(c);
      return expected && bearer && safeEqual(bearer, expected) ? `tok:${hashToken(bearer)}` : null;
    },
  }));

  // The channel token (admin/user bearer) guards every channel I/O route (401 without).
  router.use("*", channelTokenAuth);

  // ── Ingress: route an inbound user message → M4 ────────────────────────────
  router.post(
    "/:channel/send",
    describeRoute({
      tags: [TAGS.Messages],
      summary: "Send a message to a channel (ingress: continue conversation or open a new task)",
      responses: { ...jsonOk(z.any(), "Routed"), ...errors(401, 404, 409, 422) },
    }),
    validator("json", sendBody),
    (c) => {
      const db = c.get("db");
      const host = c.get("host");
      const channel = requireChannel(db, c.req.param("channel"));
      const b = c.req.valid("json");
      const result = routeInbound(db, host.sessions, channel, {
        conversationRef: b.conversation_ref,
        text: b.text,
        ...(b.attachments != null ? { attachments: b.attachments } : {}),
        ...(b.engine != null ? { engine: b.engine } : {}),
        ...(b.model != null ? { model: b.model } : {}),
        ...(b.workdir != null ? { workdir: b.workdir } : {}),
        ...(b.meta != null ? { meta: b.meta } : {}),
      }, host.events);
      return c.json(ok(result));
    },
  );

  // ── Ingress: inbound webhook (same routing; external systems push here) ─────
  router.post(
    "/:channel/webhook",
    describeRoute({
      tags: [TAGS.Messages],
      summary: "Inbound webhook for a channel (ingress: routes like /send)",
      responses: { ...jsonOk(z.any(), "Routed"), ...errors(401, 404, 409, 422) },
    }),
    validator("json", sendBody),
    (c) => {
      const db = c.get("db");
      const host = c.get("host");
      const channel = requireChannel(db, c.req.param("channel"));
      const b = c.req.valid("json");
      const result = routeInbound(db, host.sessions, channel, {
        conversationRef: b.conversation_ref,
        text: b.text,
        ...(b.attachments != null ? { attachments: b.attachments } : {}),
        ...(b.engine != null ? { engine: b.engine } : {}),
        ...(b.model != null ? { model: b.model } : {}),
        ...(b.workdir != null ? { workdir: b.workdir } : {}),
        ...(b.meta != null ? { meta: b.meta } : {}),
      }, host.events);
      return c.json(ok(result));
    },
  );

  // ── Egress: long-poll new messages (cursor-driven) ─────────────────────────
  router.get(
    "/:channel/poll",
    describeRoute({
      tags: [TAGS.Messages],
      summary: "Poll a channel for new messages (egress: at-least-once; confirm receipt via ack)",
      responses: { ...jsonOk(z.any(), "Messages"), ...errors(401, 404, 422) },
    }),
    validator("query", pollQuery),
    async (c) => {
      const q = c.req.valid("query");
      const result = await runPoll(c.get("db"), c.req.param("channel"), {
        ...(q.conversation_ref != null ? { conversationRef: q.conversation_ref } : {}),
        ...(q.task != null ? { taskId: q.task } : {}),
        ...(q.after != null ? { after: q.after } : {}),
        ...(q.limit != null ? { limit: q.limit } : {}),
        ...(q.ack != null ? { ack: q.ack } : {}),
        waitMs: q.wait ?? 0,
      });
      return c.json(ok(result));
    },
  );

  // ── Egress: poll with body options ─────────────────────────────────────────
  router.post(
    "/:channel/poll",
    describeRoute({
      tags: [TAGS.Messages],
      summary: "Poll a channel for new messages (egress; body options, at-least-once via ack)",
      responses: { ...jsonOk(z.any(), "Messages"), ...errors(401, 404, 422) },
    }),
    validator("json", pollBody),
    async (c) => {
      const b = c.req.valid("json");
      const result = await runPoll(c.get("db"), c.req.param("channel"), {
        ...(b.conversation_ref != null ? { conversationRef: b.conversation_ref } : {}),
        ...(b.task != null ? { taskId: b.task } : {}),
        ...(b.after != null ? { after: b.after } : {}),
        ...(b.limit != null ? { limit: b.limit } : {}),
        ...(b.ack != null ? { ack: b.ack } : {}),
        waitMs: b.wait ?? 0,
      });
      return c.json(ok(result));
    },
  );

  return router;
}
