// M6 — `/messages/*` (mounted at `/messages`, behind the channel token). The
// user-facing I/O surface for channels (§7):
//   POST /:channel/send       ingress — route an inbound user message → M4
//   POST /:channel/webhook    ingress — inbound webhook (same routing, adapter shape)
//   GET  /:channel/poll       egress  — long-poll new messages (cursor-driven)
//   POST /:channel/poll       egress  — poll with body options (after/peek/limit)
//
// Egress TAILS the M5 store by a per-(channel, task) cursor and NEVER writes the
// transcript (storage ⟂ distribution, §9). Ingress hands the message to M4 via
// the orchestrator (it writes only `channel_bindings`). Explicit routes only —
// no catch-all.
import type { AppDatabase } from "@/db";
import type { AppEnv } from "@/shared/lib/types";
import { Hono } from "hono";
import { z } from "zod";
import { sessions } from "@/agents/sessions";
import { pollChannel, requireChannel, routeInbound } from "@/channels";
import { ok } from "@/shared/lib/api-response";
import { describeRoute, errors, jsonOk, TAGS, validator } from "@/shared/lib/openapi";
import { channelTokenAuth } from "@/shared/middleware/auth";

/** Long-poll bounds: a poll waits at most this long for new messages, checking every interval. */
const LONG_POLL_MAX_MS = 30_000;
const LONG_POLL_INTERVAL_MS = 250;

const sendBody = z.object({
  conversation_ref: z.string().min(1),
  text: z.string().min(1),
  attachments: z.array(z.unknown()).optional(),
  engine: z.string().optional(),
  model: z.string().optional(),
  workdir: z.string().optional(),
  meta: z.record(z.string(), z.unknown()).optional(),
});

const pollBody = z.object({
  conversation_ref: z.string().optional(),
  task: z.string().optional(),
  after: z.number().int().nonnegative().optional(),
  limit: z.number().int().positive().optional(),
  peek: z.boolean().optional(),
  wait: z.number().int().nonnegative().optional(),
});

interface PollParams {
  conversationRef?: string;
  taskId?: string;
  after?: number;
  limit?: number;
  peek?: boolean;
  waitMs: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Run a (possibly long-)poll: tail immediately, then re-tail until messages appear or `waitMs` elapses. */
async function runPoll(db: AppDatabase, channelRef: string, params: PollParams) {
  const channel = requireChannel(db, channelRef);
  const base = {
    ...(params.conversationRef != null ? { conversationRef: params.conversationRef } : {}),
    ...(params.taskId != null ? { taskId: params.taskId } : {}),
    ...(params.after != null ? { after: params.after } : {}),
    ...(params.limit != null ? { limit: params.limit } : {}),
    ...(params.peek != null ? { peek: params.peek } : {}),
  };

  const deadline = Date.now() + Math.min(Math.max(params.waitMs, 0), LONG_POLL_MAX_MS);
  // First read advances the cursor; subsequent waits PEEK so we don't skip messages
  // that arrive between iterations, then a final non-peek read commits the cursor.
  let deliveries = pollChannel(db, channel, base);
  while (deliveries.length === 0 && Date.now() < deadline) {
    await sleep(LONG_POLL_INTERVAL_MS);
    deliveries = pollChannel(db, channel, base);
  }
  return { channel: channel.id, deliveries };
}

export function messagesRoutes() {
  const router = new Hono<AppEnv>();

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
      const channel = requireChannel(db, c.req.param("channel"));
      const b = c.req.valid("json");
      const result = routeInbound(db, sessions, channel, {
        conversationRef: b.conversation_ref,
        text: b.text,
        ...(b.attachments != null ? { attachments: b.attachments } : {}),
        ...(b.engine != null ? { engine: b.engine } : {}),
        ...(b.model != null ? { model: b.model } : {}),
        ...(b.workdir != null ? { workdir: b.workdir } : {}),
        ...(b.meta != null ? { meta: b.meta } : {}),
      });
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
      const channel = requireChannel(db, c.req.param("channel"));
      const b = c.req.valid("json");
      const result = routeInbound(db, sessions, channel, {
        conversationRef: b.conversation_ref,
        text: b.text,
        ...(b.attachments != null ? { attachments: b.attachments } : {}),
        ...(b.engine != null ? { engine: b.engine } : {}),
        ...(b.model != null ? { model: b.model } : {}),
        ...(b.workdir != null ? { workdir: b.workdir } : {}),
        ...(b.meta != null ? { meta: b.meta } : {}),
      });
      return c.json(ok(result));
    },
  );

  // ── Egress: long-poll new messages (cursor-driven) ─────────────────────────
  router.get(
    "/:channel/poll",
    describeRoute({
      tags: [TAGS.Messages],
      summary: "Poll a channel for new messages (egress: tail the store by cursor)",
      responses: { ...jsonOk(z.any(), "Messages"), ...errors(401, 404) },
    }),
    async (c) => {
      const conversationRef = c.req.query("conversation_ref");
      const taskId = c.req.query("task");
      const after = c.req.query("after");
      const limit = c.req.query("limit");
      const wait = c.req.query("wait");
      const result = await runPoll(c.get("db"), c.req.param("channel"), {
        ...(conversationRef != null ? { conversationRef } : {}),
        ...(taskId != null ? { taskId } : {}),
        ...(after != null ? { after: Number(after) } : {}),
        ...(limit != null ? { limit: Number(limit) } : {}),
        peek: c.req.query("peek") === "true",
        waitMs: wait != null ? Number(wait) : 0,
      });
      return c.json(ok(result));
    },
  );

  // ── Egress: poll with body options ─────────────────────────────────────────
  router.post(
    "/:channel/poll",
    describeRoute({
      tags: [TAGS.Messages],
      summary: "Poll a channel for new messages (egress; body options)",
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
        ...(b.peek != null ? { peek: b.peek } : {}),
        waitMs: b.wait ?? 0,
      });
      return c.json(ok(result));
    },
  );

  return router;
}
