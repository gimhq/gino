// M6 — `/admin/channels` control API (mounted under `/admin`, behind the channel
// token via the parent router). The channel registration + webhook + binding
// surface (§7):
//   GET    /channels                       list channels
//   POST   /channels                       register a channel ({name,type,owner_id,config?,filter?})
//   GET    /channels/:id                   one channel (+ bindings + webhooks)
//   POST   /channels/:id/webhooks          add an outbound webhook (SSRF-guarded)
//   DELETE /channels/:id/webhooks/:hookId  remove an outbound webhook
//   POST   /channels/:id/bindings          bind a conversation_ref → an existing task
//
// A channel carries an owner (OD16): the external system did user→owner; the host
// stores it verbatim. M6 never writes the transcript — these routes touch only
// `channels` / `channel_bindings`. Responses use the shared `{ success, data }`
// envelope. Webhook secrets are never returned.
import type { ChannelRow } from "@gino/host-core";
import type { HostEnv } from "../env";
import {
  addWebhook,
  bindConversation,
  getBinding,
  getChannel,
  listChannelBindings,
  listChannels,
  listWebhooks,
  registerChannel,
  removeWebhook,
  requireChannel,
} from "@gino/host-channels";
import { NotFoundError } from "@gino/host-core";
import { requireTask } from "@gino/host-core/domain";
import { Hono } from "hono";
import { z } from "zod";
import { ok } from "../api-response";
import { describeRoute, errors, jsonCreated, jsonOk, TAGS, validator } from "../openapi";

/** Public projection of a channel row — config/webhook secrets are NOT leaked. */
function serializeChannel(channel: ChannelRow) {
  return {
    id: channel.id,
    name: channel.name,
    type: channel.type,
    ownerId: channel.ownerId,
    filter: channel.msgFilter,
    createdAt: channel.createdAt,
  };
}

const registerBody = z.object({
  name: z.string().min(1),
  type: z.string().min(1),
  owner_id: z.string().min(1),
  config: z.record(z.string(), z.unknown()).optional(),
  filter: z.object({ kinds: z.array(z.string()).min(1) }).optional(),
});

const webhookBody = z.object({
  url: z.string().min(1),
  secret: z.string().optional(),
});

const bindBody = z.object({
  conversation_ref: z.string().min(1),
  task_id: z.string().min(1),
});

export function adminChannelRoutes() {
  const router = new Hono<HostEnv>();

  // ── List ───────────────────────────────────────────────────────────────────
  router.get(
    "/",
    describeRoute({
      tags: [TAGS.Channels],
      summary: "List channels",
      responses: { ...jsonOk(z.array(z.any()), "Channels"), ...errors(401) },
    }),
    (c) => {
      const storage = c.get("host").storage;
      return c.json(ok(listChannels(storage).map(serializeChannel)));
    },
  );

  // ── Register (OD16: carries owner_id + optional filter) ─────────────────────
  router.post(
    "/",
    describeRoute({
      tags: [TAGS.Channels],
      summary: "Register a channel (carries owner_id + optional kind filter, OD16)",
      responses: { ...jsonCreated(z.any(), "Registered"), ...errors(401, 422) },
    }),
    validator("json", registerBody),
    (c) => {
      const storage = c.get("host").storage;
      const b = c.req.valid("json");
      const channel = registerChannel(storage, {
        name: b.name,
        type: b.type,
        ownerId: b.owner_id,
        ...(b.config != null ? { config: b.config } : {}),
        ...(b.filter != null ? { filter: b.filter } : {}),
      });
      return c.json(ok(serializeChannel(channel)), 201);
    },
  );

  // ── Get one (+ bindings + webhooks; secrets stripped) ──────────────────────
  router.get(
    "/:id",
    describeRoute({
      tags: [TAGS.Channels],
      summary: "Get a channel (+ bindings + webhooks)",
      responses: { ...jsonOk(z.any(), "Channel"), ...errors(401, 404) },
    }),
    (c) => {
      const storage = c.get("host").storage;
      const channel = requireChannel(storage, c.req.param("id"));
      return c.json(ok({
        ...serializeChannel(channel),
        bindings: listChannelBindings(storage, channel.id).map(b => ({
          conversationRef: b.conversationRef,
          taskId: b.taskId,
          createdAt: b.createdAt,
        })),
        webhooks: listWebhooks(channel).map(w => ({ id: w.id, url: w.url })),
      }));
    },
  );

  // ── Webhook CRUD ────────────────────────────────────────────────────────────
  router.post(
    "/:id/webhooks",
    describeRoute({
      tags: [TAGS.Channels],
      summary: "Add an outbound webhook to a channel (SSRF-guarded)",
      responses: { ...jsonCreated(z.any(), "Webhook added"), ...errors(401, 404, 422) },
    }),
    validator("json", webhookBody),
    (c) => {
      const storage = c.get("host").storage;
      const id = c.req.param("id");
      requireChannel(storage, id); // 404 (resolve by id/name) before mutating
      const channel = getChannel(storage, id);
      if (!channel) {
        throw new NotFoundError("channel", id);
      }
      const b = c.req.valid("json");
      const endpoint = addWebhook(storage, channel.id, { url: b.url, ...(b.secret != null ? { secret: b.secret } : {}) });
      return c.json(ok({ id: endpoint.id, url: endpoint.url }), 201);
    },
  );

  router.delete(
    "/:id/webhooks/:hookId",
    describeRoute({
      tags: [TAGS.Channels],
      summary: "Remove an outbound webhook from a channel",
      responses: { ...jsonOk(z.any(), "Webhook removed"), ...errors(401, 404) },
    }),
    (c) => {
      const storage = c.get("host").storage;
      const channel = requireChannel(storage, c.req.param("id"));
      const hookId = c.req.param("hookId");
      const removed = removeWebhook(storage, channel.id, hookId);
      if (!removed) {
        throw new NotFoundError("webhook", hookId);
      }
      return c.json(ok({ id: hookId, removed: true }));
    },
  );

  // ── Bind a conversation to an existing task (admin) ─────────────────────────
  router.post(
    "/:id/bindings",
    describeRoute({
      tags: [TAGS.Channels],
      summary: "Bind a conversation_ref to an existing task (OD9)",
      responses: { ...jsonCreated(z.any(), "Bound"), ...errors(401, 404, 422) },
    }),
    validator("json", bindBody),
    (c) => {
      const storage = c.get("host").storage;
      const channel = requireChannel(storage, c.req.param("id"));
      const b = c.req.valid("json");
      requireTask(storage, b.task_id); // 404 if the task does not exist
      bindConversation(storage, channel.id, b.conversation_ref, b.task_id);
      const binding = getBinding(storage, channel.id, b.conversation_ref);
      return c.json(ok({
        channelId: channel.id,
        conversationRef: b.conversation_ref,
        taskId: binding?.taskId ?? b.task_id,
      }), 201);
    },
  );

  return router;
}
