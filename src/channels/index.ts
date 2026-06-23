// M6 — CHANNELS (distribution & routing; user I/O).
//
// The host's DISTRIBUTION + ROUTING layer, fully decoupled from storage. Egress
// TAILS the M5 `messages` store by a per-channel cursor and delivers (OD10);
// ingress ROUTES an inbound user message to a conversation → continue its task or
// open a NEW task stamped with the channel's owner (OD9/OD16) → hand to M4.
// Channels NEVER write the transcript (storage ⟂ distribution, §9): M6 reads M5
// by cursor and writes only `channels` / `channel_bindings` / `channel_cursors`.
//
//   registry.ts  channels CRUD (owner_id, config, filter) + bindings + webhook config
//   egress.ts    tail messages by `(channel, task)` cursor → deliver (poll/long-poll/webhook push)
//   ingress.ts   route inbound → continue existing OR new conversation = new task → M4
//   webhooks.ts  outbound webhook delivery, SSRF-guarded
export const MODULE = "channels" as const;

export {
  advanceCursor,
  type BoundTask,
  isTaskOnChannel,
  listBoundTasks,
  pollChannel,
  type PollDelivery,
  type PollOptions,
  pushChannelWebhooks,
  readCursor,
  type TailOptions,
  type TailResult,
  tailTask,
  type WebhookPushResult,
} from "./egress";

export {
  bindConversation,
  type InboundMessage,
  routeInbound,
  type RouteResult,
} from "./ingress";

export {
  addWebhook,
  type AddWebhookInput,
  getBinding,
  getChannel,
  listChannelBindings,
  listChannels,
  listWebhooks,
  registerChannel,
  type RegisterChannelInput,
  removeWebhook,
  requireChannel,
  resolveChannel,
} from "./registry";

export {
  assertSafeWebhookUrl,
  assertSafeWebhookUrlResolved,
  type DeliverOptions,
  deliverWebhook,
  type DnsResolver,
  type FetchLike,
  type FetchResponse,
  type ResolvedAddress,
  type WebhookEndpoint,
  type WebhookPayload,
  type WebhookResult,
} from "./webhooks";
