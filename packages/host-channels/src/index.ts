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
//
// FROZEN as of P3 (docs/host-sdk-design.md §5 step 8). This list is the
// package's API: an explicit allow-list, no wildcards, so the surface cannot
// grow by accident when a module gains an export.
// `tests/unit/api-surface.test.ts` snapshots it — adding or removing a name
// here fails that test until the snapshot is updated deliberately.

// ─── Egress — tail the M5 transcript by a per-channel cursor ───
export { ackCursor, advanceCursor, DEFAULT_POLL_LIMIT, isTaskOnChannel, listBoundTasks, MAX_POLL_LIMIT, pollChannel, readCursor, tailTask } from "./egress";
export type { BoundTask, PollDelivery, PollOptions, TailOptions, TailResult } from "./egress";
export { DEFAULT_WEBHOOK_MAX_ATTEMPTS, pushChannelWebhooks } from "./egress";
export type { WebhookAttemptState, WebhookPushOptions, WebhookPushResult } from "./egress";

// ─── Ingress — route an inbound message to a conversation's task (OD9/OD16) ───
export { bindConversation, routeInbound } from "./ingress";
export type { InboundMessage, RouteResult } from "./ingress";

// ─── The retrying webhook pump (subscribes to the host events) ───
export { DEFAULT_WEBHOOK_RETRY_INTERVAL_MS, startWebhookPump } from "./pump";
export type { WebhookPump, WebhookPumpDeps } from "./pump";

// ─── Registry — channels CRUD, bindings and webhook config ───
export { addWebhook, getBinding, getChannel, listChannelBindings, listChannels, listWebhooks, registerChannel, removeWebhook, requireChannel, resolveChannel } from "./registry";
export type { AddWebhookInput, RegisterChannelInput } from "./registry";

// ─── Outbound webhook delivery, SSRF-guarded ───
export { assertSafeWebhookUrl, assertSafeWebhookUrlResolved, DEFAULT_WEBHOOK_TIMEOUT_MS, deliverWebhook } from "./webhooks";
export type { DeliverOptions, DnsResolver, FetchLike, FetchResponse, ResolvedAddress, WebhookEndpoint, WebhookPayload, WebhookResult } from "./webhooks";
