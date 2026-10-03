// M6 — CHANNEL REGISTRY (channels CRUD + bindings + webhook config).
//
// A channel is registered WITH an owner: the external system did the
// user→owner assignment; the host never computes it. A `channels` row holds
// `{ name, type, owner_id, config?, msg_filter? }`. M6 owns the `channels`,
// `channel_bindings`, and `channel_cursors` tables — and ONLY those (it never
// writes the transcript, §9).
//
// Webhooks (outbound egress endpoints) have no table of their own; they live
// under `channel.config.webhooks` and are managed here. Their URLs are
// SSRF-guarded at registration time (see ./webhooks).

import type { ChannelBindingRow, ChannelRepo, ChannelRow, Storage } from "@gino/host-core";
import type { WebhookEndpoint } from "./webhooks";
import { AppError, NotFoundError } from "@gino/host-core";
import { ulid } from "@gino/host-core/domain";
import { assertSafeWebhookUrl } from "./webhooks";

/** The `ChannelRepo` is optional on the port; a host running M6 must provide it (the sqlite adapter always does). */
export function requireChannels(storage: Storage): ChannelRepo {
  if (!storage.channels) {
    throw new AppError("the storage backend does not provide the channels repository", 500, "CHANNELS_UNAVAILABLE");
  }
  return storage.channels;
}

export interface RegisterChannelInput {
  name: string;
  type: string;
  /** The external system's user→owner assignment; the host stores it verbatim. */
  ownerId: string;
  /** Opaque per-channel config (e.g. default `engine`/`model` for new conversations, webhooks). */
  config?: Record<string, unknown>;
  /** Optional egress kind filter (`{ kinds: [...] }`); absent → deliver all kinds. */
  filter?: Record<string, unknown>;
}

/** Register a channel (host mints the ULID id). */
export function registerChannel(storage: Storage, input: RegisterChannelInput): ChannelRow {
  return requireChannels(storage).insert({
    id: ulid(),
    name: input.name,
    type: input.type,
    ownerId: input.ownerId,
    config: input.config ?? null,
    msgFilter: input.filter ?? null,
  });
}

export function getChannel(storage: Storage, id: string): ChannelRow | undefined {
  return requireChannels(storage).get(id);
}

/** Resolve a `:channel` route ref — by id first, then by (first) matching name. */
export function resolveChannel(storage: Storage, ref: string): ChannelRow | undefined {
  const repo = requireChannels(storage);
  return repo.get(ref) ?? repo.getByName(ref);
}

/** Like `resolveChannel` but throws the standard 404 for the edge surface. */
export function requireChannel(storage: Storage, ref: string): ChannelRow {
  const channel = resolveChannel(storage, ref);
  if (!channel) {
    throw new NotFoundError("channel", ref);
  }
  return channel;
}

/** List every channel (admin observe), newest first. */
export function listChannels(storage: Storage): ChannelRow[] {
  return requireChannels(storage).list();
}

/** List a channel's conversation bindings (admin observe), newest first. */
export function listChannelBindings(storage: Storage, channelId: string): ChannelBindingRow[] {
  return requireChannels(storage).listBindings(channelId);
}

// ─── Webhook config (stored under `channel.config.webhooks`) ───

interface ChannelConfig {
  webhooks?: WebhookEndpoint[];
  [k: string]: unknown;
}

/** The outbound webhooks registered on a channel. */
export function listWebhooks(channel: ChannelRow): WebhookEndpoint[] {
  const config = (channel.config ?? {}) as ChannelConfig;
  return Array.isArray(config.webhooks) ? config.webhooks : [];
}

function writeWebhooks(storage: Storage, channel: ChannelRow, webhooks: WebhookEndpoint[]): void {
  const config = { ...((channel.config ?? {}) as ChannelConfig), webhooks };
  requireChannels(storage).setConfig(channel.id, config);
}

export interface AddWebhookInput {
  url: string;
  secret?: string;
}

/** Register an outbound webhook on a channel (URL is SSRF-guarded). Returns the new endpoint. */
export function addWebhook(storage: Storage, channelId: string, input: AddWebhookInput): WebhookEndpoint {
  const channel = getChannel(storage, channelId);
  if (!channel) {
    throw new NotFoundError("channel", channelId);
  }
  assertSafeWebhookUrl(input.url); // reject SSRF targets at registration
  const endpoint: WebhookEndpoint = { id: ulid(), url: input.url, ...(input.secret ? { secret: input.secret } : {}) };
  writeWebhooks(storage, channel, [...listWebhooks(channel), endpoint]);
  return endpoint;
}

/** Remove an outbound webhook from a channel. Returns true if it existed. */
export function removeWebhook(storage: Storage, channelId: string, webhookId: string): boolean {
  const channel = getChannel(storage, channelId);
  if (!channel) {
    throw new NotFoundError("channel", channelId);
  }
  const before = listWebhooks(channel);
  const after = before.filter(w => w.id !== webhookId);
  if (after.length === before.length) {
    return false;
  }
  writeWebhooks(storage, channel, after);
  return true;
}

/** Read a single binding (used by the admin surface). */
export function getBinding(storage: Storage, channelId: string, conversationRef: string): ChannelBindingRow | undefined {
  return requireChannels(storage).getBinding(channelId, conversationRef);
}
