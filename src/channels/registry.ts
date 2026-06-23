// M6 — CHANNEL REGISTRY (channels CRUD + bindings + webhook config).
//
// A channel is registered WITH an owner (OD16): the external system did the
// user→owner assignment; the host never computes it. A `channels` row holds
// `{ name, type, owner_id, config?, msg_filter? }`. M6 owns the `channels`,
// `channel_bindings`, and `channel_cursors` tables — and ONLY those (it never
// writes the transcript, §9).
//
// Webhooks (outbound egress endpoints) have no table of their own; they live
// under `channel.config.webhooks` and are managed here. Their URLs are
// SSRF-guarded at registration time (see ./webhooks).

import type { WebhookEndpoint } from "./webhooks";
import type { AppDatabase } from "@/db";
import type { Channel } from "@/db/schema";
import { and, desc, eq } from "drizzle-orm";
import { channelBindings, channels } from "@/db/schema";
import { ulid } from "@/platform";
import { NotFoundError } from "@/shared/lib/errors";
import { assertSafeWebhookUrl } from "./webhooks";

export interface RegisterChannelInput {
  name: string;
  type: string;
  /** OD16 — the external system's user→owner assignment; the host stores it verbatim. */
  ownerId: string;
  /** Opaque per-channel config (e.g. default `engine`/`model` for new conversations, webhooks). */
  config?: Record<string, unknown>;
  /** Optional egress kind filter (`{ kinds: [...] }`); absent → deliver all kinds (OD10). */
  filter?: Record<string, unknown>;
}

/** Register a channel (host mints the ULID id). */
export function registerChannel(db: AppDatabase, input: RegisterChannelInput): Channel {
  return db
    .insert(channels)
    .values({
      id: ulid(),
      name: input.name,
      type: input.type,
      ownerId: input.ownerId,
      config: input.config ?? null,
      msgFilter: input.filter ?? null,
    })
    .returning()
    .get();
}

export function getChannel(db: AppDatabase, id: string): Channel | undefined {
  return db.select().from(channels).where(eq(channels.id, id)).get();
}

/** Resolve a `:channel` route ref — by id first, then by (first) matching name. */
export function resolveChannel(db: AppDatabase, ref: string): Channel | undefined {
  return getChannel(db, ref) ?? db.select().from(channels).where(eq(channels.name, ref)).get();
}

/** Like `resolveChannel` but throws the standard 404 for the edge surface. */
export function requireChannel(db: AppDatabase, ref: string): Channel {
  const channel = resolveChannel(db, ref);
  if (!channel) {
    throw new NotFoundError("channel", ref);
  }
  return channel;
}

/** List every channel (admin observe), newest first. */
export function listChannels(db: AppDatabase): Channel[] {
  return db.select().from(channels).orderBy(desc(channels.createdAt)).all();
}

/** List a channel's conversation bindings (admin observe). */
export function listChannelBindings(db: AppDatabase, channelId: string) {
  return db
    .select()
    .from(channelBindings)
    .where(eq(channelBindings.channelId, channelId))
    .orderBy(desc(channelBindings.createdAt))
    .all();
}

// ─── Webhook config (stored under `channel.config.webhooks`) ───

interface ChannelConfig {
  webhooks?: WebhookEndpoint[];
  [k: string]: unknown;
}

/** The outbound webhooks registered on a channel. */
export function listWebhooks(channel: Channel): WebhookEndpoint[] {
  const config = (channel.config ?? {}) as ChannelConfig;
  return Array.isArray(config.webhooks) ? config.webhooks : [];
}

function writeWebhooks(db: AppDatabase, channel: Channel, webhooks: WebhookEndpoint[]): Channel {
  const config = { ...((channel.config ?? {}) as ChannelConfig), webhooks };
  return db.update(channels).set({ config }).where(eq(channels.id, channel.id)).returning().get();
}

export interface AddWebhookInput {
  url: string;
  secret?: string;
}

/** Register an outbound webhook on a channel (URL is SSRF-guarded). Returns the new endpoint. */
export function addWebhook(db: AppDatabase, channelId: string, input: AddWebhookInput): WebhookEndpoint {
  const channel = getChannel(db, channelId);
  if (!channel) {
    throw new NotFoundError("channel", channelId);
  }
  assertSafeWebhookUrl(input.url); // reject SSRF targets at registration
  const endpoint: WebhookEndpoint = { id: ulid(), url: input.url, ...(input.secret ? { secret: input.secret } : {}) };
  writeWebhooks(db, channel, [...listWebhooks(channel), endpoint]);
  return endpoint;
}

/** Remove an outbound webhook from a channel. Returns true if it existed. */
export function removeWebhook(db: AppDatabase, channelId: string, webhookId: string): boolean {
  const channel = getChannel(db, channelId);
  if (!channel) {
    throw new NotFoundError("channel", channelId);
  }
  const before = listWebhooks(channel);
  const after = before.filter(w => w.id !== webhookId);
  if (after.length === before.length) {
    return false;
  }
  writeWebhooks(db, channel, after);
  return true;
}

/** Read a single binding (used by the admin surface). */
export function getBinding(db: AppDatabase, channelId: string, conversationRef: string) {
  return db
    .select()
    .from(channelBindings)
    .where(and(eq(channelBindings.channelId, channelId), eq(channelBindings.conversationRef, conversationRef)))
    .get();
}
