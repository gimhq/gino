// M6 — INGRESS (route, OD9/OD16). Route an inbound user message to a conversation
// and hand it to M4. Channels NEVER write the transcript — ingress only resolves
// `channel_bindings` and calls the M4 orchestrator; the agent's reply lands in M5
// later and egress tails it back out (storage ⟂ distribution, §9).
//
// Routing (OD9): a `(channel, conversation_ref)` binding maps a conversation to a
// task. Continuing a known conversation → its existing task (continue its ongoing
// thread). A NEW conversation → a NEW task (OD9) stamped with the CHANNEL's
// `owner_id` (OD16 — the host never computes user→owner; the external system
// supplied it at registration) → M4 dispatch (owner-pinned, §3) → deliver the
// message. The old "group" delivery role is dissolved into this routing.
import type { AgentTransport, AttachmentDescriptor, ChannelRow, HostEvents, Storage } from "@gino/host-core";
import type { DispatchResult } from "@gino/host-core/domain";
import { ValidationError } from "@gino/host-core";
import { createTask, getTask, placeTask, requireTask, sendTaskMessage } from "@gino/host-core/domain";
import { requireChannels } from "./registry";

export interface InboundMessage {
  /** Conversation identity within the channel (`channel_bindings.conversation_ref`). */
  conversationRef: string;
  text: string;
  attachments?: AttachmentDescriptor[];
  /** Per-conversation overrides for a NEW task; else fall back to the channel's `config` defaults. */
  engine?: string;
  model?: string;
  workdir?: string;
  meta?: Record<string, unknown>;
}

export interface RouteResult {
  taskId: string;
  conversationRef: string;
  /** The thread the message was delivered on (the conversation's ongoing thread). */
  threadId: string;
  /** True if this inbound opened a NEW conversation (a new task), false if it continued one. */
  created: boolean;
  /** Whether the (new) task was placed on a permitted agent and the message delivered. */
  delivered: boolean;
  /** The `message` outbox delivery id (null if the new task is pending-assignment). */
  msgId: number | null;
  /** Dispatch outcome for a NEW conversation (absent when continuing). */
  dispatch?: DispatchResult;
}

/** Bind a conversation to a task (idempotent on `(channel, conversation_ref)`). */
export function bindConversation(storage: Storage, channelId: string, conversationRef: string, taskId: string): void {
  requireChannels(storage).bindIfAbsent({ channelId, conversationRef, taskId });
}

interface NewTaskShape {
  engine: string;
  model: string;
  workdir?: string;
  meta?: Record<string, unknown>;
}

/** Engine/model for a NEW conversation: request override → channel `config` default (else 422). */
function resolveNewTaskShape(channel: ChannelRow, input: InboundMessage): NewTaskShape {
  const config = (channel.config ?? {}) as { engine?: unknown; model?: unknown; workdir?: unknown; meta?: unknown };
  const engine = input.engine ?? (typeof config.engine === "string" ? config.engine : undefined);
  const model = input.model ?? (typeof config.model === "string" ? config.model : undefined);
  if (!engine || !model) {
    throw new ValidationError("a new conversation needs an engine + model", {
      formErrors: ["set engine + model on the send body or in the channel config"],
      fieldErrors: {},
    });
  }
  const workdir = input.workdir ?? (typeof config.workdir === "string" ? config.workdir : undefined);
  const meta = input.meta ?? (config.meta && typeof config.meta === "object" ? config.meta as Record<string, unknown> : undefined);
  return { engine, model, ...(workdir != null ? { workdir } : {}), ...(meta != null ? { meta } : {}) };
}

/**
 * Route an inbound message (OD9/OD16). Continuing a known conversation delivers
 * to its existing task; a new conversation creates a task stamped with the
 * channel owner, binds the conversation, dispatches to a permitted agent, and
 * delivers. If no agent is permitted the new task stays `pending-assignment` and
 * the message is NOT delivered yet (`delivered: false`) — the binding persists so
 * a later send/assign continues the same conversation.
 */
export function routeInbound(storage: Storage, sender: AgentTransport, channel: ChannelRow, input: InboundMessage, events?: HostEvents): RouteResult {
  if (!input.conversationRef) {
    throw new ValidationError("conversation_ref is required", { formErrors: ["conversation_ref is required"], fieldErrors: {} });
  }

  const existing = requireChannels(storage).getBinding(channel.id, input.conversationRef);
  if (existing) {
    // CONTINUE — the conversation already maps to a task (OD9).
    const task = requireTask(storage, existing.taskId);
    const sent = sendTaskMessage(storage, sender, task, {
      text: input.text,
      ...(input.attachments && input.attachments.length > 0 ? { attachments: input.attachments } : {}),
    });
    return {
      taskId: task.id,
      conversationRef: input.conversationRef,
      threadId: sent.threadId,
      created: false,
      delivered: true,
      msgId: sent.message.msgId ?? null,
    };
  }

  // NEW conversation → NEW task stamped with the channel owner (OD16).
  const shape = resolveNewTaskShape(channel, input);
  const task = createTask(storage, {
    ownerId: channel.ownerId,
    engine: shape.engine,
    model: shape.model,
    kind: "chat",
    ...(shape.workdir != null ? { workdir: shape.workdir } : {}),
    meta: {
      ...(shape.meta ?? {}),
      channel: channel.id,
      channelType: channel.type,
      conversationRef: input.conversationRef,
    },
  });
  bindConversation(storage, channel.id, input.conversationRef, task.id);

  // M4 dispatch (owner-pinned, §3).
  const dispatch = placeTask(storage, task.id, events);
  if (!dispatch.placed) {
    return {
      taskId: task.id,
      conversationRef: input.conversationRef,
      threadId: getTask(storage, task.id)?.currentThreadId ?? "",
      created: true,
      delivered: false,
      msgId: null,
      dispatch,
    };
  }

  const placed = requireTask(storage, task.id);
  const sent = sendTaskMessage(storage, sender, placed, {
    text: input.text,
    ...(input.attachments && input.attachments.length > 0 ? { attachments: input.attachments } : {}),
  });
  return {
    taskId: placed.id,
    conversationRef: input.conversationRef,
    threadId: sent.threadId,
    created: true,
    delivered: true,
    msgId: sent.message.msgId ?? null,
    dispatch,
  };
}
