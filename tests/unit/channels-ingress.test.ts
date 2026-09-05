import type { AppDatabase } from "@/db";
import type { AgentTransport } from "@/host/ports/transport";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { enrollAgent, grantOwner } from "@/agents/registry";
import { getBinding, registerChannel, routeInbound } from "@/channels";
import { createDb } from "@/db";
import { messages } from "@/db/schema";
import { getTask } from "@/tasks";

// M6 ingress (OD9/OD16) — route an inbound message: continue a known conversation
// or open a NEW task stamped with the CHANNEL's owner, dispatch it, deliver. The
// host never writes the transcript (storage ⟂ distribution).

const OWNER = "owner-ingress";

let db: AppDatabase;

// Offline AgentTransport — `sendTaskMessage` persists to the outbox without a live
// socket (delivery is replayed on connect), so ingress works with no WS.
const sender: AgentTransport = { isOnline: () => false, send: () => false, closeAgent: () => false };

/** A permitted agent for OWNER on engine `claude` (enrolled is a valid placement candidate). */
function permittedAgent(): string {
  const { agent } = enrollAgent(db, { capabilities: { engines: ["claude"] } });
  grantOwner(db, agent.id, OWNER);
  return agent.id;
}

function chatChannel(owner = OWNER, config: Record<string, unknown> = { engine: "claude", model: "sonnet" }) {
  return registerChannel(db, { name: "chat", type: "http", ownerId: owner, config });
}

beforeEach(() => {
  db = createDb(":memory:");
});

afterEach(() => {
  db.close();
});

describe("M6 ingress — new vs continue", () => {
  it("opens a NEW task stamped with the channel owner on a new conversation_ref (OD9/OD16)", () => {
    const agentId = permittedAgent();
    const ch = chatChannel();

    const result = routeInbound(db, sender, ch, { conversationRef: "conv-1", text: "hello" });
    expect(result.created).toBe(true);
    expect(result.delivered).toBe(true);
    expect(result.dispatch?.placed).toBe(true);
    expect(result.dispatch?.agentId).toBe(agentId);
    expect(result.msgId).not.toBeNull();

    // The task carries the CHANNEL's owner (OD16) + engine/model from config.
    const task = getTask(db, result.taskId)!;
    expect(task.ownerId).toBe(OWNER);
    expect(task.engine).toBe("claude");
    expect(task.model).toBe("sonnet");
    expect(task.kind).toBe("chat");
    expect((task.meta as { conversationRef?: string }).conversationRef).toBe("conv-1");

    // The conversation is bound to the task.
    expect(getBinding(db, ch.id, "conv-1")?.taskId).toBe(result.taskId);
  });

  it("continues the SAME task on a repeat conversation_ref (no new task)", () => {
    permittedAgent();
    const ch = chatChannel();

    const first = routeInbound(db, sender, ch, { conversationRef: "conv-1", text: "hello" });
    const second = routeInbound(db, sender, ch, { conversationRef: "conv-1", text: "again" });
    expect(second.created).toBe(false);
    expect(second.taskId).toBe(first.taskId);
    expect(second.delivered).toBe(true);

    // A different conversation → a different task.
    const other = routeInbound(db, sender, ch, { conversationRef: "conv-2", text: "fresh" });
    expect(other.created).toBe(true);
    expect(other.taskId).not.toBe(first.taskId);
  });

  it("stamps the channel owner regardless of any per-message hint", () => {
    permittedAgent();
    const ch = chatChannel();
    // Even if a caller tried to influence the owner, the channel owner wins (OD16).
    const result = routeInbound(db, sender, ch, { conversationRef: "c", text: "hi", meta: { user: "someone-else" } });
    expect(getTask(db, result.taskId)!.ownerId).toBe(OWNER);
  });
});

describe("M6 ingress — placement + validation", () => {
  it("creates the conversation but leaves it pending when no agent is permitted", () => {
    // No agent granted for this owner → dispatch cannot place.
    const ch = chatChannel("owner-with-no-agent");
    const result = routeInbound(db, sender, ch, { conversationRef: "conv-x", text: "hello" });
    expect(result.created).toBe(true);
    expect(result.delivered).toBe(false);
    expect(result.dispatch?.placed).toBe(false);
    expect(result.msgId).toBeNull();
    // The binding still exists so a later send/assign continues the same conversation.
    expect(getBinding(db, ch.id, "conv-x")?.taskId).toBe(result.taskId);
  });

  it("rejects a new conversation with no engine/model (body or channel config)", () => {
    permittedAgent();
    const ch = chatChannel(OWNER, {}); // no engine/model defaults
    expect(() => routeInbound(db, sender, ch, { conversationRef: "c", text: "hi" })).toThrow();
    // …but an explicit per-message engine/model succeeds.
    const ok = routeInbound(db, sender, ch, { conversationRef: "c2", text: "hi", engine: "claude", model: "sonnet" });
    expect(ok.created).toBe(true);
  });
});

describe("M6 ingress — storage ⟂ distribution", () => {
  it("never writes the transcript (the message rides the outbox, not messages)", () => {
    permittedAgent();
    const ch = chatChannel();
    routeInbound(db, sender, ch, { conversationRef: "conv-1", text: "hello" });
    routeInbound(db, sender, ch, { conversationRef: "conv-1", text: "again" });
    // Channels never author the transcript — that is the agent's job via M5.
    expect(db.select().from(messages).all()).toHaveLength(0);
  });
});
