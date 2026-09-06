import type { HostContext, WsLike } from "../../src/host";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createDb, sqliteStorage } from "@gino/host-store-sqlite";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { getAgent } from "../../src/agents/registry";
import { createHostContext } from "../../src/host";
import { noopLogger } from "../../src/logger";
import { writeAttachmentDescriptors } from "../../src/msgstore";
import { enqueue } from "../../src/protocol/outbox";
import { ForbiddenError, NotFoundError } from "../../src/shared/lib/errors";
import { createTask, setPlacement } from "../../src/tasks";

// HOST WIRE — the framework-free `/agent/*` data plane (host-sdk-design §3.2).
// Exercises `host.wire` directly against a fresh context: register envelopes,
// the audited WS lifecycle ordering (HOST-M3-02/08), the HTTP frame fallback
// (HOST-M1M2-09) and the byte channel's 403/404/304 policy (HOST-M8-14).

const REGISTER = "reg-secret-wire";
const logger = noopLogger;

interface FakeWs extends WsLike {
  sent: string[];
  closed: Array<{ code?: number | undefined; reason?: string | undefined }>;
}

function fakeWs(): FakeWs {
  const ws: FakeWs = {
    sent: [],
    closed: [],
    send: (data) => {
      ws.sent.push(data);
    },
    close: (code, reason) => {
      ws.closed.push({ code, reason });
    },
  };
  return ws;
}

let storeDir: string;
let host: HostContext;
let events: string[];

function makeHost(options: Record<string, unknown> = {}): HostContext {
  const db = createDb(":memory:");
  return createHostContext({
    logger,
    storage: sqliteStorage(db, { storeDir }),
    options: { registerToken: REGISTER, ...options },
  });
}

function registerAgent(): { agentId: string; connectToken: string } {
  const res = host.wire.register({ registerToken: REGISTER, protocolVersion: 2, capabilities: ["claude"] }, { bearer: null });
  expect(res.status).toBe(200);
  return res.body as { agentId: string; connectToken: string };
}

function placeTaskOn(agentId: string): string {
  const task = createTask(host.storage, { ownerId: `owner-${agentId}`, engine: "claude", model: "sonnet" });
  setPlacement(host.storage, task.id, agentId);
  return task.id;
}

beforeEach(() => {
  storeDir = mkdtempSync(join(tmpdir(), "gino-wire-"));
  host = makeHost();
  events = [];
  host.events.on("agent.online", () => events.push("agent.online"));
  host.events.on("agent.offline", () => events.push("agent.offline"));
});

afterEach(() => {
  host.stop();
  rmSync(storeDir, { recursive: true, force: true });
});

describe("wire.register — protocol §3 envelopes", () => {
  it("401 invalid_register_token on a missing or wrong token (body or bearer)", () => {
    expect(host.wire.register({ protocolVersion: 2 }, { bearer: null })).toEqual({ status: 401, body: { register_error: { reason: "invalid_register_token" } } });
    expect(host.wire.register({ registerToken: "nope", protocolVersion: 2 }, { bearer: null }).status).toBe(401);
    expect(host.wire.register({ protocolVersion: 2 }, { bearer: "nope" }).status).toBe(401);
  });

  it("401 fails CLOSED when no register token is configured", () => {
    const bare = makeHost({ registerToken: "" });
    expect(bare.wire.register({ registerToken: "", protocolVersion: 2 }, { bearer: null }).status).toBe(401);
    bare.stop();
  });

  it("403 registration_disabled", () => {
    const closed = makeHost({ registrationEnabled: false });
    const res = closed.wire.register({ registerToken: REGISTER, protocolVersion: 2 }, { bearer: null });
    expect(res).toEqual({ status: 403, body: { register_error: { reason: "registration_disabled" } } });
    closed.stop();
  });

  it("400 invalid_body on a non-object body (token via bearer), 400 unsupported_protocol on a stale major", () => {
    expect(host.wire.register(null, { bearer: REGISTER })).toEqual({ status: 400, body: { register_error: { reason: "invalid_body" } } });
    expect(host.wire.register([1], { bearer: REGISTER }).body).toEqual({ register_error: { reason: "invalid_body" } });
    expect(host.wire.register({ registerToken: REGISTER, protocolVersion: 1 }, { bearer: null })).toEqual({ status: 400, body: { register_error: { reason: "unsupported_protocol" } } });
  });

  it("200 with agentId + connectToken + the host config mirror", () => {
    const res = host.wire.register({ registerToken: REGISTER, protocolVersion: 2, capabilities: ["claude"], maxThreads: 3 }, { bearer: null });
    expect(res.status).toBe(200);
    const body = res.body as { agentId: string; connectToken: string; config: Record<string, unknown> };
    expect(body.connectToken.length).toBeGreaterThan(0);
    expect(body.config).toMatchObject({
      agentId: body.agentId,
      wsUrl: `/agent/ws/${body.agentId}`,
      configUrl: `/agent/config/${body.agentId}`,
      outputUrl: `/agent/output/${body.agentId}`,
      heartbeatIntervalMs: host.options.heartbeatIntervalMs,
      protocolVersion: 2,
      entries: [],
    });
    expect(typeof body.config.version).toBe("number");
    expect(getAgent(host.storage, body.agentId)?.capabilities).toEqual({ engines: ["claude"], max_threads: 3 });

    expect(host.wire.resolveAgent(body.connectToken)).toEqual({ agentId: body.agentId });
    expect(host.wire.resolveAgent("bogus")).toBeNull();
    expect(host.wire.authorizeAgent(body.agentId, body.connectToken)).toBe(true);
    expect(host.wire.authorizeAgent(body.agentId, "bogus")).toBe(false);
    expect(host.wire.configFor(body.agentId)).toEqual({ status: 200, body: body.config });
  });
});

describe("wire socket lifecycle — attach / supersede / detach (HOST-M3-02/08)", () => {
  it("attachSocket: online + lastSeen stamped + agent.online + unacked outbox replayed in order", () => {
    const { agentId } = registerAgent();
    enqueue(host.storage, host.sessions, { agentId, type: "task_assign", data: { n: 1 } });
    enqueue(host.storage, host.sessions, { agentId, type: "task_assign", data: { n: 2 } });

    const ws = fakeWs();
    host.wire.attachSocket(agentId, ws);

    const agent = getAgent(host.storage, agentId);
    expect(agent?.status).toBe("online");
    expect(agent?.lastSeen).not.toBeNull();
    expect(events).toEqual(["agent.online"]);
    expect(host.sessions.isOnline(agentId)).toBe(true);
    const frames = ws.sent.map(raw => JSON.parse(raw) as { type: string; data: { n: number } });
    expect(frames.map(f => f.type)).toEqual(["task_assign", "task_assign"]);
    expect(frames.map(f => f.data.n)).toEqual([1, 2]);
  });

  it("a second attach supersedes (1012); detaching the OLD socket keeps the agent online, detaching the current flips it offline", () => {
    const { agentId } = registerAgent();
    const first = fakeWs();
    const second = fakeWs();
    host.wire.attachSocket(agentId, first);
    host.wire.attachSocket(agentId, second);
    expect(first.closed).toEqual([{ code: 1012, reason: "superseded by reconnect" }]);

    host.wire.detachSocket(agentId, first);
    expect(getAgent(host.storage, agentId)?.status).toBe("online");
    expect(host.sessions.isOnline(agentId)).toBe(true);
    expect(events).toEqual(["agent.online", "agent.online"]);

    host.wire.detachSocket(agentId, second);
    expect(getAgent(host.storage, agentId)?.status).toBe("offline");
    expect(host.sessions.isOnline(agentId)).toBe(false);
    expect(events).toEqual(["agent.online", "agent.online", "agent.offline"]);
  });
});

describe("wire.ingestHttpFrame — HTTP fallback (HOST-M1M2-09)", () => {
  it("400 BAD_FRAME on garbage; 200 ack on a valid heartbeat", () => {
    const { agentId } = registerAgent();
    const bad = host.wire.ingestHttpFrame(agentId, "{not json");
    expect(bad.status).toBe(400);
    expect(bad.body).toEqual({ success: false, error: { code: "BAD_FRAME", message: "malformed frame" } });

    const ok = host.wire.ingestHttpFrame(agentId, JSON.stringify({ type: "heartbeat", data: { activeThreads: 0 } }));
    // A heartbeat carries no frame-level ack — the fallback answers `{ ok: true }` (protocol §2).
    expect(ok).toEqual({ status: 200, body: { ok: true } });
    expect(getAgent(host.storage, agentId)?.lastSeen).not.toBeNull();
  });
});

describe("wire.attachments — byte channel policy (HOST-M8-12/14)", () => {
  it("serveById: 404 unknown, 403 foreign task, 200 verified bytes with etag, 304 on If-None-Match", () => {
    const a = registerAgent();
    const b = registerAgent();
    const taskId = placeTaskOn(a.agentId);
    const bytes = new TextEncoder().encode("the quick brown fox");
    const stored = host.wire.attachments.ingest(a.agentId, {
      name: "report.txt",
      taskId,
      threadId: "th1",
      contentType: "text/plain",
      direction: "outbound",
      bytes,
    });
    expect(stored.status).toBe(201);
    const descriptor = stored.body as { id: string; sha256: string; ref: string };
    expect(descriptor.ref).toBe(`/agent/attachments/${descriptor.id}`);

    expect(() => host.wire.attachments.serveById(a.agentId, "att_unknown", {})).toThrow(NotFoundError);
    expect(() => host.wire.attachments.serveById(b.agentId, descriptor.id, {})).toThrow(ForbiddenError);

    const res = host.wire.attachments.serveById(a.agentId, descriptor.id, {});
    expect(res.status).toBe(200);
    expect(res.headers).toMatchObject({
      "content-type": "text/plain",
      "content-length": String(bytes.byteLength),
      "etag": `"${descriptor.sha256}"`,
      "x-content-sha256": descriptor.sha256,
    });
    expect(new TextDecoder().decode(res.body as Uint8Array)).toBe("the quick brown fox");

    const cached = host.wire.attachments.serveById(a.agentId, descriptor.id, { ifNoneMatch: `"${descriptor.sha256}"` });
    expect(cached.status).toBe(304);
    expect(cached.body).toBeNull();
    expect(cached.headers?.etag).toBe(`"${descriptor.sha256}"`);

    const bySha = host.wire.attachments.serveBySha(a.agentId, descriptor.sha256, {});
    expect(bySha.status).toBe(200);
    expect(() => host.wire.attachments.serveBySha(b.agentId, descriptor.sha256, {})).toThrow(ForbiddenError);
  });

  it("ingest: 400 without a name, 400 on a half-bound task/thread, 403 on a foreign task, 400 on a sha mismatch", () => {
    const a = registerAgent();
    const b = registerAgent();
    const foreign = placeTaskOn(b.agentId);
    const bytes = new TextEncoder().encode("x");
    const base = { contentType: "text/plain", direction: "outbound" as const, bytes };
    expect(host.wire.attachments.ingest(a.agentId, { ...base, name: undefined }).status).toBe(400);
    expect(host.wire.attachments.ingest(a.agentId, { ...base, name: "a.txt", taskId: foreign }).status).toBe(400);
    expect(() => host.wire.attachments.ingest(a.agentId, { ...base, name: "a.txt", taskId: foreign, threadId: "th1" })).toThrow(ForbiddenError);
    const mismatch = host.wire.attachments.ingest(a.agentId, { ...base, name: "a.txt", declaredSha: "00" });
    expect(mismatch.status).toBe(400);
    expect((mismatch.body as { error: { code: string } }).error.code).toBe("SHA_MISMATCH");
  });

  it("serveById: a dangling descriptor whose bytes are gone is 404, never 500 (HOST-M8-12)", () => {
    const a = registerAgent();
    const taskId = placeTaskOn(a.agentId);
    writeAttachmentDescriptors(host.storage, {
      taskId,
      threadId: "th1",
      descriptors: [{ id: "att_dangling", name: "gone.bin", content_type: "application/octet-stream", size: 1, sha256: "ab".repeat(32), ref: "/agent/attachments/att_dangling" }],
    });
    expect(() => host.wire.attachments.serveById(a.agentId, "att_dangling", {})).toThrow(NotFoundError);
  });
});
