import type { WsLike } from "../../src/agents/sessions";
import { describe, expect, it } from "bun:test";
import { RemoteOps } from "../../src/agents/remote";
import { SessionRegistry } from "../../src/agents/sessions";
import { sha256Hex } from "../../src/configstore/store";
import { AppError } from "../../src/shared/lib/errors";
import { memoryStorage } from "../../src/store/memory";

// Engine-free agent operations — the host side of the live-only
// `fs_request` / `fs_response` pair: correlate by `reqId`, never queue.

function fakeWs(): WsLike & { sent: Array<{ type: string; reqId: string; [key: string]: unknown }> } {
  const sent: Array<{ type: string; reqId: string; [key: string]: unknown }> = [];
  return {
    sent,
    send: (data: string) => {
      sent.push(JSON.parse(data));
    },
    close: () => {},
  };
}

function setup(timeoutMs = 1_000) {
  const sessions = new SessionRegistry();
  const { blobs } = memoryStorage();
  const remote = new RemoteOps(sessions, blobs, { timeoutMs, transferTimeoutMs: timeoutMs });
  sessions.onDrop = agentId => remote.dropAgent(agentId);
  return { sessions, remote, blobs };
}

async function rejection(promise: Promise<unknown>): Promise<AppError> {
  try {
    await promise;
  }
  catch (err) {
    expect(err).toBeInstanceOf(AppError);
    return err as AppError;
  }
  throw new Error("expected a rejection");
}

describe("remote ops — fs request/response", () => {
  it("sends an fs_request and resolves with the matching fs_response data", async () => {
    const { sessions, remote } = setup();
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const pending = remote.fs("a1", { op: "stat", path: "notes.txt" });
    expect(ws.sent).toHaveLength(1);
    const frame = ws.sent[0]!;
    expect(frame.type).toBe("fs_request");
    expect(frame.data).toEqual({ op: "stat", path: "notes.txt" });
    expect(typeof frame.reqId).toBe("string");
    // Live-only: no delivery ids, so nothing acks or replays it.
    expect(frame.msgId).toBeUndefined();
    expect(frame.outboxId).toBeUndefined();

    remote.resolve("a1", frame.reqId, { ok: true, stat: { type: "file", size: 2 } });
    expect(await pending).toEqual({ ok: true, stat: { type: "file", size: 2 } });
  });

  it("passes an agent-side refusal through as data", async () => {
    const { sessions, remote } = setup();
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const pending = remote.fs("a1", { op: "list", path: "." });
    remote.resolve("a1", ws.sent[0]!.reqId, { ok: false, error: { code: "disabled", message: "off" } });
    expect(await pending).toEqual({ ok: false, error: { code: "disabled", message: "off" } });
  });

  it("refuses an offline agent at once", async () => {
    const { remote } = setup();
    const err = await rejection(remote.fs("a1", { op: "list", path: "." }));
    expect(err.statusCode).toBe(409);
    expect(err.code).toBe("AGENT_OFFLINE");
  });

  it("times out when the agent never answers", async () => {
    const { sessions, remote } = setup(10);
    sessions.attach("a1", fakeWs());
    const err = await rejection(remote.fs("a1", { op: "list", path: "." }));
    expect(err.statusCode).toBe(504);
    expect(err.code).toBe("AGENT_TIMEOUT");
  });

  it("ignores an unknown reqId and a response from another agent", async () => {
    const { sessions, remote } = setup(30);
    const ws = fakeWs();
    sessions.attach("a1", ws);
    sessions.attach("a2", fakeWs());

    const pending = remote.fs("a1", { op: "list", path: "." });
    remote.resolve("a1", "no-such-request", { ok: true });
    remote.resolve("a2", ws.sent[0]!.reqId, { ok: true, entries: [] });
    // Neither settled it: the request still runs into its timeout.
    expect((await rejection(pending)).code).toBe("AGENT_TIMEOUT");
  });

  it("rejects a malformed response", async () => {
    const { sessions, remote } = setup();
    const ws = fakeWs();
    sessions.attach("a1", ws);
    const pending = remote.fs("a1", { op: "list", path: "." });
    remote.resolve("a1", ws.sent[0]!.reqId, { nope: 1 });
    expect((await rejection(pending)).code).toBe("BAD_AGENT_RESPONSE");
  });

  it("rejects every pending request when the agent's session drops", async () => {
    for (const drop of [
      (s: SessionRegistry, ws: WsLike) => s.detach("a1", ws),
      (s: SessionRegistry) => s.closeAgent("a1", 1008, "removed"),
      (s: SessionRegistry) => s.closeAll(1001, "shutdown"),
      (s: SessionRegistry) => s.attach("a1", fakeWs()),
    ]) {
      const { sessions, remote } = setup();
      const ws = fakeWs();
      sessions.attach("a1", ws);
      const pending = remote.fs("a1", { op: "list", path: "." });
      drop(sessions, ws);
      const err = await rejection(pending);
      expect(err.code).toBe("AGENT_DISCONNECTED");
    }
  });

  it("a superseded socket's late detach does not reject the live session's requests", async () => {
    const { sessions, remote } = setup();
    const stale = fakeWs();
    const fresh = fakeWs();
    sessions.attach("a1", stale);
    sessions.attach("a1", fresh);

    const pending = remote.fs("a1", { op: "list", path: "." });
    sessions.detach("a1", stale);
    remote.resolve("a1", fresh.sent[0]!.reqId, { ok: true, entries: [] });
    expect(await pending).toEqual({ ok: true, entries: [] });
  });
});

describe("remote ops — file transfers over the byte channel", () => {
  const bytes = new TextEncoder().encode("hello from the host\n");

  it("putFile stages the bytes behind a one-shot grant for that agent only, then removes them", async () => {
    const { sessions, remote, blobs } = setup();
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const pending = remote.putFile("a1", "dir/note.txt", bytes);
    const frame = ws.sent[0]! as unknown as { reqId: string; data: { op: string; path: string; ref: string; sha256: string; size: number } };
    expect(frame.data).toMatchObject({ op: "put", path: "dir/note.txt", sha256: sha256Hex(bytes), size: bytes.byteLength });
    const id = frame.data.ref.replace("/agent/attachments/", "");
    expect(frame.data.ref).toBe(`/agent/attachments/${id}`);

    // The agent it was minted for can fetch it; any other agent cannot.
    expect(remote.grantedBytes(id, "a1")).toEqual(bytes);
    expect(() => remote.grantedBytes(id, "a2")).toThrow(AppError);
    expect(remote.grantedBytes("unknown", "a1")).toBeNull();

    remote.resolve("a1", frame.reqId, { ok: true });
    expect(await pending).toEqual({ ok: true });
    expect(remote.grantedBytes(id, "a1")).toBeNull();
    expect(blobs.listLeaves()).toEqual([]);
  });

  it("putFile removes the staged bytes when the agent refuses or never answers", async () => {
    const { sessions, remote, blobs } = setup(10);
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const refused = remote.putFile("a1", "f", bytes);
    remote.resolve("a1", ws.sent[0]!.reqId, { ok: false, error: { code: "denied", message: "no" } });
    expect(await refused).toEqual({ ok: false, error: { code: "denied", message: "no" } });
    expect(blobs.listLeaves()).toEqual([]);

    expect((await rejection(remote.putFile("a1", "f", bytes))).code).toBe("AGENT_TIMEOUT");
    expect(blobs.listLeaves()).toEqual([]);
  });

  it("getFile returns the bytes the agent uploaded under the transfer name, then removes them", async () => {
    const { sessions, remote, blobs } = setup();
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const pending = remote.getFile("a1", "dir/note.txt");
    const frame = ws.sent[0]! as unknown as { reqId: string; data: { op: string; path: string; upload: string } };
    expect(frame.data).toMatchObject({ op: "get", path: "dir/note.txt" });
    // The agent uploads over the byte channel under the host-minted name…
    blobs.put(frame.data.upload, bytes);
    remote.resolve("a1", frame.reqId, { ok: true, sha256: sha256Hex(bytes), size: bytes.byteLength });

    const result = await pending;
    expect(result).toEqual({ ok: true, bytes, sha256: sha256Hex(bytes), size: bytes.byteLength });
    expect(blobs.listLeaves()).toEqual([]);
  });

  it("getFile passes a refusal through and fails when the upload is missing", async () => {
    const { sessions, remote, blobs } = setup();
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const refused = remote.getFile("a1", "nope");
    remote.resolve("a1", ws.sent[0]!.reqId, { ok: false, error: { code: "not_found", message: "no" } });
    expect(await refused).toEqual({ ok: false, error: { code: "not_found", message: "no" } });

    const missing = remote.getFile("a1", "f");
    remote.resolve("a1", ws.sent[1]!.reqId, { ok: true, sha256: sha256Hex(bytes), size: bytes.byteLength });
    expect((await rejection(missing)).code).toBe("BAD_AGENT_RESPONSE");
    expect(blobs.listLeaves()).toEqual([]);
  });

  it("getFile sweeps an upload left behind by a request that timed out", async () => {
    const { sessions, remote, blobs } = setup(10);
    const ws = fakeWs();
    sessions.attach("a1", ws);

    const pending = remote.getFile("a1", "f");
    blobs.put((ws.sent[0]!.data as { upload: string }).upload, bytes);
    expect((await rejection(pending)).code).toBe("AGENT_TIMEOUT");
    expect(blobs.listLeaves()).toEqual([]);
  });
});
