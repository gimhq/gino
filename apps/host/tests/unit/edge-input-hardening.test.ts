import type { Storage } from "@gino/host-core";
import type { AppDatabase } from "@gino/host-store-sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registerChannel } from "@gino/host-channels";
import { createTask, enrollAgent, grantOwner, setPlacement } from "@gino/host-core/domain";
import { sqliteStorage } from "@gino/host-store-sqlite";
import { createDb } from "@gino/host-store-sqlite/bun";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createApp, createAppHost } from "@/app";
import { loadConfigStrict } from "@/config";
import { createLogger } from "@/shared/lib/logger";

// Edge input hardening regressions:
//   Byte-ingest endpoints enforce MAX_UPLOAD_BYTES (413), on the
//                  declared Content-Length AND the actual streamed size.
//   `attachments` validated against the wire AttachmentDescriptor
//                  schema at both message-bearing edges (422 on mismatch).
//   Every register failure renders the protocol §3
//                  `register_error` envelope (all five reasons reachable;
//                  `rate_limited` is additionally covered by rate-limit.test.ts).
//   GET /:channel/poll validates its query with the same zod
//                  contract as the POST body (garbage → 422, never NaN).

const REGISTER = "reg-secret-hardening";
const CHANNEL = "chan-secret-hardening";
const OWNER = "owner-hardening";
const MAX_UPLOAD = 1024;

let storeDir: string;
let db: AppDatabase;
let storage: Storage;
let app: ReturnType<typeof createApp>;

function makeApp(env: Record<string, string> = {}) {
  const config = loadConfigStrict({
    NODE_ENV: "test",
    GINO_STORE_DIR: storeDir,
    GINO_REGISTER_TOKEN: REGISTER,
    GINO_HTTP_CHANNEL_TOKEN: CHANNEL,
    // Explicit same-secret reuse so one bearer drives both surfaces in this test.
    GINO_HTTP_ADMIN_TOKEN: CHANNEL,
    GINO_LOG_LEVEL: "error",
    GINO_LOG_TO_STDOUT: "true",
    GINO_MAX_UPLOAD_BYTES: String(MAX_UPLOAD),
    ...env,
  });
  return createApp({ config, host: createAppHost({ config, db, logger: createLogger(config) }) });
}

beforeAll(() => {
  storeDir = mkdtempSync(join(tmpdir(), "gino-hardening-"));
  db = createDb(":memory:");
  storage = sqliteStorage(db);
  app = makeApp();
});

afterAll(() => {
  db.close();
  rmSync(storeDir, { recursive: true, force: true });
});

// ── helpers ──────────────────────────────────────────────────────────────────

function registerReq(body: BodyInit, headers: Record<string, string> = {}) {
  return new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json", ...headers },
    body,
  });
}

async function registerAgent(): Promise<{ agentId: string; connectToken: string }> {
  const res = await app.fetch(registerReq(JSON.stringify({ registerToken: REGISTER, protocolVersion: 1, source: "manual", capabilities: ["claude"] })));
  expect(res.status).toBe(200);
  return await res.json() as { agentId: string; connectToken: string };
}

function placeTaskOn(agentId: string): string {
  const task = createTask(storage, { ownerId: OWNER, engine: "claude", model: "sonnet" });
  setPlacement(storage, task.id, agentId);
  return task.id;
}

function channelHeaders() {
  return { "authorization": `Bearer ${CHANNEL}`, "content-type": "application/json" };
}

// ── register_error envelope on every failure path ─────────────

describe("HOST-M1M2-14 — register failures use the protocol §3 register_error envelope", () => {
  it("401 register_error{invalid_register_token} on a wrong token", async () => {
    const res = await app.fetch(registerReq(JSON.stringify({ registerToken: "nope", protocolVersion: 1 })));
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ register_error: { reason: "invalid_register_token" } });
  });

  it("401 register_error{invalid_register_token} when no token is supplied at all", async () => {
    const res = await app.fetch(registerReq(JSON.stringify({ protocolVersion: 1 })));
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ register_error: { reason: "invalid_register_token" } });
  });

  it("fails closed with the envelope when the expected token is unconfigured", async () => {
    const bare = makeApp({ GINO_REGISTER_TOKEN: "" });
    const res = await bare.fetch(registerReq(JSON.stringify({ registerToken: "", protocolVersion: 1 })));
    expect(res.status).toBe(401);
    expect(await res.json()).toEqual({ register_error: { reason: "invalid_register_token" } });
  });

  it("400 register_error{invalid_body} on an unparseable body (token via bearer fallback)", async () => {
    const res = await app.fetch(registerReq("this is not json {{{", { authorization: `Bearer ${REGISTER}` }));
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ register_error: { reason: "invalid_body" } });
  });

  it("400 register_error{invalid_body} on a non-object JSON body", async () => {
    const res = await app.fetch(registerReq(JSON.stringify([1, 2, 3]), { authorization: `Bearer ${REGISTER}` }));
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ register_error: { reason: "invalid_body" } });
  });

  it("400 register_error{unsupported_protocol} on a version-gate failure", async () => {
    const res = await app.fetch(registerReq(JSON.stringify({ registerToken: REGISTER, protocolVersion: 2 })));
    expect(res.status).toBe(400);
    expect(await res.json()).toEqual({ register_error: { reason: "unsupported_protocol" } });
  });

  it("403 register_error{registration_disabled} when GINO_REGISTER_ENABLED=false (valid token)", async () => {
    const disabled = makeApp({ GINO_REGISTER_ENABLED: "false" });
    const res = await disabled.fetch(registerReq(JSON.stringify({ registerToken: REGISTER, protocolVersion: 1 })));
    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ register_error: { reason: "registration_disabled" } });
  });

  it("429 register_error{rate_limited} once the register budget is exhausted", async () => {
    const throttled = makeApp({ GINO_RATE_LIMIT_REGISTER_MAX: "1", GINO_RATE_LIMIT_REGISTER_WINDOW_MS: "60000" });
    const first = await throttled.fetch(registerReq(JSON.stringify({ registerToken: REGISTER, protocolVersion: 1, capabilities: ["claude"] })));
    expect(first.status).toBe(200);
    const over = await throttled.fetch(registerReq(JSON.stringify({ registerToken: REGISTER, protocolVersion: 1, capabilities: ["claude"] })));
    expect(over.status).toBe(429);
    expect(await over.json()).toEqual({ register_error: { reason: "rate_limited" } });
  });
});

// ── upload size cap on the byte-ingest endpoints ───────────────

describe("HOST-M1M2-11 — POST /agent/attachments enforces MAX_UPLOAD_BYTES", () => {
  let connectToken: string;
  let taskId: string;

  beforeAll(async () => {
    const enrolled = await registerAgent();
    connectToken = enrolled.connectToken;
    taskId = placeTaskOn(enrolled.agentId);
  });

  function ingest(body: BodyInit, headers: Record<string, string> = {}) {
    return app.fetch(new Request(`http://local/api/agent/attachments?name=blob.bin&task_id=${taskId}&thread_id=th-1`, {
      method: "POST",
      headers: { "x-internal-token": connectToken, ...headers },
      body,
    }));
  }

  it("accepts a body under the cap", async () => {
    const res = await ingest("small payload");
    expect(res.status).toBe(201);
  });

  it("413 on an actual body over the cap", async () => {
    const res = await ingest("x".repeat(MAX_UPLOAD + 1));
    expect(res.status).toBe(413);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("PAYLOAD_TOO_LARGE");
  });

  it("413 on an oversize DECLARED Content-Length (tiny actual body)", async () => {
    const res = await ingest("tiny", { "content-length": String(MAX_UPLOAD * 1000) });
    expect(res.status).toBe(413);
  });

  it("413 on a LYING small Content-Length hiding an oversize body", async () => {
    const res = await ingest("x".repeat(MAX_UPLOAD * 4), { "content-length": "10" });
    expect(res.status).toBe(413);
  });
});

describe("HOST-M1M2-11 — POST /admin/config/artifacts enforces MAX_UPLOAD_BYTES", () => {
  function upload(body: BodyInit, headers: Record<string, string> = {}) {
    return app.fetch(new Request("http://local/api/admin/config/artifacts?name=skill.md&kind=file", {
      method: "POST",
      headers: { authorization: `Bearer ${CHANNEL}`, ...headers },
      body,
    }));
  }

  it("accepts an artifact under the cap", async () => {
    const res = await upload("# a small skill");
    expect(res.status).toBe(201);
  });

  it("413 on an actual body over the cap", async () => {
    const res = await upload("x".repeat(MAX_UPLOAD + 1));
    expect(res.status).toBe(413);
    const body = await res.json() as { error: { code: string } };
    expect(body.error.code).toBe("PAYLOAD_TOO_LARGE");
  });

  it("413 on an oversize DECLARED Content-Length (tiny actual body)", async () => {
    const res = await upload("tiny", { "content-length": String(MAX_UPLOAD * 1000) });
    expect(res.status).toBe(413);
  });

  it("413 on a LYING small Content-Length hiding an oversize body", async () => {
    const res = await upload("x".repeat(MAX_UPLOAD * 4), { "content-length": "10" });
    expect(res.status).toBe(413);
  });
});

// ── attachment descriptors validated at both edges ────────────

const VALID_DESCRIPTOR = {
  id: "att_1",
  name: "file.txt",
  content_type: "text/plain",
  size: 4,
  sha256: "a".repeat(64),
  ref: "store/file.txt/aaaaaaaaaaaa",
};

describe("HOST-M1M2-12 — attachments validated against the wire schema", () => {
  beforeAll(() => {
    // A routable channel: an owner with a permitted (granted) agent.
    const { agent } = enrollAgent(storage, { capabilities: { engines: ["claude"] } });
    grantOwner(storage, agent.id, OWNER);
    registerChannel(storage, { name: "hardening-chat", type: "http", ownerId: OWNER, config: { engine: "claude", model: "sonnet" } });
  });

  function send(attachments: unknown) {
    return app.fetch(new Request("http://local/api/messages/hardening-chat/send", {
      method: "POST",
      headers: channelHeaders(),
      body: JSON.stringify({ conversation_ref: "conv-h1", text: "hello", attachments }),
    }));
  }

  it("channel send: 422 on non-object attachment entries", async () => {
    const res = await send([42]);
    expect(res.status).toBe(422);
  });

  it("channel send: 422 on a descriptor missing required fields", async () => {
    const res = await send([{ id: "att_1", name: "file.txt" }]);
    expect(res.status).toBe(422);
  });

  it("channel send: a valid descriptor passes validation and routes", async () => {
    const res = await send([VALID_DESCRIPTOR]);
    expect(res.status).toBe(200);
  });

  it("admin task message: 422 on a malformed descriptor", async () => {
    const res = await app.fetch(new Request("http://local/api/admin/tasks/task-x/message", {
      method: "POST",
      headers: channelHeaders(),
      body: JSON.stringify({ text: "hi", attachments: [{ id: "only-an-id" }] }),
    }));
    expect(res.status).toBe(422);
  });

  it("admin task message: a valid descriptor passes validation (unknown task → 404, not 422)", async () => {
    const res = await app.fetch(new Request("http://local/api/admin/tasks/task-missing/message", {
      method: "POST",
      headers: channelHeaders(),
      body: JSON.stringify({ text: "hi", attachments: [VALID_DESCRIPTOR] }),
    }));
    expect(res.status).toBe(404);
  });
});

// ── GET poll query validation ──────────────────────────────────

describe("HOST-M1M2-17 — GET /:channel/poll rejects non-numeric query params", () => {
  function poll(query: string) {
    return app.fetch(new Request(`http://local/api/messages/hardening-chat/poll?${query}`, {
      headers: { authorization: `Bearer ${CHANNEL}` },
    }));
  }

  it("422 on a garbage cursor (?after=latest)", async () => {
    const res = await poll("after=latest");
    expect(res.status).toBe(422);
  });

  it("422 on a garbage wait (?wait=5000x)", async () => {
    const res = await poll("wait=5000x");
    expect(res.status).toBe(422);
  });

  it("422 on a non-positive limit", async () => {
    const res = await poll("limit=0");
    expect(res.status).toBe(422);
  });

  it("422 on a negative ack", async () => {
    const res = await poll("ack=-1");
    expect(res.status).toBe(422);
  });

  it("valid numeric params still poll (200)", async () => {
    const res = await poll("after=0&limit=5&wait=0");
    expect(res.status).toBe(200);
  });

  it("an unknown channel with VALID params is a 404 (validation ran first)", async () => {
    const res = await app.fetch(new Request("http://local/api/messages/no-such-channel/poll?after=0", {
      headers: { authorization: `Bearer ${CHANNEL}` },
    }));
    expect(res.status).toBe(404);
  });
});
