// Gino docker-compose e2e — drives the live host's HTTP surface end-to-end with
// a REAL gino-agent container dialed in over WebSocket. This is the first test
// exercising host-v2 ↔ runner-v2 together (they were built in separate
// campaigns), so it doubles as an integration smoke for wire compatibility.
//
// Run inside an oven/bun container on the compose network (see docker-compose.yml).
// Exits 0 if every check passes, 1 otherwise — so `docker compose up
// --exit-code-from tester` yields a single pass/fail.

const BASE = process.env.BASE ?? "http://host:3000/api";
const REG = process.env.REGISTER_TOKEN ?? "e2e-register-secret";
const CHAN = process.env.CHANNEL_TOKEN ?? "e2e-channel-secret";
const AGENT_NAME = process.env.AGENT_NAME ?? "e2e-runner";
const INJECTED_KEY = process.env.PROXY_KEY ?? "sk-ant-host-real";

// ── tiny test harness ──────────────────────────────────────────────────────

interface Result { name: string; ok: boolean; detail?: string }
const results: Result[] = [];
const knownIssues: { name: string; detail: string }[] = [];
let currentGroup = "";

function group(name: string): void {
  currentGroup = name;
  // eslint-disable-next-line no-console
  console.log(`\n━━ ${name} ━━`);
}

function record(name: string, ok: boolean, detail?: string): void {
  results.push({ name: `${currentGroup} › ${name}`, ok, detail });
  const mark = ok ? "  ✓" : "  ✗";
  // eslint-disable-next-line no-console
  console.log(`${mark} ${name}${detail && !ok ? `  — ${detail}` : ""}`);
}

function check(name: string, cond: boolean, detail?: string): void {
  record(name, cond, detail);
}

/** Record a pre-existing/known issue: reported loudly, but never fails the suite. */
function note(name: string, detail: string): void {
  knownIssues.push({ name: `${currentGroup} › ${name}`, detail });
  // eslint-disable-next-line no-console
  console.log(`  ⚠ KNOWN ISSUE: ${name} — ${detail}`);
}

/** Run an async step; a throw is a failed check (so one bug never aborts the suite). */
async function step(name: string, fn: () => Promise<void>): Promise<void> {
  try {
    await fn();
  }
  catch (err) {
    record(name, false, err instanceof Error ? err.message : String(err));
  }
}

function assert(cond: unknown, msg: string): asserts cond {
  if (!cond)
    throw new Error(msg);
}

const sleep = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms));

// ── http helpers ───────────────────────────────────────────────────────────

interface Res { status: number; json: any; text: string }

async function http(method: string, path: string, opts: { headers?: Record<string, string>; body?: string } = {}): Promise<Res> {
  const res = await fetch(`${BASE}${path}`, { method, headers: opts.headers, body: opts.body });
  const text = await res.text();
  let json: any = null;
  try { json = text ? JSON.parse(text) : null; }
  catch { /* non-json body */ }
  return { status: res.status, json, text };
}

const jsonHeaders = { "content-type": "application/json" };
const bearer = { Authorization: `Bearer ${CHAN}` };

const admin = (method: string, path: string, body?: unknown): Promise<Res> =>
  http(method, `/admin${path}`, { headers: { ...bearer, ...jsonHeaders }, body: body == null ? undefined : JSON.stringify(body) });

const msg = (method: string, path: string, body?: unknown): Promise<Res> =>
  http(method, `/messages${path}`, { headers: { ...bearer, ...jsonHeaders }, body: body == null ? undefined : JSON.stringify(body) });

async function registerAgent(name: string): Promise<{ status: number; agentId: string; connectToken: string; config: any }> {
  const r = await http("POST", "/agent/register", {
    headers: jsonHeaders,
    body: JSON.stringify({ registerToken: REG, protocolVersion: 2, source: "manual", name, capabilities: ["claude"] }),
  });
  return { status: r.status, agentId: r.json?.agentId, connectToken: r.json?.connectToken, config: r.json?.config };
}

/** Poll `fn` until it returns truthy or the timeout elapses. */
async function waitFor<T>(label: string, fn: () => Promise<T | null | undefined | false>, timeoutMs = 30_000, everyMs = 750): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const v = await fn();
      if (v)
        return v as T;
      last = v;
    }
    catch (err) { last = err instanceof Error ? err.message : String(err); }
    await sleep(everyMs);
  }
  throw new Error(`timeout waiting for ${label} (last=${JSON.stringify(last)})`);
}

// shared state across groups
const ctx: { realAgentId?: string } = {};

// ── 1. health & version (public) ─────────────────────────────────────────────

async function testHealth(): Promise<void> {
  group("health & version");
  await step("GET /health → 200 ok", async () => {
    const r = await http("GET", "/health");
    assert(r.status === 200, `status ${r.status}`);
    check("status=ok", r.json?.status === "ok", JSON.stringify(r.json));
  });
  await step("GET /health/ready → 200 ready", async () => {
    const r = await http("GET", "/health/ready");
    check("ready (200)", r.status === 200, `status ${r.status} body ${r.text.slice(0, 120)}`);
  });
  await step("GET /system/version → build info", async () => {
    const r = await http("GET", "/system/version");
    check("200 + data", r.status === 200 && r.json?.success === true, `status ${r.status} body ${r.text.slice(0, 160)}`);
  });
}

// ── 2. auth gates ────────────────────────────────────────────────────────────

async function testAuthGates(): Promise<void> {
  group("auth gates (fail-closed)");
  await step("register w/ wrong token → 401", async () => {
    const r = await http("POST", "/agent/register", { headers: jsonHeaders, body: JSON.stringify({ registerToken: "WRONG", protocolVersion: 2, capabilities: ["claude"] }) });
    check("401", r.status === 401, `status ${r.status}`);
  });
  await step("admin w/o bearer → 401", async () => {
    const r = await http("GET", "/admin/agents");
    check("401", r.status === 401, `status ${r.status}`);
  });
  await step("llm w/o credential → 401", async () => {
    const r = await http("GET", "/llm/v1/models");
    check("401", r.status === 401, `status ${r.status}`);
  });
}

// ── 3. the real runner registers itself & comes online ───────────────────────

async function testAgentOnline(): Promise<void> {
  group("real agent enrollment (over WS)");
  const agent = await step("agent appears online in /admin/agents", async () => {
    const found = await waitFor(`agent '${AGENT_NAME}' online`, async () => {
      const r = await admin("GET", "/agents");
      const list: any[] = r.json?.data ?? [];
      return list.find(a => a.name === AGENT_NAME && a.online === true) ?? null;
    }, 45_000);
    ctx.realAgentId = found.id;
    check("online=true", found.online === true);
    check("capability claude", Array.isArray(found.capabilities?.engines) && found.capabilities.engines.includes("claude"), JSON.stringify(found.capabilities));
    // eslint-disable-next-line no-console
    console.log(`    realAgentId=${found.id} status=${found.status}`);
  });
  void agent;
}

// ── 4. task create → dispatch → message → run → transcript (CORE round-trip) ──

async function testTaskRoundTrip(): Promise<void> {
  group("task round-trip through the real runner");
  assert(ctx.realAgentId, "no real agent id (enrollment failed)");
  const owner = "owner-e2e";
  const agentId = ctx.realAgentId;

  await step("grant owner → real agent", async () => {
    const r = await admin("POST", `/agents/${agentId}/grant`, { owner_id: owner });
    check("granted", r.status === 200 && r.json?.data?.granted === true, `status ${r.status} body ${r.text.slice(0, 160)}`);
  });

  let taskId = "";
  await step("create task → auto-dispatched to real agent", async () => {
    const r = await admin("POST", "/tasks", { owner_id: owner, engine: "claude", model: "sonnet", name: "round-trip" });
    assert(r.status === 201, `status ${r.status} body ${r.text.slice(0, 200)}`);
    taskId = r.json?.data?.task?.id;
    check("dispatch placed", r.json?.data?.dispatch?.placed === true, JSON.stringify(r.json?.data?.dispatch));
    check("placed on real agent", r.json?.data?.dispatch?.agentId === agentId, JSON.stringify(r.json?.data?.dispatch));
  });

  const nonce = `nonce-${Date.now().toString(36)}-rt`;
  await step("send message (seeds task_upsert + message)", async () => {
    assert(taskId, "no taskId");
    const r = await admin("POST", `/tasks/${taskId}/message`, { text: `please echo ${nonce}` });
    assert(r.status === 200, `status ${r.status} body ${r.text.slice(0, 200)}`);
    check("seeded", r.json?.data?.seeded === true, JSON.stringify(r.json?.data));
    check("threadId returned", typeof r.json?.data?.threadId === "string");
  });

  await step("runner runs the thread → transcript mirrors echoed prompt", async () => {
    assert(taskId, "no taskId");
    const view = await waitFor("thread to succeed", async () => {
      const r = await admin("GET", `/tasks/${taskId}`);
      const threads: any[] = r.json?.data?.threads ?? [];
      const done = threads.find(t => t.run?.status === "succeeded");
      return done ?? null;
    }, 40_000);
    const contents: string[] = (view.messages ?? []).map((m: any) => m.content);
    check("thread run succeeded", view.run?.status === "succeeded", JSON.stringify(view.run));
    check("transcript carries the echoed nonce", contents.some(c => typeof c === "string" && c.includes(nonce)), JSON.stringify(contents));
    // eslint-disable-next-line no-console
    console.log(`    transcript=${JSON.stringify(contents)} session=${view.run?.engineSessionId ?? view.run?.engine_session_id}`);
  });
}

// ── 5. channels ingress → run → egress poll ──────────────────────────────────

async function testChannels(): Promise<void> {
  group("channels (ingress → run → egress)");
  assert(ctx.realAgentId, "no real agent id");
  const owner = "owner-ch";
  const agentId = ctx.realAgentId;
  let channelId = "";

  await step("grant owner-ch → real agent", async () => {
    const r = await admin("POST", `/agents/${agentId}/grant`, { owner_id: owner });
    check("granted", r.status === 200, `status ${r.status}`);
  });
  await step("create channel", async () => {
    const r = await admin("POST", "/channels", { name: "chat", type: "http", owner_id: owner, config: { engine: "claude", model: "sonnet" } });
    assert(r.status === 201, `status ${r.status} body ${r.text.slice(0, 200)}`);
    channelId = r.json?.data?.id;
    check("channel id", typeof channelId === "string");
  });

  const nonce = `nonce-${Date.now().toString(36)}-ch`;
  let taskId = "";
  await step("ingress: /send new conversation → new task placed on agent", async () => {
    assert(channelId, "no channel");
    const r = await msg("POST", `/${channelId}/send`, { conversation_ref: "c1", text: `please echo ${nonce}` });
    assert(r.status === 200, `status ${r.status} body ${r.text.slice(0, 200)}`);
    taskId = r.json?.data?.taskId;
    check("created new task", r.json?.data?.created === true, JSON.stringify(r.json?.data));
    check("dispatch placed", r.json?.data?.dispatch?.placed === true, JSON.stringify(r.json?.data?.dispatch));
  });
  await step("same conversation_ref → continues same task", async () => {
    assert(channelId, "no channel");
    const r = await msg("POST", `/${channelId}/send`, { conversation_ref: "c1", text: "follow up" });
    check("created=false (continued)", r.json?.data?.created === false, JSON.stringify(r.json?.data));
    check("same taskId", r.json?.data?.taskId === taskId, `${r.json?.data?.taskId} vs ${taskId}`);
  });
  await step("egress: poll surfaces the assistant transcript", async () => {
    assert(channelId, "no channel");
    const deliveries = await waitFor("assistant echo in poll", async () => {
      const r = await msg("POST", `/${channelId}/poll`, { conversation_ref: "c1", after: 0, peek: true });
      const ds: any[] = r.json?.data?.deliveries ?? [];
      const msgs = ds.flatMap(d => d.messages ?? []);
      const hit = msgs.find((m: any) => typeof m.content === "string" && m.content.includes(nonce));
      return hit ? { msgs } : null;
    }, 40_000);
    const contents = deliveries.msgs.map((m: any) => `${m.kind}:${m.content}`);
    check("assistant echo present in egress", deliveries.msgs.some((m: any) => typeof m.content === "string" && m.content.includes(nonce)), JSON.stringify(contents));
  });
}

// ── 6. crons (host command surface + mirror) ─────────────────────────────────

async function testCrons(): Promise<void> {
  group("crons (command → agent + host mirror)");
  assert(ctx.realAgentId, "no real agent id");
  const owner = "owner-cron";
  const agentId = ctx.realAgentId;
  let taskId = "";
  let cronId = "";

  await step("setup: grant + task + seed task_upsert via a message", async () => {
    await admin("POST", `/agents/${agentId}/grant`, { owner_id: owner });
    const r = await admin("POST", "/tasks", { owner_id: owner, engine: "claude", model: "sonnet", name: "cron-host" });
    assert(r.status === 201, `status ${r.status}`);
    taskId = r.json?.data?.task?.id;
    // The agent's `crons` table FK-references its local `tasks` row, which only
    // exists after a `task_upsert` frame — seeded by the FIRST message. Schedule a
    // cron BEFORE any message and the agent rejects cron_upsert on an unknown task
    // (FK constraint). Seed it here; the probe at the end exercises the broken order.
    await admin("POST", `/tasks/${taskId}/message`, { text: "seed task_upsert" });
    await sleep(1500); // let the agent apply task_upsert before the cron arrives
  });
  await step("POST cron (fast interval) → 201 (cron_upsert emitted)", async () => {
    assert(taskId, "no task");
    const r = await admin("POST", `/tasks/${taskId}/crons`, {
      kind: "schedule", continuity: "continue", schedule_type: "interval", schedule_value: "1000", prompt: "e2e tick",
    });
    assert(r.status === 201, `status ${r.status} body ${r.text.slice(0, 200)}`);
    cronId = r.json?.data?.cronId;
    check("cronId + outboxId", typeof cronId === "string" && typeof r.json?.data?.outboxId === "number", JSON.stringify(r.json?.data));
  });
  await step("host mirror populates from the agent's cron_state (proves it FIRED)", async () => {
    assert(taskId, "no task");
    // The mirror is written ONLY from an inbound `cron_state`, which the agent
    // emits when a cron FIRES — so the cron appearing here proves the full
    // emit→apply→fire→cron_state→mirror round-trip (OD14: crons live on the agent).
    const cron = await waitFor("cron_state mirror", async () => {
      const r = await admin("GET", `/tasks/${taskId}/crons`);
      const crons: any[] = r.json?.data ?? [];
      return crons.find(c => c.id === cronId) ?? null;
    }, 25_000);
    check("mirror has the cron", cron.id === cronId, JSON.stringify(cron));
    check("mirror carries scheduling fields", cron.scheduleType === "interval", JSON.stringify({ scheduleType: cron.scheduleType, status: cron.status }));
    check("mirror shows it fired (roundCount ≥ 1)", (cron.roundCount ?? 0) >= 1, `roundCount=${cron.roundCount}`);
  });
  await step("DELETE cron → 200 (cron_delete emitted)", async () => {
    assert(taskId && cronId, "missing ids");
    const r = await admin("DELETE", `/tasks/${taskId}/crons/${cronId}`);
    check("200", r.status === 200, `status ${r.status}`);
  });
  await step("probe: cron on a never-messaged task (ordering)", async () => {
    // Create a task and schedule a cron WITHOUT ever messaging it. The host
    // accepts (201) but the agent has no task_upsert yet → cron_upsert hits the
    // FK constraint and is silently dropped (never fires, mirror stays empty).
    const owner2 = "owner-cron-probe";
    await admin("POST", `/agents/${agentId}/grant`, { owner_id: owner2 });
    const t = await admin("POST", "/tasks", { owner_id: owner2, engine: "claude", model: "sonnet", name: "cron-noseed" });
    const t2 = t.json?.data?.task?.id;
    const c = await admin("POST", `/tasks/${t2}/crons`, { kind: "schedule", continuity: "continue", schedule_type: "interval", schedule_value: "1000", prompt: "x" });
    check("host accepts the cron (201)", c.status === 201, `status ${c.status}`);
    await sleep(6000);
    const mirror = (await admin("GET", `/tasks/${t2}/crons`)).json?.data ?? [];
    if (mirror.length === 0) {
      note(
        "scheduling a cron before the first message silently fails on the agent",
        "host returns 201 but emits no task_upsert until the first message; the agent's crons FK-references an unknown task → `FOREIGN KEY constraint failed`, the cron never applies/fires. Fix host to seed task_upsert on first cron (as it does on first message).",
      );
    }
    else {
      check("cron on un-messaged task applied", true);
    }
  });
}

// ── 7. attachment byte channel (connect-token authz + IDOR) ──────────────────

async function testAttachments(): Promise<void> {
  group("attachments (byte channel + IDOR)");
  const owner = "owner-att";
  // Two probe agents (registered over HTTP; no WS needed for the byte channel).
  let a: Awaited<ReturnType<typeof registerAgent>> | undefined;
  let b: Awaited<ReturnType<typeof registerAgent>> | undefined;
  let taskId = "";
  let attId = "";
  let attSha = "";
  const payload = "the quick brown fox";

  await step("register two probe agents", async () => {
    a = await registerAgent("e2e-att-A");
    b = await registerAgent("e2e-att-B");
    check("A enrolled", a.status === 200 && !!a.connectToken, `status ${a.status}`);
    check("B enrolled", b.status === 200 && !!b.connectToken, `status ${b.status}`);
  });
  await step("place a task on probe A (grant + force-assign)", async () => {
    assert(a, "no agent A");
    await admin("POST", `/agents/${a.agentId}/grant`, { owner_id: owner });
    const t = await admin("POST", "/tasks", { owner_id: owner, engine: "claude", model: "sonnet", name: "att" });
    assert(t.status === 201, `create status ${t.status}`);
    taskId = t.json?.data?.task?.id;
    const asg = await admin("POST", `/tasks/${taskId}/assign`, { agent_id: a.agentId });
    check("assigned to A", asg.status === 200 && asg.json?.data?.agentId === a.agentId, JSON.stringify(asg.json?.data));
  });
  await step("A ingests attachment for its task → 201", async () => {
    assert(a && taskId, "missing");
    const r = await http("POST", `/agent/attachments?name=report.txt&task_id=${taskId}&thread_id=th1&content_type=text/plain`, {
      headers: { "x-internal-token": a.connectToken, "content-type": "text/plain" },
      body: payload,
    });
    assert(r.status === 201, `status ${r.status} body ${r.text.slice(0, 200)}`);
    attId = r.json?.data?.id;
    attSha = r.json?.data?.sha256;
    check("size matches", r.json?.data?.size === payload.length, JSON.stringify(r.json?.data));
  });
  await step("A downloads by id → verified bytes", async () => {
    assert(a && attId, "missing");
    const res = await fetch(`${BASE}/agent/attachments/${attId}`, { headers: { "x-internal-token": a.connectToken } });
    const body = await res.text();
    check("200 + bytes", res.status === 200 && body === payload, `status ${res.status} body=${body.slice(0, 60)}`);
    check("x-content-sha256 header", res.headers.get("x-content-sha256") === attSha);
  });
  await step("A downloads by sha256 → verified bytes", async () => {
    assert(a && attSha, "missing");
    const res = await fetch(`${BASE}/agent/attachments?sha256=${attSha}`, { headers: { "x-internal-token": a.connectToken } });
    check("200 + bytes", res.status === 200 && (await res.text()) === payload, `status ${res.status}`);
  });
  await step("IDOR: probe B cannot read A's attachment → 403", async () => {
    assert(b && attId, "missing");
    const res = await fetch(`${BASE}/agent/attachments/${attId}`, { headers: { "x-internal-token": b.connectToken } });
    check("403", res.status === 403, `status ${res.status}`);
  });
}

// ── 8. /llm credential proxy (auth + inject + strip via mock upstream) ────────

async function testLlmProxy(): Promise<void> {
  group("/llm credential proxy");
  const probe = await registerAgent("e2e-llm");
  assert(probe.status === 200 && probe.connectToken, `probe enroll failed: ${probe.status}`);
  const tok = { "x-internal-token": probe.connectToken };

  await step("POST /llm/v1/messages with connect token → 200 (forwarded, cred injected)", async () => {
    const r = await http("POST", "/llm/v1/messages", { headers: { ...tok, ...jsonHeaders }, body: JSON.stringify({ model: "claude-sonnet", max_tokens: 5, messages: [{ role: "user", content: "hi" }] }) });
    assert(r.status === 200, `status ${r.status} body ${r.text.slice(0, 200)}`);
    check("reached mock upstream", r.json?.mock === true, JSON.stringify(r.json));
    check("/llm prefix stripped", r.json?.path === "/v1/messages", `path=${r.json?.path}`);
    check("real upstream key injected", r.json?.seen_auth === INJECTED_KEY, `seen_auth=${r.json?.seen_auth}`);
  });
  await step("GET /llm/v1/responses (no Upgrade) → 426", async () => {
    const r = await http("GET", "/llm/v1/responses", { headers: tok });
    check("426 upgrade required", r.status === 426, `status ${r.status}`);
  });
}

// ── 9. config / skill provisioning (artifacts → entries → register) ──────────

async function testProvisioning(): Promise<void> {
  group("config provisioning (global desired set)");
  let fileSha = "";
  let dirSha = "";

  await step("upload CLAUDE.md (file) + skill dir → 201 (+ dedup)", async () => {
    const f = await http("POST", "/admin/config/artifacts?name=CLAUDE.md&kind=file&content_type=text/markdown", { headers: { ...bearer }, body: "# e2e global instructions" });
    assert(f.status === 201, `file status ${f.status} body ${f.text.slice(0, 160)}`);
    fileSha = f.json?.data?.sha256;
    const d = await http("POST", "/admin/config/artifacts?name=e2e-skill&kind=dir&content_type=application/gzip", { headers: { ...bearer }, body: "skill-bundle-bytes" });
    assert(d.status === 201, `dir status ${d.status}`);
    dirSha = d.json?.data?.sha256;
    const again = await http("POST", "/admin/config/artifacts?name=CLAUDE.md&kind=file&content_type=text/markdown", { headers: { ...bearer }, body: "# e2e global instructions" });
    check("dedup → same sha", again.json?.data?.sha256 === fileSha, `${again.json?.data?.sha256} vs ${fileSha}`);
  });
  await step("PUT global desired entries → host resolves targets", async () => {
    assert(fileSha && dirSha, "missing shas");
    const r = await admin("PUT", "/config/entries", { scope: "global", entries: [{ name: "CLAUDE.md", sha256: fileSha, kind: "file" }, { name: "e2e-skill", sha256: dirSha, kind: "dir" }] });
    assert(r.status === 200, `status ${r.status} body ${r.text.slice(0, 200)}`);
    const targets = (r.json?.data?.entries ?? []).map((e: any) => e.target).sort();
    check("targets resolved", targets.includes("CLAUDE.md") && targets.includes(".claude/skills/e2e-skill"), JSON.stringify(targets));
  });
  await step("a freshly-registered agent receives the resolved entries", async () => {
    const fresh = await registerAgent("e2e-prov");
    assert(fresh.status === 200, `status ${fresh.status}`);
    const targets = (fresh.config?.entries ?? []).map((e: any) => e.target).sort();
    check("register config carries entries", targets.includes("CLAUDE.md") && targets.includes(".claude/skills/e2e-skill"), JSON.stringify(targets));
  });
}

// ── 10. agent control (stop / remove a probe agent) ──────────────────────────

async function testAgentControl(): Promise<void> {
  group("agent OOB control");
  const probe = await registerAgent("e2e-control");
  assert(probe.status === 200, `enroll failed ${probe.status}`);
  await step("stop probe agent → 200", async () => {
    const r = await admin("POST", `/agents/${probe.agentId}/stop`);
    check("200", r.status === 200, `status ${r.status} body ${r.text.slice(0, 160)}`);
  });
  await step("remove probe agent → 200", async () => {
    const r = await admin("POST", `/agents/${probe.agentId}/remove`);
    check("200", r.status === 200, `status ${r.status} body ${r.text.slice(0, 160)}`);
  });
  await step("revoke owner grant → 200", async () => {
    assert(ctx.realAgentId, "no real agent");
    const r = await admin("POST", `/agents/${ctx.realAgentId}/revoke`, { owner_id: "owner-cron" });
    check("200 granted=false", r.status === 200 && r.json?.data?.granted === false, `status ${r.status}`);
  });
}

// ── 11. task lifecycle control (interrupt / close) — RUNS LAST ───────────────
// `close` is destructive to the shared real agent (see below), so this group is
// ordered after every agent-dependent group.

/** Create + seed a task on the real agent and wait for a thread to succeed (proof of liveness). */
async function runOnRealAgent(agentId: string, owner: string, name: string, text: string, timeoutMs = 18_000): Promise<string> {
  const t = await admin("POST", "/tasks", { owner_id: owner, engine: "claude", model: "sonnet", name });
  assert(t.status === 201, `create ${name} → ${t.status}`);
  const id = t.json?.data?.task?.id;
  assert(t.json?.data?.dispatch?.agentId === agentId, `task ${name} placed on ${t.json?.data?.dispatch?.agentId}, not the real agent`);
  await admin("POST", `/tasks/${id}/message`, { text });
  await waitFor(`${name} run`, async () => {
    const v = await admin("GET", `/tasks/${id}`);
    return (v.json?.data?.threads ?? []).some((th: any) => th.run?.status === "succeeded") ? id : null;
  }, timeoutMs);
  return id;
}

/** Does the agent still execute a brand-new task within `windowMs`? (work-based liveness). */
async function agentExecutesNewTask(agentId: string, owner: string, name: string, windowMs: number): Promise<boolean> {
  const t = await admin("POST", "/tasks", { owner_id: owner, engine: "claude", model: "sonnet", name });
  const id = t.json?.data?.task?.id;
  if (t.json?.data?.dispatch?.agentId !== agentId)
    return false;
  await admin("POST", `/tasks/${id}/message`, { text: "are you there?" });
  const deadline = Date.now() + windowMs;
  while (Date.now() < deadline) {
    const v = await admin("GET", `/tasks/${id}`);
    if ((v.json?.data?.threads ?? []).some((th: any) => th.run?.status === "succeeded"))
      return true;
    await sleep(750);
  }
  return false;
}

async function testLifecycle(): Promise<void> {
  group("task lifecycle (interrupt / close)");
  assert(ctx.realAgentId, "no real agent id");
  // Detection is WORK-based, not via the `online` flag: a dead agent still reads
  // online for the offline-grace window (~3× heartbeat ≈ 90s), so a fresh task
  // that never runs is the only reliable signal that the runner is gone.
  const owner = "owner-life";
  const agentId = ctx.realAgentId;
  let l1 = "";

  await step("grant owner → ONLY the real agent (deterministic placement)", async () => {
    const r = await admin("POST", `/agents/${agentId}/grant`, { owner_id: owner });
    check("granted", r.status === 200, `status ${r.status}`);
  });

  await step("interrupt is non-destructive (agent keeps executing)", async () => {
    l1 = await runOnRealAgent(agentId, owner, "lifecycle", "lifecycle ping");
    const r = await admin("POST", `/tasks/${l1}/interrupt`);
    check("interrupt 200", r.status === 200, `status ${r.status}`);
    const alive = await agentExecutesNewTask(agentId, owner, "post-interrupt", 15_000);
    check("agent still runs a new task after interrupt", alive);
  });

  await step("close → 200 (host side)", async () => {
    assert(l1, "no l1");
    const r = await admin("POST", `/tasks/${l1}/close`);
    check("close 200", r.status === 200, `status ${r.status}`);
  });

  await step("per-task close must NOT stop the agent serving other tasks", async () => {
    const alive = await agentExecutesNewTask(agentId, owner, "post-close", 12_000);
    check("agent still runs a new task after a per-task close", alive);
  });
}

// ── main ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  // eslint-disable-next-line no-console
  console.log(`gino docker e2e → ${BASE}`);

  // Wait for the host to answer at all before driving it.
  await waitFor("host /health", async () => {
    try { return (await http("GET", "/health")).status === 200 ? true : null; }
    catch { return null; }
  }, 60_000, 1000);

  await testHealth();
  await testAuthGates();
  await testAgentOnline();
  await testTaskRoundTrip();
  await testChannels();
  await testCrons();
  await testAttachments();
  await testLlmProxy();
  await testProvisioning();
  await testAgentControl();
  await testLifecycle(); // destructive `close` → runs last

  // summary
  const passed = results.filter(r => r.ok).length;
  const failed = results.filter(r => !r.ok);
  // eslint-disable-next-line no-console
  console.log(`\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`);
  // eslint-disable-next-line no-console
  console.log(`RESULT: ${passed}/${results.length} checks passed`);
  if (knownIssues.length) {
    // eslint-disable-next-line no-console
    console.log(`\nKNOWN ISSUES (reported, not counted as failures): ${knownIssues.length}`);
    for (const k of knownIssues)
      // eslint-disable-next-line no-console
      console.log(`  ⚠ ${k.name}\n      ${k.detail}`);
  }
  if (failed.length) {
    // eslint-disable-next-line no-console
    console.log(`\nFAILED:`);
    for (const f of failed)
      // eslint-disable-next-line no-console
      console.log(`  ✗ ${f.name}${f.detail ? `  — ${f.detail}` : ""}`);
  }
  process.exit(failed.length ? 1 : 0);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error("e2e crashed:", err);
  process.exit(2);
});
