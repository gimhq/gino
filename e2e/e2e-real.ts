// Real-flow e2e: drive ONE task through the live host to a REAL gino-agent
// running the REAL Claude CLI, whose model call flows through the host /llm
// credential proxy to REAL Anthropic. Proves the production data path end to end.
//
// Outcomes:
//   PASS         — the model replied; the transcript carries a real, non-empty
//                  assistant message (and usually the requested token).
//   RATE-LIMITED — the request reached Anthropic but was throttled (429). The flow
//                  is proven up to the upstream; not a wiring failure.
//   FAIL         — the flow broke before/inside the host or runner.

const BASE = process.env.BASE ?? "http://host:3000/api";
const REG = process.env.REGISTER_TOKEN ?? "e2e-register-secret";
const ADMIN = process.env.ADMIN_TOKEN ?? "e2e-admin-secret";
const AGENT_NAME = process.env.AGENT_NAME ?? "e2e-real-runner";
const MODEL = process.env.REAL_MODEL ?? "claude-haiku";
const TOKEN = "PONG-7Q2";

const sleep = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms));
// Every call this driver makes is control-plane (/admin/*), so it authenticates
// with the ADMIN bearer (the channel token no longer reaches /admin).
const bearer = { Authorization: `Bearer ${ADMIN}`, "content-type": "application/json" };

async function http(method: string, path: string, body?: unknown): Promise<{ status: number; json: any; text: string }> {
  const res = await fetch(`${BASE}${path}`, { method, headers: bearer, body: body == null ? undefined : JSON.stringify(body) });
  const text = await res.text();
  let json: any = null;
  try { json = text ? JSON.parse(text) : null; } catch {}
  return { status: res.status, json, text };
}

async function waitFor<T>(label: string, fn: () => Promise<T | null>, timeoutMs: number, everyMs = 1500): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try { const v = await fn(); if (v) return v; last = v; }
    catch (e) { last = e instanceof Error ? e.message : String(e); }
    await sleep(everyMs);
  }
  throw new Error(`timeout waiting for ${label} (last=${JSON.stringify(last)})`);
}

function log(s: string): void {
  // eslint-disable-next-line no-console
  console.log(s);
}

async function main(): Promise<void> {
  log(`gino REAL-flow e2e → ${BASE}  (engine: real Claude CLI, model: ${MODEL})`);

  await waitFor("host /health", async () => (await fetch(`${BASE}/health`).then(r => r.ok).catch(() => false)) ? true : null, 60_000, 1000);
  log("✓ host healthy");

  // 1) the real runner registers + attaches over WS
  const agent = await waitFor(`agent '${AGENT_NAME}' online`, async () => {
    const r = await http("GET", "/admin/agents");
    return (r.json?.data ?? []).find((a: any) => a.name === AGENT_NAME && a.online === true) ?? null;
  }, 60_000);
  log(`✓ real agent online: ${agent.id}`);

  // 2) grant + create a task on it
  const owner = "owner-real";
  await http("POST", `/admin/agents/${agent.id}/grant`, { owner_id: owner });
  const created = await http("POST", "/admin/tasks", { owner_id: owner, engine: "claude", model: MODEL, name: "real-flow" });
  if (created.status !== 201) { log(`✗ FAIL: create task ${created.status} ${created.text.slice(0, 200)}`); process.exit(1); }
  const taskId = created.json.data.task.id;
  if (created.json.data.dispatch?.agentId !== agent.id) { log(`✗ FAIL: not placed on the real agent`); process.exit(1); }
  log(`✓ task ${taskId} placed on the real agent`);

  // 3) send a real prompt and let the real CLI call the real model via /llm
  const prompt = `Reply with exactly this token and nothing else: ${TOKEN}`;
  const msg = await http("POST", `/admin/tasks/${taskId}/message`, { text: prompt });
  if (msg.status !== 200) { log(`✗ FAIL: message ${msg.status} ${msg.text.slice(0, 200)}`); process.exit(1); }
  log(`✓ prompt sent; waiting for the real model to answer (≤150s)…`);

  // 4) wait for a terminal thread (succeeded or failed)
  let view: any;
  try {
    view = await waitFor("a terminal thread", async () => {
      const r = await http("GET", `/admin/tasks/${taskId}`);
      const threads: any[] = r.json?.data?.threads ?? [];
      const done = threads.find(t => t.run && t.run.status !== "running" && t.run.status !== "dispatched");
      return done ?? null;
    }, 150_000);
  }
  catch (e) {
    log(`✗ FAIL: ${e instanceof Error ? e.message : String(e)} — no terminal thread (runner/engine stalled)`);
    process.exit(1);
  }

  const status = view.run?.status;
  // The task view carries thread SUMMARIES only; the transcript is paged per
  // thread via `?thread=`.
  const page = await http("GET", `/admin/tasks/${taskId}?thread=${view.threadId}`);
  const transcript: string[] = (page.json?.data?.messages ?? []).map((m: any) => m.content).filter((c: any) => typeof c === "string");
  const blob = `${JSON.stringify(view.run)} ${transcript.join(" ")}`;
  const rateLimited = /429|rate.?limit|overloaded|provider_/i.test(blob);
  const assistant = transcript.join("\n").trim();

  log(`\n── result ──`);
  log(`thread status : ${status}`);
  log(`run           : ${JSON.stringify(view.run)}`);
  log(`transcript    : ${JSON.stringify(transcript)}`);

  if (status === "succeeded" && assistant.length > 0) {
    const hit = assistant.includes(TOKEN);
    log(`\n✓ REAL-FLOW PASS — the real model answered through host→runner→CLI→/llm→Anthropic→back.`);
    log(hit ? `  exact token "${TOKEN}" present (content fidelity end to end).` : `  (non-empty real model output; token match not required)`);
    process.exit(0);
  }
  if (rateLimited) {
    log(`\n⚠ RATE-LIMITED — the request reached real Anthropic but was throttled (429).`);
    log(`  The flow (auth + /llm injection + upstream) is proven; the shared subscription is rate-limited.`);
    log(`  Re-run later or use a dedicated API key (GINO_PROXY_ANTHROPIC_API_KEY).`);
    process.exit(3); // distinct: infra/rate-limit, not a flow bug
  }
  log(`\n✗ REAL-FLOW FAIL — terminal status ${status} without a usable completion.`);
  process.exit(1);
}

main().catch((e) => { log(`real e2e crashed: ${e instanceof Error ? e.stack : e}`); process.exit(2); });
