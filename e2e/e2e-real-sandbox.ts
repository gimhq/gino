// Real-engine sandbox e2e: drive the REAL Claude CLI through the live host on a
// task whose `meta.isolation` asks the agent to run the engine in a bubblewrap
// sandbox, and assert from machine-checkable markers what the engine can do.
//
// SANDBOX_NETWORK (default `none`):
//   none       the sandbox has loopback only, so the engine cannot reach the
//              host /llm proxy: the run must end without a model answer, and
//              not because the sandbox itself failed to start.
//   allowlist  only the LLM endpoint is reachable: the model answers, the
//              filesystem/uid markers hold, a second turn resumes the session,
//              and a disallowed host (SANDBOX_DENIED_HOST) is unreachable.
//   full       as allowlist, without the disallowed-host check.
//
// SMOKE=1: create the sandboxed task and dispatch one message with network
// `none` (no model call can leave the sandbox), check the host stored the
// meta, then exit; run-real-sandbox.sh verifies the agent side.
//
// Exit: 0 pass, 3 rate-limited (429), 1 failure.

const BASE = process.env.BASE ?? "http://host:3000/api";
const ADMIN = process.env.ADMIN_TOKEN ?? "e2e-admin-secret";
const AGENT_NAME = process.env.AGENT_NAME ?? "e2e-real-sandbox-runner";
const MODEL = process.env.REAL_MODEL ?? "claude-haiku";
const SMOKE = process.env.SMOKE === "1";
const NETWORK = SMOKE ? "none" : (process.env.SANDBOX_NETWORK ?? "none");
const DENIED_HOST = process.env.SANDBOX_DENIED_HOST ?? "github.com";
const WORKSPACE = "/workspace/group";

const sleep = (ms: number): Promise<void> => new Promise(r => setTimeout(r, ms));
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

function fail(msg: string): never {
  log(`✗ FAIL: ${msg}`);
  process.exit(1);
}

const isTerminal = (run: any): boolean => run != null && run.status !== "running" && run.status !== "dispatched";

/** Wait for the thread's run to be terminal and differ from `previous` (a new turn). */
async function waitTurn(taskId: string, threadId: string, previous: string | null, timeoutMs: number): Promise<any> {
  return waitFor("a terminal run", async () => {
    const r = await http("GET", `/admin/tasks/${taskId}`);
    const t = (r.json?.data?.threads ?? []).find((x: any) => x.threadId === threadId);
    return t && isTerminal(t.run) && JSON.stringify(t.run) !== previous ? t : null;
  }, timeoutMs);
}

async function transcript(taskId: string, threadId: string): Promise<{ blob: string }> {
  const page = await http("GET", `/admin/tasks/${taskId}?thread=${threadId}`);
  return { blob: JSON.stringify(page.json?.data?.messages ?? []) };
}

function rateLimited(view: any, blob: string): boolean {
  return /429|rate.?limit|overloaded|provider_/i.test(`${JSON.stringify(view.run)} ${blob}`);
}

async function main(): Promise<void> {
  log(`gino REAL sandbox e2e → ${BASE}  (model: ${MODEL}, network: ${NETWORK}${SMOKE ? ", smoke" : ""})`);
  if (!["none", "allowlist", "full"].includes(NETWORK)) fail(`unknown SANDBOX_NETWORK '${NETWORK}'`);

  await waitFor("host /health", async () => (await fetch(`${BASE}/health`).then(r => r.ok).catch(() => false)) ? true : null, 60_000, 1000);
  log("✓ host healthy");

  const agent = await waitFor(`agent '${AGENT_NAME}' online`, async () => {
    const r = await http("GET", "/admin/agents");
    return (r.json?.data ?? []).find((a: any) => a.name === AGENT_NAME && a.online === true) ?? null;
  }, 60_000);
  log(`✓ agent online: ${agent.id}`);

  // The configured isolation default is off; the task meta tightens it.
  const isolation = { mode: "sandbox", network: { mode: NETWORK } };
  const owner = "owner-real-sandbox";
  await http("POST", `/admin/agents/${agent.id}/grant`, { owner_id: owner });
  const created = await http("POST", "/admin/tasks", { owner_id: owner, engine: "claude", model: MODEL, name: "real-sandbox", meta: { isolation } });
  if (created.status !== 201) fail(`create task ${created.status} ${created.text.slice(0, 200)}`);
  const taskId: string = created.json.data.task.id;
  if (created.json.data.dispatch?.agentId !== agent.id) fail("task not placed on the agent");
  const stored = (await http("GET", `/admin/tasks/${taskId}`)).json?.data?.meta;
  if (JSON.stringify(stored?.isolation) !== JSON.stringify(isolation)) fail(`host task meta ${JSON.stringify(stored)}`);
  log(`✓ task ${taskId} created with meta.isolation=${JSON.stringify(isolation)}`);

  // Turn 1. Markers are assembled at runtime so none appears literally in the prompt.
  const n = 1000 + Math.floor(Math.random() * 9000);
  const readback = `SBX_READBACK_${n * 7}`;
  const script = [
    `echo "SBX_UID=$(id -u)"`,
    `echo "SBX_READBACK_$((${n}*7))" > sbx-proof.txt && echo "SBX_WRITE_$(cat sbx-proof.txt >/dev/null && echo OK)"`,
    `echo "SBX_DB_$(test -e ${WORKSPACE}/agent.db && echo VISIBLE || echo HIDDEN)"`,
    `echo "SBX_ROOT_$(test -e ${WORKSPACE}/canary.txt && echo VISIBLE || echo HIDDEN)"`,
    `echo "SBX_OTHER_$(test -e ${WORKSPACE}/threads/decoy-thread/secret.txt && echo VISIBLE || echo HIDDEN)"`,
    ...(NETWORK === "allowlist"
      ? [`echo "SBX_DENIED_$(curl -sS -o /dev/null --max-time 10 https://${DENIED_HOST} >/dev/null 2>&1 && echo REACHED || echo BLOCKED)"`]
      : []),
  ].join("\n");
  const prompt1 = `Run this exact bash script with your Bash tool, in your current working directory, then reply with its stdout verbatim and nothing else:\n\n${script}`;
  const msg1 = await http("POST", `/admin/tasks/${taskId}/message`, { text: prompt1 });
  if (msg1.status !== 200) fail(`message ${msg1.status} ${msg1.text.slice(0, 200)}`);
  const threadId: string = msg1.json.data.threadId;
  log(`✓ turn 1 sent on thread ${threadId}`);

  if (SMOKE) {
    log("✓ SMOKE: task + meta.isolation dispatched; agent-side checks follow in run-real-sandbox.sh");
    log(`SMOKE_TASK_ID=${taskId}`);
    process.exit(0);
  }

  let view: any;
  try { view = await waitTurn(taskId, threadId, null, 300_000); }
  catch (e) { fail(`${e instanceof Error ? e.message : String(e)} — no terminal thread`); }
  let t = await transcript(taskId, threadId);
  log(`turn 1 run        : ${JSON.stringify(view.run)}`);
  log(`turn 1 transcript : ${t.blob.slice(0, 4000)}`);

  if (NETWORK === "none") {
    // The proxy is unreachable from the sandbox: no model answer may appear, and
    // the failure must not be the sandbox refusing to start.
    const runBlob = JSON.stringify(view.run);
    if (/sandbox is unavailable|sandbox needs|not supported yet|invalid_request/i.test(runBlob)) fail(`sandbox did not start: ${runBlob}`);
    if (view.run?.status === "succeeded" || /SBX_UID=\d/.test(t.blob)) fail("engine reached the model with network none");
    log(`\n✓ REAL-SANDBOX PASS (none) — the sandboxed engine started and could not reach the model proxy.`);
    process.exit(0);
  }

  if (view.run?.status !== "succeeded") {
    if (rateLimited(view, t.blob)) { log("\n⚠ RATE-LIMITED (429)"); process.exit(3); }
    fail(`turn 1 status ${view.run?.status}`);
  }

  const checks: Array<[string, boolean]> = [
    ["engine uid is not 0", /SBX_UID=([1-9]\d*)/.test(t.blob) && !/SBX_UID=0\b/.test(t.blob)],
    ["write inside work dir", t.blob.includes("SBX_WRITE_OK")],
    ["agent.db hidden", t.blob.includes("SBX_DB_HIDDEN") && !t.blob.includes("SBX_DB_VISIBLE")],
    ["workspace root hidden", t.blob.includes("SBX_ROOT_HIDDEN") && !t.blob.includes("SBX_ROOT_VISIBLE")],
    ["other thread dir hidden", t.blob.includes("SBX_OTHER_HIDDEN") && !t.blob.includes("SBX_OTHER_VISIBLE")],
  ];
  if (NETWORK === "allowlist") {
    checks.push([`${DENIED_HOST} unreachable`, t.blob.includes("SBX_DENIED_BLOCKED") && !t.blob.includes("SBX_DENIED_REACHED")]);
  }

  // Turn 2 on the same thread: the resumed session reads back the turn-1 file.
  const previous = JSON.stringify(view.run);
  const msg2 = await http("POST", `/admin/tasks/${taskId}/message`, { text: "Run `cat sbx-proof.txt` with your Bash tool and reply with its output verbatim and nothing else." });
  if (msg2.status !== 200) fail(`message 2 ${msg2.status} ${msg2.text.slice(0, 200)}`);
  if (msg2.json?.data?.threadId !== threadId) fail(`turn 2 went to thread ${msg2.json?.data?.threadId}`);
  try { view = await waitTurn(taskId, threadId, previous, 300_000); }
  catch (e) { fail(`${e instanceof Error ? e.message : String(e)} — turn 2 did not finish`); }
  t = await transcript(taskId, threadId);
  log(`turn 2 run        : ${JSON.stringify(view.run)}`);
  if (view.run?.status !== "succeeded" && rateLimited(view, t.blob)) { log("\n⚠ RATE-LIMITED (429)"); process.exit(3); }
  checks.push(["turn 2 succeeded", view.run?.status === "succeeded"]);
  checks.push(["session resume reads the turn-1 file", t.blob.includes(readback)]);

  log("\n── checks ──");
  for (const [name, ok] of checks) log(`${ok ? "✓" : "✗"} ${name}`);
  if (checks.some(([, ok]) => !ok)) fail("sandbox assertions failed");
  log(`\n✓ REAL-SANDBOX PASS (${NETWORK})`);
  process.exit(0);
}

main().catch((e) => { log(`real sandbox e2e crashed: ${e instanceof Error ? e.stack : e}`); process.exit(2); });
