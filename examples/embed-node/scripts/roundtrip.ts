// Round-trip one task through the embed-node host and the REAL gino-agent
// runner, under plain `node`:
//
//   start the server in-process (port 0) → spawn e2e/gino-agent.bin with the
//   fake `claude` engine first on PATH → agent.online → POST /api/tasks (pinned
//   to that agent) → POST /api/tasks/:id/messages → thread.result → the thread
//   transcript carries the echoed prompt → SIGTERM the runner → stop → exit 0.
//
// Any failure or timeout exits non-zero with a one-line reason. Needs the
// runner binary staged at <repo>/e2e/gino-agent.bin (scripts/roundtrip.sh
// builds it when missing) — hence this is `e2e`, not `check`.
/* eslint-disable no-console */
import type { HostEventMap, HostEventName, HostEvents } from "@gino/host-core";
import type { ChildProcess } from "node:child_process";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { consoleLogger, startServer } from "../src/server.ts";

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const RUNNER_BIN = join(REPO_ROOT, "e2e/gino-agent.bin");
const FAKE_CLAUDE = join(REPO_ROOT, "e2e/fake-claude");

const AGENT_NAME = "embed-node-runner";
const REGISTER_TOKEN = "embed-node-register-secret";
const OWNER_ID = "embed-node-operator";
const PROMPT = "hello from embed-node";

const ONLINE_TIMEOUT_MS = 30_000;
const RESULT_TIMEOUT_MS = 60_000;
const RUNNER_EXIT_TIMEOUT_MS = 10_000;

const log = (msg: string): void => console.log(`[roundtrip] ${msg}`);

function fail(msg: string): never {
  throw new Error(msg);
}

/** Poll `fn` until it returns a value or the deadline passes. */
async function waitFor<T>(label: string, fn: () => T | undefined, timeoutMs: number, everyMs = 250): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = fn();
    if (value !== undefined) {
      return value;
    }
    await new Promise(r => setTimeout(r, everyMs));
  }
  return fail(`timed out after ${timeoutMs} ms waiting for ${label}`);
}

/** Resolve with the first `name` event payload accepted by `pick`, or reject at the deadline. */
function waitForEvent<K extends HostEventName>(events: HostEvents, name: K, pick: (payload: HostEventMap[K]) => boolean, timeoutMs: number): Promise<HostEventMap[K]> {
  return new Promise((resolvePayload, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const off = events.on(name, (payload) => {
      if (pick(payload)) {
        clearTimeout(timer);
        off();
        resolvePayload(payload);
      }
    });
    timer = setTimeout(() => {
      off();
      reject(new Error(`timed out after ${timeoutMs} ms waiting for host event ${name}`));
    }, timeoutMs);
  });
}

/** Pipe a child's output to the console line by line under a prefix. */
function pipeOutput(child: ChildProcess, prefix: string): void {
  for (const stream of [child.stdout, child.stderr]) {
    let pending = "";
    stream?.setEncoding("utf8");
    stream?.on("data", (chunk: string) => {
      pending += chunk;
      const lines = pending.split("\n");
      pending = lines.pop() ?? "";
      for (const line of lines) {
        console.log(`${prefix} ${line}`);
      }
    });
    stream?.on("end", () => {
      if (pending) {
        console.log(`${prefix} ${pending}`);
      }
    });
  }
}

function stopRunner(runner: ChildProcess): Promise<number | null> {
  return new Promise((resolveCode) => {
    if (runner.exitCode !== null || runner.signalCode !== null) {
      resolveCode(runner.exitCode);
      return;
    }
    const killTimer = setTimeout(() => {
      log("runner did not exit on SIGTERM; sending SIGKILL");
      runner.kill("SIGKILL");
    }, RUNNER_EXIT_TIMEOUT_MS);
    runner.once("exit", (code) => {
      clearTimeout(killTimer);
      resolveCode(code);
    });
    runner.kill("SIGTERM");
  });
}

async function api<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  const text = await res.text();
  if (!res.ok) {
    fail(`${init?.method ?? "GET"} ${url} → ${res.status} ${text.slice(0, 300)}`);
  }
  return JSON.parse(text) as T;
}

const jsonPost = (body: unknown): RequestInit => ({ method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });

async function main(): Promise<void> {
  if (!existsSync(RUNNER_BIN)) {
    fail(`runner binary missing at ${RUNNER_BIN} — run \`bash e2e/run.sh --build-only\` (or scripts/roundtrip.sh)`);
  }

  const tmp = mkdtempSync(join(tmpdir(), "gino-embed-node-"));
  const binDir = join(tmp, "bin");
  const workspaceDir = join(tmp, "workspace");
  mkdirSync(binDir);
  mkdirSync(workspaceDir);
  symlinkSync(FAKE_CLAUDE, join(binDir, "claude"));

  let runner: ChildProcess | undefined;
  const server = await startServer({
    port: 0,
    registerToken: REGISTER_TOKEN,
    dbPath: join(tmp, "host", "gino.db"),
    storeDir: join(tmp, "host", "store"),
    logger: consoleLogger(),
  });
  const { host, url } = server;
  const api_ = `${url}/api`;

  // Observed event order — reported at the end, asserted in prose by the reader.
  const sequence: string[] = [];
  const watched: HostEventName[] = ["agent.enrolled", "agent.online", "task.placed", "transcript.message", "thread.result", "outbox.acked", "agent.offline"];
  for (const name of watched) {
    host.events.on(name, (payload) => {
      sequence.push(name);
      log(`event ${name} ${JSON.stringify(payload)}`);
    });
  }

  try {
    const online = waitForEvent(host.events, "agent.online", ({ agentId }) => host.agents.get(agentId)?.name === AGENT_NAME, ONLINE_TIMEOUT_MS);

    log(`spawning ${RUNNER_BIN} → GINO_HOST_URL=${api_}`);
    runner = spawn(RUNNER_BIN, [], {
      env: {
        ...process.env,
        PATH: `${binDir}:${process.env.PATH ?? ""}`,
        GINO_HOST_URL: api_,
        GINO_REGISTER_TOKEN: REGISTER_TOKEN,
        GINO_AGENT_NAME: AGENT_NAME,
        GINO_WORKSPACE_DIR: workspaceDir,
        GINO_DB_PATH: join(tmp, "agent.db"),
        RUST_LOG: "info",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    pipeOutput(runner, "[runner]");
    const runnerExited = new Promise<never>((_, reject) => {
      runner?.once("exit", (code, signal) => reject(new Error(`runner exited early (code=${code} signal=${signal})`)));
      runner?.once("error", reject);
    });

    // Event first; the agent list is the fallback if the event was already emitted.
    const { agentId } = await Promise.race([
      online,
      runnerExited,
      waitFor("agent online (list poll)", () => {
        const row = host.agents.list().find(a => a.name === AGENT_NAME && a.status === "online");
        return row ? { agentId: row.id } : undefined;
      }, ONLINE_TIMEOUT_MS),
    ]);
    log(`agent online: ${agentId}`);

    const created = await api<{ task: { id: string }; dispatch: { placed: boolean; agentId: string | null } }>(
      `${api_}/tasks`,
      jsonPost({ title: "embed-node round-trip", ownerId: OWNER_ID, agentId }),
    );
    if (!created.dispatch.placed || created.dispatch.agentId !== agentId) {
      fail(`task not placed on ${agentId}: ${JSON.stringify(created.dispatch)}`);
    }
    const taskId = created.task.id;
    log(`task ${taskId} placed on ${agentId}`);

    const result = waitForEvent(host.events, "thread.result", p => p.taskId === taskId, RESULT_TIMEOUT_MS);
    const sent = await api<{ threadId: string; seeded: boolean }>(`${api_}/tasks/${taskId}/messages`, jsonPost({ text: PROMPT }));
    log(`message sent on thread ${sent.threadId} (seeded task_upsert: ${sent.seeded})`);

    const { threadId, data } = await Promise.race([result, runnerExited]);
    if (threadId !== sent.threadId) {
      fail(`thread.result arrived for ${threadId}, expected ${sent.threadId}`);
    }
    if (data.status !== "succeeded") {
      fail(`thread ${threadId} did not succeed: ${JSON.stringify(data)}`);
    }

    const thread = await api<{ run: { status: string } | null; messages: { kind: string; content: string | null }[] }>(`${api_}/tasks/${taskId}/threads/${threadId}`);
    const contents = thread.messages.map(m => m.content ?? "");
    if (!contents.some(c => c.includes(PROMPT))) {
      fail(`transcript does not echo the prompt: ${JSON.stringify(contents)}`);
    }
    log(`transcript ok: ${JSON.stringify(contents)} (run status ${thread.run?.status})`);

    const threads = await api<{ threads: { threadId: string }[] }>(`${api_}/tasks/${taskId}/threads`);
    if (!threads.threads.some(t => t.threadId === threadId)) {
      fail(`GET /threads does not list ${threadId}`);
    }

    log(`event sequence: ${sequence.join(" → ")}`);
    log("PASS");
  }
  finally {
    if (runner) {
      const code = await stopRunner(runner);
      log(`runner exited (code=${code})`);
    }
    await server.stop();
    rmSync(tmp, { recursive: true, force: true });
  }
}

main().then(
  () => process.exit(0),
  (err: unknown) => {
    console.error(`[roundtrip] FAIL — ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  },
);
