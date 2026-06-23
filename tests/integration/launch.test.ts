import type { SpawnFn } from "@/agents/launchers";
import type { AppDatabase } from "@/db";
import { rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import process from "node:process";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import { createDockerLauncher, dockerLauncher, registerLauncher } from "@/agents/launchers";
import { createApp } from "@/app";
import { loadConfigStrict } from "@/config";
import { createDb } from "@/db";
import { createLogger } from "@/shared/lib/logger";

// D-B2b — launch + launch↔register nonce correlation (OD17), end-to-end through
// the HTTP surface. The docker launcher's shell-out is MOCKED (a recording
// `spawn`) and registered into the kind→launcher registry, so the real
// `/admin/agents/launch` → `control.launchAgent` → docker launcher path runs
// with NO real docker. Restored to the real docker launcher after the suite.

const REGISTER_TOKEN = "reg-secret-launch";
const CHANNEL_TOKEN = "chan-secret-launch";

const dbPath = resolve(tmpdir(), `gino-launch-${process.pid}-${Date.now()}.db`);
const config = loadConfigStrict({
  NODE_ENV: "test",
  GINO_DB_PATH: dbPath,
  GINO_REGISTER_TOKEN: REGISTER_TOKEN,
  GINO_HTTP_CHANNEL_TOKEN: CHANNEL_TOKEN,
  GINO_LOG_LEVEL: "error",
  GINO_LOG_TO_STDOUT: "true",
});

let db: AppDatabase;
let app: ReturnType<typeof createApp>;

/** Captured docker argv across the suite (the recording `spawn` pushes here). */
let spawnCalls: string[][] = [];

const recordingSpawn: SpawnFn = async (cmd) => {
  spawnCalls.push([...cmd]);
  // `docker run -d` (the only command the launch path issues) prints the id.
  return { exitCode: 0, stdout: "ctr-launch-1\n", stderr: "" };
};

beforeAll(() => {
  db = createDb(config.DB_PATH);
  app = createApp({ config, db, logger: createLogger(config) });
  // Swap the global "docker" launcher for a mocked one (recording spawn + a
  // fixed image so a body without `image` still launches).
  registerLauncher(createDockerLauncher({ image: "gino/agent:test", spawn: recordingSpawn }));
});

afterAll(() => {
  // Restore the real docker launcher so the global registry is left as found.
  registerLauncher(dockerLauncher);
  db.close();
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      rmSync(`${dbPath}${suffix}`, { force: true });
    }
    catch {}
  }
});

beforeEach(() => {
  spawnCalls = [];
});

function admin(path: string, init?: RequestInit) {
  return app.fetch(new Request(`http://local/api/admin${path}`, {
    ...init,
    headers: { authorization: `Bearer ${CHANNEL_TOKEN}`, ...(init?.headers ?? {}) },
  }));
}

async function launch(body: Record<string, unknown>) {
  const res = await admin("/agents/launch", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  return res;
}

async function register(body: Record<string, unknown>) {
  const res = await app.fetch(new Request("http://local/api/agent/register", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ registerToken: REGISTER_TOKEN, protocolVersion: 2, capabilities: ["claude"], ...body }),
  }));
  return res;
}

describe("D-B2b /admin/agents/launch — auth + wiring", () => {
  it("401 without the channel bearer", async () => {
    const res = await app.fetch(new Request("http://local/api/admin/agents/launch", { method: "POST" }));
    expect(res.status).toBe(401);
  });

  it("invokes the (mocked) docker launcher and returns launcherKind + nonce + ref", async () => {
    const res = await launch({ kind: "docker", hostUrl: "https://host.example" });
    expect(res.status).toBe(200);
    const body = await res.json() as { success: boolean; data: { launcherKind: string; nonce: string; ref: string } };
    expect(body.success).toBe(true);
    expect(body.data.launcherKind).toBe("docker");
    expect(body.data.ref).toBe("ctr-launch-1");
    expect(body.data.nonce).toMatch(/^[0-9a-f]{48}$/); // 24 bytes hex

    // The launcher shelled out `docker run -d` with the host-generated nonce
    // and the host-authoritative bootstrap env (NEVER the client's register token).
    expect(spawnCalls).toHaveLength(1);
    const cmd = spawnCalls[0]!;
    expect(cmd.slice(0, 3)).toEqual(["docker", "run", "-d"]);
    expect(cmd).toContain("GINO_HOST_URL=https://host.example");
    expect(cmd).toContain(`GINO_REGISTER_TOKEN=${REGISTER_TOKEN}`);
    expect(cmd).toContain(`GINO_LAUNCH_NONCE=${body.data.nonce}`);
    expect(cmd).toContain("ANTHROPIC_BASE_URL=https://host.example/llm");
  });

  it("422 when hostUrl is missing (route is validated)", async () => {
    const res = await launch({ kind: "docker" });
    expect(res.status).toBe(422);
  });

  it("422 when the kind cannot launch (manual is safe-by-default)", async () => {
    const res = await launch({ kind: "manual", hostUrl: "https://host.example" });
    expect(res.status).toBe(422);
    expect(await res.json()).toMatchObject({ success: false, error: { code: "LAUNCH_UNSUPPORTED" } });
  });
});

describe("D-B2b launch↔register correlation (OD17)", () => {
  it("a register echoing the launch nonce binds the agent to launcher_kind + launch_ref", async () => {
    const launched = await launch({ kind: "docker", hostUrl: "https://host.example", image: "custom/img" });
    const { data } = await launched.json() as { data: { nonce: string; ref: string } };

    const reg = await register({ source: "docker", launchNonce: data.nonce });
    expect(reg.status).toBe(200);
    const { agentId, connectToken } = await reg.json() as { agentId: string; connectToken: string };
    expect(agentId).toBeTruthy();
    expect(connectToken).toBeTruthy(); // register still returns the connect token normally

    const get = await admin(`/agents/${agentId}`);
    const agent = (await get.json() as { data: { launcherKind: string | null; launchRef: string | null } }).data;
    expect(agent.launcherKind).toBe("docker");
    expect(agent.launchRef).toBe(data.ref);
  });

  it("a stale/unknown nonce binds nothing (register still succeeds)", async () => {
    const reg = await register({ launchNonce: "does-not-exist" });
    expect(reg.status).toBe(200);
    const { agentId } = await reg.json() as { agentId: string };

    const get = await admin(`/agents/${agentId}`);
    const agent = (await get.json() as { data: { launcherKind: string | null; launchRef: string | null } }).data;
    expect(agent.launcherKind).toBeNull();
    expect(agent.launchRef).toBeNull();
  });

  it("a register WITHOUT a nonce is unaffected (backward-compatible manual enroll)", async () => {
    const reg = await register({});
    expect(reg.status).toBe(200);
    const body = await reg.json() as { agentId: string; connectToken: string; config: { protocolVersion: number } };
    // Exactly the pre-B2b register contract: agentId + connectToken + config.
    expect(body.agentId).toBeTruthy();
    expect(body.connectToken).toBeTruthy();
    expect(body.config.protocolVersion).toBe(2);

    const get = await admin(`/agents/${body.agentId}`);
    const agent = (await get.json() as { data: { launcherKind: string | null; launchRef: string | null; source: string } }).data;
    expect(agent.launcherKind).toBeNull(); // no launch correlated
    expect(agent.launchRef).toBeNull();
    expect(agent.source).toBe("manual");
  });
});

describe("D-B2b control.launchAgent — nonce is single-use", () => {
  it("a nonce correlates only once (second register with the same nonce binds nothing)", async () => {
    const launched = await launch({ kind: "docker", hostUrl: "https://host.example" });
    const { data } = await launched.json() as { data: { nonce: string; ref: string } };

    const first = await register({ launchNonce: data.nonce });
    const { agentId: firstId } = await first.json() as { agentId: string };
    const second = await register({ launchNonce: data.nonce });
    const { agentId: secondId } = await second.json() as { agentId: string };

    const firstAgent = (await (await admin(`/agents/${firstId}`)).json() as { data: { launchRef: string | null } }).data;
    const secondAgent = (await (await admin(`/agents/${secondId}`)).json() as { data: { launchRef: string | null } }).data;
    expect(firstAgent.launchRef).toBe(data.ref); // first consumed the pending launch
    expect(secondAgent.launchRef).toBeNull(); // nonce already consumed
  });
});
