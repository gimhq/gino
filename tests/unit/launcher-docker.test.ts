import type { SpawnFn, SpawnOptions, SpawnResult } from "@/agents/launchers";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { describe, expect, it } from "bun:test";
import { createDockerLauncher, DOCKER_LAUNCHER_KIND, dockerLauncher, DockerOpTimeoutError } from "@/agents/launchers";

// M3 docker launcher — the first REAL launcher, exercised against a MOCKED
// docker CLI (the `spawn` seam). NO real docker, NO containers. Assertions
// pin the exact `docker` argv (incl. the injected bootstrap env) the launcher
// shells out, and the inspect→state mapping.

/** A recording `spawn` double: captures every argv (+opts) and returns a canned result. */
function recordingSpawn(result: Partial<SpawnResult> = {}) {
  const calls: string[][] = [];
  const optsCalls: (SpawnOptions | undefined)[] = [];
  const spawn: SpawnFn = async (cmd, opts) => {
    calls.push([...cmd]);
    optsCalls.push(opts);
    return { exitCode: 0, stdout: "container-abc123\n", stderr: "", ...result };
  };
  return { calls, optsCalls, spawn };
}

describe("M3 docker launcher — registry + capabilities", () => {
  it("is registered as the 'docker' kind with all capabilities true", () => {
    expect(dockerLauncher.kind).toBe(DOCKER_LAUNCHER_KIND);
    expect(dockerLauncher.kind).toBe("docker");
    expect(dockerLauncher.capabilities).toEqual({
      canLaunch: true,
      canStop: true,
      canRestart: true,
      canInspect: true,
    });
  });

  it("constructs without touching docker at import time", () => {
    // No spawn given → no shell-out happens just from building the launcher.
    expect(() => createDockerLauncher()).not.toThrow();
  });
});

describe("M3 docker launcher — launch (docker run -d + injected env)", () => {
  it("injects GINO_HOST_URL, GINO_REGISTER_TOKEN, the nonce + LLM base urls", async () => {
    const { calls, optsCalls, spawn } = recordingSpawn();
    const launcher = createDockerLauncher({ image: "gino/agent:latest", spawn });

    const res = await launcher.launch({
      hostUrl: "https://host.example",
      registerToken: "reg-tok",
      workload: { taskId: "t-1" },
      env: { GINO_LAUNCH_NONCE: "nonce-xyz", EXTRA: "1" },
    });

    expect(res.ref).toBe("container-abc123"); // stdout trimmed = container id
    const cmd = calls[0]!;
    expect(cmd[0]).toBe("docker");
    expect(cmd.slice(1, 3)).toEqual(["run", "-d"]);
    // Bootstrap env named via value-less `-e KEY` (HOST-M3-11); the values
    // ride the spawn seam's `env` option, off the argv.
    for (const key of ["GINO_HOST_URL", "GINO_REGISTER_TOKEN", "GINO_LAUNCH_NONCE", "ANTHROPIC_BASE_URL", "OPENAI_BASE_URL", "EXTRA"]) {
      expect(cmd).toContain(key);
      expect(cmd[cmd.indexOf(key) - 1]).toBe("-e");
    }
    expect(optsCalls[0]?.env).toEqual({
      GINO_HOST_URL: "https://host.example",
      GINO_REGISTER_TOKEN: "reg-tok",
      GINO_LAUNCH_NONCE: "nonce-xyz",
      ANTHROPIC_BASE_URL: "https://host.example/llm",
      OPENAI_BASE_URL: "https://host.example/llm",
      EXTRA: "1",
    });
    // The image is the final positional arg (after every flag).
    expect(cmd.at(-1)).toBe("gino/agent:latest");
  });

  it("HOST-M3-11: no env value — the register token above all — appears in the argv", async () => {
    const { calls, spawn } = recordingSpawn();
    await createDockerLauncher({ image: "img", spawn }).launch({
      hostUrl: "https://host.example",
      registerToken: "super-secret-token",
      workload: {},
      env: { GINO_LAUNCH_NONCE: "nonce-abc" },
    });
    const cmd = calls[0]!;
    expect(cmd.some(arg => arg.includes("super-secret-token"))).toBe(false);
    expect(cmd.some(arg => arg.includes("nonce-abc"))).toBe(false);
    expect(cmd.some(arg => arg.includes("="))).toBe(false); // no -e KEY=VALUE form at all
  });

  it("trims a trailing slash off hostUrl when building the /llm base", async () => {
    const { optsCalls, spawn } = recordingSpawn();
    await createDockerLauncher({ image: "img", spawn }).launch({
      hostUrl: "https://host.example/",
      registerToken: "t",
      workload: {},
    });
    expect(optsCalls[0]?.env?.ANTHROPIC_BASE_URL).toBe("https://host.example/llm");
    expect(optsCalls[0]?.env?.GINO_HOST_URL).toBe("https://host.example/"); // raw host url preserved
  });

  it("takes the image from req.resources.image over the constructor default", async () => {
    const { calls, spawn } = recordingSpawn();
    const launcher = createDockerLauncher({ image: "default-img", spawn });
    await launcher.launch({ hostUrl: "http://h", registerToken: "t", workload: {}, resources: { image: "override-img" } });
    expect(calls[0]!.at(-1)).toBe("override-img");
  });

  it("inserts configured runArgs after the env (before the image)", async () => {
    const { calls, spawn } = recordingSpawn();
    const launcher = createDockerLauncher({ image: "img", runArgs: ["--rm", "--network", "host"], spawn });
    await launcher.launch({ hostUrl: "http://h", registerToken: "t", workload: {} });
    const cmd = calls[0]!;
    expect(cmd).toContain("--rm");
    expect(cmd.indexOf("--rm")).toBeLessThan(cmd.length - 1); // before the image
    expect(cmd.at(-1)).toBe("img");
  });

  it("throws when no image is configured", async () => {
    const launcher = createDockerLauncher({ spawn: recordingSpawn().spawn });
    await expect(launcher.launch({ hostUrl: "http://h", registerToken: "t", workload: {} })).rejects.toThrow(/image/);
  });

  it("throws when `docker run` exits non-zero", async () => {
    const spawn: SpawnFn = async () => ({ exitCode: 125, stdout: "", stderr: "no such image" });
    const launcher = createDockerLauncher({ image: "img", spawn });
    await expect(launcher.launch({ hostUrl: "http://h", registerToken: "t", workload: {} })).rejects.toThrow(/docker run/);
  });
});

describe("M3 docker launcher — stop / restart / remove", () => {
  it("stop → `docker stop <ref>`", async () => {
    const { calls, spawn } = recordingSpawn();
    await createDockerLauncher({ spawn }).stop("c-1");
    expect(calls[0]).toEqual(["docker", "stop", "c-1"]);
  });

  it("restart → `docker restart <ref>`", async () => {
    const { calls, spawn } = recordingSpawn();
    await createDockerLauncher({ spawn }).restart("c-2");
    expect(calls[0]).toEqual(["docker", "restart", "c-2"]);
  });

  it("remove → `docker rm -f <ref>` (force teardown)", async () => {
    const { calls, spawn } = recordingSpawn();
    await createDockerLauncher({ spawn }).remove("c-3");
    expect(calls[0]).toEqual(["docker", "rm", "-f", "c-3"]);
  });

  it("a failed runtime op throws (loud, not silent)", async () => {
    const spawn: SpawnFn = async () => ({ exitCode: 1, stdout: "", stderr: "boom" });
    await expect(createDockerLauncher({ spawn }).stop("x")).rejects.toThrow();
  });

  it("honours a custom docker binary path", async () => {
    const { calls, spawn } = recordingSpawn();
    await createDockerLauncher({ docker: "/usr/local/bin/podman", spawn }).stop("c");
    expect(calls[0]![0]).toBe("/usr/local/bin/podman");
  });
});

describe("M3 docker launcher — inspect (docker inspect → coarse state)", () => {
  function inspectWith(stdout: string, exitCode = 0) {
    return createDockerLauncher({ spawn: async () => ({ exitCode, stdout, stderr: "" }) }).inspect("c");
  }

  it("maps the container State.Status → InspectState", async () => {
    expect(await inspectWith("running\n")).toBe("running");
    expect(await inspectWith("exited\n")).toBe("exited");
    expect(await inspectWith("dead\n")).toBe("exited");
    expect(await inspectWith("created\n")).toBe("unknown");
    expect(await inspectWith("paused\n")).toBe("unknown");
  });

  it("a docker error (e.g. no such container) → 'unknown' (no throw)", async () => {
    expect(await inspectWith("", 1)).toBe("unknown");
  });

  it("calls `docker inspect -f {{.State.Status}} <ref>`", async () => {
    const { calls, spawn } = recordingSpawn({ stdout: "running\n" });
    await createDockerLauncher({ spawn }).inspect("c-9");
    expect(calls[0]).toEqual(["docker", "inspect", "-f", "{{.State.Status}}", "c-9"]);
  });
});

describe("M3 docker launcher — per-op deadline (HOST-M3-10)", () => {
  /** A `spawn` double that never resolves — a wedged docker daemon. */
  function hangingSpawn() {
    const signals: (AbortSignal | undefined)[] = [];
    const spawn: SpawnFn = (_cmd, opts) => {
      signals.push(opts?.signal);
      return new Promise<SpawnResult>(() => {}); // hangs forever
    };
    return { signals, spawn };
  }

  it("a hanging control op rejects with the DISTINCT DockerOpTimeoutError at the deadline", async () => {
    const { signals, spawn } = hangingSpawn();
    const launcher = createDockerLauncher({ spawn, opTimeoutMs: 25 });
    const err = await launcher.stop("c-1").then(
      () => null,
      (e: unknown) => e,
    );
    expect(err).toBeInstanceOf(DockerOpTimeoutError);
    expect((err as Error).message).toMatch(/timed out after 25ms/);
    // The kill path fired: the seam's abort signal was aborted.
    expect(signals[0]?.aborted).toBe(true);
  });

  it("a hanging launch times out too (no admin request hangs on `docker run`)", async () => {
    const { spawn } = hangingSpawn();
    const launcher = createDockerLauncher({ image: "img", spawn, opTimeoutMs: 25 });
    await expect(launcher.launch({ hostUrl: "http://h", registerToken: "t", workload: {} }))
      .rejects
      .toBeInstanceOf(DockerOpTimeoutError);
  });

  it("a hanging inspect degrades to 'unknown' (best-effort observation, no throw)", async () => {
    const { spawn } = hangingSpawn();
    const launcher = createDockerLauncher({ spawn, opTimeoutMs: 25 });
    expect(await launcher.inspect("c-1")).toBe("unknown");
  });

  it("kills the REAL child process at the deadline (default Bun.spawn seam)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gino-docker-timeout-"));
    const pidFile = join(dir, "pid");
    const script = join(dir, "fake-docker.sh");
    // A fake docker binary that records its pid and wedges.
    writeFileSync(script, `#!/bin/sh\necho $$ > ${pidFile}\nexec sleep 60\n`, { mode: 0o755 });

    try {
      const launcher = createDockerLauncher({ docker: script, opTimeoutMs: 200 });
      await expect(launcher.stop("c-1")).rejects.toBeInstanceOf(DockerOpTimeoutError);

      const pid = Number.parseInt(readFileSync(pidFile, "utf8").trim(), 10);
      expect(pid).toBeGreaterThan(0);
      // The child must be dead (or dying) after the deadline fired.
      const deadline = Date.now() + 2000;
      let alive = true;
      while (alive && Date.now() < deadline) {
        try {
          process.kill(pid, 0);
          await new Promise(r => setTimeout(r, 50));
        }
        catch {
          alive = false;
        }
      }
      expect(alive).toBe(false);
    }
    finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
