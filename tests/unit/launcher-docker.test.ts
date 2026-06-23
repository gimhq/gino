import type { SpawnFn, SpawnResult } from "@/agents/launchers";
import { describe, expect, it } from "bun:test";
import { createDockerLauncher, DOCKER_LAUNCHER_KIND, dockerLauncher } from "@/agents/launchers";

// M3 docker launcher — the first REAL launcher, exercised against a MOCKED
// docker CLI (the `spawn` seam). NO real docker, NO containers. Assertions
// pin the exact `docker` argv (incl. the injected bootstrap env) the launcher
// shells out, and the inspect→state mapping.

/** A recording `spawn` double: captures every argv and returns a canned result. */
function recordingSpawn(result: Partial<SpawnResult> = {}) {
  const calls: string[][] = [];
  const spawn: SpawnFn = async (cmd) => {
    calls.push([...cmd]);
    return { exitCode: 0, stdout: "container-abc123\n", stderr: "", ...result };
  };
  return { calls, spawn };
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
    const { calls, spawn } = recordingSpawn();
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
    // Bootstrap env injected as `-e K=V` pairs.
    expect(cmd).toContain("GINO_HOST_URL=https://host.example");
    expect(cmd).toContain("GINO_REGISTER_TOKEN=reg-tok");
    expect(cmd).toContain("GINO_LAUNCH_NONCE=nonce-xyz");
    expect(cmd).toContain("ANTHROPIC_BASE_URL=https://host.example/llm");
    expect(cmd).toContain("OPENAI_BASE_URL=https://host.example/llm");
    expect(cmd).toContain("EXTRA=1"); // caller env passed through
    // The image is the final positional arg (after every flag).
    expect(cmd.at(-1)).toBe("gino/agent:latest");
  });

  it("trims a trailing slash off hostUrl when building the /llm base", async () => {
    const { calls, spawn } = recordingSpawn();
    await createDockerLauncher({ image: "img", spawn }).launch({
      hostUrl: "https://host.example/",
      registerToken: "t",
      workload: {},
    });
    expect(calls[0]).toContain("ANTHROPIC_BASE_URL=https://host.example/llm");
    expect(calls[0]).toContain("GINO_HOST_URL=https://host.example/"); // raw host url preserved
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
