import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import process from "node:process";
import { createDockerLauncher, DockerOpTimeoutError } from "@gino/host-core";
import { describe, expect, it } from "bun:test";
import { bunSpawn } from "@/agents-bun-spawn";

// The app's `Bun.spawn` shell-out adapter behind `createDockerLauncher({ spawn })`:
// the launcher's per-op abort signal must kill the REAL child.

describe("bunSpawn — docker launcher deadline (real child process)", () => {
  it("kills the REAL child process at the deadline (the app's Bun.spawn seam)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "gino-docker-timeout-"));
    const pidFile = join(dir, "pid");
    const script = join(dir, "fake-docker.sh");
    // A fake docker binary that records its pid and wedges.
    writeFileSync(script, `#!/bin/sh\necho $$ > ${pidFile}\nexec sleep 60\n`, { mode: 0o755 });

    try {
      const launcher = createDockerLauncher({ docker: script, opTimeoutMs: 200, spawn: bunSpawn });
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
