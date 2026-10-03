// The app's shell-out adapter for `createDockerLauncher({ spawn })`: `Bun.spawn`
// with the launcher's extra env merged over the host process env. Core owns no
// process API (host-sdk-design D8), so the merge lives here.
import type { SpawnFn, SpawnOptions, SpawnResult } from "@gino/host-core";
import process from "node:process";

/**
 * Shell out via `Bun.spawn`. Only reached when an op actually runs (never at
 * import), so the docker binary is not required to load the module.
 */
export const bunSpawn: SpawnFn = async (cmd: readonly string[], opts?: SpawnOptions): Promise<SpawnResult> => {
  const proc = Bun.spawn([...cmd], {
    stdout: "pipe",
    stderr: "pipe",
    stdin: "ignore",
    // Extra env (secrets) merged over the host env so the docker
    // CLI keeps PATH/DOCKER_HOST while the values stay off its argv.
    ...(opts?.env ? { env: { ...process.env, ...opts.env } } : {}),
  });
  const onAbort = () => proc.kill();
  opts?.signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    const exitCode = await proc.exited;
    return { exitCode, stdout, stderr };
  }
  finally {
    opts?.signal?.removeEventListener("abort", onAbort);
  }
};
