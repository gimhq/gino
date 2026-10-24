# 20261024-0058-engine-sandbox Filesystem and network restrictions for engine runs

- **status**: implementing
- **createdAt**: 2026-10-24 00:58
- **approvedAt**: 2026-10-24 02:30
- **relatedTask**: 20261024-0058-engine-sandbox

## Context

- Deployment decision: gino-agent runs natively on the host as an unprivileged user (not in
  Docker) and launches every engine run through bubblewrap. Goal: filesystem and network
  restrictions, and task-level isolation (task A's run cannot see task B's context).
- Engine children are spawned at `engine/claude/mod.rs:397` and `engine/codex/mod.rs:349`
  with `--dangerously-skip-permissions` / `--sandbox danger-full-access`; isolation today is
  only the container boundary.
- One thread = one engine session; its work dir is `<workspace>/threads/<thread-id>`
  (`executor.rs:1441`). Threads of one task never share a run, so per-thread confinement
  implies per-task isolation.
- Cross-task context reachable from any run today:
  1. sibling thread work dirs;
  2. the shared engine config dir `<workspace>/engine-config` (`CLAUDE_CONFIG_DIR` /
     `CODEX_HOME`): all session transcripts, `.claude.json`, todos, shell snapshots;
  3. `<workspace>/conversations` (PreCompact archive of all sessions);
  4. `<workspace>/agent.db`: every task's messages (`outbox`), prompts (`threads`), the agent
     token (`identity`); the in-engine `gino` CLI opens it directly;
  5. shared `/tmp` and `$HOME`;
  6. other engine processes (`/proc/<pid>/cmdline|environ`).
- bubblewrap probe (sibling containers, non-root): full mode (`--unshare-all --proc /proc`)
  works once user namespaces are available; on a native host they are by default (Fedora).
- Codex cancel sends SIGINT to the child pid (`engine/codex/mod.rs:532`); claude cancels
  in-band. Under bwrap the direct child is `bwrap`, so signal delivery must be preserved.
- The host treats the config overlay as opaque; no host change is needed.

## Proposal

### Launch chain

```
gino-agent (daemon) ── per-run exit: transparent nsplane-netstack + DNS responder + policy
 └─ bwrap <mounts> --unshare-all --uid 0 --gid 0 --cap-add CAP_NET_ADMIN            ▲
          --die-with-parent --new-session --proc /proc --dev /dev --dev-bind /dev/net/tun
     └─ gino-agent sandbox-init --ctl-sock /run/gino/ctl.sock --                     │ TUN fd
         │  creates tun0, configures address/route/resolver, sends the TUN fd ───────┘ (SCM_RIGHTS)
         └─ [nested user namespace, uid 1000, no capabilities] claude … | codex …
```

- `sandbox-init` (new hidden subcommand) runs as uid 0 of the sandbox's own user namespace
  (unprivileged outside) with only `CAP_NET_ADMIN`. It creates `tun0` with `10.0.2.15/24`,
  default route and resolver `10.0.2.2`, passes the TUN fd to the daemon over the bound control
  socket, closes its own copy, then spawns the engine in a nested user namespace mapped to a
  non-root uid with no capabilities (claude refuses `--dangerously-skip-permissions` as root).
  It forwards SIGINT/SIGTERM to the engine and exits with its status. No network component
  keeps running inside the sandbox.
- The daemon spawns `bwrap` with the same stdio pipes it uses today, so the claude control
  protocol and codex JSON stream are unchanged.

### Mounts (everything else does not exist inside the sandbox)

- Read-only: `/usr`, `/etc`, the `/bin` `/lib` `/lib64` `/sbin` symlinks, the resolved engine
  binary and its install dir, the agent binary, the global provisioning dir, and
  `readPaths` from config.
- Read-write, at the same paths: this thread's work dir and `writePaths` from config.
- tmpfs: `/tmp`, `$HOME`. `TMPDIR=/tmp`.
- Per-thread engine home: `CLAUDE_CONFIG_DIR` / `CODEX_HOME` = `<work_dir>/.engine-home`, with
  the global `CLAUDE.md` and `skills/` read-only bind-mounted into it. Transcripts and resume
  state live there.
- Not mounted: `<workspace>` root, other threads, `agent.db`, global engine config,
  `conversations/`, `/run`, `/var`, the rest of `$HOME`.

### Network

The agent is the gateway: each run's guest traffic is terminated in a userspace stack inside the
daemon and allowed flows are re-originated from host sockets. Unprivileged, no extra
infrastructure, and SSH / raw TCP / UDP clients work without configuration.

- `--unshare-net` gives the sandbox its own loopback plus `tun0`; local servers and clients
  inside one run (including raw TCP/UDP protocol tests on `127.0.0.1`) never leave the sandbox.
- Per run, the daemon adopts the TUN fd (`nsplane_tun::Tun::from_fd`) and pumps it into its own
  transparent `nsplane-netstack` (upstream request R1). Runs are isolated by construction; a run
  ends by dropping its stack.
- DNS: a gino responder on `10.0.2.2` (UDP/TCP 53 accepted by the stack) resolves allowed names
  to addresses from a private pool (`198.18.0.0/15`) mapped back to the name; other names get
  `NXDOMAIN`. Every accepted flow therefore carries the name it was dialed by.
- Policy runs in the stack's asynchronous accept hook before the handshake completes (new flows
  wait in nsplane's bounded pending table; a late decision counts as a rejection): name (or
  literal-IP `cidr`), port and protocol against the rules. For an allowed flow the daemon
  resolves the real address and opens the host socket inside the hook, then accepts; refused or
  unreachable destinations get RST (TCP) or an ICMP error (UDP), never accept-then-close. The
  hook timeout is set above the upstream connect timeout. Private, loopback and link-local real addresses are refused unless a rule
  names them. Every decision is logged by gino from the accept hook (thread id, protocol,
  target, rule or reason).
- Per-run stack limits: TCP buffers 128 KiB, a TCP connection cap (default 256, also checked in
  the hook), `max_udp_flows` 1024. Upstream measured ~45 KiB per idle stack and ~330 KiB per
  busy connection at 128 KiB buffers (~6.6 MiB for a run with 20 busy connections).
- The LLM endpoint (`ANTHROPIC_BASE_URL` host or `api.anthropic.com`, the codex API host) is
  always allowed.
- `network.mode`: `none` (loopback only, no TUN), `allowlist` (rules), `full` (no
  `--unshare-net`, host network; explicit escape hatch for trusted tasks).
- VM mode reuses the same per-run exit behind an ethernet adapter (upstream R3) attached to
  libkrun virtio-net.
- Remote exit over WireGuard (fixed egress IP, other sites' networks) is a later extension:
  selected destinations go to one nsplane engine with one local identity per run and a
  WireGuard peer (upstream R5, R6).

Rules:

```json
"network": { "mode": "allowlist", "allow": [
  { "host": "github.com", "ports": [22, 443] },
  { "host": "*.npmjs.org", "ports": [443] },
  { "cidr": "10.20.0.0/16", "ports": "*", "proto": "udp" }
]}
```

`proto` defaults to `tcp`; `host` accepts exact names or `*.suffix`; `cidr` matches literal
IP destinations.

SSH keys never enter the sandbox: a follow-up adds a per-run ssh-agent socket served by the
daemon with task-scoped keys (`SSH_AUTH_SOCK`) and a read-only `known_hosts`.

### Other

- PreCompact hook: `gino-agent archive` runs inside the sandbox and writes to
  `<work_dir>/conversations` (`GINO_ARCHIVE_DIR`).
- `gino` CLI: `agent.db` is not mounted, so under the sandbox it reports that agent state is
  unavailable. Follow-up task: the CLI talks to the daemon over a bound Unix socket with a
  per-run token scoped to `$GINO_TASK_ID`.
- Fail closed: if `enabled` and `bwrap` is missing or a probe (`bwrap --unshare-all true`)
  fails at startup, engine runs fail with a dedicated error.
- Scope: claude and codex engine runs. Host-driven PTY and file operations are unchanged.

### Isolation backends

Engine spawning goes through one `Isolation` seam in the executor with three modes:

- `off`: spawn directly (current behavior).
- `sandbox`: bubblewrap, as specified above. Implemented in this plan.
- `vm`: a short-lived microVM per engine session, booted by the agent and torn down when the
  session ends. Distinct from host launchers: the host launches long-lived, registered agents;
  the agent's VM is an ephemeral execution environment with no registration and no protocol of
  its own. The thread work dir and engine home are shared into the guest, stdio rides on vsock,
  the guest NIC is libkrun virtio-net attached to the same net backend, and the guest has its
  own Docker daemon, so tasks that build or run containers use this mode. Separate task
  `20261024-0130-engine-vm-isolation`: libkrun, a custom trimmed guest kernel, virtiofs, and a
  host-side pull-through registry mirror for Docker images. The net backend, rules and
  per-thread engine home from this plan are reused.

The mode is chosen per run: task `meta.isolation` (already stored on the agent) overrides the
configured default. Asking for `vm` before that backend exists fails the run.

### Config

```json
"engine": { "isolation": {
  "mode": "sandbox",
  "network": { "mode": "allowlist", "allow": [ { "host": "github.com", "ports": [22, 443] } ] },
  "readPaths": [],
  "writePaths": []
}}
```

Absent block = `off`. Paths are validated: absolute, and not the workspace root or one of its
parents. Task `meta.isolation` may override `mode` and `network`.

### Files

`src/sandbox/{mod.rs,bwrap.rs,init.rs}` and `src/net/{mod.rs,exit.rs,dns.rs,policy.rs}` (new),
`src/lib.rs` (subcommand), `config.rs`, `executor.rs` (per-thread home and paths),
`engine/claude/mod.rs`, `engine/codex/mod.rs`, `archive.rs`, `gino_cli.rs`,
`docs/agent-architecture.md` (native deployment and sandbox section). New crates:
`nsplane-netstack`, `nsplane-tun` (git); `nix` gains the features needed for TUN ioctls,
`SCM_RIGHTS` and user-namespace setup.

### Tests

- Unit: config merge/validation, bwrap argv derivation, domain matching, CONNECT parsing,
  private-address refusal.
- Integration (need bwrap + user namespaces + `/dev/net/tun`; run in a sibling container with
  the custom seccomp profile, `systempaths=unconfined` and `--device /dev/net/tun`, skipped
  otherwise): two sandboxed threads — A cannot see B's work dir, B's engine home, `agent.db` or
  B's processes; writes outside the work dir fail; TCP and UDP to an allowed host:port succeed
  (including SSH-style TCP on a non-HTTP port) and to anything else fail; a non-allowed name
  gets NXDOMAIN; loopback TCP/UDP inside the sandbox works in `none` mode; SIGINT reaches the
  engine.
- Net unit tests: rule matching, DNS answers and pool mapping, accept-hook decisions, relay
  close/RST propagation, driven through an in-memory packet pipe into the transparent stack.
- Real engine: claude starts, runs a tool, resumes a session and compacts (archive hook)
  under the sandbox.

## Risks

- Deployment change: the agent leaves Docker, so the host user account is the outer boundary.
  Run it as a dedicated unprivileged user; bwrap must be installed and user namespaces enabled.
- No TLS termination or payload inspection: once a host:port is allowed, any protocol can be
  tunnelled to it.
- A client that connects to a literal IP bypasses virtual DNS and only matches `cidr` rules.
- The TUN path needs `/dev/net/tun` on the host and `CAP_NET_ADMIN` inside the sandbox's own
  user namespace (probe: TUN and TAP creation work with `--uid 0 --cap-add CAP_NET_ADMIN`;
  fail as a non-zero sandbox uid). Running the engine in a nested non-root user namespace must
  be verified against the real claude binary.
- The `allowlist` mode depends on upstream nsplane work (R1 transparent netstack, accepted
  upstream; DNS stays in gino);
  nsplane is git-only today, so `deny.toml` needs `allow-git` entries for `dotns/nsplane` and
  `dotns/smoltcp`. Userspace TCP throughput is below the kernel stack; large downloads need a
  measurement.
- Credentials: `claude login` state in the shared config dir is not visible in per-thread
  homes; engines authenticate through env (`ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`) or
  the host LLM proxy. Env secrets remain visible to the engine itself, as today.
- Until the CLI follow-up lands, the `gino` skill (cron management) does not work in sandboxed
  runs.
- Engine path needs (extra caches, install locations) must be tuned against the real claude
  and codex binaries; `readPaths` covers deployment-specific cases.
- Dev environment has no user namespaces; sandbox integration tests run only in a privileged
  sibling container or on a native host.

## Scope

Two steps. Step 1 (no upstream dependency): isolation seam, bubblewrap mounts, per-thread engine
home and archive, `sandbox-init`, network modes `none` and `full`. Step 2: TUN creation and
handover, rules and their matching, the upstream connect path, the DNS name/pool policy and
the per-run limits are built and tested before nsplane lands; only the glue to the transparent
netstack (and to nsplane's DNS responder, if it ships) waits for the upstream release. About 16 files in `gino-agent`,
roughly 2000–2500 lines including tests. No protocol, host or
schema change. Follow-up tasks: `gino` CLI over a task-scoped daemon socket; native deployment
packaging (systemd unit); the `vm` isolation backend; container builds for sandboxed tasks (the Docker socket is never
mounted into the sandbox — it is host root; builds go through a daemon-mediated build
operation instead).

## Alternatives

1. Landlock (in-process, no namespaces): works inside default Docker, but port-only network
   rules, no UDP/DNS control, allowlist-only paths. Superseded by the native-host decision.
2. Anthropic sandbox-runtime (srt): bubblewrap plus its own HTTP/SOCKS proxies, TypeScript and
   a Node/Bun runtime, socat and ripgrep; pre-1.0. Same mechanism as this proposal with more
   dependencies.
3. Claude Code's built-in `sandbox` setting: srt applied to the Bash tool only; Read/Edit/Grep
   are not confined, so it cannot provide task isolation.
4. Policy in `can_use_tool`: filters tool intents, not syscalls. Complementary only.
5. Raw `smoltcp` with a hand-written transparent layer instead of `nsplane-netstack`: no
   upstream dependency, but connection tracking, backpressure, PMTU and revocation would be
   rewritten in gino. Rejected in favour of adding a transparent mode to nsplane-netstack.
6. WireGuard to a separate exit gateway (kernel WireGuard + nftables, or remote): authoritative
   gateway-side policy and fixed egress IP, but needs root or extra infrastructure and degrades
   name rules to address rules. Kept as a later remote-exit extension.

## Annotations

- 2026-10-24: user asked for task-level isolation (task A must not see task B's context).
- 2026-10-24: user reviewed srt and chose bubblewrap; gino-agent will run natively on the host
  instead of in Docker.
- 2026-10-24: user asked for two modes, sandbox and VM, with the agent itself booting
  short-lived VMs for one-shot or short tasks (host launchers stay for long-lived agents).
  Added the isolation seam; the VM backend gets its own plan.
- 2026-10-24: user asked how SSH and raw TCP/UDP work; network design switched from an HTTP
  proxy to transparent TUN capture (tun2proxy, SOCKS5 + udpgw, virtual DNS) with host/port/proto
  rules and a per-task `none|allowlist|full` mode.
- 2026-10-24: user chose a TAP device with the userspace stack in the daemon (smoltcp) over
  tun2proxy, so the sandbox and VM modes share one ethernet-frame net backend.
- 2026-10-24: user prefers WireGuard egress through nsplane (dotns) to an exit gateway over an
  in-agent transparent stack; upstream requirements handed to the dotns/nsplane maintainers.
- 2026-10-24: agent is its own gateway: per-run transparent nsplane-netstack terminates guest
  flows and re-originates allowed ones from host sockets; WireGuard only for a later remote
  exit. Upstream requirements reprioritized (R1 transparent netstack is P0).
- 2026-10-24: upstream accepted the transparent netstack, ethernet adapter, decision events,
  dynamic membership and address sets (the last breaking, 0.17.0); DNS interception declined and
  stays in gino. Requested order: transparent netstack, ethernet adapter, events.
- 2026-10-24: upstream design answers: transparent mode inside `nsplane-netstack`; async accept
  hook with a bounded pending table and timeout-as-reject; remote exit uses one engine with a
  local identity per run.
- 2026-10-24: upstream measured per-stack cost and will add a per-stack TCP connection cap to
  R1; decision events (R4) are not needed for the local exit because gino logs from its hook.
- 2026-10-24: upstream started R1 and is now also taking a DNS responder; gino keeps the name
  policy and fake-address pool and plugs into the responder's resolve callback when it ships.
- 2026-10-24: VM backend direction: libkrun + custom trimmed kernel + virtiofs, Docker images
  cached through a registry mirror (task `20261024-0130-engine-vm-isolation`).
