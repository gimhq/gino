# 20261024-2050-engine-vm-isolation VM isolation backend for engine runs

- **status**: draft
- **createdAt**: 2026-10-24 20:50
- **approvedAt**: (pending)
- **relatedTask**: 20261024-0130-engine-vm-isolation

## Context

- `engine.isolation.mode` already accepts `vm` and orders it as the strictest mode
  (`gino-agent/src/sandbox/mod.rs`, `IsolationMode::Vm`, strictness 2), but every `vm` run fails
  before spawn with `IsolationError::NotSupported`.
- The sandbox mode (docs/agent-architecture.md §9d) provides everything a VM run can reuse:
  - per-thread work dir and engine home, per-thread archive dir;
  - the per-run network exit `net::exit::Exit::start_with(source, sink, ExitConfig)`
    (`gino-agent/src/net/exit.rs`): transparent `nsplane-netstack`, async admission, rules,
    limits, DNS on `10.0.2.2`, decision log, LLM endpoint handling (loopback rewrite to
    `10.0.2.2`, floor-derived endpoint, link-local refusal);
  - the local agent API (docs/agent-architecture.md §10b): run tokens, `GINO_AGENT_SOCK`,
    `GINO_AGENT_TOKEN`, the `gino` CLI as a client.
- `nsplane-ether` (nsplane v0.19.0, already a transitive dependency family of `nsplane-*`)
  turns guest ethernet frames into IP packets: `spawn_port(reader, writer, EtherConfig) ->
  (EtherSource, EtherSink, EtherHandle)` over a length-prefixed byte stream (the framing of
  passt / QEMU stream / libkrun `unixstream`), with ARP/ND for the gateway, optional
  single-lease DHCPv4 and a neighbor proxy.
- libkrun 1.19.x builder C API (`include/libkrun.h`):
  - `krun_payload_load_external(kernel, format, initrd, cmdline)` — custom kernel;
  - `krun_fs_device_new(tag, host_path)` / `_read_only` / `krun_fs_overlay_*` — virtiofs shares
    and injected files;
  - `krun_console_builder_add_inout_port(name, in_fd, out_fd)` — guest console ports on host fds;
  - `krun_vsock_device_new(cid, tsi_flags)` with TSI disabled, `krun_vsock_device_add_unix_port`
    — guest vsock port to a host Unix socket;
  - `krun_net_device_new_unixstream_path` — virtio-net on a Unix stream;
  - `krun_vmm_builder_build` / `krun_vmm_run` (blocks; the VMM owns the process).
  The header also ships `*_fn` typedefs for loading the library with `dlopen`.
- gino-agent forbids `unsafe` (`[lints.rust] unsafe_code = "forbid"`), so FFI to libkrun cannot
  live in the agent crate.
- Decisions already taken (task `20261024-0130-engine-vm-isolation`): libkrun, a custom trimmed
  guest kernel, virtiofs shares, one short-lived microVM per engine session started and torn down
  by the agent (distinct from host launchers, which manage long-lived agents). No Docker in the
  first version (user, 2026-10-24).
- **Environment constraint**: the current development host is a GCE VM (AMD EPYC 7B13) without
  nested virtualization — no `svm`/`vmx` flag, no `kvm` module, no `/dev/kvm`. VM runs cannot be
  developed or tested here. Implementation needs a KVM-capable host (GCE nested virtualization if
  the machine type supports it, a KVM-capable machine, or CI runners exposing `/dev/kvm`).

## Proposal

### Overview

```
gino-agent (daemon, unprivileged, /dev/kvm access)
 ├─ per-run dir (0700): spec.json, net.sock (ether), ctl.sock
 ├─ net::exit (unchanged) ◀── nsplane-ether ◀── net.sock ◀── virtio-net (libkrun unixstream)
 ├─ agent API socket ◀──────────────────────────────── vsock port (guest → host Unix socket)
 └─ gino-vmm <run spec>        (helper process; libkrun FFI; dies with the daemon)
     └─ microVM: custom kernel, virtiofs root (read-only image) + shares
         └─ gino-agent guest-init (PID 1)
             ├─ mounts, eth0 10.0.2.15/24 via 10.0.2.2, resolver 10.0.2.2
             ├─ /run/gino/agent.sock ⇄ vsock forwarder
             └─ engine (claude | codex) as uid 1000, no capabilities, no_new_privs
                 stdin/stdout/stderr = virtio-console port = daemon pipes
```

### 1. `gino-vmm` helper (new crate)

- A separate binary crate `gino-vmm/` in the repository (its own `Cargo.toml`, lint policy that
  allows `unsafe` only in its FFI module), so the agent crate keeps `unsafe_code = "forbid"`.
- Loads `libkrun.so.1` with `dlopen` through the header's function typedefs (no build-time link,
  so agents without VM support do not need libkrun installed); checks the library version at
  start and fails with a clear error outside the supported 1.19.x range.
- Input: a JSON run spec on stdin (kernel, format, cmdline, root image dir, shares, overlay
  files, vcpus, memory MiB, vsock port → host socket, net unixstream path, guest MAC). Output:
  the guest console on inherited stdin/stdout/stderr. Sets `PR_SET_PDEATHSIG` so it dies with the
  daemon. Exit status: the guest's reported engine status (see §3), or a dedicated code for VMM
  failures.
- TSI is always disabled: the guest has no network path except virtio-net to the daemon.

### 2. Guest image (kernel + root)

- Kernel: a trimmed config committed under `vm/kernel/` (virtio-mmio/pci, virtiofs, vsock,
  virtio-net, virtio-console, ext4 off, no modules, no USB/sound/graphics), built by
  `vm/kernel/build.sh` in a sibling container; output `vmlinux` (ELF) or `bzImage`, version
  pinned in the script.
- Root: a read-only directory image built from `vm/rootfs/Dockerfile` (Debian bookworm slim, the
  pinned claude CLI, optionally codex, CA certificates, git, coreutils, a shell, common tools) and
  exported to a directory by `vm/rootfs/build.sh`.
- The current `gino-agent` binary is injected at `/opt/gino/gino-agent` through a virtiofs
  overlay file on every run (so the guest init, the CLI and the archive hook always match the
  daemon version); the rootfs is built on the same Debian release as the agent binary.
- Artifacts are not committed; their paths come from configuration (§5).

### 3. Guest init (`gino-agent guest-init`, hidden subcommand)

PID 1 in the guest, started via the kernel cmdline `init=/opt/gino/gino-agent guest-init`:

- mounts `/proc`, `/sys`, `/dev`, `/tmp`, `/run`; mounts the shares by tag: the thread work dir
  (rw, at the same absolute path as on the host), the per-thread engine home (rw), the global
  skills and `CLAUDE.md` (ro), the per-run control share (ro, holds `spec.json`);
- configures `lo` and `eth0` (`10.0.2.15/24`, gateway and resolver `10.0.2.2`, MTU from the spec)
  for `allowlist`; no `eth0` for `none`;
- starts a forwarder from `/run/gino/agent.sock` to the vsock port of the agent API, sets
  `GINO_AGENT_SOCK=/run/gino/agent.sock`;
- reads argv, env (including `GINO_AGENT_TOKEN`, the rewritten `ANTHROPIC_BASE_URL`) and cwd from
  `spec.json`, starts the engine as uid 1000 with all capabilities dropped and `no_new_privs`,
  stdio on the console port;
- forwards control requests (signal INT/TERM to the engine) received on a second console port
  or vsock port from the daemon, reaps children, and on engine exit writes the status to the
  console control port and powers off.

### 4. Daemon integration (`gino-agent/src/sandbox/vm.rs`)

- `IsolationMode::Vm` launch, behind the existing seam used by both engines:
  1. create the per-run dir, write `spec.json`, register the run token (existing executor path);
  2. bind `net.sock` and `ctl.sock`; spawn `gino-vmm` with the engine stdio pipes as its stdio;
  3. accept the guest's net connection, wrap it with `nsplane-ether::spawn_port`, start
     `net::exit::Exit::start_with(source, sink, cfg)` with the same rules, limits, LLM endpoint
     and decision log as the sandbox `allowlist` mode;
  4. on session end or cancel: send the control request, then kill `gino-vmm` after a grace
     period; remove the run dir.
- Codex cancel: the daemon's SIGINT to the child becomes a control request to guest-init (the
  VMM cannot forward signals into the guest). Claude cancels in-band as today.
- Network modes: `none` (no net device), `allowlist` (exit as above). `full` maps to the exit with
  an allow-all public rule set (host loopback and private ranges still refused); TSI is never used.
- Fail closed: `vm` resolves only when `/dev/kvm` opens read-write, `gino-vmm` and libkrun load,
  and the configured kernel and root image exist; otherwise the run fails before spawn with a
  dedicated `IsolationError::Unavailable` message. Probed once per process like the bwrap probe.

### 5. Configuration

`GINO_ISOLATION_FILE` (local floor) gains a `vm` block; the host can never set it:

```json
"engine": { "isolation": {
  "mode": "vm",
  "network": { "mode": "allowlist", "allow": [ ... ] },
  "vm": { "kernel": "/opt/gino/vm/vmlinux", "rootfs": "/opt/gino/vm/rootfs",
          "vmm": "/opt/gino/bin/gino-vmm", "vcpus": 2, "memoryMib": 2048 }
}}
```

Task `meta.isolation` may select `vm` under the existing tightening rules (sandbox → vm is a
tightening); VM resources and paths come from the local file only.

### 6. Specification and docs

docs/agent-architecture.md gains §9e "Engine VM mode" (requirements, image build, launch chain,
shares, network, agent API over vsock, config, fail-closed behaviour, limitations) and a line in
§10b on the vsock transport.

### 7. Tests

- Unit (no KVM): run spec generation, share/overlay layout, cmdline, config validation, VM
  probe failure paths, guest-init pure logic (spec parsing, mount table, signal mapping).
- `gino-vmm` unit tests for spec parsing and version checks (FFI untested without KVM).
- KVM integration (`#[ignore]`, env-gated `GINO_VM_TESTS=1`), run by `gino-agent/scripts/vm-it.sh`
  in a sibling container with `--device /dev/kvm`: guest boots and runs a fake engine; the work dir
  is writable and visible on the host; other threads, `agent.db` and host paths are invisible;
  engine runs as uid 1000 without capabilities; `gino cron create/list` works through vsock and
  sees only its own task; allowlist TCP/UDP/DNS behave as in `sandbox-it.sh`; codex-style SIGINT
  reaches the engine; the VM is gone after the session; missing `/dev/kvm` fails closed.
- Real engine: `e2e/run-real-sandbox.sh` gains `SANDBOX_MODE=vm` (claude in `none` and
  `allowlist`).

### Phases

0. **Spike (go/no-go)**: on a KVM host, boot the trimmed kernel with a virtiofs root via
   `gino-vmm`, run a shell command on the console, reach a host Unix socket over vsock, and pass
   frames to `nsplane-ether`. Record boot time and memory.
1. `gino-vmm` crate, kernel and rootfs build scripts.
2. Guest init and daemon integration (none mode, then allowlist), agent API over vsock.
3. Integration suite, real-engine harness, docs.

## Risks

- **No KVM on the current host**: nothing beyond unit tests can run here; the implementer needs a
  KVM-capable environment before phase 0.
- libkrun API churn (the builder API is new in 1.19): pin the supported range, check the version
  at load, keep the FFI surface in one module.
- Kernel and rootfs maintenance: security updates for the guest kernel and image become an
  operator task; the build scripts must be reproducible and pinned.
- Boot latency and memory per run (expected well under a second and a few hundred MiB for the
  guest, to be measured in phase 0); concurrency limits must account for guest memory.
- virtiofs semantics: file ownership on the host follows the daemon uid, not the guest uid; tools
  that check ownership inside the guest may behave differently. Measure virtiofs performance for
  large repositories.
- A crashed `gino-vmm` must not leave the engine session hanging: the daemon treats VMM exit as
  engine exit and fails the run with a runner-side reason.
- Docker inside the guest is out of scope; tasks that need it stay unsupported until a later step
  (block-device data-root, registry mirror).

## Scope

New crate `gino-vmm/` (~600 lines), new `vm/` build scripts and kernel config, agent changes in
`sandbox/{mod.rs,vm.rs}`, `lib.rs` (guest-init subcommand), a guest-init module, `config.rs`,
executor/engine wiring, `scripts/vm-it.sh`, the e2e harness, docs — roughly 2500–3500 lines
including tests. CI: the `gino-vmm` crate added to the agent job; KVM tests only where `/dev/kvm`
exists.

## Alternatives

1. Firecracker: mature and small, but no virtiofs (block devices only), so work dirs would need
   sync or block images. Rejected for this design.
2. Cloud Hypervisor + virtiofsd: virtiofs and vsock supported, but two daemons per run and a REST
   control plane; heavier than libkrun's in-process VMM.
3. libkrunfw's bundled kernel instead of a custom one: simplest start, less control over the
   kernel surface; acceptable for the phase-0 spike if the custom kernel build is not ready.
4. Linking libkrun into the agent crate: would require lifting `unsafe_code = "forbid"`. Rejected
   in favour of the helper process.

## Annotations

- 2026-10-24: user asked for the plan to be written and committed; implementation will be done by
  colleagues on a KVM-capable host.
