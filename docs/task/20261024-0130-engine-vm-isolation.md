# 20261024-0130-engine-vm-isolation VM isolation backend for engine runs

- **status**: pending
- **priority**: P2
- **owner**: (unassigned)
- **createdAt**: 2026-10-24 01:30

## Description

Add the `vm` mode to the engine isolation seam introduced by `20261024-0058-engine-sandbox`:
the agent boots a short-lived microVM per engine session and tears it down when the session
ends. Host launchers keep owning long-lived agents; this VM has no registration or protocol.

Decisions so far (user, 2026-10-24):

- Hypervisor: libkrun (1.19.7 at time of writing; builder C API).
- Kernel: a custom trimmed guest kernel loaded through `krun_payload_load_external`.
- Shared filesystem: virtiofs (`krun_fs_device_new` / read-only variant) for the thread work
  dir, per-thread engine home and a read-only guest rootfs.
- Docker: deferred (user, 2026-10-24: no Docker in the first VM version). Later: dockerd inside
  the guest, images pulled through a host-side pull-through registry mirror.
- In-engine tooling reaches the daemon through the local agent API socket mapped over vsock
  (task 20261024-1758-agent-local-api).

Open points for the plan: TSI disabled; the guest NIC is libkrun virtio-net attached to the
agent's per-run exit through an ethernet adapter (`krun_net_device_new_unixstream_path`,
nsplane request R3); stdio and agent IPC over vsock
(`krun_vsock_device_add_unix_port`); Docker data-root on a block device (overlayfs does not run
on virtiofs) — per-run scratch vs per-task persistent disk; mirror coverage beyond Docker Hub
(`registry-mirrors` only applies to Docker Hub; other registries need per-registry mirror
config); mirror stays pull-only and never caches authenticated pulls; VMM in a helper
subprocess; `/dev/kvm` access for the agent user; kernel and rootfs build pipeline. Testing:
the dev container has no `/dev/kvm`, but a sibling container with `--device /dev/kvm` can open
it (verified upstream 2026-10-24), so real libkrun guests can run in sibling-container tests.

Acceptance criteria: a task with `meta.isolation = "vm"` runs claude inside a microVM with the
same file and network guarantees as the sandbox mode, can use the `gino` CLI through the local
agent API, and leaves no VM behind after the session ends. Docker in the guest is a later step.

## ActiveForm

Adding a VM isolation backend

## Dependencies

- **blocked by**: 20261024-1758-agent-local-api
- **blocks**: (none)

## Notes

Plan: [20261024-2050-engine-vm-isolation](../plan/20261024-2050-engine-vm-isolation.md). The
current development host (GCE, no nested virtualization) has no `/dev/kvm`; implement on a
KVM-capable host.


- unclaim: Plan written; implementation handed to colleagues on a KVM-capable host.
