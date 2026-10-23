# 20261012-2019-remote-ops-review-fixes Fix remote operations review findings

- **status**: completed
- **createdAt**: 2026-10-12 20:19
- **approvedAt**: 2026-10-12 20:19
- **relatedTask**: 20261012-2019-remote-ops-review-fixes

## Context

A review of the uncommitted file and terminal operations ran the agent and host gates and a
probe of the real host against the real agent binary. Four behaviors were reproduced:

1. `AttachmentManager::download_file` (`gino-agent/src/attachments.rs`) writes a new temp file
   and renames it over the destination. An existing `0600` or `0755` file becomes `0644`, and
   a symlink destination is replaced by a regular file.
2. `drain` in `gino-agent/src/remote_ops/pty.rs` reads the pty until EOF for up to five
   seconds after a natural exit. A descendant in another process group keeps the pty open, so
   `pty_exit` arrives five seconds late and carries `slow_consumer`.
3. `Sessions::handle` cancels a session when its 16-chunk input queue is full or an input
   frame is malformed. Both end as `closed`, the same as an ordinary close, and the SDK
   `write` gives the caller no signal.
4. `BlobStore.delete` (`packages/host-core/src/configstore/store.ts`) removes the leaf only.
   Every transfer uses a unique name, so each one leaves an empty directory that
   `listLeaves` then walks on every later `getFile`.

## Proposal

1. `download_file`: resolve the destination with `canonicalize` when it exists, place the temp
   file beside the resolved path, and copy the existing file's permissions onto the temp file
   before the rename.
2. `drain`: stop when a read stays idle for 100 ms, since everything the exited child wrote is
   already buffered. Keep the overall bound. Only a failed or stalled emit counts as
   `slow_consumer`.
3. Record why a session was cancelled and report `input_overflow` or `invalid` in `pty_exit`.
   Raise the input queue from 16 to 64 chunks (1 MiB), matching the connection output queue.
4. `BlobStore.delete`: remove the leaf's directory when it is left empty, never the store root.
5. Update `docs/protocol.md` §5.1 and §5.2 for the changed behavior and limits.

## Risks

- Writing through a symlink changes which path a `put` replaces; a dangling link is still
  replaced by a regular file.
- The 100 ms idle bound could cut output from a descendant that is still writing after the
  shell exits; that output belongs to a process the terminal no longer owns.
- A burst larger than 1 MiB still ends the terminal. End-to-end flow control is not part of
  this change.

## Scope

Agent: `attachments.rs`, `remote_ops/pty.rs`, and their tests. Host: `configstore/store.ts`
and its test. Documentation: `docs/protocol.md`, `docs/changelog.md`. No dependency, schema or
protocol version change.

## Alternatives

- Credit-based flow control for terminal input and output. It removes the overflow entirely
  but adds frames and state on both sides; left for a separate task.
- Deleting staged transfers by name through a new blob port method. It would avoid the
  `listLeaves` scan in `getFile` but changes the frozen storage port.

## Annotations

The user asked for the fixes directly on 2026-10-12 after reading the review.
