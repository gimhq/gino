//! Content-addressed store + symlink primitives (agent-architecture §9b; PLAN-007
//! §2/§3). Pure filesystem: bytes/bundle → store, store → symlink. The caller (the
//! reconcile L3) supplies absolute paths and pre-verified bytes; this module does
//! no fetching, hashing-for-trust, or DB work.
//!
//! Layout under a workspace `store_root`:
//!
//! ```text
//! <store_root>/store/<name>/<sha256[:12]>     # kind=file: a single FILE
//! <store_root>/store/<name>/<sha256[:12]>/    # kind=dir:  an unpacked tar DIRECTORY
//! ```
//!
//! Loading is by symlink: `<scope_root>/<target> ─▶ store/<name>/<short>[/]`
//! (zero-copy; update = repoint). All store writes are atomic (temp sibling +
//! rename) and idempotent (content-addressed ⇒ already-present is a no-op).

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::debug;

use super::{ProvisioningError, safe_link_path, safe_name, short_id};

/// Fixed sub-directory of the workspace `store_root` holding the store tree.
const STORE_SUBDIR: &str = "store";

/// Process-local sequence making temp sibling names unique without a clock/RNG
/// (both unavailable / undesirable here): `.tmp.<final>.<pid>.<seq>`.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// The two artifact shapes (PLAN-007 §1): a single file or a directory bundle.
/// [`link`] uses it to validate the store entry's on-disk type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntryKind {
    /// A single file (e.g. `CLAUDE.md` / `AGENTS.md`).
    File,
    /// A directory bundle (e.g. a skill = `SKILL.md` + files), unpacked from tar.
    Dir,
}

impl EntryKind {
    /// Lowercase label for diagnostics / error messages.
    fn as_str(self) -> &'static str {
        match self {
            EntryKind::File => "file",
            EntryKind::Dir => "dir",
        }
    }
}

/// The store path for `(name, sha256)` — `store_root/store/<name>/<short>` — with
/// `name` sanitized to a single safe component and `short` = `sha256[:12]`. This
/// is the single place the store key is built, so every materialize/remove call
/// shares the same traversal guard.
pub(crate) fn store_path(
    store_root: &Path,
    name: &str,
    sha256: &str,
) -> Result<PathBuf, ProvisioningError> {
    let safe = safe_name(name)?;
    Ok(store_root
        .join(STORE_SUBDIR)
        .join(safe)
        .join(short_id(sha256)))
}

/// Materialize a `kind=file` artifact: write `bytes` to `store/<name>/<short>`
/// atomically (temp sibling + rename). Idempotent — an already-present entry is a
/// no-op. Returns the store path.
pub(crate) fn materialize_file(
    store_root: &Path,
    name: &str,
    sha256: &str,
    bytes: &[u8],
) -> Result<PathBuf, ProvisioningError> {
    let path = store_path(store_root, name, sha256)?;
    // Content-addressed: a present file already holds exactly these bytes.
    if path.is_file() {
        return Ok(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = tmp_sibling(&path);
    // Clean any stale temp from a crashed prior run, then write + rename.
    let _ = std::fs::remove_file(&tmp);
    std::fs::write(&tmp, bytes)?;
    match std::fs::rename(&tmp, &path) {
        Ok(()) => Ok(path),
        Err(err) => {
            let _ = std::fs::remove_file(&tmp);
            // A concurrent writer landing the same content-addressed entry first is
            // success, not failure (the bytes are identical by construction).
            if path.is_file() {
                Ok(path)
            } else {
                Err(err.into())
            }
        }
    }
}

/// Materialize a `kind=dir` artifact: unpack the **uncompressed tar** `tar_bytes`
/// into `store/<name>/<short>/` atomically (unpack into a temp sibling dir, then
/// rename into place). Idempotent. Rejects any tar member whose path is absolute
/// or contains `..` (no escape outside the store dir). Returns the store path.
pub(crate) fn materialize_dir(
    store_root: &Path,
    name: &str,
    sha256: &str,
    tar_bytes: &[u8],
) -> Result<PathBuf, ProvisioningError> {
    let path = store_path(store_root, name, sha256)?;
    if path.is_dir() {
        return Ok(path);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = tmp_sibling(&path);
    // Unpack into a fresh temp dir so a failure never leaves a half-populated
    // store entry; rename the whole tree into place once it is complete.
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp)?;
    if let Err(err) = unpack_tar_safe(tar_bytes, &tmp) {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(err);
    }
    match std::fs::rename(&tmp, &path) {
        Ok(()) => Ok(path),
        Err(err) => {
            let _ = std::fs::remove_dir_all(&tmp);
            if path.is_dir() {
                Ok(path)
            } else {
                Err(err.into())
            }
        }
    }
}

/// Create (or REPOINT) a symlink at `<scope_root>/<target>` pointing to
/// `store_path` (PLAN-007 §2). Parent dirs of the link are created as needed; an
/// existing link/file is replaced atomically (symlink to a temp sibling, then
/// rename over). `kind` validates the store entry's on-disk type. Returns the
/// link path.
pub(crate) fn link(
    scope_root: &Path,
    target: &str,
    store_path: &Path,
    kind: EntryKind,
) -> Result<PathBuf, ProvisioningError> {
    let link_path = safe_link_path(scope_root, target)?;
    // The store entry must exist and match the declared kind — catches a caller
    // passing a file path for a dir entry (or vice versa) or a GC'd/corrupt entry.
    let type_ok = match kind {
        EntryKind::File => store_path.is_file(),
        EntryKind::Dir => store_path.is_dir(),
    };
    if !type_ok {
        return Err(ProvisioningError::StoreKindMismatch {
            expected: kind.as_str(),
            path: store_path.to_path_buf(),
        });
    }
    if let Some(parent) = link_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Build the new symlink under a temp sibling name, then rename it over any
    // existing entry — repoint is atomic (no window where <target> is absent).
    let tmp = tmp_sibling(&link_path);
    let _ = std::fs::remove_file(&tmp);
    std::os::unix::fs::symlink(store_path, &tmp)?;
    if std::fs::rename(&tmp, &link_path).is_err() {
        // The slot is squatted by a non-symlink (e.g. a real dir) that rename(2)
        // will not replace; remove it and retry once. We only ever manage
        // symlinks here, so clobbering whatever is in the slot is intentional.
        remove_path(&link_path);
        if let Err(err) = std::fs::rename(&tmp, &link_path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(err.into());
        }
    }
    debug!(target, kind = ?kind, store = %store_path.display(), "linked provisioning entry into scope");
    Ok(link_path)
}

/// Remove the symlink at `<scope_root>/<target>` if present (best-effort; a
/// missing link is `Ok`). The store entry it points at is untouched — GC of the
/// store is [`remove_store_entry`], gated by the DB refcount in the reconcile L3.
pub(crate) fn remove_link(scope_root: &Path, target: &str) -> Result<(), ProvisioningError> {
    let link_path = safe_link_path(scope_root, target)?;
    // symlink_metadata does not follow the link, and remove_file removes the link
    // itself (never its target), so the store entry is left intact.
    match std::fs::symlink_metadata(&link_path) {
        Ok(_) => Ok(std::fs::remove_file(&link_path)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Remove the store file/dir for `(name, sha256)` — the refcount-GC primitive
/// (the DB refcount check lives in the reconcile L3, not here). Best-effort: a
/// missing entry is `Ok`.
pub(crate) fn remove_store_entry(
    store_root: &Path,
    name: &str,
    sha256: &str,
) -> Result<(), ProvisioningError> {
    let path = store_path(store_root, name, sha256)?;
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if meta.is_dir() => Ok(std::fs::remove_dir_all(&path)?),
        Ok(_) => Ok(std::fs::remove_file(&path)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// A unique temp sibling of `final_path` in the same directory, so a write/unpack
/// there can be `rename`d atomically onto `final_path` (same filesystem).
fn tmp_sibling(final_path: &Path) -> PathBuf {
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let stem = final_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("entry");
    final_path.with_file_name(format!(".tmp.{stem}.{pid}.{seq}"))
}

/// Unpack an uncompressed tar into `dest`, rejecting any member that would escape
/// it. Two layers: an explicit absolute/`..` component check on each member path,
/// and `Entry::unpack_in` (which itself refuses to write outside `dest`).
fn unpack_tar_safe(tar_bytes: &[u8], dest: &Path) -> Result<(), ProvisioningError> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(tar_bytes));
    let entries = archive.entries().map_err(|err| ProvisioningError::Unpack {
        reason: err.to_string(),
    })?;
    for entry in entries {
        let mut entry = entry.map_err(|err| ProvisioningError::Unpack {
            reason: err.to_string(),
        })?;
        // Own the path so the immutable borrow ends before the mutable unpack.
        let member = entry
            .path()
            .map_err(|err| ProvisioningError::Unpack {
                reason: err.to_string(),
            })?
            .into_owned();
        check_member_path(&member)?;
        let unpacked = entry
            .unpack_in(dest)
            .map_err(|err| ProvisioningError::Unpack {
                reason: err.to_string(),
            })?;
        // `unpack_in` returns false when it skipped a member for escaping `dest`.
        if !unpacked {
            return Err(ProvisioningError::UnsafeMember {
                member: member.display().to_string(),
            });
        }
    }
    Ok(())
}

/// Reject a tar member path that is absolute or climbs out via `..` (a `.`
/// component is harmless and allowed).
fn check_member_path(member: &Path) -> Result<(), ProvisioningError> {
    for component in member.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(ProvisioningError::UnsafeMember {
                    member: member.display().to_string(),
                });
            }
        }
    }
    Ok(())
}

/// Best-effort removal of whatever is at `path` (symlink, file or directory),
/// acting on the link itself (not its target).
fn remove_path(path: &Path) {
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if meta.is_dir() {
            let _ = std::fs::remove_dir_all(path);
        } else {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EntryKind, link, materialize_dir, materialize_file, remove_link, remove_store_entry,
        store_path,
    };
    use crate::attachments::sha256_hex;
    use crate::provisioning::{ProvisioningError, short_id};

    /// Build an uncompressed tar in-memory from `(path, bytes)` members.
    fn build_tar(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, data) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            builder.append_data(&mut header, path, *data).unwrap();
        }
        builder.into_inner().unwrap()
    }

    /// Build a one-member tar whose member path is written **raw** into the GNU
    /// header name field, bypassing the writer's own `..`/absolute validation
    /// (`Builder::append_data` refuses such paths). This forges the exact hostile
    /// archive an attacker could ship, so the unpack-side guard is what's tested.
    fn build_evil_tar(path: &[u8], data: &[u8]) -> Vec<u8> {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        header.as_gnu_mut().unwrap().name[..path.len()].copy_from_slice(path);
        header.set_cksum();
        let mut builder = tar::Builder::new(Vec::new());
        builder.append(&header, data).unwrap();
        builder.into_inner().unwrap()
    }

    #[test]
    fn materialize_file_writes_short_path_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let bytes = b"# CLAUDE.md\nhello\n";
        let sha = sha256_hex(bytes);

        let path = materialize_file(root.path(), "claude-md", &sha, bytes).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        // Laid out at store/<name>/<sha256[:12]>.
        assert_eq!(path.file_name().unwrap().to_str().unwrap(), short_id(&sha));
        assert_eq!(path.file_name().unwrap().to_str().unwrap().len(), 12);
        assert!(
            path.starts_with(root.path().join("store").join("claude-md")),
            "unexpected store path {path:?}"
        );

        // Second call is a no-op returning the same path (content-addressed).
        let again = materialize_file(root.path(), "claude-md", &sha, bytes).unwrap();
        assert_eq!(again, path);
    }

    #[test]
    fn materialize_dir_unpacks_bundle_and_is_idempotent() {
        let root = tempfile::tempdir().unwrap();
        let tar = build_tar(&[
            ("SKILL.md", b"# Demo skill\n"),
            ("scripts/run.sh", b"#!/bin/sh\necho hi\n"),
        ]);
        let sha = sha256_hex(&tar);

        let dir = materialize_dir(root.path(), "demo-skill", &sha, &tar).unwrap();
        assert!(dir.is_dir());
        assert_eq!(dir.file_name().unwrap().to_str().unwrap(), short_id(&sha));
        assert_eq!(
            std::fs::read(dir.join("SKILL.md")).unwrap(),
            b"# Demo skill\n"
        );
        assert_eq!(
            std::fs::read(dir.join("scripts").join("run.sh")).unwrap(),
            b"#!/bin/sh\necho hi\n"
        );

        // Idempotent: a second unpack of the same (name, sha) is a no-op.
        let again = materialize_dir(root.path(), "demo-skill", &sha, &tar).unwrap();
        assert_eq!(again, dir);
    }

    #[test]
    fn materialize_dir_rejects_escaping_members() {
        let root = tempfile::tempdir().unwrap();
        let sha = sha256_hex(b"evil-bundle");

        // Traversal (`..`) and absolute members are both rejected, no escape.
        for evil in [
            &b"../escape.sh"[..],
            b"a/b/../../../escape.sh",
            b"/abs/escape.sh",
        ] {
            let tar = build_evil_tar(evil, b"PWNED");
            let err = materialize_dir(root.path(), "evil", &sha, &tar).unwrap_err();
            assert!(
                matches!(
                    err,
                    ProvisioningError::UnsafeMember { .. } | ProvisioningError::Unpack { .. }
                ),
                "expected {:?} rejected, got {err:?}",
                String::from_utf8_lossy(evil)
            );
        }
        // Nothing escaped the store, and no half-populated entry was left behind.
        assert!(!root.path().join("escape.sh").exists());
        assert!(!store_path(root.path(), "evil", &sha).unwrap().exists());
    }

    #[test]
    fn link_creates_symlink_resolving_to_store_bytes() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("project");
        let bytes = b"CONFIG\n";
        let sha = sha256_hex(bytes);
        let store = materialize_file(root.path(), "claude-md", &sha, bytes).unwrap();

        let link_path = link(&scope, "CLAUDE.md", &store, EntryKind::File).unwrap();
        assert_eq!(link_path, scope.join("CLAUDE.md"));
        assert_eq!(std::fs::read_link(&link_path).unwrap(), store);
        // Resolves through the symlink to the stored bytes.
        assert_eq!(std::fs::read(&link_path).unwrap(), bytes);
    }

    #[test]
    fn link_auto_creates_nested_parent_dirs() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("project");
        let tar = build_tar(&[("SKILL.md", b"# demo\n")]);
        let sha = sha256_hex(&tar);
        let store = materialize_dir(root.path(), "demo", &sha, &tar).unwrap();

        // The `.claude/skills/` parents do not exist yet — link must create them.
        let link_path = link(&scope, ".claude/skills/demo", &store, EntryKind::Dir).unwrap();
        assert_eq!(link_path, scope.join(".claude").join("skills").join("demo"));
        assert_eq!(std::fs::read_link(&link_path).unwrap(), store);
        assert_eq!(
            std::fs::read(link_path.join("SKILL.md")).unwrap(),
            b"# demo\n"
        );
    }

    #[test]
    fn link_repoints_to_a_new_store_path() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("project");

        let v1 = b"VERSION-1\n";
        let sha1 = sha256_hex(v1);
        let store1 = materialize_file(root.path(), "claude-md", &sha1, v1).unwrap();
        let link_path = link(&scope, "CLAUDE.md", &store1, EntryKind::File).unwrap();
        assert_eq!(std::fs::read_link(&link_path).unwrap(), store1);

        // A new version lands a new store entry; relinking the same target repoints.
        let v2 = b"VERSION-2\n";
        let sha2 = sha256_hex(v2);
        let store2 = materialize_file(root.path(), "claude-md", &sha2, v2).unwrap();
        assert_ne!(store1, store2);
        let relinked = link(&scope, "CLAUDE.md", &store2, EntryKind::File).unwrap();
        assert_eq!(relinked, link_path);
        assert_eq!(std::fs::read_link(&link_path).unwrap(), store2);
        assert_eq!(std::fs::read(&link_path).unwrap(), v2);
    }

    #[test]
    fn remove_link_drops_symlink_but_keeps_store_entry() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("project");
        let bytes = b"KEEP-ME\n";
        let sha = sha256_hex(bytes);
        let store = materialize_file(root.path(), "claude-md", &sha, bytes).unwrap();
        let link_path = link(&scope, "CLAUDE.md", &store, EntryKind::File).unwrap();

        remove_link(&scope, "CLAUDE.md").unwrap();
        assert!(!link_path.exists());
        // The store entry it pointed at is untouched.
        assert!(store.is_file());
        assert_eq!(std::fs::read(&store).unwrap(), bytes);

        // Removing a missing link is a no-op (best-effort).
        remove_link(&scope, "CLAUDE.md").unwrap();
    }

    #[test]
    fn remove_store_entry_removes_file_and_dir() {
        let root = tempfile::tempdir().unwrap();

        let bytes = b"FILE\n";
        let fsha = sha256_hex(bytes);
        let file = materialize_file(root.path(), "cfg", &fsha, bytes).unwrap();
        assert!(file.is_file());
        remove_store_entry(root.path(), "cfg", &fsha).unwrap();
        assert!(!file.exists());

        let tar = build_tar(&[("SKILL.md", b"x")]);
        let dsha = sha256_hex(&tar);
        let dir = materialize_dir(root.path(), "skill", &dsha, &tar).unwrap();
        assert!(dir.is_dir());
        remove_store_entry(root.path(), "skill", &dsha).unwrap();
        assert!(!dir.exists());

        // Removing a missing entry is a no-op.
        remove_store_entry(root.path(), "cfg", &fsha).unwrap();
    }

    #[test]
    fn link_rejects_unsafe_target() {
        let root = tempfile::tempdir().unwrap();
        let scope = root.path().join("project");
        let bytes = b"x";
        let sha = sha256_hex(bytes);
        let store = materialize_file(root.path(), "cfg", &sha, bytes).unwrap();
        let err = link(&scope, "../escape", &store, EntryKind::File).unwrap_err();
        assert!(matches!(err, ProvisioningError::UnsafeTarget { .. }));
    }
}
