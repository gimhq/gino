//! Out-of-band attachment channel (agent-architecture §9 / §14 A1–A6; PLAN-005).
//!
//! Bytes never ride a protocol frame — only **descriptors** do
//! (`{ id, name, content_type, size, sha256, ref, direction? }`, see
//! [`crate::protocol::v2::AttachmentDescriptor`]). This module owns the byte
//! plane:
//!
//! * a **config-selected backend** ([`build_backend`]): **host-proxy** by default
//!   (`GET`/`POST /agent/attachments`, connect-token auth, bytes through the
//!   host); the **presigned/CDN** arm is left unimplemented this campaign and
//!   returns [`AttachmentError::Unsupported`] (A2).
//! * a **content-addressed cache** keyed by `sha256` under the workspace dir,
//!   shared within a workspace, with a size-cap + LRU GC (A4).
//! * the **materialize-inbound / collect-outbound** flow the executor drives:
//!   inbound descriptors → `work_dir/inputs/<name>`; everything under
//!   `work_dir/output/` → uploaded, descriptors returned on `thread_result` (A3).
//!
//! Safety: `sha256` is verified on every download (mismatch rejected), names are
//! sanitized against `../`/absolute paths, and size limits (100 MB single /
//! 500 MB per execution) + an optional content-type allowlist are enforced (A5).
//!
//! Construction (`build_backend`, [`AttachmentManager::new`],
//! [`AttachmentSource`]) is wired by the runner (Lane G); the executor only
//! drives [`AttachmentManager::materialize_inputs`] /
//! [`AttachmentManager::collect_outputs`]. The not-yet-wired construction surface
//! keeps a module-level `dead_code` allow, mirroring the executor v2 module; the
//! unit tests below exercise the whole surface.
#![allow(
    dead_code,
    reason = "attachment backend construction is wired by the runner (Lane G); tests cover it"
)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use async_trait::async_trait;
use secrecy::SecretString;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::{debug, warn};

use crate::config::{AttachmentBackendKind, AttachmentsConfig};
use crate::db::ThreadRow;
use crate::protocol::v2::AttachmentDescriptor;
use crate::transport::http::{HttpClient, HttpError, UploadedAttachment};

/// Fixed sub-directory of a thread's `work_dir` for inbound attachments (A3).
const INPUTS_SUBDIR: &str = "inputs";
/// Fixed sub-directory of a thread's `work_dir` scanned for outbound artifacts (A3).
const OUTPUT_SUBDIR: &str = "output";
/// Content-addressed cache directory under the workspace dir (A4).
const CACHE_SUBDIR: &str = "attachments-cache";
/// `direction` marker for an agent-produced (outbound) artifact descriptor.
const DIRECTION_OUTBOUND: &str = "outbound";

/// Errors raised by the attachment layer. Byte content is never included.
#[derive(Debug, Error)]
pub(crate) enum AttachmentError {
    /// A single attachment exceeds the per-file size cap (A5).
    #[error("attachment exceeds the single-file size limit ({size} > {limit} bytes)")]
    SizeLimitExceeded { size: u64, limit: u64 },
    /// The inbound attachments for one execution exceed the per-execution cap (A5).
    #[error("attachments exceed the per-execution size limit ({total} > {limit} bytes)")]
    ExecutionLimitExceeded { total: u64, limit: u64 },
    /// The content type is not in the (non-empty) allowlist (A5).
    #[error("content type {content_type:?} is not allowed")]
    ContentTypeNotAllowed { content_type: String },
    /// The downloaded bytes did not hash to the descriptor's `sha256`.
    #[error("sha256 mismatch (expected {expected}, got {actual})")]
    IntegrityMismatch { expected: String, actual: String },
    /// The attachment name is unsafe (path traversal / absolute / separators).
    #[error("unsafe attachment name {name:?}")]
    UnsafeName { name: String },
    /// The selected backend cannot service this operation (e.g. presigned/CDN
    /// is config-selected but not implemented this campaign).
    #[error("attachment backend not configured/supported")]
    Unsupported,
    /// The byte transfer (host-proxy GET/POST) failed.
    #[error("attachment byte transfer failed: {0}")]
    Backend(#[from] HttpError),
    /// A filesystem error while materializing/collecting bytes.
    #[error("attachment filesystem error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Byte backend (A2): host-proxy default; presigned/CDN left unimplemented.
// ---------------------------------------------------------------------------

/// The byte plane: download/upload raw bytes for a descriptor. Integrity,
/// caching, limits and materialization live in [`AttachmentManager`]; an impl
/// only moves bytes.
#[async_trait]
pub(crate) trait AttachmentBackend: Send + Sync {
    /// Fetch the bytes referenced by `descriptor` (`ref` is backend-specific).
    async fn download(&self, descriptor: &AttachmentDescriptor)
    -> Result<Vec<u8>, AttachmentError>;

    /// Upload `bytes`; return the host-assigned `{id, ref}` (A1).
    async fn upload(
        &self,
        name: &str,
        content_type: &str,
        sha256: &str,
        bytes: Vec<u8>,
    ) -> Result<UploadedAttachment, AttachmentError>;
}

/// Select the byte backend from config (A2). **Host-proxy is the default**;
/// `Presigned` returns a [`PresignedBackend`] whose ops all error
/// [`AttachmentError::Unsupported`] until object storage is wired.
pub(crate) fn build_backend(
    config: &AttachmentsConfig,
    http: HttpClient,
    http_base: String,
    connect_token: SecretString,
) -> Box<dyn AttachmentBackend> {
    match config.backend {
        AttachmentBackendKind::HostProxy => {
            Box::new(HostProxyBackend::new(http, http_base, connect_token))
        }
        AttachmentBackendKind::Presigned => Box::new(PresignedBackend),
    }
}

/// Host-proxy backend (A2 default): the host streams bytes through
/// `GET /agent/attachments/:id` and `POST /agent/attachments`, authenticated
/// with the connect token (the same header as register/config/output).
#[derive(Clone, Debug)]
pub(crate) struct HostProxyBackend {
    http: HttpClient,
    /// Orchestrator HTTP base (no trailing slash), used to resolve a relative
    /// `ref` and to build the upload URL.
    http_base: String,
    connect_token: SecretString,
}

impl HostProxyBackend {
    pub(crate) fn new(
        http: HttpClient,
        mut http_base: String,
        connect_token: SecretString,
    ) -> Self {
        // Trim trailing slashes in place so the owned String is consumed (no
        // needless re-allocation), keeping `{base}{/path}` joins clean.
        http_base.truncate(http_base.trim_end_matches('/').len());
        Self {
            http,
            http_base,
            connect_token,
        }
    }

    /// Resolve a descriptor `ref` to a full download URL. An absolute URL is used
    /// as-is; a host-proxy path (`/agent/attachments/:id`) is joined to the base.
    fn resolve(&self, reference: &str) -> String {
        if reference.starts_with("http://") || reference.starts_with("https://") {
            reference.to_owned()
        } else if let Some(rest) = reference.strip_prefix('/') {
            format!("{}/{rest}", self.http_base)
        } else {
            format!("{}/{reference}", self.http_base)
        }
    }

    fn upload_url(&self) -> String {
        format!("{}/agent/attachments", self.http_base)
    }
}

#[async_trait]
impl AttachmentBackend for HostProxyBackend {
    async fn download(
        &self,
        descriptor: &AttachmentDescriptor,
    ) -> Result<Vec<u8>, AttachmentError> {
        let url = self.resolve(&descriptor.reference);
        Ok(self
            .http
            .download_attachment(&url, &self.connect_token)
            .await?)
    }

    async fn upload(
        &self,
        name: &str,
        content_type: &str,
        sha256: &str,
        bytes: Vec<u8>,
    ) -> Result<UploadedAttachment, AttachmentError> {
        let url = self.upload_url();
        Ok(self
            .http
            .upload_attachment(&url, &self.connect_token, name, content_type, sha256, bytes)
            .await?)
    }
}

/// Presigned-URL / CDN backend (A2). Config-selected when object storage is
/// configured — **not implemented this campaign**; every op is unsupported.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PresignedBackend;

#[async_trait]
impl AttachmentBackend for PresignedBackend {
    async fn download(
        &self,
        _descriptor: &AttachmentDescriptor,
    ) -> Result<Vec<u8>, AttachmentError> {
        Err(AttachmentError::Unsupported)
    }

    async fn upload(
        &self,
        _name: &str,
        _content_type: &str,
        _sha256: &str,
        _bytes: Vec<u8>,
    ) -> Result<UploadedAttachment, AttachmentError> {
        Err(AttachmentError::Unsupported)
    }
}

// ---------------------------------------------------------------------------
// Inbound descriptor source (executor seam; mirrors `executor::v2::PromptSource`).
// ---------------------------------------------------------------------------

/// Resolves the inbound attachment descriptors to materialize for a thread. The
/// `threads` row carries no attachment column (they arrive on the inbox
/// `message`), so the executor takes a resolver; Lane G supplies the real one.
pub(crate) trait AttachmentSource: Send + Sync {
    /// Inbound descriptors bound to `thread` (empty if none).
    fn inbound_for(&self, thread: &ThreadRow) -> Vec<AttachmentDescriptor>;
}

// ---------------------------------------------------------------------------
// Manager: integrity, content-addressed cache, limits, materialization.
// ---------------------------------------------------------------------------

/// Owns the attachment byte plane for an executor: a [`AttachmentBackend`], the
/// content-addressed cache directory, and the configured limits.
pub(crate) struct AttachmentManager {
    backend: Box<dyn AttachmentBackend>,
    cache_dir: PathBuf,
    config: AttachmentsConfig,
}

impl std::fmt::Debug for AttachmentManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AttachmentManager")
            .field("cache_dir", &self.cache_dir)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl AttachmentManager {
    /// Build a manager over `backend`, caching content-addressed bytes under
    /// `<workspace_dir>/attachments-cache` and enforcing `config`'s limits.
    pub(crate) fn new(
        backend: Box<dyn AttachmentBackend>,
        workspace_dir: &Path,
        config: AttachmentsConfig,
    ) -> Self {
        Self {
            backend,
            cache_dir: workspace_dir.join(CACHE_SUBDIR),
            config,
        }
    }

    /// Materialize every inbound `descriptor` into `work_dir/inputs/<name>` (A3),
    /// verifying `sha256`, enforcing the size caps + content-type allowlist, and
    /// sanitizing names against traversal. Outbound descriptors are skipped.
    /// Returns the written paths. The caller fails the thread on `Err`.
    pub(crate) async fn materialize_inputs(
        &self,
        work_dir: &Path,
        descriptors: &[AttachmentDescriptor],
    ) -> Result<Vec<PathBuf>, AttachmentError> {
        // Pre-check declared sizes against the per-execution budget so a flood of
        // oversized descriptors is rejected before any byte transfer.
        let declared_total = descriptors
            .iter()
            .filter(|d| !is_outbound(d))
            .fold(0_u64, |acc, d| {
                acc.saturating_add(u64::try_from(d.size).unwrap_or(0))
            });
        if declared_total > self.config.max_execution_bytes {
            return Err(AttachmentError::ExecutionLimitExceeded {
                total: declared_total,
                limit: self.config.max_execution_bytes,
            });
        }

        let inputs_dir = work_dir.join(INPUTS_SUBDIR);
        let mut written = Vec::new();
        let mut total = 0_u64;
        for descriptor in descriptors {
            if is_outbound(descriptor) {
                continue;
            }
            let safe = sanitize_name(&descriptor.name)?;
            let bytes = self.fetch(descriptor).await?;
            total = total.saturating_add(bytes.len() as u64);
            if total > self.config.max_execution_bytes {
                return Err(AttachmentError::ExecutionLimitExceeded {
                    total,
                    limit: self.config.max_execution_bytes,
                });
            }
            std::fs::create_dir_all(&inputs_dir)?;
            let dest = inputs_dir.join(&safe);
            std::fs::write(&dest, &bytes)?;
            debug!(name = %safe, bytes = bytes.len(), "materialized inbound attachment");
            written.push(dest);
        }
        Ok(written)
    }

    /// Upload every regular file under `work_dir/output/` (A3) and return the
    /// resulting outbound descriptors. Best-effort per file: a file that exceeds
    /// the single-file cap, fails the allowlist, or fails to upload is logged and
    /// skipped (a partial artifact set must not sink an otherwise-good run); the
    /// per-execution budget caps the total. Symlinks are ignored (no traversal).
    pub(crate) async fn collect_outputs(&self, work_dir: &Path) -> Vec<AttachmentDescriptor> {
        let output_dir = work_dir.join(OUTPUT_SUBDIR);
        let files = match list_files(&output_dir) {
            Ok(files) => files,
            Err(err) => {
                warn!(error = %err, "failed to scan output dir for attachments");
                return Vec::new();
            }
        };
        let mut descriptors = Vec::new();
        let mut total = 0_u64;
        for (abs, rel) in files {
            let size = match std::fs::metadata(&abs) {
                Ok(meta) => meta.len(),
                Err(err) => {
                    warn!(file = %rel, error = %err, "skipping output artifact (stat failed)");
                    continue;
                }
            };
            if size > self.config.max_single_bytes {
                warn!(file = %rel, size, "skipping oversized output artifact");
                continue;
            }
            if total.saturating_add(size) > self.config.max_execution_bytes {
                warn!(file = %rel, "per-execution output budget reached; stopping");
                break;
            }
            match self.upload_one(&abs, &rel).await {
                Ok(descriptor) => {
                    total = total.saturating_add(size);
                    descriptors.push(descriptor);
                }
                Err(err) => warn!(file = %rel, error = %err, "skipping output artifact"),
            }
        }
        descriptors
    }

    /// The configured single-file size cap (A5). The config/skills provisioner
    /// ([`crate::provisioning`], PLAN-007) picks this as its [`fetch_verified`]
    /// `max_bytes`.
    ///
    /// [`fetch_verified`]: AttachmentManager::fetch_verified
    pub(crate) fn max_single_bytes(&self) -> u64 {
        self.config.max_single_bytes
    }

    /// Fetch the verified bytes for a content-addressed artifact by `reference` +
    /// `sha256`, independent of any attachment **descriptor** — the byte seam the
    /// config/skills provisioner ([`crate::provisioning`], PLAN-007 §2/§5) fetches
    /// through.
    ///
    /// Serves from the content-addressed cache by `sha256` when present (no
    /// download); otherwise downloads via the backend (a minimal descriptor carries
    /// only `reference` + `sha256`, the rest defaulted — the byte plane reads
    /// nothing else), verifies the bytes hash to `sha256` (rejecting
    /// [`AttachmentError::IntegrityMismatch`] on a miss), enforces `max_bytes` as
    /// the single-file cap, caches, and returns. Unlike [`fetch`] **no content-type
    /// allowlist** is applied — config artifacts carry no declared content type.
    ///
    /// [`fetch`]: AttachmentManager::fetch
    pub(crate) async fn fetch_verified(
        &self,
        reference: &str,
        sha256: &str,
        max_bytes: u64,
    ) -> Result<Vec<u8>, AttachmentError> {
        if let Some(bytes) = self.cache_get(sha256) {
            debug!(sha256 = %sha256, "content-addressed artifact cache hit");
            return Ok(bytes);
        }
        // Only `ref` + `sha256` matter to the byte backend; the rest is defaulted.
        let descriptor = AttachmentDescriptor {
            id: String::new(),
            name: String::new(),
            content_type: String::new(),
            size: 0,
            sha256: sha256.to_owned(),
            reference: reference.to_owned(),
            direction: None,
        };
        let bytes = self.backend.download(&descriptor).await?;
        let actual = sha256_hex(&bytes);
        if !actual.eq_ignore_ascii_case(sha256) {
            return Err(AttachmentError::IntegrityMismatch {
                expected: sha256.to_owned(),
                actual,
            });
        }
        let size = bytes.len() as u64;
        if size > max_bytes {
            return Err(AttachmentError::SizeLimitExceeded {
                size,
                limit: max_bytes,
            });
        }
        self.cache_put(&actual, &bytes);
        Ok(bytes)
    }

    /// Fetch one descriptor's verified bytes: enforce the single-file cap + the
    /// content-type allowlist, then download/verify/cache via [`fetch_verified`]
    /// (the cap + cache + integrity check are shared; only the inbound-specific
    /// content-type allowlist and the cheap declared-size pre-check live here).
    ///
    /// [`fetch_verified`]: AttachmentManager::fetch_verified
    async fn fetch(&self, descriptor: &AttachmentDescriptor) -> Result<Vec<u8>, AttachmentError> {
        self.check_content_type(&descriptor.content_type)?;
        // Pre-empt an oversized download off the declared size (a config artifact
        // carries no declared size, so this gate is descriptor-only).
        let declared = u64::try_from(descriptor.size).unwrap_or(0);
        if declared > self.config.max_single_bytes {
            return Err(AttachmentError::SizeLimitExceeded {
                size: declared,
                limit: self.config.max_single_bytes,
            });
        }
        self.fetch_verified(
            &descriptor.reference,
            &descriptor.sha256,
            self.config.max_single_bytes,
        )
        .await
    }

    /// Read, hash, cache and upload one output file; build its descriptor.
    async fn upload_one(
        &self,
        abs: &Path,
        rel: &str,
    ) -> Result<AttachmentDescriptor, AttachmentError> {
        let bytes = std::fs::read(abs)?;
        let size = bytes.len();
        if size as u64 > self.config.max_single_bytes {
            return Err(AttachmentError::SizeLimitExceeded {
                size: size as u64,
                limit: self.config.max_single_bytes,
            });
        }
        let content_type = guess_content_type(rel);
        self.check_content_type(&content_type)?;
        let sha256 = sha256_hex(&bytes);
        // Cache before the move so a later inbound fetch of the same bytes hits.
        self.cache_put(&sha256, &bytes);
        let uploaded = self
            .backend
            .upload(rel, &content_type, &sha256, bytes)
            .await?;
        Ok(AttachmentDescriptor {
            id: uploaded.id,
            name: rel.to_owned(),
            content_type,
            size: i64::try_from(size).unwrap_or(i64::MAX),
            sha256,
            reference: uploaded.reference,
            direction: Some(DIRECTION_OUTBOUND.to_owned()),
        })
    }

    fn check_content_type(&self, content_type: &str) -> Result<(), AttachmentError> {
        if self.config.content_type_allowlist.is_empty()
            || self
                .config
                .content_type_allowlist
                .iter()
                .any(|allowed| allowed == content_type)
        {
            Ok(())
        } else {
            Err(AttachmentError::ContentTypeNotAllowed {
                content_type: content_type.to_owned(),
            })
        }
    }

    // -- Content-addressed cache (A4) -----------------------------------------

    /// Read cached bytes for `sha256` (verifying they still hash correctly; a
    /// corrupt entry is removed). `None` on miss or unsafe key.
    fn cache_get(&self, sha256: &str) -> Option<Vec<u8>> {
        let key = cache_key(sha256)?;
        let path = self.cache_dir.join(&key);
        let bytes = std::fs::read(&path).ok()?;
        if sha256_hex(&bytes) == key {
            touch(&path);
            Some(bytes)
        } else {
            let _ = std::fs::remove_file(&path);
            None
        }
    }

    /// Store `bytes` under their `sha256` (content-addressed), then GC the cache
    /// back under the size cap. Best-effort: any failure leaves the cache intact
    /// and the caller keeps the bytes it already has.
    fn cache_put(&self, sha256: &str, bytes: &[u8]) {
        let Some(key) = cache_key(sha256) else {
            return;
        };
        // A single blob larger than the whole cap can never fit; skip caching it.
        if bytes.len() as u64 > self.config.cache_max_bytes {
            return;
        }
        if std::fs::create_dir_all(&self.cache_dir).is_err() {
            return;
        }
        let tmp = self.cache_dir.join(format!(".tmp.{key}"));
        if std::fs::write(&tmp, bytes).is_err() {
            return;
        }
        let final_path = self.cache_dir.join(&key);
        if std::fs::rename(&tmp, &final_path).is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        self.gc();
    }

    /// Evict least-recently-used cache entries until total size ≤ the cap (A4).
    /// Recency is the file `modified` time, bumped on each cache hit by [`touch`].
    fn gc(&self) {
        let cap = self.config.cache_max_bytes;
        let Ok(entries) = std::fs::read_dir(&self.cache_dir) else {
            return;
        };
        let mut files: Vec<(PathBuf, u64, SystemTime)> = Vec::new();
        let mut total = 0_u64;
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            // Only real cache entries count; skip `.tmp.*` and anything stray.
            if cache_key(&name).is_none() {
                continue;
            }
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            total = total.saturating_add(meta.len());
            let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            files.push((entry.path(), meta.len(), modified));
        }
        if total <= cap {
            return;
        }
        files.sort_by_key(|(_, _, modified)| *modified);
        for (path, len, _) in files {
            if total <= cap {
                break;
            }
            if std::fs::remove_file(&path).is_ok() {
                total = total.saturating_sub(len);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------------

/// Lowercase hex SHA-256 of `bytes` — the integrity check + cache key (A4).
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        // Writing to a String is infallible.
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// A 64-char lowercase hex string usable as a cache filename, or `None` if the
/// input is not a valid sha256 hex (so an untrusted descriptor hash can never
/// escape the cache dir as a path component).
fn cache_key(sha256: &str) -> Option<String> {
    let key = sha256.to_ascii_lowercase();
    if key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(key)
    } else {
        None
    }
}

/// Whether a descriptor is an agent-produced (outbound) artifact (skipped on
/// inbound materialization).
fn is_outbound(descriptor: &AttachmentDescriptor) -> bool {
    descriptor.direction.as_deref() == Some(DIRECTION_OUTBOUND)
}

/// Validate an attacker-supplied attachment name down to a single safe path
/// component: reject empties, NULs, separators (`/`/`\`), `.`/`..`, so it can be
/// joined under `inputs/` without `../`/absolute-path escape (PLAN-005 risk 3).
fn sanitize_name(name: &str) -> Result<String, AttachmentError> {
    let unsafe_name = || AttachmentError::UnsafeName {
        name: name.to_owned(),
    };
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.contains('\0')
        || name.contains('/')
        || name.contains('\\')
    {
        return Err(unsafe_name());
    }
    Ok(name.to_owned())
}

/// Bump a cache file's modified time to now so it survives LRU GC longer
/// (best-effort; failure is ignored — GC still works off insertion time).
fn touch(path: &Path) {
    if let Ok(file) = std::fs::OpenOptions::new().write(true).open(path) {
        let _ = file.set_modified(SystemTime::now());
    }
}

/// Best-effort MIME guess from a file extension; unknown → octet-stream. The
/// attachment bytes are opaque (no redaction over binaries, PLAN-005 §5).
fn guess_content_type(name: &str) -> String {
    let ext = Path::new(name)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let content_type = match ext.as_str() {
        "txt" | "log" | "md" => "text/plain",
        "json" => "application/json",
        "csv" => "text/csv",
        "html" | "htm" => "text/html",
        "xml" => "application/xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" | "tgz" => "application/gzip",
        "wasm" => "application/wasm",
        _ => "application/octet-stream",
    };
    content_type.to_owned()
}

/// List every **regular file** under `root` (recursively), returning
/// `(absolute path, relative-path name with `/` separators)`. Symlinks are
/// skipped (`file_type` does not follow them), so neither a symlink loop nor a
/// symlink pointing outside `root` can be uploaded. A missing `root` → empty.
fn list_files(root: &Path) -> std::io::Result<Vec<(PathBuf, String)>> {
    let mut out = Vec::new();
    if root.is_dir() {
        collect_files(root, root, &mut out)?;
    }
    Ok(out)
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, String)>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_files(root, &path, out)?;
        } else if file_type.is_file()
            && let Ok(rel) = path.strip_prefix(root)
        {
            let name = rel.to_string_lossy().replace('\\', "/");
            out.push((path.clone(), name));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;

    use super::{AttachmentBackend, AttachmentError, AttachmentManager, sanitize_name, sha256_hex};
    use crate::config::AttachmentsConfig;
    use crate::protocol::v2::AttachmentDescriptor;
    use crate::transport::http::UploadedAttachment;

    /// In-memory backend: serves downloads from `store` (keyed by `ref`), records
    /// uploads, and counts downloads (to prove cache hits avoid re-download). The
    /// trait is implemented on `Arc<MemBackend>` so the manager owns a `'static`
    /// box while the test keeps a handle to inspect it.
    #[derive(Default)]
    struct MemBackend {
        store: Mutex<std::collections::HashMap<String, Vec<u8>>>,
        uploads: Mutex<Vec<(String, String, Vec<u8>)>>, // (name, content_type, bytes)
        downloads: AtomicUsize,
    }

    impl MemBackend {
        fn with(reference: &str, bytes: &[u8]) -> Arc<Self> {
            let backend = Self::default();
            backend
                .store
                .lock()
                .unwrap()
                .insert(reference.to_owned(), bytes.to_vec());
            Arc::new(backend)
        }
        fn download_count(&self) -> usize {
            self.downloads.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl AttachmentBackend for Arc<MemBackend> {
        async fn download(
            &self,
            descriptor: &AttachmentDescriptor,
        ) -> Result<Vec<u8>, AttachmentError> {
            self.downloads.fetch_add(1, Ordering::SeqCst);
            self.store
                .lock()
                .unwrap()
                .get(&descriptor.reference)
                .cloned()
                .ok_or(AttachmentError::Unsupported)
        }

        async fn upload(
            &self,
            name: &str,
            content_type: &str,
            _sha256: &str,
            bytes: Vec<u8>,
        ) -> Result<UploadedAttachment, AttachmentError> {
            let mut uploads = self.uploads.lock().unwrap();
            let id = format!("att-{}", uploads.len());
            uploads.push((name.to_owned(), content_type.to_owned(), bytes));
            Ok(UploadedAttachment {
                id: id.clone(),
                reference: format!("/agent/attachments/{id}"),
            })
        }
    }

    fn descriptor(name: &str, bytes: &[u8], reference: &str) -> AttachmentDescriptor {
        AttachmentDescriptor {
            id: "host-id".to_owned(),
            name: name.to_owned(),
            content_type: "text/plain".to_owned(),
            size: i64::try_from(bytes.len()).unwrap(),
            sha256: sha256_hex(bytes),
            reference: reference.to_owned(),
            direction: Some("inbound".to_owned()),
        }
    }

    fn manager(
        backend: &Arc<MemBackend>,
        ws: &std::path::Path,
        config: AttachmentsConfig,
    ) -> AttachmentManager {
        AttachmentManager::new(Box::new(Arc::clone(backend)), ws, config)
    }

    #[test]
    fn sha256_hex_is_lowercase_hex_of_the_known_vector() {
        // SHA-256("abc") — the canonical NIST test vector.
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[tokio::test]
    async fn materialize_verifies_sha256_and_writes_into_inputs() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/a-1", b"INPUT-DATA");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        let work_dir = ws.path().join("threads").join("th-1");

        let descriptors = vec![descriptor(
            "doc.txt",
            b"INPUT-DATA",
            "/agent/attachments/a-1",
        )];
        let written = mgr
            .materialize_inputs(&work_dir, &descriptors)
            .await
            .unwrap();

        assert_eq!(written.len(), 1);
        let dest = work_dir.join("inputs").join("doc.txt");
        assert_eq!(std::fs::read(&dest).unwrap(), b"INPUT-DATA");
    }

    #[tokio::test]
    async fn materialize_rejects_sha256_mismatch() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/bad", b"TAMPERED");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        let work_dir = ws.path().join("th");

        // Descriptor claims the hash of "ORIGINAL" but the backend serves "TAMPERED".
        let mut d = descriptor("doc.txt", b"ORIGINAL", "/agent/attachments/bad");
        d.size = i64::try_from("TAMPERED".len()).unwrap();
        let err = mgr.materialize_inputs(&work_dir, &[d]).await.unwrap_err();
        assert!(matches!(err, AttachmentError::IntegrityMismatch { .. }));
        // Nothing was written on rejection.
        assert!(!work_dir.join("inputs").join("doc.txt").exists());
    }

    #[tokio::test]
    async fn cache_dedups_second_fetch_without_redownload() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/a-1", b"CACHE-ME");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());

        let descriptors = vec![descriptor("a.txt", b"CACHE-ME", "/agent/attachments/a-1")];
        mgr.materialize_inputs(&ws.path().join("t1"), &descriptors)
            .await
            .unwrap();
        assert_eq!(backend.download_count(), 1, "first fetch downloads");

        // A second materialization (different thread, same workspace cache) is a
        // content-addressed cache hit — no second download.
        mgr.materialize_inputs(&ws.path().join("t2"), &descriptors)
            .await
            .unwrap();
        assert_eq!(backend.download_count(), 1, "second fetch is a cache hit");
        assert_eq!(
            std::fs::read(ws.path().join("t2").join("inputs").join("a.txt")).unwrap(),
            b"CACHE-ME"
        );
    }

    #[tokio::test]
    async fn enforces_single_file_size_limit_before_download() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/big", b"too-big-payload");
        let config = AttachmentsConfig {
            max_single_bytes: 4,
            ..AttachmentsConfig::default()
        };
        let mgr = manager(&backend, ws.path(), config);

        let d = descriptor("big.bin", b"too-big-payload", "/agent/attachments/big");
        let err = mgr
            .materialize_inputs(&ws.path().join("t"), &[d])
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::SizeLimitExceeded { .. }));
        // The size gate runs off the descriptor — no byte transfer was attempted.
        assert_eq!(backend.download_count(), 0);
    }

    #[tokio::test]
    async fn enforces_per_execution_size_limit() {
        let ws = tempfile::tempdir().unwrap();
        let backend = Arc::new(MemBackend::default());
        backend
            .store
            .lock()
            .unwrap()
            .insert("/r/1".to_owned(), b"AAAA".to_vec());
        backend
            .store
            .lock()
            .unwrap()
            .insert("/r/2".to_owned(), b"BBBB".to_vec());
        let config = AttachmentsConfig {
            max_execution_bytes: 6, // two 4-byte files = 8 > 6
            ..AttachmentsConfig::default()
        };
        let mgr = manager(&backend, ws.path(), config);

        let descriptors = vec![
            descriptor("a.txt", b"AAAA", "/r/1"),
            descriptor("b.txt", b"BBBB", "/r/2"),
        ];
        let err = mgr
            .materialize_inputs(&ws.path().join("t"), &descriptors)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            AttachmentError::ExecutionLimitExceeded { .. }
        ));
    }

    #[tokio::test]
    async fn enforces_content_type_allowlist() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/r/png", b"PNGDATA");
        let config = AttachmentsConfig {
            content_type_allowlist: vec!["image/png".to_owned()],
            ..AttachmentsConfig::default()
        };
        let mgr = manager(&backend, ws.path(), config);

        // descriptor() uses content_type "text/plain", which is not allowed.
        let d = descriptor("note.txt", b"PNGDATA", "/r/png");
        let err = mgr
            .materialize_inputs(&ws.path().join("t"), &[d])
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::ContentTypeNotAllowed { .. }));
    }

    #[test]
    fn sanitize_name_rejects_traversal_and_absolute_paths() {
        for bad in [
            "../escape",
            "/etc/passwd",
            "..",
            ".",
            "",
            "a/b",
            "a\\b",
            "x\0y",
        ] {
            assert!(
                matches!(sanitize_name(bad), Err(AttachmentError::UnsafeName { .. })),
                "expected {bad:?} to be rejected"
            );
        }
        for good in ["report.txt", "data-1.json", "a_b.csv", "naïve.txt"] {
            assert_eq!(sanitize_name(good).unwrap(), good);
        }
    }

    #[tokio::test]
    async fn unsafe_inbound_name_is_rejected_before_writing() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/r/x", b"DATA");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        let d = descriptor("../../escape.txt", b"DATA", "/r/x");
        let err = mgr
            .materialize_inputs(&ws.path().join("t"), &[d])
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::UnsafeName { .. }));
    }

    #[tokio::test]
    async fn collect_outputs_uploads_and_returns_descriptors() {
        let ws = tempfile::tempdir().unwrap();
        let backend = Arc::new(MemBackend::default());
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());

        let work_dir = ws.path().join("threads").join("th-out");
        let output_dir = work_dir.join("output");
        std::fs::create_dir_all(output_dir.join("sub")).unwrap();
        std::fs::write(output_dir.join("report.txt"), b"OUTPUT-A").unwrap();
        std::fs::write(output_dir.join("sub").join("data.json"), b"{\"k\":1}").unwrap();

        let mut descriptors = mgr.collect_outputs(&work_dir).await;
        descriptors.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].name, "report.txt");
        assert_eq!(descriptors[0].content_type, "text/plain");
        assert_eq!(descriptors[0].sha256, sha256_hex(b"OUTPUT-A"));
        assert_eq!(descriptors[0].direction.as_deref(), Some("outbound"));
        assert_eq!(descriptors[1].name, "sub/data.json");
        assert_eq!(descriptors[1].content_type, "application/json");
        assert_eq!(descriptors[1].sha256, sha256_hex(b"{\"k\":1}"));
        // Every descriptor carries the host-proxy `ref` minted on upload (the
        // exact id depends on filesystem scan order, so just match the shape).
        for descriptor in &descriptors {
            assert!(
                descriptor.reference.starts_with("/agent/attachments/att-"),
                "unexpected ref {:?}",
                descriptor.reference
            );
        }
        // Both files were uploaded as bytes (out-of-band), not left behind.
        assert_eq!(backend.uploads.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn collect_outputs_on_missing_dir_is_empty() {
        let ws = tempfile::tempdir().unwrap();
        let backend = Arc::new(MemBackend::default());
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        assert!(
            mgr.collect_outputs(&ws.path().join("nope"))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn presigned_backend_is_unsupported() {
        use super::{PresignedBackend, build_backend};
        let ws = tempfile::tempdir().unwrap();
        let config = AttachmentsConfig {
            backend: crate::config::AttachmentBackendKind::Presigned,
            ..AttachmentsConfig::default()
        };
        let backend = build_backend(
            &config,
            crate::transport::http::HttpClient::new().unwrap(),
            "https://host.example".to_owned(),
            secrecy::SecretString::from("connect"),
        );
        let mgr = AttachmentManager::new(backend, ws.path(), config);
        let d = descriptor("x.txt", b"DATA", "/r/x");
        let err = mgr
            .materialize_inputs(&ws.path().join("t"), &[d])
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::Unsupported));

        // The unit type also errors directly.
        let presigned = PresignedBackend;
        assert!(matches!(
            presigned.download(&descriptor("y", b"y", "/r/y")).await,
            Err(AttachmentError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn fetch_verified_caches_and_avoids_redownload() {
        // The config/skills provisioning seam: by `(ref, sha256)`, no descriptor.
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/claude-md", b"# CLAUDE.md\n");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        let sha = sha256_hex(b"# CLAUDE.md\n");

        let first = mgr
            .fetch_verified("/agent/attachments/claude-md", &sha, 1 << 20)
            .await
            .unwrap();
        assert_eq!(first, b"# CLAUDE.md\n");
        assert_eq!(backend.download_count(), 1, "first fetch downloads");

        // A second fetch of the same sha is a content-addressed cache hit — even a
        // different `ref` resolves from cache (the sha is the key).
        let second = mgr
            .fetch_verified("/agent/attachments/other", &sha, 1 << 20)
            .await
            .unwrap();
        assert_eq!(second, b"# CLAUDE.md\n");
        assert_eq!(backend.download_count(), 1, "second fetch is a cache hit");
    }

    #[tokio::test]
    async fn fetch_verified_rejects_sha256_mismatch() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/bad", b"TAMPERED");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        // Ask for the hash of "ORIGINAL"; the backend serves "TAMPERED".
        let wrong = sha256_hex(b"ORIGINAL");
        let err = mgr
            .fetch_verified("/agent/attachments/bad", &wrong, 1 << 20)
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::IntegrityMismatch { .. }));
    }

    #[tokio::test]
    async fn fetch_verified_enforces_max_bytes() {
        let ws = tempfile::tempdir().unwrap();
        let backend = MemBackend::with("/agent/attachments/big", b"way-too-large-config");
        let mgr = manager(&backend, ws.path(), AttachmentsConfig::default());
        let sha = sha256_hex(b"way-too-large-config");
        // The cap is the explicit `max_bytes` arg (not the manager's config).
        let err = mgr
            .fetch_verified("/agent/attachments/big", &sha, 4)
            .await
            .unwrap_err();
        assert!(matches!(err, AttachmentError::SizeLimitExceeded { .. }));
    }
}
