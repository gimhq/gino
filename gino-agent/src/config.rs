//! Global configuration layer (PLAN-003 §3).
//!
//! Every setting the runner needs — the orchestrator URL, identity, engine
//! credentials, scheduler limits, paths and log level — is resolved here into a
//! single typed [`Config`]. Credentials are *just* configuration values (an
//! `ANTHROPIC_API_KEY` is the same shape whether it is a raw key or a scoped
//! proxy token), so there is no A-vs-B code fork; the deployment decides what the
//! values hold.
//!
//! ## Precedence (later wins)
//!
//! ```text
//! defaults → env/CLI bootstrap → register response → HTTP refresh → WS update
//! ```
//!
//! Each source produces a [`ConfigOverlay`] (a partial, all-optional view) that
//! is merged onto the running [`Config`] with [`Config::apply`]. Only fields the
//! overlay actually sets override what is already there; absent fields are left
//! untouched. This module performs **no** network IO — the register/HTTP/WS
//! wiring that produces those overlays lands in later changes; here we expose
//! only the merge surface.
//!
//! ## Secrets
//!
//! Resolved credentials and tokens are wrapped in [`secrecy::SecretString`] so
//! they are redacted in `Debug`/log output and never echoed. Overlays carry the
//! raw strings only transiently and are deliberately not `Debug`-printable.

use std::path::PathBuf;

use secrecy::SecretString;
use serde::Deserialize;
use thiserror::Error;

use crate::protocol::v2::ProvisioningEntry;

/// Default tracing filter directive when neither `RUST_LOG` nor `-v` is set.
const DEFAULT_LOG_LEVEL: &str = "info";
/// Default workspace directory (configurable; never a hardcoded Docker mount).
const DEFAULT_WORKSPACE_DIR: &str = "/var/lib/gino-agent";
/// Default SQLite filename within the workspace directory.
const DEFAULT_DB_FILE: &str = "agent.db";
/// Default transport liveness ping cadence (ms) when the host sets none.
const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 30_000;
/// Default max bytes for a single attachment (agent-architecture §9, A5: 100 MB).
const DEFAULT_ATTACHMENT_MAX_SINGLE_BYTES: u64 = 100 * 1024 * 1024;
/// Default max bytes materialized per execution (A5: 500 MB).
const DEFAULT_ATTACHMENT_MAX_EXECUTION_BYTES: u64 = 500 * 1024 * 1024;
/// Default content-addressed attachment cache cap before LRU GC (A4: 1 GiB).
const DEFAULT_ATTACHMENT_CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;
/// Default days a terminal queue row (acked outbox / processed inbox / cron_logs
/// ledger line / terminal-thread attachment snapshot) is retained before the
/// periodic GC sweep deletes it (audit P1-9).
const DEFAULT_RETENTION_DAYS: u32 = 30;

/// Errors raised while validating a resolved [`Config`].
#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    /// No orchestrator host URL was provided by any layer.
    #[error("orchestrator host URL is required (set --host or GINO_HOST_URL)")]
    MissingHost,
    /// The host URL did not start with a supported scheme.
    #[error("orchestrator host URL must start with http:// or https://, got {0:?}")]
    InvalidHostScheme(String),
}

/// Fully-resolved runner configuration.
///
/// Build the baseline with [`Config::default`], layer overrides with
/// [`Config::apply`], then check required fields with [`Config::validate`].
#[derive(Clone, Debug)]
pub(crate) struct Config {
    /// Orchestrator base URL, e.g. `https://gino.example.com`. Required.
    pub(crate) host_url: Option<String>,
    /// Human-readable agent label (CLI `--name`).
    pub(crate) agent_name: Option<String>,
    /// Host-generated agent id, assigned at registration and persisted locally.
    pub(crate) agent_id: Option<String>,
    /// Enrollment credential — used only to register, never as the WS token.
    pub(crate) register_token: Option<SecretString>,
    /// Per-agent runtime token negotiated at registration; used for the WS.
    pub(crate) connect_token: Option<SecretString>,
    /// Engine environment and defaults handed to the execution backend.
    pub(crate) engine: EngineConfig,
    /// Thread-executor limits (watchdog thresholds, retry budget).
    pub(crate) executor: ExecutorConfig,
    /// Out-of-band attachment channel limits + backend selection (PLAN-005).
    pub(crate) attachments: AttachmentsConfig,
    /// Queue/ledger GC retention window (audit P1-9).
    pub(crate) retention: RetentionConfig,
    /// Global-scope provisioning entries — the desired set of instruction files +
    /// skills materialized into `$ENGINE_CONFIG_DIR/` (PLAN-007 §5). The host
    /// re-pushes the full desired global set each refresh; an overlay carrying
    /// `entries` **replaces** this vec. Default = empty.
    pub(crate) entries: Vec<ProvisioningEntry>,
    /// Host-assigned GLOBAL config version (PLAN-007 §6, version gate). Monotonic
    /// and server-owned; it scopes the agent's global config set (`entries`) and
    /// drives the startup/`config_update` reconcile decision: the runner reconciles
    /// the global scope **only when this advances** past the persisted applied
    /// version, else repairs-only (store-durable across restarts). `None` until a
    /// versioned payload (register / GET `/agent/config` / `config_update`) lands.
    // `config_version` (not bare `version`) is deliberate: it is the GLOBAL *config*
    // version, distinct from the wire `protocol_version` and an entry's `sha256`.
    #[allow(
        clippy::struct_field_names,
        reason = "PLAN-007 §6 names this the config version; the `config_` qualifier disambiguates it"
    )]
    pub(crate) config_version: Option<i64>,
    /// Transport liveness ping cadence, in milliseconds (`heartbeatIntervalMs`).
    pub(crate) heartbeat_interval_ms: u64,
    /// Filesystem layout (workspace + DB path).
    pub(crate) paths: Paths,
    /// Tracing filter directive (e.g. `info`, `gino_agent=debug`).
    pub(crate) log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host_url: None,
            agent_name: None,
            agent_id: None,
            register_token: None,
            connect_token: None,
            engine: EngineConfig::default(),
            executor: ExecutorConfig::default(),
            attachments: AttachmentsConfig::default(),
            retention: RetentionConfig::default(),
            entries: Vec::new(),
            config_version: None,
            heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
            paths: Paths::default(),
            log_level: DEFAULT_LOG_LEVEL.to_owned(),
        }
    }
}

/// Engine environment exposed to the execution backend.
///
/// To the engine these are plain environment variables; the source (in-guest
/// secret vs proxy token) is purely configuration.
#[derive(Clone, Debug, Default)]
pub(crate) struct EngineConfig {
    /// `ANTHROPIC_API_KEY`.
    pub(crate) anthropic_api_key: Option<SecretString>,
    /// `CLAUDE_CODE_OAUTH_TOKEN`.
    pub(crate) claude_code_oauth_token: Option<SecretString>,
    /// `ANTHROPIC_BASE_URL` (e.g. a host credential proxy).
    pub(crate) anthropic_base_url: Option<String>,
    /// Model id the engine should default to.
    pub(crate) model: Option<String>,
    /// Tools the engine is permitted to use.
    pub(crate) allowed_tools: Vec<String>,
}

/// Thread-executor limits (agent-architecture §5/§6). These promote the executor's
/// in-file `WatchdogCfg`/retry defaults to host-overridable config so a deployment
/// can tune the reap thresholds and retry budget; the runner maps them onto the
/// executor's `WatchdogCfg` and per-thread `max_attempts` at construction.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExecutorConfig {
    /// Td — max seconds a thread may sit `dispatched` before the watchdog reaps it.
    pub(crate) dispatched_timeout_secs: i64,
    /// Tr — max seconds a `running` thread may take before the watchdog reaps it.
    pub(crate) running_timeout_secs: i64,
    /// Max seconds the engine may emit no events before the in-run watchdog kills
    /// it; `0` disables the inactivity watchdog.
    pub(crate) inactivity_timeout_secs: u64,
    /// Default per-thread retry budget for executor-created (chat/api) threads.
    pub(crate) max_attempts: i64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        // Mirrors the executor's `WatchdogCfg::default` (Td 60s, Tr 30m, inactivity
        // 5m). The retry budget defaults to 3 so retryable (`runner.*`) failures
        // actually retry under the shipped config — the gate is
        // `attempt < max_attempts`, so 1 would make every failure terminal.
        Self {
            dispatched_timeout_secs: 60,
            running_timeout_secs: 1_800,
            inactivity_timeout_secs: 300,
            max_attempts: 3,
        }
    }
}

/// Byte backend for the out-of-band attachment channel (agent-architecture §9,
/// decision A2). **Host-proxy is the always-available default** (`GET`/`POST
/// /agent/attachments`, connect-token auth); `Presigned` (URL/CDN) is selected
/// only when object storage is configured and is **not implemented this
/// campaign** — the runner returns an `unsupported` error for it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum AttachmentBackendKind {
    /// Host streams the bytes through `GET`/`POST /agent/attachments`.
    #[default]
    HostProxy,
    /// Presigned URL / CDN (object storage). Not yet implemented.
    Presigned,
}

/// Out-of-band attachment channel limits + backend selection (agent-architecture
/// §9, decisions A2/A4/A5; PLAN-005). Only descriptors ride the wire; bytes move
/// on this channel, bounded by the limits below.
#[derive(Clone, Debug)]
pub(crate) struct AttachmentsConfig {
    /// Which byte backend the runner uses (A2).
    pub(crate) backend: AttachmentBackendKind,
    /// Max bytes for a single attachment (A5; default 100 MB).
    pub(crate) max_single_bytes: u64,
    /// Max bytes materialized into one execution's `work_dir` (A5; default 500 MB).
    pub(crate) max_execution_bytes: u64,
    /// Allowed content types; **empty = allow all** under the size caps (A5).
    pub(crate) content_type_allowlist: Vec<String>,
    /// Size cap for the content-addressed (`sha256`) cache before LRU GC (A4).
    pub(crate) cache_max_bytes: u64,
}

impl Default for AttachmentsConfig {
    fn default() -> Self {
        Self {
            backend: AttachmentBackendKind::HostProxy,
            max_single_bytes: DEFAULT_ATTACHMENT_MAX_SINGLE_BYTES,
            max_execution_bytes: DEFAULT_ATTACHMENT_MAX_EXECUTION_BYTES,
            content_type_allowlist: Vec::new(),
            cache_max_bytes: DEFAULT_ATTACHMENT_CACHE_MAX_BYTES,
        }
    }
}

/// Queue/ledger GC retention window (audit P1-9). The periodic sweep deletes
/// **terminal** rows — acked outbox, processed inbox, cron_logs ledger lines,
/// and attachment snapshots of terminal threads — older than this window.
/// Un-acked/un-processed rows are never deleted regardless of age (the
/// at-least-once contract).
#[derive(Clone, Copy, Debug)]
pub(crate) struct RetentionConfig {
    /// Days a terminal row survives before the GC sweep deletes it.
    pub(crate) days: u32,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            days: DEFAULT_RETENTION_DAYS,
        }
    }
}

/// Filesystem layout for local state.
#[derive(Clone, Debug)]
pub(crate) struct Paths {
    /// Workspace root holding engine working directories and local state.
    pub(crate) workspace_dir: PathBuf,
    /// SQLite database path (outbox/inbox/tasks/identity).
    pub(crate) db_path: PathBuf,
}

impl Default for Paths {
    fn default() -> Self {
        let workspace_dir = PathBuf::from(DEFAULT_WORKSPACE_DIR);
        let db_path = workspace_dir.join(DEFAULT_DB_FILE);
        Self {
            workspace_dir,
            db_path,
        }
    }
}

impl Config {
    /// Merge a partial overlay onto this config. Fields the overlay sets win;
    /// absent fields are left untouched. This is the layering primitive used by
    /// every config source (bootstrap, register response, HTTP refresh, WS).
    pub(crate) fn apply(&mut self, overlay: ConfigOverlay) {
        let ConfigOverlay {
            host_url,
            agent_name,
            agent_id,
            register_token,
            connect_token,
            engine,
            executor,
            attachments,
            retention,
            entries,
            version,
            heartbeat_interval_ms,
            paths,
            log_level,
        } = overlay;

        overlay_field(&mut self.host_url, host_url);
        overlay_field(&mut self.agent_name, agent_name);
        overlay_field(&mut self.agent_id, agent_id);
        overlay_secret(&mut self.register_token, register_token);
        overlay_secret(&mut self.connect_token, connect_token);
        if let Some(level) = log_level {
            self.log_level = level;
        }
        if let Some(ms) = heartbeat_interval_ms {
            self.heartbeat_interval_ms = ms;
        }

        let EngineOverlay {
            anthropic_api_key,
            claude_code_oauth_token,
            anthropic_base_url,
            model,
            allowed_tools,
        } = engine;
        overlay_secret(&mut self.engine.anthropic_api_key, anthropic_api_key);
        overlay_secret(
            &mut self.engine.claude_code_oauth_token,
            claude_code_oauth_token,
        );
        overlay_field(&mut self.engine.anthropic_base_url, anthropic_base_url);
        overlay_field(&mut self.engine.model, model);
        if let Some(tools) = allowed_tools {
            self.engine.allowed_tools = tools;
        }

        let ExecutorOverlay {
            dispatched_timeout_secs,
            running_timeout_secs,
            inactivity_timeout_secs,
            max_attempts,
        } = executor;
        if let Some(value) = dispatched_timeout_secs {
            self.executor.dispatched_timeout_secs = value;
        }
        if let Some(value) = running_timeout_secs {
            self.executor.running_timeout_secs = value;
        }
        if let Some(value) = inactivity_timeout_secs {
            self.executor.inactivity_timeout_secs = value;
        }
        if let Some(value) = max_attempts {
            self.executor.max_attempts = value;
        }

        self.apply_attachments(attachments);

        let RetentionOverlay { days } = retention;
        if let Some(value) = days {
            self.retention.days = value;
        }

        // Provisioning entries are a replace-when-Some list (cf. `allowed_tools` /
        // `content_type_allowlist`): the host re-pushes the full desired global set
        // each refresh, so `Some` replaces wholesale and `None` leaves it intact.
        if let Some(list) = entries {
            self.entries = list;
        }

        // The GLOBAL config version is latest-Some-wins (PLAN-007 §6): a versioned
        // layer pins it, an unversioned layer leaves the prior version untouched.
        overlay_field(&mut self.config_version, version);

        let PathsOverlay {
            workspace_dir,
            db_path,
        } = paths;
        if let Some(dir) = workspace_dir {
            self.paths.workspace_dir = dir;
        }
        if let Some(path) = db_path {
            self.paths.db_path = path;
        }
    }

    /// Merge the attachment-channel overlay (PLAN-005 limits + backend). Split out
    /// of [`Self::apply`] to keep that merge readable; an unrecognized backend
    /// string leaves the current selection (host-proxy default) untouched rather
    /// than failing the whole config refresh.
    fn apply_attachments(&mut self, overlay: AttachmentsOverlay) {
        let AttachmentsOverlay {
            backend,
            max_single_bytes,
            max_execution_bytes,
            content_type_allowlist,
            cache_max_bytes,
        } = overlay;
        if let Some(kind) = backend {
            match kind.as_str() {
                "host_proxy" | "hostProxy" => {
                    self.attachments.backend = AttachmentBackendKind::HostProxy;
                }
                "presigned" => self.attachments.backend = AttachmentBackendKind::Presigned,
                _ => {}
            }
        }
        if let Some(value) = max_single_bytes {
            self.attachments.max_single_bytes = value;
        }
        if let Some(value) = max_execution_bytes {
            self.attachments.max_execution_bytes = value;
        }
        if let Some(list) = content_type_allowlist {
            self.attachments.content_type_allowlist = list;
        }
        if let Some(value) = cache_max_bytes {
            self.attachments.cache_max_bytes = value;
        }
    }

    /// Verify that required fields are present and well-formed.
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        let host = self.host_url.as_deref().ok_or(ConfigError::MissingHost)?;
        if !(host.starts_with("http://") || host.starts_with("https://")) {
            return Err(ConfigError::InvalidHostScheme(host.to_owned()));
        }
        Ok(())
    }
}

/// Orchestrator URL derivation. These are part of the resolved config surface
/// consumed by the register/transport modules added in later changes; they are
/// already validated by unit tests here but not yet called from `main`.
#[allow(dead_code)]
impl Config {
    /// The orchestrator HTTP base URL with any trailing slash trimmed.
    pub(crate) fn http_base(&self) -> Result<&str, ConfigError> {
        let host = self.host_url.as_deref().ok_or(ConfigError::MissingHost)?;
        Ok(host.trim_end_matches('/'))
    }

    /// `POST {host}/agent/register` — the enrollment endpoint (PLAN-003 §2.1).
    pub(crate) fn register_url(&self) -> Result<String, ConfigError> {
        Ok(format!("{}/agent/register", self.http_base()?))
    }

    /// `GET {host}/agent/config/{agent_id}` — the authoritative config fetch
    /// (PLAN-003 §2 / PLAN-007 §6). On a fresh enrollment this re-supplies what the
    /// register response already carried; on a **restart** (persisted identity, no
    /// second register) it is the only source of the host's desired config —
    /// including the versioned global provisioning set the reconcile version gate
    /// reads.
    pub(crate) fn config_url(&self, agent_id: &str) -> Result<String, ConfigError> {
        Ok(format!("{}/agent/config/{agent_id}", self.http_base()?))
    }

    /// `WS {host}/agent/ws/{agent_id}` — the primary channel. `http` maps to
    /// `ws` and `https` to `wss`.
    pub(crate) fn ws_url(&self, agent_id: &str) -> Result<String, ConfigError> {
        let base = self.http_base()?;
        let ws_base = match base.split_once("://") {
            Some(("https", rest)) => format!("wss://{rest}"),
            Some(("http", rest)) => format!("ws://{rest}"),
            _ => return Err(ConfigError::InvalidHostScheme(base.to_owned())),
        };
        Ok(format!("{ws_base}/agent/ws/{agent_id}"))
    }
}

/// A partial view of [`Config`]: every field is optional. One overlay is
/// produced per config source and merged with [`Config::apply`].
///
/// The host-assigned layers (register response / HTTP refresh / WS
/// `config_update`) deserialize directly into this type via
/// [`ConfigOverlay::from_json`]. `register_token` is bootstrap-only and is never
/// accepted from a host payload (`#[serde(skip)]`).
#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct ConfigOverlay {
    host_url: Option<String>,
    agent_name: Option<String>,
    agent_id: Option<String>,
    #[serde(skip)]
    register_token: Option<String>,
    connect_token: Option<String>,
    engine: EngineOverlay,
    executor: ExecutorOverlay,
    attachments: AttachmentsOverlay,
    retention: RetentionOverlay,
    /// Full desired global provisioning set (PLAN-007 §5). Unlike `register_token`
    /// this is host-sent, so it is **not** `#[serde(skip)]`; when present it
    /// replaces `Config::entries` wholesale.
    entries: Option<Vec<ProvisioningEntry>>,
    /// Host-assigned GLOBAL config version (PLAN-007 §6). Monotonic; carried on
    /// register-config / GET `/agent/config` / `config_update` alongside `entries`,
    /// and the sole signal the reconcile version gate keys on.
    version: Option<i64>,
    heartbeat_interval_ms: Option<u64>,
    paths: PathsOverlay,
    log_level: Option<String>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct EngineOverlay {
    anthropic_api_key: Option<String>,
    claude_code_oauth_token: Option<String>,
    anthropic_base_url: Option<String>,
    model: Option<String>,
    allowed_tools: Option<Vec<String>>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ExecutorOverlay {
    dispatched_timeout_secs: Option<i64>,
    running_timeout_secs: Option<i64>,
    inactivity_timeout_secs: Option<u64>,
    max_attempts: Option<i64>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct AttachmentsOverlay {
    backend: Option<String>,
    max_single_bytes: Option<u64>,
    max_execution_bytes: Option<u64>,
    content_type_allowlist: Option<Vec<String>>,
    cache_max_bytes: Option<u64>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct RetentionOverlay {
    days: Option<u32>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct PathsOverlay {
    workspace_dir: Option<PathBuf>,
    db_path: Option<PathBuf>,
}

impl ConfigOverlay {
    /// Build the env/CLI bootstrap overlay from the PLAN-003 §2.1 inputs. The
    /// register token is the only secret carried by the bootstrap layer.
    pub(crate) fn bootstrap(
        host_url: Option<String>,
        register_token: Option<String>,
        agent_name: Option<String>,
    ) -> Self {
        Self {
            host_url,
            agent_name,
            register_token,
            ..Self::default()
        }
    }

    /// Build the env/CLI **paths** bootstrap overlay from `GINO_WORKSPACE_DIR` /
    /// `GINO_DB_PATH` (PLAN-003 §3 config precedence; ARCHITECTURE "Config layer").
    /// Part of the same env/CLI bootstrap layer as [`Self::bootstrap`]; kept a
    /// separate builder so the daemon can resolve filesystem paths *before* it
    /// opens the DB without entangling them with the host/identity inputs (which
    /// flow from different env vars / CLI flags).
    ///
    /// `db_path` follows the resolved `workspace_dir`: an explicit `GINO_DB_PATH`
    /// wins, otherwise — when only `GINO_WORKSPACE_DIR` is set — the DB defaults to
    /// `{workspace_dir}/agent.db`, so relocating the workspace moves the DB with it
    /// (the documented default). With neither set the overlay is empty and the
    /// built-in defaults (`/var/lib/gino-agent`, `{workspace}/agent.db`) stand.
    /// Later layers (register response / HTTP refresh / WS `config_update`) may
    /// still override these per the normal precedence.
    pub(crate) fn paths_bootstrap(
        workspace_dir: Option<PathBuf>,
        db_path: Option<PathBuf>,
    ) -> Self {
        let db_path =
            db_path.or_else(|| workspace_dir.as_ref().map(|dir| dir.join(DEFAULT_DB_FILE)));
        Self {
            paths: PathsOverlay {
                workspace_dir,
                db_path,
            },
            ..Self::default()
        }
    }

    /// Parse a host-assigned config payload (register response / HTTP refresh /
    /// WS `config_update`) into an overlay. Unknown fields are rejected so a
    /// typo in a host payload surfaces loudly instead of being silently dropped.
    ///
    /// Consumed by the register/transport modules added in later changes; here
    /// it is exercised only by unit tests.
    #[allow(dead_code)]
    pub(crate) fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }

    /// The global provisioning entries this overlay carries, if any (PLAN-007 §5).
    /// Lets the runner re-reconcile the global scope on a live `config_update`
    /// without applying (and discarding) the whole overlay: `Some` is the full
    /// re-pushed desired set; `None` means the update did not touch provisioning.
    pub(crate) fn entries(&self) -> Option<&[ProvisioningEntry]> {
        self.entries.as_deref()
    }

    /// The host-assigned GLOBAL config version this overlay carries, if any
    /// (PLAN-007 §6). Mirrors [`Self::entries`]: lets the `config_update` route read
    /// the version to apply the reconcile version gate without consuming the overlay.
    pub(crate) fn version(&self) -> Option<i64> {
        self.version
    }
}

/// Overwrite `dst` with `src` only when the overlay actually carries a value.
fn overlay_field<T>(dst: &mut Option<T>, src: Option<T>) {
    if let Some(value) = src {
        *dst = Some(value);
    }
}

/// Like [`overlay_field`], wrapping the incoming raw string into a secret at the
/// config boundary so resolved secrets are always redacted.
fn overlay_secret(dst: &mut Option<SecretString>, src: Option<String>) {
    if let Some(value) = src {
        *dst = Some(SecretString::from(value));
    }
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::{Config, ConfigError, ConfigOverlay};

    fn secret(config_secret: Option<&secrecy::SecretString>) -> Option<&str> {
        config_secret.map(ExposeSecret::expose_secret)
    }

    #[test]
    fn defaults_have_expected_values() {
        let config = Config::default();
        assert_eq!(config.log_level, "info");
        assert_eq!(
            config.executor.max_attempts, 3,
            "shipped default must let retryable failures actually retry"
        );
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/var/lib/gino-agent")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/var/lib/gino-agent/agent.db")
        );
        assert!(config.host_url.is_none());
        assert!(config.register_token.is_none());
        assert!(config.engine.allowed_tools.is_empty());
        assert_eq!(config.retention.days, 30);
    }

    #[test]
    fn from_json_overlays_retention_window() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::from_json(r#"{ "retention": { "days": 7 } }"#).unwrap());
        assert_eq!(config.retention.days, 7);

        // An overlay without `retention` leaves the prior value untouched.
        config.apply(ConfigOverlay::from_json(r#"{ "logLevel": "debug" }"#).unwrap());
        assert_eq!(config.retention.days, 7);
    }

    #[test]
    fn bootstrap_overlay_sets_host_name_and_token() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("https://gino.example.com".to_owned()),
            Some("register-secret".to_owned()),
            Some("worker-1".to_owned()),
        ));

        assert_eq!(config.host_url.as_deref(), Some("https://gino.example.com"));
        assert_eq!(config.agent_name.as_deref(), Some("worker-1"));
        assert_eq!(
            secret(config.register_token.as_ref()),
            Some("register-secret")
        );
    }

    #[test]
    fn later_layers_win_and_preserve_unset_fields() {
        let mut config = Config::default();

        // Layer 1 — bootstrap.
        config.apply(ConfigOverlay::bootstrap(
            Some("https://old.example.com".to_owned()),
            Some("register-secret".to_owned()),
            Some("worker-1".to_owned()),
        ));

        // Layer 2 — register response: new host + agent id + connect token + model.
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "hostUrl": "https://new.example.com",
                    "agentId": "agent-123",
                    "connectToken": "connect-secret",
                    "engine": { "model": "claude-opus-4-8" }
                }"#,
            )
            .unwrap(),
        );

        // Layer 3 — HTTP refresh: overrides only the model.
        config.apply(
            ConfigOverlay::from_json(r#"{ "engine": { "model": "claude-sonnet-4-6" } }"#).unwrap(),
        );

        // Layer 4 — live WS update: overrides only the log level.
        config.apply(ConfigOverlay::from_json(r#"{ "logLevel": "debug" }"#).unwrap());

        // Latest writer wins for each field…
        assert_eq!(config.host_url.as_deref(), Some("https://new.example.com"));
        assert_eq!(config.engine.model.as_deref(), Some("claude-sonnet-4-6"));
        assert_eq!(config.log_level, "debug");
        // …and fields a later layer left unset are preserved.
        assert_eq!(config.agent_name.as_deref(), Some("worker-1"));
        assert_eq!(config.agent_id.as_deref(), Some("agent-123"));
        assert_eq!(
            secret(config.register_token.as_ref()),
            Some("register-secret")
        );
        assert_eq!(
            secret(config.connect_token.as_ref()),
            Some("connect-secret")
        );
    }

    #[test]
    fn empty_overlay_changes_nothing() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("https://gino.example.com".to_owned()),
            None,
            None,
        ));
        let before = format!("{config:?}");
        config.apply(ConfigOverlay::default());
        assert_eq!(before, format!("{config:?}"));
    }

    #[test]
    fn from_json_parses_nested_host_assigned_config() {
        let mut config = Config::default();
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "engine": {
                        "anthropicApiKey": "sk-test",
                        "anthropicBaseUrl": "https://proxy.example.com",
                        "allowedTools": ["Bash", "Read", "Write"]
                    },
                    "executor": { "maxAttempts": 5 },
                    "paths": { "workspaceDir": "/data/ws" }
                }"#,
            )
            .unwrap(),
        );

        assert_eq!(
            secret(config.engine.anthropic_api_key.as_ref()),
            Some("sk-test")
        );
        assert_eq!(
            config.engine.anthropic_base_url.as_deref(),
            Some("https://proxy.example.com")
        );
        assert_eq!(config.engine.allowed_tools, ["Bash", "Read", "Write"]);
        assert_eq!(config.executor.max_attempts, 5);
        // A field the payload omitted keeps its default.
        assert_eq!(config.executor.dispatched_timeout_secs, 60);
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/data/ws")
        );
    }

    #[test]
    fn from_json_ignores_unknown_fields() {
        // Protocol §1/§4: host config objects are open (additive-compatible) —
        // unknown fields are accepted (ignored), never rejected. The host's
        // register config legitimately carries `wsUrl`/`configUrl`/`outputUrl`/
        // `protocolVersion` (docs/protocol.md §3) that this overlay does not model.
        // The retired `scheduler` block (`maxConcurrentTasks`/`maxQueuedTasks` —
        // execution is strictly serial) is likewise just an unknown field now.
        let overlay = ConfigOverlay::from_json(
            r#"{ "wsUrl": "/agent/ws/a", "configUrl": "/agent/config/a", "outputUrl": "/agent/output/a", "protocolVersion": 2, "bogusField": 1, "scheduler": { "maxConcurrentTasks": 8, "maxQueuedTasks": 16 }, "engine": { "model": "m" } }"#,
        )
        .expect("unknown host config fields must be ignored, not rejected");
        assert_eq!(overlay.engine.model.as_deref(), Some("m"));
    }

    #[test]
    fn attachment_defaults_match_the_locked_limits() {
        let config = Config::default();
        assert_eq!(
            config.attachments.backend,
            super::AttachmentBackendKind::HostProxy
        );
        assert_eq!(config.attachments.max_single_bytes, 100 * 1024 * 1024);
        assert_eq!(config.attachments.max_execution_bytes, 500 * 1024 * 1024);
        assert!(config.attachments.content_type_allowlist.is_empty());
        assert_eq!(config.attachments.cache_max_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn from_json_overlays_attachment_limits_and_backend() {
        let mut config = Config::default();
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "attachments": {
                        "backend": "presigned",
                        "maxSingleBytes": 1048576,
                        "maxExecutionBytes": 4194304,
                        "contentTypeAllowlist": ["text/plain", "image/png"],
                        "cacheMaxBytes": 2097152
                    }
                }"#,
            )
            .unwrap(),
        );
        assert_eq!(
            config.attachments.backend,
            super::AttachmentBackendKind::Presigned
        );
        assert_eq!(config.attachments.max_single_bytes, 1_048_576);
        assert_eq!(config.attachments.max_execution_bytes, 4_194_304);
        assert_eq!(
            config.attachments.content_type_allowlist,
            ["text/plain", "image/png"]
        );
        assert_eq!(config.attachments.cache_max_bytes, 2_097_152);
    }

    #[test]
    fn unknown_attachment_backend_keeps_the_current_selection() {
        let mut config = Config::default();
        config
            .apply(ConfigOverlay::from_json(r#"{ "attachments": { "backend": "ftp" } }"#).unwrap());
        // An unrecognized backend leaves the host-proxy default untouched.
        assert_eq!(
            config.attachments.backend,
            super::AttachmentBackendKind::HostProxy
        );
    }

    #[test]
    fn from_json_parses_global_provisioning_entries() {
        use crate::protocol::v2::EntryKind;

        let mut config = Config::default();
        assert!(config.entries.is_empty());
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "entries": [
                        {
                            "id": "clauderc",
                            "sha256": "1111111111111111111111111111111111111111111111111111111111111111",
                            "target": "CLAUDE.md",
                            "kind": "file"
                        },
                        {
                            "id": "demo",
                            "sha256": "2222222222222222222222222222222222222222222222222222222222222222",
                            "target": ".claude/skills/demo",
                            "kind": "dir"
                        }
                    ]
                }"#,
            )
            .unwrap(),
        );
        assert_eq!(config.entries.len(), 2);
        assert_eq!(config.entries[0].id, "clauderc");
        assert_eq!(config.entries[0].target, "CLAUDE.md");
        assert_eq!(config.entries[0].kind, EntryKind::File);
        assert_eq!(config.entries[1].id, "demo");
        assert_eq!(config.entries[1].target, ".claude/skills/demo");
        assert_eq!(config.entries[1].kind, EntryKind::Dir);
    }

    #[test]
    fn entries_overlay_replaces_when_some_and_preserves_when_absent() {
        let mut config = Config::default();

        // First overlay installs one global entry.
        config.apply(
            ConfigOverlay::from_json(
                r#"{ "entries": [
                    { "id": "a", "sha256": "aa", "target": "CLAUDE.md", "kind": "file" }
                ] }"#,
            )
            .unwrap(),
        );
        assert_eq!(config.entries.len(), 1);

        // An overlay WITHOUT `entries` leaves the prior set intact.
        config.apply(ConfigOverlay::from_json(r#"{ "logLevel": "debug" }"#).unwrap());
        assert_eq!(config.entries.len(), 1);
        assert_eq!(config.entries[0].id, "a");

        // The host re-pushes the full desired set: an explicit `[]` replaces to empty.
        config.apply(ConfigOverlay::from_json(r#"{ "entries": [] }"#).unwrap());
        assert!(config.entries.is_empty());
    }

    #[test]
    fn config_version_parses_and_is_latest_some_wins() {
        let mut config = Config::default();
        assert!(config.config_version.is_none());

        // A versioned overlay pins the global config version.
        config.apply(ConfigOverlay::from_json(r#"{ "version": 1 }"#).unwrap());
        assert_eq!(config.config_version, Some(1));

        // An UNVERSIONED overlay leaves the prior version untouched (latest-Some wins).
        config.apply(ConfigOverlay::from_json(r#"{ "logLevel": "debug" }"#).unwrap());
        assert_eq!(config.config_version, Some(1));

        // A newer version advances it; the overlay's `version()` accessor mirrors it.
        let overlay = ConfigOverlay::from_json(r#"{ "version": 7, "entries": [] }"#).unwrap();
        assert_eq!(overlay.version(), Some(7));
        config.apply(overlay);
        assert_eq!(config.config_version, Some(7));
    }

    #[test]
    fn derives_config_url() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("https://gino.example.com/".to_owned()),
            None,
            None,
        ));
        assert_eq!(
            config.config_url("agent-123").unwrap(),
            "https://gino.example.com/agent/config/agent-123"
        );
    }

    #[test]
    fn paths_bootstrap_resolves_both_paths_from_env() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::paths_bootstrap(
            Some(std::path::PathBuf::from("/data/ws")),
            Some(std::path::PathBuf::from("/srv/state/runner.db")),
        ));
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/data/ws")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/srv/state/runner.db")
        );
    }

    #[test]
    fn paths_bootstrap_db_defaults_under_resolved_workspace() {
        // Only `GINO_WORKSPACE_DIR` is set → the DB follows the relocated workspace.
        let mut config = Config::default();
        config.apply(ConfigOverlay::paths_bootstrap(
            Some(std::path::PathBuf::from("/data/ws")),
            None,
        ));
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/data/ws")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/data/ws/agent.db")
        );
    }

    #[test]
    fn paths_bootstrap_db_override_keeps_default_workspace() {
        // Only `GINO_DB_PATH` is set → workspace stays the default, DB is honored.
        let mut config = Config::default();
        config.apply(ConfigOverlay::paths_bootstrap(
            None,
            Some(std::path::PathBuf::from("/tmp/hermetic.db")),
        ));
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/var/lib/gino-agent")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/tmp/hermetic.db")
        );
    }

    #[test]
    fn paths_bootstrap_empty_keeps_documented_defaults() {
        // Neither env var set → the built-in defaults stand untouched.
        let mut config = Config::default();
        config.apply(ConfigOverlay::paths_bootstrap(None, None));
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/var/lib/gino-agent")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/var/lib/gino-agent/agent.db")
        );
    }

    #[test]
    fn validate_requires_a_host() {
        let config = Config::default();
        assert!(matches!(config.validate(), Err(ConfigError::MissingHost)));
    }

    #[test]
    fn validate_rejects_a_bad_scheme() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("gino.example.com".to_owned()),
            None,
            None,
        ));
        assert!(matches!(
            config.validate(),
            Err(ConfigError::InvalidHostScheme(_))
        ));
    }

    #[test]
    fn validate_accepts_https() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("https://gino.example.com/".to_owned()),
            None,
            None,
        ));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn derives_ws_and_register_urls() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("https://gino.example.com/".to_owned()),
            None,
            None,
        ));
        assert_eq!(
            config.register_url().unwrap(),
            "https://gino.example.com/agent/register"
        );
        assert_eq!(
            config.ws_url("agent-123").unwrap(),
            "wss://gino.example.com/agent/ws/agent-123"
        );

        let mut insecure = Config::default();
        insecure.apply(ConfigOverlay::bootstrap(
            Some("http://localhost:9000".to_owned()),
            None,
            None,
        ));
        assert_eq!(
            insecure.ws_url("a").unwrap(),
            "ws://localhost:9000/agent/ws/a"
        );
    }

    #[test]
    fn debug_redacts_every_secret() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some("https://gino.example.com".to_owned()),
            Some("REGISTER-PLAINTEXT".to_owned()),
            None,
        ));
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "connectToken": "CONNECT-PLAINTEXT",
                    "engine": {
                        "anthropicApiKey": "APIKEY-PLAINTEXT",
                        "claudeCodeOauthToken": "OAUTH-PLAINTEXT"
                    }
                }"#,
            )
            .unwrap(),
        );

        let rendered = format!("{config:?}");
        for plaintext in [
            "REGISTER-PLAINTEXT",
            "CONNECT-PLAINTEXT",
            "APIKEY-PLAINTEXT",
            "OAUTH-PLAINTEXT",
        ] {
            assert!(
                !rendered.contains(plaintext),
                "Debug output leaked a secret: {plaintext}"
            );
        }
        assert!(
            rendered.contains("REDACTED"),
            "expected secrecy redaction marker in: {rendered}"
        );
    }
}
