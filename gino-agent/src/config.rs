//! Global configuration layer.
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
//! untouched. This module performs **no** network IO — it exposes only the
//! merge surface; the register/HTTP/WS wiring produces the overlays.
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

use crate::protocol::frames::ProvisioningEntry;
use crate::sandbox::IsolationSpec;

/// Default workspace directory (configurable; never a hardcoded Docker mount).
const DEFAULT_WORKSPACE_DIR: &str = "/var/lib/gino-agent";
/// Default SQLite filename within the workspace directory.
const DEFAULT_DB_FILE: &str = "agent.db";
/// Sub-directory of the workspace dir holding the GLOBAL engine-config scope root:
/// the runner materializes the global instruction file + skills
/// here and launches the engine with `ENGINE_CONFIG_DIR`/`CLAUDE_CONFIG_DIR`
/// pointed at it. Overridden by `CLAUDE_CONFIG_USE_DEFAULT` (see
/// [`ConfigOverlay::paths_from_env`]).
pub(crate) const ENGINE_CONFIG_SUBDIR: &str = "engine-config";
/// Claude's own default config dir under `$HOME`.
const CLAUDE_DEFAULT_CONFIG_SUBDIR: &str = ".claude";
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
/// periodic GC sweep deletes it.
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
    /// `CLAUDE_CONFIG_USE_DEFAULT` was set but claude's default config dir could
    /// not be derived.
    #[error(
        "CLAUDE_CONFIG_USE_DEFAULT is set but neither CLAUDE_CONFIG_DIR nor HOME is set, so claude's default config dir cannot be resolved"
    )]
    MissingHome,
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
    /// Out-of-band attachment channel limits + backend selection.
    pub(crate) attachments: AttachmentsConfig,
    /// Queue/ledger GC retention window.
    pub(crate) retention: RetentionConfig,
    /// Global-scope provisioning entries — the desired set of instruction files +
    /// skills materialized into `$ENGINE_CONFIG_DIR/`. The host
    /// re-pushes the full desired global set each refresh; an overlay carrying
    /// `entries` **replaces** this vec. Default = empty.
    pub(crate) entries: Vec<ProvisioningEntry>,
    /// Host-assigned GLOBAL config version (version gate). Monotonic
    /// and server-owned; it scopes the agent's global config set (`entries`) and
    /// drives the startup/`config_update` reconcile decision: the runner reconciles
    /// the global scope **only when this advances** past the persisted applied
    /// version, else repairs-only (store-durable across restarts). `None` until a
    /// versioned payload (register / GET `/agent/config` / `config_update`) lands.
    // `config_version` (not bare `version`) is deliberate: it is the GLOBAL *config*
    // version, distinct from the wire `protocol_version` and an entry's `sha256`.
    #[allow(
        clippy::struct_field_names,
        reason = "the `config_` qualifier disambiguates it from the protocol version"
    )]
    pub(crate) config_version: Option<i64>,
    /// Transport liveness ping cadence, in milliseconds (`heartbeatIntervalMs`).
    pub(crate) heartbeat_interval_ms: u64,
    /// Host-enabled engine-free operation groups (`remoteOps`); all off by default.
    pub(crate) remote_ops: RemoteOpsConfig,
    /// Filesystem layout (workspace + DB path).
    pub(crate) paths: Paths,
    // No `log_level` here: the tracing filter is fixed at process start from
    // `RUST_LOG`/`-v` (see `init_tracing`), before any host config layer exists;
    // a host-sent `logLevel` was never consulted and is now just an ignored
    // unknown field.
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
            remote_ops: RemoteOpsConfig::default(),
            paths: Paths::default(),
        }
    }
}

/// Which engine-free operation groups the host has enabled for this agent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RemoteOpsConfig {
    /// File operations (`fs_request`).
    pub(crate) fs: bool,
    /// Interactive terminal sessions.
    pub(crate) pty: bool,
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
    /// Settings for the `codex` backend, namespaced under `engine.codex`.
    pub(crate) codex: CodexConfig,
    /// Default engine isolation (`engine.isolation`); empty means off.
    // Not read outside tests until isolation is wired into engine spawning.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) isolation: IsolationSpec,
}

/// Settings for the `codex` backend (`engine.codex`).
///
/// Every field defaults to `None`/empty, which means "the backend's own defaults
/// stand" — `CodexSettings::default()` keeps its non-interactive argv and its
/// `codex`-on-`PATH` binary. That is what makes this block purely additive: a
/// deployment that never sends one behaves exactly as it did before the block
/// existed.
#[derive(Clone, Debug, Default)]
pub(crate) struct CodexConfig {
    /// The `codex` executable to spawn; `None` keeps the backend's `codex` default.
    pub(crate) binary: Option<PathBuf>,
    /// Model id passed via `--model`.
    pub(crate) model: Option<String>,
    /// Approval policy passed via `--ask-for-approval`.
    pub(crate) approval_policy: Option<String>,
    /// Sandbox mode passed via `--sandbox`.
    pub(crate) sandbox_mode: Option<String>,
    /// Whether to pass `--skip-git-repo-check`; `None` keeps the backend default.
    pub(crate) skip_git_repo_check: Option<bool>,
    /// Extra arguments appended verbatim to the `codex` argv.
    pub(crate) extra_args: Vec<String>,
    /// `OPENAI_API_KEY`.
    pub(crate) openai_api_key: Option<SecretString>,
    /// `OPENAI_BASE_URL` (e.g. a host credential proxy).
    pub(crate) openai_base_url: Option<String>,
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
/// /agent/attachments`, connect-token auth). The spec's presigned-URL/CDN
/// backend was never implemented — its selectable-but-unusable variant is
/// deleted; re-add it here together with a real backend impl if object storage
/// lands. An unrecognized `backend` string (including `"presigned"`) leaves the
/// host-proxy selection untouched.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum AttachmentBackendKind {
    /// Host streams the bytes through `GET`/`POST /agent/attachments`.
    #[default]
    HostProxy,
}

/// Out-of-band attachment channel limits + backend selection (agent-architecture
/// §9, decisions A2/A4/A5). Only descriptors ride the wire; bytes move
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

/// Queue/ledger GC retention window. The periodic sweep deletes
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
    /// The engine config dir (`CLAUDE_CONFIG_DIR` for the claude child and the
    /// GLOBAL provisioning scope root). `{workspace}/engine-config` by default;
    /// claude's own default dir when `CLAUDE_CONFIG_USE_DEFAULT` is set.
    pub(crate) engine_config_dir: PathBuf,
}

impl Default for Paths {
    fn default() -> Self {
        let workspace_dir = PathBuf::from(DEFAULT_WORKSPACE_DIR);
        let db_path = workspace_dir.join(DEFAULT_DB_FILE);
        let engine_config_dir = workspace_dir.join(ENGINE_CONFIG_SUBDIR);
        Self {
            workspace_dir,
            db_path,
            engine_config_dir,
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
            remote_ops,
            paths,
        } = overlay;

        overlay_field(&mut self.host_url, host_url);
        overlay_field(&mut self.agent_name, agent_name);
        overlay_field(&mut self.agent_id, agent_id);
        overlay_secret(&mut self.register_token, register_token);
        overlay_secret(&mut self.connect_token, connect_token);
        if let Some(ms) = heartbeat_interval_ms {
            self.heartbeat_interval_ms = ms;
        }
        if let Some(enabled) = remote_ops.pty {
            self.remote_ops.pty = enabled;
        }
        if let Some(enabled) = remote_ops.fs {
            self.remote_ops.fs = enabled;
        }

        let EngineOverlay {
            anthropic_api_key,
            claude_code_oauth_token,
            anthropic_base_url,
            model,
            allowed_tools,
            codex,
            isolation,
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

        apply_codex_overlay(&mut self.engine.codex, codex);
        if let Some(isolation) = isolation {
            self.engine.isolation = isolation;
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

        // The GLOBAL config version is latest-Some-wins: a versioned
        // layer pins it, an unversioned layer leaves the prior version untouched.
        overlay_field(&mut self.config_version, version);

        let PathsOverlay {
            workspace_dir,
            db_path,
            engine_config_dir,
        } = paths;
        if let Some(dir) = workspace_dir {
            self.paths.workspace_dir = dir;
        }
        if let Some(path) = db_path {
            self.paths.db_path = path;
        }
        if let Some(dir) = engine_config_dir {
            self.paths.engine_config_dir = dir;
        }
    }

    /// Merge the attachment-channel overlay (limits + backend). Split out
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
        if let Some(kind) = backend
            && matches!(kind.as_str(), "host_proxy" | "hostProxy")
        {
            self.attachments.backend = AttachmentBackendKind::HostProxy;
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

/// Orchestrator URL derivation, consumed by the register/runner wiring.
impl Config {
    /// The orchestrator HTTP base URL with any trailing slash trimmed.
    pub(crate) fn http_base(&self) -> Result<&str, ConfigError> {
        let host = self.host_url.as_deref().ok_or(ConfigError::MissingHost)?;
        Ok(host.trim_end_matches('/'))
    }

    /// `POST {host}/agent/register` — the enrollment endpoint.
    pub(crate) fn register_url(&self) -> Result<String, ConfigError> {
        Ok(format!("{}/agent/register", self.http_base()?))
    }

    /// `GET {host}/agent/config/{agent_id}` — the authoritative config fetch.
    /// On a fresh enrollment this re-supplies what the
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

/// Resolve the `CLAUDE_CONFIG_USE_DEFAULT` opt-in: `None` when the flag is unset
/// or falsy (the runner keeps its private `{workspace}/engine-config`); otherwise
/// the dir claude itself would use — an explicit `CLAUDE_CONFIG_DIR` from the
/// daemon's environment, else `$HOME/.claude`. Pure so it is unit-testable.
pub(crate) fn claude_default_config_dir(
    use_default: Option<&str>,
    claude_config_dir: Option<PathBuf>,
    home: Option<PathBuf>,
) -> Result<Option<PathBuf>, ConfigError> {
    let enabled = use_default.is_some_and(|raw| {
        matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    });
    if !enabled {
        return Ok(None);
    }
    claude_config_dir
        .or_else(|| home.map(|home| home.join(CLAUDE_DEFAULT_CONFIG_SUBDIR)))
        .map(Some)
        .ok_or(ConfigError::MissingHome)
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
    /// Full desired global provisioning set. Unlike `register_token`
    /// this is host-sent, so it is **not** `#[serde(skip)]`; when present it
    /// replaces `Config::entries` wholesale.
    entries: Option<Vec<ProvisioningEntry>>,
    /// Host-assigned GLOBAL config version. Monotonic; carried on
    /// register-config / GET `/agent/config` / `config_update` alongside `entries`,
    /// and the sole signal the reconcile version gate keys on.
    version: Option<i64>,
    heartbeat_interval_ms: Option<u64>,
    remote_ops: RemoteOpsOverlay,
    paths: PathsOverlay,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct RemoteOpsOverlay {
    fs: Option<bool>,
    pty: Option<bool>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct EngineOverlay {
    anthropic_api_key: Option<String>,
    claude_code_oauth_token: Option<String>,
    anthropic_base_url: Option<String>,
    model: Option<String>,
    allowed_tools: Option<Vec<String>>,
    codex: CodexOverlay,
    isolation: Option<IsolationSpec>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct CodexOverlay {
    binary: Option<PathBuf>,
    model: Option<String>,
    approval_policy: Option<String>,
    sandbox_mode: Option<String>,
    skip_git_repo_check: Option<bool>,
    extra_args: Option<Vec<String>>,
    openai_api_key: Option<String>,
    openai_base_url: Option<String>,
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
    /// Settable only by the env/CLI bootstrap layer ([`ConfigOverlay::paths_bootstrap`]).
    /// `#[serde(skip)]`: the DB is opened before any host layer applies, so a
    /// host-sent `paths.dbPath` could never take effect — it is ignored instead
    /// of silently accepted-and-dead.
    #[serde(skip)]
    db_path: Option<PathBuf>,
    /// Env/CLI bootstrap only, like `db_path`: the engine dir is created and the
    /// global scope reconciled at startup, before any host layer applies.
    #[serde(skip)]
    engine_config_dir: Option<PathBuf>,
}

impl ConfigOverlay {
    /// Build the env/CLI bootstrap overlay from the inputs. The
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

    /// Build the local engine-auth overlay from the token stored by
    /// `gino-agent login` (`{workspace}/claude-oauth-token`). Applied right after
    /// the env/CLI bootstrap and *before* any host layer, so a host-supplied
    /// `claudeCodeOauthToken` still overrides it per the normal precedence.
    pub(crate) fn local_engine_oauth_token(token: String) -> Self {
        Self {
            engine: EngineOverlay {
                claude_code_oauth_token: Some(token),
                ..EngineOverlay::default()
            },
            ..Self::default()
        }
    }

    /// Build the env/CLI **paths** bootstrap overlay from `GINO_WORKSPACE_DIR` /
    /// `GINO_DB_PATH`.
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
    ///
    /// `engine_config_dir` follows `workspace_dir` the same way (`{workspace}/
    /// engine-config`) unless given explicitly.
    pub(crate) fn paths_bootstrap(
        workspace_dir: Option<PathBuf>,
        db_path: Option<PathBuf>,
        engine_config_dir: Option<PathBuf>,
    ) -> Self {
        let db_path =
            db_path.or_else(|| workspace_dir.as_ref().map(|dir| dir.join(DEFAULT_DB_FILE)));
        let engine_config_dir = engine_config_dir.or_else(|| {
            workspace_dir
                .as_ref()
                .map(|dir| dir.join(ENGINE_CONFIG_SUBDIR))
        });
        Self {
            paths: PathsOverlay {
                workspace_dir,
                db_path,
                engine_config_dir,
            },
            ..Self::default()
        }
    }

    /// [`Self::paths_bootstrap`] fed from the process environment:
    /// `GINO_WORKSPACE_DIR`, `GINO_DB_PATH`, and — when `CLAUDE_CONFIG_USE_DEFAULT`
    /// is truthy — claude's own default config dir as the engine config dir (see
    /// [`claude_default_config_dir`]), so an already-logged-in local claude
    /// (credentials, settings, plugins) is reused as-is. Shared by the daemon and
    /// the `login` subcommand so both resolve the same directories.
    pub(crate) fn paths_from_env() -> Result<Self, ConfigError> {
        let engine_config_dir = claude_default_config_dir(
            std::env::var("CLAUDE_CONFIG_USE_DEFAULT").ok().as_deref(),
            std::env::var_os("CLAUDE_CONFIG_DIR").map(PathBuf::from),
            std::env::var_os("HOME").map(PathBuf::from),
        )?;
        Ok(Self::paths_bootstrap(
            std::env::var_os("GINO_WORKSPACE_DIR").map(PathBuf::from),
            std::env::var_os("GINO_DB_PATH").map(PathBuf::from),
            engine_config_dir,
        ))
    }

    /// Parse a host-assigned config payload into an overlay. Production callers
    /// deserialize from an already-parsed `Value` (`serde_json::from_value`);
    /// this string-input wrapper is test-only convenience.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "test-only convenience over serde_json::from_str")
    )]
    pub(crate) fn from_json(raw: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(raw)
    }

    /// The global provisioning entries this overlay carries, if any.
    /// Lets the runner re-reconcile the global scope on a live `config_update`
    /// without applying (and discarding) the whole overlay: `Some` is the full
    /// re-pushed desired set; `None` means the update did not touch provisioning.
    pub(crate) fn entries(&self) -> Option<&[ProvisioningEntry]> {
        self.entries.as_deref()
    }

    /// The host-assigned GLOBAL config version this overlay carries, if any.
    /// Mirrors [`Self::entries`]: lets the `config_update` route read
    /// the version to apply the reconcile version gate without consuming the overlay.
    pub(crate) fn version(&self) -> Option<i64> {
        self.version
    }
}

/// Apply the `engine.codex` overlay under the same omission-preserves rule as the
/// rest of [`ConfigOverlay::apply`]: an absent field leaves the current value, and
/// `extra_args` replaces wholesale when present (like `allowed_tools`).
fn apply_codex_overlay(config: &mut CodexConfig, overlay: CodexOverlay) {
    let CodexOverlay {
        binary,
        model,
        approval_policy,
        sandbox_mode,
        skip_git_repo_check,
        extra_args,
        openai_api_key,
        openai_base_url,
    } = overlay;
    overlay_field(&mut config.binary, binary);
    overlay_field(&mut config.model, model);
    overlay_field(&mut config.approval_policy, approval_policy);
    overlay_field(&mut config.sandbox_mode, sandbox_mode);
    overlay_field(&mut config.skip_git_repo_check, skip_git_repo_check);
    overlay_secret(&mut config.openai_api_key, openai_api_key);
    overlay_field(&mut config.openai_base_url, openai_base_url);
    if let Some(args) = extra_args {
        config.extra_args = args;
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
    use std::path::PathBuf;

    use secrecy::ExposeSecret;

    use super::{Config, ConfigError, ConfigOverlay};
    use crate::sandbox::IsolationSpec;

    fn secret(config_secret: Option<&secrecy::SecretString>) -> Option<&str> {
        config_secret.map(ExposeSecret::expose_secret)
    }

    #[test]
    fn defaults_have_expected_values() {
        let config = Config::default();
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

        // Layer 4 — live WS update: overrides only the heartbeat cadence.
        config.apply(ConfigOverlay::from_json(r#"{ "heartbeatIntervalMs": 5000 }"#).unwrap());

        // Latest writer wins for each field…
        assert_eq!(config.host_url.as_deref(), Some("https://new.example.com"));
        assert_eq!(config.engine.model.as_deref(), Some("claude-sonnet-4-6"));
        assert_eq!(config.heartbeat_interval_ms, 5000);
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
    fn from_json_parses_the_codex_engine_block() {
        let mut config = Config::default();
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "engine": {
                        "codex": {
                            "binary": "/opt/bin/codex",
                            "model": "gpt-5-codex",
                            "approvalPolicy": "on-request",
                            "sandboxMode": "workspace-write",
                            "skipGitRepoCheck": false,
                            "extraArgs": ["--profile", "ci"],
                            "openaiApiKey": "sk-openai",
                            "openaiBaseUrl": "https://proxy.example.com/v1"
                        }
                    }
                }"#,
            )
            .unwrap(),
        );

        let codex = &config.engine.codex;
        assert_eq!(
            codex.binary.as_deref(),
            Some(std::path::Path::new("/opt/bin/codex"))
        );
        assert_eq!(codex.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(codex.approval_policy.as_deref(), Some("on-request"));
        assert_eq!(codex.sandbox_mode.as_deref(), Some("workspace-write"));
        assert_eq!(codex.skip_git_repo_check, Some(false));
        assert_eq!(codex.extra_args, ["--profile", "ci"]);
        // The key is wrapped at the config boundary, so it is redacted in Debug.
        assert_eq!(secret(codex.openai_api_key.as_ref()), Some("sk-openai"));
        assert!(!format!("{codex:?}").contains("sk-openai"));
        assert_eq!(
            codex.openai_base_url.as_deref(),
            Some("https://proxy.example.com/v1")
        );
    }

    #[test]
    fn an_overlay_without_a_codex_block_leaves_the_earlier_one_untouched() {
        let mut config = Config::default();
        config.apply(
            ConfigOverlay::from_json(
                r#"{ "engine": { "codex": { "model": "gpt-5-codex", "extraArgs": ["--profile", "ci"] } } }"#,
            )
            .unwrap(),
        );
        // A later update that speaks only about claude must not clear codex.
        config.apply(ConfigOverlay::from_json(r#"{ "engine": { "model": "m" } }"#).unwrap());

        assert_eq!(config.engine.codex.model.as_deref(), Some("gpt-5-codex"));
        assert_eq!(config.engine.codex.extra_args, ["--profile", "ci"]);
        assert_eq!(config.engine.model.as_deref(), Some("m"));
    }

    #[test]
    fn isolation_defaults_to_an_empty_spec() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::from_json(r#"{ "engine": { "model": "m" } }"#).unwrap());
        assert_eq!(config.engine.isolation, IsolationSpec::default());
        assert_eq!(config.engine.model.as_deref(), Some("m"));
    }

    #[test]
    fn from_json_parses_the_isolation_block() {
        let mut config = Config::default();
        config.apply(
            ConfigOverlay::from_json(
                r#"{
                    "engine": {
                        "model": "m",
                        "isolation": {
                            "mode": "sandbox",
                            "network": { "mode": "full" },
                            "readPaths": ["/data/ro"],
                            "writePaths": ["/data/rw", "/data/cache"]
                        }
                    }
                }"#,
            )
            .unwrap(),
        );

        let isolation = &config.engine.isolation;
        assert_eq!(isolation.mode.as_deref(), Some("sandbox"));
        assert_eq!(
            isolation.network.as_ref().and_then(|n| n.mode.as_deref()),
            Some("full")
        );
        assert_eq!(
            isolation.read_paths.as_deref(),
            Some(&[PathBuf::from("/data/ro")][..])
        );
        assert_eq!(
            isolation.write_paths.as_deref(),
            Some(&[PathBuf::from("/data/rw"), PathBuf::from("/data/cache")][..])
        );
        assert_eq!(config.engine.model.as_deref(), Some("m"));
    }

    #[test]
    fn an_isolation_block_replaces_the_earlier_one_wholesale() {
        let mut config = Config::default();
        config.apply(
            ConfigOverlay::from_json(
                r#"{ "engine": { "isolation": { "mode": "sandbox", "readPaths": ["/data/ro"] } } }"#,
            )
            .unwrap(),
        );
        // An overlay without the block keeps it.
        config.apply(ConfigOverlay::from_json(r#"{ "engine": { "model": "m" } }"#).unwrap());
        assert_eq!(config.engine.isolation.mode.as_deref(), Some("sandbox"));
        assert!(config.engine.isolation.read_paths.is_some());

        // A new block replaces it, including fields it omits.
        config.apply(
            ConfigOverlay::from_json(
                r#"{ "engine": { "isolation": { "network": { "mode": "none" } } } }"#,
            )
            .unwrap(),
        );
        assert!(config.engine.isolation.mode.is_none());
        assert!(config.engine.isolation.read_paths.is_none());
        assert_eq!(
            config
                .engine
                .isolation
                .network
                .as_ref()
                .and_then(|n| n.mode.as_deref()),
            Some("none")
        );
    }

    #[test]
    fn codex_defaults_defer_to_the_backend() {
        let codex = Config::default().engine.codex;
        assert!(codex.binary.is_none());
        assert!(codex.model.is_none());
        assert!(codex.approval_policy.is_none());
        assert!(codex.sandbox_mode.is_none());
        assert!(codex.skip_git_repo_check.is_none());
        assert!(codex.extra_args.is_empty());
        assert!(codex.openai_api_key.is_none());
        assert!(codex.openai_base_url.is_none());
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
            r#"{ "wsUrl": "/agent/ws/a", "configUrl": "/agent/config/a", "outputUrl": "/agent/output/a", "protocolVersion": 1, "bogusField": 1, "scheduler": { "maxConcurrentTasks": 8, "maxQueuedTasks": 16 }, "engine": { "model": "m" } }"#,
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
        // "presigned" was deleted with its unimplemented backend: it now parses
        // as an unrecognized selection and host-proxy stays in effect, while the
        // limits from the same overlay still apply.
        assert_eq!(
            config.attachments.backend,
            super::AttachmentBackendKind::HostProxy
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
        use crate::protocol::frames::EntryKind;

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
            None,
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
        assert_eq!(
            config.paths.engine_config_dir,
            std::path::PathBuf::from("/data/ws/engine-config")
        );
    }

    #[test]
    fn paths_bootstrap_engine_config_dir_override_wins() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::paths_bootstrap(
            Some(std::path::PathBuf::from("/data/ws")),
            None,
            Some(std::path::PathBuf::from("/home/op/.claude")),
        ));
        assert_eq!(
            config.paths.engine_config_dir,
            std::path::PathBuf::from("/home/op/.claude")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/data/ws/agent.db")
        );
    }

    #[test]
    fn claude_config_use_default_unset_or_falsy_keeps_private_dir() {
        let home = Some(std::path::PathBuf::from("/home/op"));
        for flag in [
            None,
            Some(""),
            Some("0"),
            Some("false"),
            Some("no"),
            Some("off"),
        ] {
            assert_eq!(
                super::claude_default_config_dir(flag, None, home.clone()).unwrap(),
                None,
                "flag {flag:?}"
            );
        }
    }

    #[test]
    fn claude_config_use_default_resolves_home_dot_claude() {
        for flag in ["1", "true", "TRUE", " yes ", "on"] {
            assert_eq!(
                super::claude_default_config_dir(
                    Some(flag),
                    None,
                    Some(std::path::PathBuf::from("/home/op"))
                )
                .unwrap(),
                Some(std::path::PathBuf::from("/home/op/.claude")),
                "flag {flag:?}"
            );
        }
    }

    #[test]
    fn claude_config_use_default_prefers_explicit_claude_config_dir() {
        assert_eq!(
            super::claude_default_config_dir(
                Some("true"),
                Some(std::path::PathBuf::from("/opt/claude-cfg")),
                Some(std::path::PathBuf::from("/home/op"))
            )
            .unwrap(),
            Some(std::path::PathBuf::from("/opt/claude-cfg"))
        );
    }

    #[test]
    fn claude_config_use_default_without_home_is_an_error() {
        assert!(matches!(
            super::claude_default_config_dir(Some("true"), None, None),
            Err(ConfigError::MissingHome)
        ));
    }

    #[test]
    fn paths_bootstrap_db_override_keeps_default_workspace() {
        // Only `GINO_DB_PATH` is set → workspace stays the default, DB is honored.
        let mut config = Config::default();
        config.apply(ConfigOverlay::paths_bootstrap(
            None,
            Some(std::path::PathBuf::from("/tmp/hermetic.db")),
            None,
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
        config.apply(ConfigOverlay::paths_bootstrap(None, None, None));
        assert_eq!(
            config.paths.workspace_dir,
            std::path::PathBuf::from("/var/lib/gino-agent")
        );
        assert_eq!(
            config.paths.db_path,
            std::path::PathBuf::from("/var/lib/gino-agent/agent.db")
        );
        assert_eq!(
            config.paths.engine_config_dir,
            std::path::PathBuf::from("/var/lib/gino-agent/engine-config")
        );
    }

    #[test]
    fn local_engine_oauth_token_is_overridden_by_a_host_layer() {
        let mut config = Config::default();
        config.apply(ConfigOverlay::local_engine_oauth_token(
            "local-tok".to_owned(),
        ));
        assert_eq!(
            secret(config.engine.claude_code_oauth_token.as_ref()),
            Some("local-tok")
        );
        // A host layer without a token leaves the local one in place.
        config.apply(ConfigOverlay::from_json(r#"{"engine":{"model":"m"}}"#).unwrap());
        assert_eq!(
            secret(config.engine.claude_code_oauth_token.as_ref()),
            Some("local-tok")
        );
        // A host-supplied token wins.
        config.apply(
            ConfigOverlay::from_json(r#"{"engine":{"claudeCodeOauthToken":"host-tok"}}"#).unwrap(),
        );
        assert_eq!(
            secret(config.engine.claude_code_oauth_token.as_ref()),
            Some("host-tok")
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

    #[test]
    fn remote_ops_are_off_until_a_host_layer_enables_them() {
        let mut config = Config::default();
        assert!(!config.remote_ops.fs);
        assert!(!config.remote_ops.pty);
        config.apply(ConfigOverlay::from_json(r#"{ "remoteOps": { "pty": true } }"#).unwrap());
        assert!(config.remote_ops.pty);
        assert!(!config.remote_ops.fs);

        config.apply(ConfigOverlay::from_json(r#"{ "remoteOps": { "fs": true } }"#).unwrap());
        assert!(config.remote_ops.fs);

        // A layer that does not mention the group leaves it as it was.
        config.apply(ConfigOverlay::from_json(r#"{ "heartbeatIntervalMs": 5000 }"#).unwrap());
        assert!(config.remote_ops.fs);

        config.apply(ConfigOverlay::from_json(r#"{ "remoteOps": { "fs": false } }"#).unwrap());
        assert!(!config.remote_ops.fs);
        assert!(config.remote_ops.pty);
        config.apply(ConfigOverlay::from_json(r#"{ "remoteOps": { "pty": false } }"#).unwrap());
        assert!(!config.remote_ops.pty);
    }
}
