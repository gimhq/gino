//! Agent enrollment flow (PLAN-003 §2.1).
//!
//! Two tokens, two jobs. The **register token** is a bootstrap credential
//! (env/CLI) used *only* to enroll. Enrollment exchanges it — over
//! `POST /agent/register` — for a host-generated `agentId`, a per-agent
//! **connect token**, and the runner's assigned config. The connect token is
//! persisted locally and is what authenticates the WebSocket; the register token
//! never touches the WS.
//!
//! [`load_or_register`] is the single entry point. If an identity is already
//! persisted (a restart / reconnect), it is reused verbatim — no second
//! registration — which is exactly how the runner survives host restarts without
//! the `.reconnect-token` file hack the TS runner needed. Otherwise it registers,
//! persists the identity *before* returning, and folds the host-assigned config
//! overlay onto the running [`Config`].

use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use thiserror::Error;
use tracing::warn;

use crate::config::{Config, ConfigError, ConfigOverlay};
use crate::db::{Db, DbError};
use crate::protocol::{PROTOCOL_VERSION, Redacted, RegisterRequest, RegisterResponse};
use crate::transport::http::{HttpClient, HttpError};

/// Engine capabilities advertised at registration. The only engine in the plan
/// (PLAN-001) is the Claude CLI engine; later engine L3s extend this list.
const CAPABILITIES: &[&str] = &["claude"];

/// The `register_error` reason the host returns when it cannot speak the
/// agent's `protocolVersion` (docs/protocol.md §1). Triggers a clean exit.
const UNSUPPORTED_PROTOCOL: &str = "unsupported_protocol";

/// Errors raised by the enrollment flow.
#[derive(Debug, Error)]
pub(crate) enum RegisterError {
    /// No register token was supplied, and no identity is persisted to reuse.
    #[error(
        "a register token is required to enroll (set GINO_REGISTER_TOKEN, GINO_REGISTER_TOKEN_FILE, or the deprecated --register-token flag)"
    )]
    MissingRegisterToken,
    /// The host returned a `register_error {reason}` envelope.
    #[error("host rejected registration: {0}")]
    Rejected(String),
    /// Protocol-version negotiation failed: the host either rejected enrollment
    /// with `unsupported_protocol`, or its register response declared a
    /// `protocolVersion` other than ours. The agent exits without connecting.
    #[error("protocol negotiation failed: {0}")]
    UnsupportedProtocol(String),
    /// The host's config overlay could not be parsed (e.g. an unknown key).
    #[error("host register response carried an invalid config payload: {0}")]
    InvalidConfig(serde_json::Error),
    /// An HTTP-layer failure (transport, status, decode).
    #[error(transparent)]
    Http(#[from] HttpError),
    /// Persisting or reading the local identity failed.
    #[error(transparent)]
    Db(#[from] DbError),
    /// The resolved config could not produce the register URL.
    #[error(transparent)]
    Config(#[from] ConfigError),
}

/// The per-agent identity used to open the WebSocket connection.
#[derive(Clone, Debug)]
pub(crate) struct ResolvedIdentity {
    pub(crate) agent_id: String,
    pub(crate) connect_token: SecretString,
}

/// Reuse a persisted identity (reconnect) or perform a fresh registration.
///
/// On the reconnect path the persisted `agentId` + connect token are mirrored
/// onto `config` and returned — there is no second `POST /agent/register`, and
/// the register token is never read. On the enrollment path the runner registers,
/// persists the identity, applies the host config overlay, and pins the new
/// identity onto `config`.
pub(crate) async fn load_or_register(
    config: &mut Config,
    db: &Db,
    http: &HttpClient,
) -> Result<ResolvedIdentity, RegisterError> {
    // Loud startup warning, not a hard reject: loopback/dev deployments may
    // legitimately run over plain HTTP, but on any real network both the
    // register token and the connect token would travel in cleartext.
    if let Some(message) = insecure_host_warning(config.host_url.as_deref()) {
        warn!("{message}");
    }
    if let Some(identity) = db.get_identity()? {
        config.agent_id = Some(identity.agent_id.clone());
        config.connect_token = Some(identity.connect_token.clone());
        return Ok(ResolvedIdentity {
            agent_id: identity.agent_id,
            connect_token: identity.connect_token,
        });
    }
    register(config, db, http).await
}

/// Perform a fresh `POST /agent/register` and persist the result.
async fn register(
    config: &mut Config,
    db: &Db,
    http: &HttpClient,
) -> Result<ResolvedIdentity, RegisterError> {
    let register_token = config
        .register_token
        .as_ref()
        .ok_or(RegisterError::MissingRegisterToken)?;
    let request = RegisterRequest {
        register_token: Redacted::new(register_token.expose_secret().to_owned()),
        protocol_version: PROTOCOL_VERSION,
        name: config.agent_name.clone(),
        capabilities: CAPABILITIES.iter().map(|cap| (*cap).to_owned()).collect(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        platform: platform_string(),
    };

    let url = config.register_url()?;
    let mut ok = match http.register(&url, &request).await? {
        RegisterResponse::Ok(ok) => ok,
        // The host gate (docs/protocol.md §1): an `unsupported_protocol` rejection
        // means our `protocolVersion` is not one it speaks. Surface it distinctly
        // so the agent exits cleanly instead of retrying a doomed enrollment.
        RegisterResponse::Error(envelope) => {
            let reason = envelope.register_error.reason;
            if reason == UNSUPPORTED_PROTOCOL {
                warn!(
                    agent_protocol_version = PROTOCOL_VERSION,
                    "host rejected enrollment: unsupported_protocol — exiting"
                );
                return Err(RegisterError::UnsupportedProtocol(format!(
                    "the host rejected protocol v{PROTOCOL_VERSION} with `{UNSUPPORTED_PROTOCOL}`"
                )));
            }
            return Err(RegisterError::Rejected(reason));
        }
    };

    // Belt-and-suspenders to the host gate: the success response echoes the
    // version the host will speak (docs/protocol.md §1/§3). Pull it off the config
    // overlay — which is parsed strictly — and refuse a version we cannot speak.
    // A declared-but-non-integer version fails CLOSED: it is a host we cannot
    // reason about, so the agent refuses to connect rather than assuming a match.
    let host_version = take_host_protocol_version(&mut ok.config).map_err(|raw| {
        warn!(
            agent_protocol_version = PROTOCOL_VERSION,
            host_protocol_version = %raw,
            "host declared a non-integer protocolVersion — exiting"
        );
        RegisterError::UnsupportedProtocol(format!(
            "the host declared a non-integer protocolVersion ({raw})"
        ))
    })?;
    if let Some(host_version) = host_version
        && host_version != u64::from(PROTOCOL_VERSION)
    {
        warn!(
            agent_protocol_version = PROTOCOL_VERSION,
            host_protocol_version = host_version,
            "host will speak an unsupported protocol version — exiting"
        );
        return Err(RegisterError::UnsupportedProtocol(format!(
            "the host will speak v{host_version}, not v{PROTOCOL_VERSION}"
        )));
    }

    let connect_token = SecretString::from(ok.connect_token.expose().to_owned());
    // Persist identity first: a crash after this point takes the reconnect path.
    db.set_identity(&ok.agent_id, &connect_token)?;
    // Fold the host-assigned config overlay on, then pin the identity.
    apply_config_overlay(config, &ok.config)?;
    config.agent_id = Some(ok.agent_id.clone());
    config.connect_token = Some(connect_token.clone());

    Ok(ResolvedIdentity {
        agent_id: ok.agent_id,
        connect_token,
    })
}

/// Merge the host config `Value` (register response / refresh) onto `config`. A
/// `null`/absent payload is a no-op; any other shape is parsed strictly (unknown
/// keys are rejected by [`ConfigOverlay`]).
fn apply_config_overlay(config: &mut Config, value: &Value) -> Result<(), RegisterError> {
    if value.is_null() {
        return Ok(());
    }
    let overlay: ConfigOverlay =
        serde_json::from_value(value.clone()).map_err(RegisterError::InvalidConfig)?;
    config.apply(overlay);
    Ok(())
}

/// Remove and return the host's declared `protocolVersion` from a register
/// response `config` overlay. Stripping it keeps the strict (`deny_unknown_fields`)
/// [`ConfigOverlay`] parse happy — the version is consumed by the gate, not config.
/// Absent ⇒ `Ok(None)` (the host gate via `register_error` still applies).
/// Declared but non-integer ⇒ `Err` with the raw value rendered — the caller
/// fails CLOSED on it rather than connecting to a host it cannot reason about.
fn take_host_protocol_version(config: &mut Value) -> Result<Option<u64>, String> {
    let Some(object) = config.as_object_mut() else {
        return Ok(None);
    };
    let Some(raw) = object.remove("protocolVersion") else {
        return Ok(None);
    };
    raw.as_u64().map(Some).ok_or_else(|| raw.to_string())
}

/// A short `arch-os` platform label for the agent registry (e.g. `x86_64-linux`).
fn platform_string() -> String {
    format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS)
}

/// The warning to log when the host URL is not TLS (`http://`): both bootstrap
/// tokens would cross the network in cleartext. `None` for `https://` (or an
/// absent host, which fails validation elsewhere). Split from the `warn!` call
/// site so the decision is unit-testable.
fn insecure_host_warning(host_url: Option<&str>) -> Option<String> {
    let host = host_url?;
    host.starts_with("http://").then(|| {
        format!(
            "host URL {host} is not TLS — the register and connect tokens travel in CLEARTEXT; \
             use https:// for anything beyond a loopback deployment"
        )
    })
}

/// Env var carrying the register token directly.
const REGISTER_TOKEN_ENV: &str = "GINO_REGISTER_TOKEN";
/// Env var naming a file whose trimmed contents are the register token.
const REGISTER_TOKEN_FILE_ENV: &str = "GINO_REGISTER_TOKEN_FILE";

/// Resolve the bootstrap register token from, in order of preference,
/// `GINO_REGISTER_TOKEN`, a file named by `GINO_REGISTER_TOKEN_FILE`, and — kept
/// only for compatibility — the `--register-token` CLI flag. The flag is
/// deprecated because argv is world-readable via `/proc/*/cmdline`; env and file
/// deliberately win over it.
pub(crate) fn resolve_register_token(cli_flag: Option<String>) -> Option<String> {
    resolve_register_token_from(
        std::env::var(REGISTER_TOKEN_ENV).ok(),
        std::env::var_os(REGISTER_TOKEN_FILE_ENV)
            .map(std::path::PathBuf::from)
            .as_deref(),
        cli_flag,
    )
}

/// Testable core of [`resolve_register_token`]: env value > token file > CLI flag.
fn resolve_register_token_from(
    env_token: Option<String>,
    token_file: Option<&std::path::Path>,
    cli_flag: Option<String>,
) -> Option<String> {
    if let Some(token) = env_token.filter(|token| !token.is_empty()) {
        return Some(token);
    }
    if let Some(path) = token_file {
        match std::fs::read_to_string(path) {
            Ok(contents) => {
                let token = contents.trim();
                if token.is_empty() {
                    warn!(path = %path.display(), "register token file is empty — ignoring it");
                } else {
                    return Some(token.to_owned());
                }
            }
            Err(err) => {
                warn!(path = %path.display(), %err, "could not read the register token file — ignoring it");
            }
        }
    }
    if cli_flag.is_some() {
        warn!(
            "--register-token on the command line is DEPRECATED: argv is world-readable via \
             /proc/*/cmdline; set {REGISTER_TOKEN_ENV} or {REGISTER_TOKEN_FILE_ENV} instead"
        );
    }
    cli_flag
}

#[cfg(test)]
mod tests {
    use secrecy::{ExposeSecret, SecretString};
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{RegisterError, load_or_register};
    use crate::config::{Config, ConfigOverlay};
    use crate::db::Db;
    use crate::transport::http::HttpClient;

    fn bootstrap_config(host: &str, register_token: Option<&str>) -> Config {
        let mut config = Config::default();
        config.apply(ConfigOverlay::bootstrap(
            Some(host.to_owned()),
            register_token.map(str::to_owned),
            Some("worker-1".to_owned()),
        ));
        config
    }

    #[tokio::test]
    async fn register_success_persists_identity_and_applies_config() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            // The request must carry the bootstrap fields (capabilities/name).
            .and(body_partial_json(json!({
                "name": "worker-1",
                "capabilities": ["claude"],
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "agentId": "agent-9",
                "connectToken": "connect-xyz",
                "config": { "engine": { "model": "claude-opus-4-8" } },
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("reg-tok"));

        let identity = load_or_register(&mut config, &db, &http).await.unwrap();

        assert_eq!(identity.agent_id, "agent-9");
        assert_eq!(identity.connect_token.expose_secret(), "connect-xyz");
        // Host config overlay applied.
        assert_eq!(config.engine.model.as_deref(), Some("claude-opus-4-8"));
        // Identity pinned onto config + persisted durably.
        assert_eq!(config.agent_id.as_deref(), Some("agent-9"));
        let persisted = db.get_identity().unwrap().unwrap();
        assert_eq!(persisted.agent_id, "agent-9");
        assert_eq!(persisted.connect_token.expose_secret(), "connect-xyz");
    }

    #[tokio::test]
    async fn register_error_is_surfaced_and_nothing_is_persisted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "register_error": { "reason": "invalid register token" }
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("bad-tok"));

        let err = load_or_register(&mut config, &db, &http).await.unwrap_err();
        assert!(
            matches!(&err, RegisterError::Rejected(reason) if reason == "invalid register token"),
            "unexpected error: {err:?}"
        );
        assert!(db.get_identity().unwrap().is_none());
        assert!(config.connect_token.is_none());
    }

    #[tokio::test]
    async fn missing_register_token_is_an_error() {
        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        // Host set, but no register token and no persisted identity.
        let mut config = bootstrap_config("http://127.0.0.1:9", None);

        let err = load_or_register(&mut config, &db, &http).await.unwrap_err();
        assert!(matches!(err, RegisterError::MissingRegisterToken));
    }

    #[tokio::test]
    async fn persisted_identity_is_reused_without_registering() {
        let db = Db::open_in_memory().unwrap();
        db.set_identity("agent-existing", &SecretString::from("connect-existing"))
            .unwrap();
        let http = HttpClient::new().unwrap();
        // No register token: if this tried to register it would fail; reuse must win.
        let mut config = bootstrap_config("http://127.0.0.1:9", None);

        let identity = load_or_register(&mut config, &db, &http).await.unwrap();

        assert_eq!(identity.agent_id, "agent-existing");
        assert_eq!(identity.connect_token.expose_secret(), "connect-existing");
        assert_eq!(config.agent_id.as_deref(), Some("agent-existing"));
        assert_eq!(
            config
                .connect_token
                .as_ref()
                .map(ExposeSecret::expose_secret),
            Some("connect-existing")
        );
    }

    #[tokio::test]
    async fn register_request_carries_protocol_version_two() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            // The negotiated wire version must ride the register body (§1/§3).
            .and(body_partial_json(json!({ "protocolVersion": 2 })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "agentId": "agent-9",
                "connectToken": "connect-xyz",
                "config": {},
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("reg-tok"));

        // The mock only matches when protocolVersion==2 is present, so a success
        // here *is* the assertion that the field was sent.
        let identity = load_or_register(&mut config, &db, &http).await.unwrap();
        assert_eq!(identity.agent_id, "agent-9");
    }

    #[tokio::test]
    async fn unsupported_protocol_rejection_aborts_without_persisting() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            .respond_with(ResponseTemplate::new(426).set_body_json(json!({
                "register_error": { "reason": "unsupported_protocol" }
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("reg-tok"));

        let err = load_or_register(&mut config, &db, &http).await.unwrap_err();
        // Gated distinctly from a generic rejection so the caller exits, not retries.
        assert!(
            matches!(&err, RegisterError::UnsupportedProtocol(_)),
            "unexpected error: {err:?}"
        );
        // The agent must not connect: no identity persisted, none pinned on config.
        assert!(db.get_identity().unwrap().is_none());
        assert!(config.connect_token.is_none());
    }

    #[tokio::test]
    async fn host_declaring_a_different_protocol_version_aborts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            // Success envelope, but the host says it will speak v1 — a version we
            // dropped. The agent must refuse rather than connect on a v1 wire.
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "agentId": "agent-9",
                "connectToken": "connect-xyz",
                "config": { "protocolVersion": 1 },
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("reg-tok"));

        let err = load_or_register(&mut config, &db, &http).await.unwrap_err();
        assert!(
            matches!(&err, RegisterError::UnsupportedProtocol(_)),
            "unexpected error: {err:?}"
        );
        // Rejected before persistence: nothing stored, nothing pinned.
        assert!(db.get_identity().unwrap().is_none());
        assert!(config.connect_token.is_none());
    }

    #[tokio::test]
    async fn non_integer_host_protocol_version_fails_closed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            // Success envelope, but the declared version is a STRING. The check
            // must fail closed (refuse), not fall through as "no version".
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "agentId": "agent-9",
                "connectToken": "connect-xyz",
                "config": { "protocolVersion": "2" },
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("reg-tok"));

        let err = load_or_register(&mut config, &db, &http).await.unwrap_err();
        assert!(
            matches!(&err, RegisterError::UnsupportedProtocol(_)),
            "unexpected error: {err:?}"
        );
        // Refused before persistence: nothing stored, nothing pinned.
        assert!(db.get_identity().unwrap().is_none());
        assert!(config.connect_token.is_none());
    }

    #[tokio::test]
    async fn matching_host_protocol_version_is_stripped_and_config_applies() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/register"))
            // The host echoes v2 *and* a config overlay. The version must be
            // consumed by the gate (not the strict overlay parser) so the rest of
            // the overlay still applies cleanly.
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "agentId": "agent-9",
                "connectToken": "connect-xyz",
                "config": { "protocolVersion": 2, "engine": { "model": "claude-opus-4-8" } },
            })))
            .mount(&server)
            .await;

        let db = Db::open_in_memory().unwrap();
        let http = HttpClient::new().unwrap();
        let mut config = bootstrap_config(&server.uri(), Some("reg-tok"));

        let identity = load_or_register(&mut config, &db, &http).await.unwrap();
        assert_eq!(identity.agent_id, "agent-9");
        // protocolVersion stripped ⇒ the overlay parsed and applied.
        assert_eq!(config.engine.model.as_deref(), Some("claude-opus-4-8"));
        assert_eq!(config.agent_id.as_deref(), Some("agent-9"));
    }

    #[test]
    fn insecure_host_warns_and_tls_host_does_not() {
        // http:// ⇒ a loud cleartext warning naming the host; https:// / absent ⇒ silence.
        let message = super::insecure_host_warning(Some("http://gino.example.com")).unwrap();
        assert!(message.contains("http://gino.example.com"));
        assert!(message.contains("CLEARTEXT"));
        assert!(super::insecure_host_warning(Some("https://gino.example.com")).is_none());
        assert!(super::insecure_host_warning(None).is_none());
    }

    #[test]
    fn register_token_env_wins_over_file_and_flag() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "file-tok\n").unwrap();
        let token = super::resolve_register_token_from(
            Some("env-tok".to_owned()),
            Some(&file),
            Some("flag-tok".to_owned()),
        );
        assert_eq!(token.as_deref(), Some("env-tok"));
    }

    #[test]
    fn register_token_file_is_read_trimmed_and_beats_the_flag() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "  file-tok\n").unwrap();
        // An EMPTY env value must not shadow the file.
        let token = super::resolve_register_token_from(
            Some(String::new()),
            Some(&file),
            Some("flag-tok".to_owned()),
        );
        assert_eq!(token.as_deref(), Some("file-tok"));
    }

    #[test]
    fn register_token_flag_is_the_deprecated_fallback() {
        // No env, an unreadable file path, an empty file: the flag still works
        // (compat), it is just deprecated.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert_eq!(
            super::resolve_register_token_from(None, Some(&missing), Some("flag-tok".to_owned()))
                .as_deref(),
            Some("flag-tok")
        );
        let empty = dir.path().join("empty");
        std::fs::write(&empty, "\n").unwrap();
        assert_eq!(
            super::resolve_register_token_from(None, Some(&empty), Some("flag-tok".to_owned()))
                .as_deref(),
            Some("flag-tok")
        );
        assert_eq!(super::resolve_register_token_from(None, None, None), None);
    }
}
