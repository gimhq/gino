//! Engine isolation: config types and per-run resolution.
//!
//! The config default (`engine.isolation`) and an optional task override
//! (`tasks.meta.isolation`) are kept as raw strings and resolved per run into an
//! [`Isolation`]. Unknown or unsupported values fail the run at resolve time
//! instead of breaking parsing of the whole config overlay.
//!
//! [`launch`] turns an engine's direct launch into the one a run actually
//! spawns: unchanged when off, wrapped in `bwrap` + `sandbox-init` when
//! sandboxed. In allowlist mode the launch also carries a [`NetHandover`]; the
//! spawner runs [`start_network`] right after the spawn to receive the guest
//! TUN device and start the run's network exit.

pub(crate) mod bwrap;
pub(crate) mod init;

use std::ffi::OsString;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs as _};
use std::os::unix::fs::DirBuilderExt as _;
use std::os::unix::net::UnixListener;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use nsplane_tun::Tun;
use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use self::bwrap::{
    CTL_SOCK, CTL_SOCK_NAME, HostRoot, ProvisionedGlobals, RESOLV_CONF_NAME, SandboxLayout,
    bwrap_args, engine_home, host_root, prepare_engine_home, resolve_binary,
};
use crate::engine::EngineError;
use crate::net::exit::{Exit, ExitConfig, GatewayForward};
use crate::net::fdpass::recv_fd;
use crate::net::rules::{AlwaysAllow, Rule, RuleError, RuleSet, normalize_name};
use crate::net::tun::TunConfig;
use crate::net::upstream::is_local;

/// Sandbox `$HOME` when the daemon has none.
const DEFAULT_SANDBOX_HOME: &str = "/home/gino";

/// Env var naming the LLM API the engine talks to.
const LLM_BASE_URL_ENV: &str = "ANTHROPIC_BASE_URL";

/// LLM API endpoint always allowed when no base URL is set.
const DEFAULT_LLM_HOST: &str = "api.anthropic.com";

/// Byte the daemon sends `sandbox-init` once the run's network exit runs.
pub(crate) const NET_READY: u8 = 0x52;

/// How long the daemon waits for `sandbox-init` to hand over the TUN device.
const HANDOVER_TIMEOUT: Duration = Duration::from_secs(30);

/// Bytes of `sandbox-init` stderr kept in a handover error.
const HANDOVER_STDERR_TAIL: usize = 1024;

/// How an engine run is isolated from the host.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum IsolationMode {
    /// Run the engine directly (no isolation).
    #[default]
    Off,
    /// Run the engine inside a bubblewrap sandbox.
    Sandbox,
    /// Run the engine inside a VM.
    // Reserved: `vm` currently resolves to `NotSupported`.
    #[allow(dead_code)]
    Vm,
}

/// Network access granted to an isolated run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum NetworkMode {
    /// No network access.
    #[default]
    None,
    /// The host's network, unrestricted.
    Full,
    /// Only the destinations in [`Isolation::allowlist`].
    Allowlist,
}

/// Raw isolation block, as found in config (`engine.isolation`) or a task
/// override (`meta.isolation`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct IsolationSpec {
    pub(crate) mode: Option<String>,
    pub(crate) network: Option<NetworkSpec>,
    pub(crate) read_paths: Option<Vec<PathBuf>>,
    pub(crate) write_paths: Option<Vec<PathBuf>>,
    /// Lets a task override loosen the default; read from the config default only.
    pub(crate) allow_task_override: Option<bool>,
}

/// Raw `network` sub-block of an [`IsolationSpec`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(crate) struct NetworkSpec {
    pub(crate) mode: Option<String>,
    /// Allowlist rules; read from the config default only.
    pub(crate) allow: Option<Vec<Rule>>,
}

/// Resolved isolation for one run.
#[derive(Clone, Debug)]
pub(crate) struct Isolation {
    pub(crate) mode: IsolationMode,
    pub(crate) network: NetworkMode,
    /// Compiled rules when the network mode is allowlist.
    pub(crate) allowlist: Option<Arc<RuleSet>>,
    /// Allowlist only: the LLM endpoint on host loopback, reached by the guest
    /// on the gateway address.
    pub(crate) llm_gateway: Option<GatewayForward>,
    /// Extra host paths mounted read-only.
    pub(crate) read_paths: Vec<PathBuf>,
    /// Extra host paths mounted read-write.
    pub(crate) write_paths: Vec<PathBuf>,
}

/// Compares the allowlist by identity.
impl PartialEq for Isolation {
    fn eq(&self, other: &Self) -> bool {
        let same_allowlist = match (&self.allowlist, &other.allowlist) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        self.mode == other.mode
            && self.network == other.network
            && same_allowlist
            && self.llm_gateway == other.llm_gateway
            && self.read_paths == other.read_paths
            && self.write_paths == other.write_paths
    }
}

impl Eq for Isolation {}

impl Isolation {
    /// No isolation: the engine runs directly on the host.
    pub(crate) fn off() -> Self {
        Self {
            mode: IsolationMode::Off,
            network: NetworkMode::None,
            allowlist: None,
            llm_gateway: None,
            read_paths: Vec::new(),
            write_paths: Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
pub(crate) enum IsolationError {
    #[error("unknown isolation mode `{0}`")]
    UnknownMode(String),
    #[error("unknown isolation network mode `{0}`")]
    UnknownNetworkMode(String),
    #[error("`{0}` is not supported yet")]
    NotSupported(String),
    #[error(
        "task meta.isolation would loosen {setting} from `{from}` to `{to}`; \
         set engine.isolation.allowTaskOverride to allow this"
    )]
    OverrideLoosens {
        setting: &'static str,
        from: String,
        to: String,
    },
    #[error("invalid isolation path `{}`: {reason}", path.display())]
    InvalidPath { path: PathBuf, reason: &'static str },
    #[error("invalid network allowlist: {0}")]
    InvalidAllowlist(#[source] RuleError),
    #[error("invalid {LLM_BASE_URL_ENV} for the network allowlist")]
    InvalidBaseUrl,
    #[error("invalid isolation in task meta: {0}")]
    InvalidTaskMeta(#[source] serde_json::Error),
    #[error("engine sandbox is unavailable: {0}")]
    Unavailable(String),
    #[error("engine sandbox needs {0}")]
    Missing(&'static str),
    #[error("engine sandbox network setup failed: {0}")]
    Network(String),
}

/// Resolve the effective isolation for a run.
///
/// The override's `mode` and `network.mode` win over the default's; extra paths
/// and allowlist rules come from the config default only. Unless the default
/// sets `allowTaskOverride`, an override may only tighten the default. When the
/// resolved mode is off nothing else is validated. An allowlist also admits the
/// LLM endpoint at `llm_base_url` (default `api.anthropic.com:443`).
pub(crate) fn resolve(
    default: &IsolationSpec,
    task_override: Option<&IsolationSpec>,
    workspace_dir: &Path,
    llm_base_url: Option<&str>,
) -> Result<Isolation, IsolationError> {
    if let Some(task) = task_override
        && !default.allow_task_override.unwrap_or(false)
    {
        check_tightens(default, task)?;
    }
    let mode = task_override
        .and_then(|o| o.mode.as_deref())
        .or(default.mode.as_deref());
    let mode = match mode {
        None | Some("off") => return Ok(Isolation::off()),
        Some("sandbox") => IsolationMode::Sandbox,
        Some("vm") => return Err(IsolationError::NotSupported("isolation.mode: vm".into())),
        Some(other) => return Err(IsolationError::UnknownMode(other.to_owned())),
    };

    let network = task_override
        .and_then(|o| o.network.as_ref())
        .and_then(|n| n.mode.as_deref())
        .or_else(|| default.network.as_ref().and_then(|n| n.mode.as_deref()));
    let network = match network {
        None | Some("none") => NetworkMode::None,
        Some("full") => NetworkMode::Full,
        Some("allowlist") => NetworkMode::Allowlist,
        Some(other) => return Err(IsolationError::UnknownNetworkMode(other.to_owned())),
    };
    let (allowlist, llm_gateway) = match network {
        NetworkMode::Allowlist => {
            let (rules, gateway) = allowlist(default, llm_base_url)?;
            (Some(rules), gateway)
        }
        NetworkMode::None | NetworkMode::Full => (None, None),
    };

    let workspace = normalize(workspace_dir);
    let read_paths = validate_paths(default.read_paths.as_deref(), &workspace)?;
    let write_paths = validate_paths(default.write_paths.as_deref(), &workspace)?;

    Ok(Isolation {
        mode,
        network,
        allowlist,
        llm_gateway,
        read_paths,
        write_paths,
    })
}

/// Compile the config default's allow rules plus the always-allowed LLM
/// endpoint. A loopback endpoint is not a rule but the returned gateway
/// forward; an IP-literal one is an always-allowed address.
fn allowlist(
    default: &IsolationSpec,
    llm_base_url: Option<&str>,
) -> Result<(Arc<RuleSet>, Option<GatewayForward>), IsolationError> {
    let endpoint = llm_endpoint(llm_base_url)?;
    let rules = default
        .network
        .as_ref()
        .and_then(|n| n.allow.as_deref())
        .unwrap_or_default();
    let host = normalize_name(&endpoint.host);
    let literal = host.parse::<IpAddr>().ok();
    let (rules, gateway) = if host == "localhost" || literal.is_some_and(is_local) {
        if endpoint.port == 0 {
            return Err(IsolationError::InvalidBaseUrl);
        }
        let target = match literal {
            Some(ip) => SocketAddr::new(ip, endpoint.port),
            None => localhost_addr(endpoint.port),
        };
        let gateway = GatewayForward {
            port: endpoint.port,
            target,
        };
        (RuleSet::compile(rules, &[]), Some(gateway))
    } else if let Some(ip) = literal {
        let addr = SocketAddr::new(ip, endpoint.port);
        let rules = RuleSet::compile(rules, &[]).and_then(|set| set.with_always_addr(addr));
        (rules, None)
    } else {
        (RuleSet::compile(rules, &[endpoint]), None)
    };
    let rules = rules.map_err(IsolationError::InvalidAllowlist)?;
    Ok((Arc::new(rules), gateway))
}

/// `localhost:port` as resolved on the host, IPv4 first; 127.0.0.1 when the
/// lookup yields no loopback address.
fn localhost_addr(port: u16) -> SocketAddr {
    let resolved: Vec<SocketAddr> = ("localhost", port)
        .to_socket_addrs()
        .map(|addrs| addrs.filter(|addr| addr.ip().is_loopback()).collect())
        .unwrap_or_default();
    resolved
        .iter()
        .find(|addr| addr.is_ipv4())
        .or_else(|| resolved.first())
        .copied()
        .unwrap_or_else(|| SocketAddr::new(Ipv4Addr::LOCALHOST.into(), port))
}

/// `url` with its host replaced by the guest gateway address; scheme,
/// userinfo, port, path and query are kept.
fn gateway_url(url: &str) -> Option<String> {
    let (scheme, rest) = url.split_once("://")?;
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(end);
    let (userinfo, host_port) = match authority.rsplit_once('@') {
        Some((userinfo, host_port)) => (Some(userinfo), host_port),
        None => (None, authority),
    };
    let port = match host_port.strip_prefix('[') {
        Some(bracketed) => bracketed.split_once(']')?.1,
        None => host_port.rfind(':').map_or("", |at| &host_port[at..]),
    };
    let userinfo = userinfo.map(|u| format!("{u}@")).unwrap_or_default();
    let gateway = TunConfig::default().gateway;
    Some(format!("{scheme}://{userinfo}{gateway}{port}{tail}"))
}

/// The LLM API endpoint: `llm_base_url`'s host and port, else the default host.
fn llm_endpoint(llm_base_url: Option<&str>) -> Result<AlwaysAllow, IsolationError> {
    match llm_base_url {
        Some(url) => AlwaysAllow::from_url(url).ok_or(IsolationError::InvalidBaseUrl),
        None => Ok(AlwaysAllow {
            host: DEFAULT_LLM_HOST.to_owned(),
            port: 443,
        }),
    }
}

/// `ANTHROPIC_BASE_URL` as the engine sees it: the launch env, else the
/// inherited one. Empty means unset.
fn llm_base_url(env: &[(String, OsString)]) -> Option<String> {
    env.iter()
        .rev()
        .find(|(key, _)| key == LLM_BASE_URL_ENV)
        .map(|(_, value)| value.clone())
        .or_else(|| std::env::var_os(LLM_BASE_URL_ENV))
        .and_then(|value| value.into_string().ok())
        .filter(|value| !value.is_empty())
}

/// Fail when `task` would loosen `default`'s mode or network mode.
///
/// Modes order `off` < `sandbox` < `vm`; network modes order `full` <
/// `allowlist` < `none`. A default that is off confines no network, so any
/// network override tightens it.
fn check_tightens(default: &IsolationSpec, task: &IsolationSpec) -> Result<(), IsolationError> {
    let from = default.mode.as_deref().unwrap_or("off");
    let default_mode = mode_strictness(from)?;
    if let Some(to) = task.mode.as_deref()
        && mode_strictness(to)? < default_mode
    {
        return Err(loosens("isolation.mode", from, to));
    }
    if default_mode == 0 {
        return Ok(());
    }
    let from = network_mode(default).unwrap_or("none");
    if let Some(to) = network_mode(task)
        && network_strictness(to)? < network_strictness(from)?
    {
        return Err(loosens("isolation.network.mode", from, to));
    }
    Ok(())
}

fn network_mode(spec: &IsolationSpec) -> Option<&str> {
    spec.network.as_ref().and_then(|n| n.mode.as_deref())
}

fn mode_strictness(mode: &str) -> Result<u8, IsolationError> {
    match mode {
        "off" => Ok(0),
        "sandbox" => Ok(1),
        "vm" => Ok(2),
        other => Err(IsolationError::UnknownMode(other.to_owned())),
    }
}

fn network_strictness(network: &str) -> Result<u8, IsolationError> {
    match network {
        "full" => Ok(0),
        "allowlist" => Ok(1),
        "none" => Ok(2),
        other => Err(IsolationError::UnknownNetworkMode(other.to_owned())),
    }
}

fn loosens(setting: &'static str, from: &str, to: &str) -> IsolationError {
    IsolationError::OverrideLoosens {
        setting,
        from: from.to_owned(),
        to: to.to_owned(),
    }
}

/// Extract the task override from a task's free-form `meta` JSON.
///
/// Missing, non-JSON or non-object meta, or meta without an `isolation` key,
/// means no override; an `isolation` value of the wrong shape is an error.
/// `network.allow` is dropped: allowlist rules never come from a task.
pub(crate) fn override_from_meta(
    meta: Option<&str>,
) -> Result<Option<IsolationSpec>, IsolationError> {
    let Some(Ok(serde_json::Value::Object(mut map))) = meta.map(serde_json::from_str) else {
        return Ok(None);
    };
    let Some(mut value) = map.remove("isolation") else {
        return Ok(None);
    };
    if let Some(network) = value
        .get_mut("network")
        .and_then(serde_json::Value::as_object_mut)
    {
        network.remove("allow");
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(IsolationError::InvalidTaskMeta)
}

/// One engine launch, built but not spawned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Launch {
    pub(crate) program: PathBuf,
    pub(crate) args: Vec<OsString>,
    /// Variables layered onto the inherited environment, in order.
    pub(crate) env: Vec<(String, OsString)>,
    /// Working directory; empty means the current directory.
    pub(crate) cwd: PathBuf,
    /// Whether `program` is `bwrap` wrapping the engine.
    pub(crate) sandboxed: bool,
    /// Allowlist only: the network handover to run after the spawn.
    pub(crate) net: Option<Arc<NetHandover>>,
}

/// Private per-run daemon dir (mode 0700) holding the control socket and the
/// sandbox `resolv.conf`; removed on drop.
#[derive(Debug)]
struct RunDir(PathBuf);

impl RunDir {
    fn create() -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!("gino-net-{}", uuid::Uuid::new_v4().simple()));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
}

impl Drop for RunDir {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_dir_all(&self.0) {
            tracing::debug!(error = %err, "removing the sandbox network dir");
        }
    }
}

/// Daemon side of an allowlist sandbox's network, prepared before the spawn:
/// the bound control socket and the settings of the run's exit.
#[derive(Debug)]
pub(crate) struct NetHandover {
    dir: RunDir,
    listener: UnixListener,
    exit: ExitConfig,
}

/// Compares by identity.
impl PartialEq for NetHandover {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for NetHandover {}

impl NetHandover {
    /// Create the per-run dir with `resolv.conf` and bind the control socket.
    fn prepare(exit: ExitConfig) -> std::io::Result<Self> {
        let dir = RunDir::create()?;
        let gateway = TunConfig::default().gateway;
        std::fs::write(
            dir.0.join(RESOLV_CONF_NAME),
            format!("nameserver {gateway}\n"),
        )?;
        let listener = UnixListener::bind(dir.0.join(CTL_SOCK_NAME))?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            dir,
            listener,
            exit,
        })
    }
}

/// A running allowlist network: the exit and its per-run dir. Dropping it
/// stops the exit and removes the dir.
#[derive(Debug)]
pub(crate) struct NetExit {
    _exit: Exit,
    _dir: RunDir,
}

/// Receive the guest TUN device from the just-spawned sandbox `child`, start
/// the run's exit on it and release the engine. On failure the sandbox is
/// killed and the run fails with [`IsolationError::Network`].
pub(crate) async fn start_network(
    net: Option<Arc<NetHandover>>,
    child: &mut tokio::process::Child,
) -> Result<Option<NetExit>, EngineError> {
    let Some(net) = net else {
        return Ok(None);
    };
    let result = match Arc::into_inner(net) {
        Some(net) => hand_over(net, child).await,
        None => Err("the network handover is shared".to_owned()),
    };
    match result {
        Ok(exit) => Ok(Some(exit)),
        Err(reason) => {
            if let Err(err) = child.start_kill() {
                tracing::debug!(error = %err, "killing the sandbox after a failed network setup");
            }
            Err(IsolationError::Network(reason).into())
        }
    }
}

async fn hand_over(net: NetHandover, child: &mut tokio::process::Child) -> Result<NetExit, String> {
    let NetHandover {
        dir,
        listener,
        exit,
    } = net;
    let handover = tokio::time::timeout(HANDOVER_TIMEOUT, start_exit(listener, exit));
    // `Child::wait` closes stdin; keep it for the engine.
    let stdin = child.stdin.take();
    let exited = tokio::select! {
        result = handover => {
            let exit = result.map_err(|_| {
                format!("no TUN device from the sandbox within {}s", HANDOVER_TIMEOUT.as_secs())
            })??;
            child.stdin = stdin;
            return Ok(NetExit { _exit: exit, _dir: dir });
        }
        status = child.wait() => status,
    };
    let status = exited.map_or_else(|err| err.to_string(), |status| status.to_string());
    let mut stderr = Vec::new();
    if let Some(pipe) = child.stderr.as_mut() {
        let mut limited = pipe.take(64 * 1024);
        let _ =
            tokio::time::timeout(Duration::from_secs(1), limited.read_to_end(&mut stderr)).await;
    }
    let start = stderr.len().saturating_sub(HANDOVER_STDERR_TAIL);
    let tail = String::from_utf8_lossy(&stderr[start..]);
    Err(format!(
        "the sandbox exited ({status}) before handing over its network: {}",
        tail.trim()
    ))
}

/// Accept the sandbox's connection, adopt the TUN device it sends, start the
/// exit and send the ready byte.
async fn start_exit(listener: UnixListener, cfg: ExitConfig) -> Result<Exit, String> {
    let listener = tokio::net::UnixListener::from_std(listener)
        .map_err(|err| format!("control socket: {err}"))?;
    let (mut stream, _) = listener
        .accept()
        .await
        .map_err(|err| format!("accepting on the control socket: {err}"))?;
    // Nothing else may connect once the sandbox has.
    drop(listener);
    let fd = recv_fd(&stream, HANDOVER_TIMEOUT)
        .await
        .map_err(|err| format!("receiving the TUN device: {err}"))?;
    let tun = Tun::from_fd(fd, cfg.mtu).map_err(|err| format!("adopting the TUN device: {err}"))?;
    let exit = Exit::start(tun, cfg).map_err(|err| format!("starting the network exit: {err}"))?;
    stream
        .write_all(&[NET_READY])
        .await
        .map_err(|err| format!("releasing the sandbox: {err}"))?;
    Ok(exit)
}

impl Launch {
    /// The engine run directly on the host.
    pub(crate) fn direct(
        program: PathBuf,
        args: Vec<String>,
        env: Vec<(String, OsString)>,
        cwd: PathBuf,
    ) -> Self {
        Self {
            program,
            args: args.into_iter().map(OsString::from).collect(),
            env,
            cwd,
            sandboxed: false,
            net: None,
        }
    }

    /// A command with piped stdio that kills the child on drop.
    pub(crate) fn command(&self) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(&self.program);
        command
            .args(&self.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if !self.cwd.as_os_str().is_empty() {
            command.current_dir(&self.cwd);
        }
        for (key, value) in &self.env {
            command.env(key, value);
        }
        command
    }
}

/// Per-run inputs deciding whether and how a launch is sandboxed.
#[derive(Clone, Copy, Debug)]
pub(crate) struct LaunchContext<'a> {
    /// Configured default (`engine.isolation`).
    pub(crate) isolation: &'a IsolationSpec,
    /// The task's override, if any.
    pub(crate) task_override: Option<&'a IsolationSpec>,
    pub(crate) workspace_dir: &'a Path,
    /// The run's thread, for network decision logs.
    pub(crate) thread_id: Option<&'a str>,
    /// The thread's work dir.
    pub(crate) work_dir: Option<&'a Path>,
    /// The gino-agent binary.
    pub(crate) self_exe: Option<&'a Path>,
    /// Provisioning dir holding the global `CLAUDE.md` and `skills/`.
    pub(crate) global_config_dir: Option<&'a Path>,
    /// Env keys naming the engine config dir; sandboxed, they name the engine home.
    pub(crate) config_dir_keys: &'a [&'a str],
}

/// Host facts a sandboxed launch is built from.
#[derive(Clone, Debug)]
pub(crate) struct SandboxHost {
    /// The probed `bwrap` binary.
    pub(crate) bwrap: PathBuf,
    pub(crate) root: HostRoot,
    /// `PATH` the engine binary is resolved against.
    pub(crate) path_env: Option<OsString>,
    /// Sandbox `$HOME`.
    pub(crate) home: PathBuf,
}

impl SandboxHost {
    fn current(bwrap: PathBuf) -> Self {
        Self {
            bwrap,
            root: host_root(),
            path_env: std::env::var_os("PATH"),
            home: std::env::var_os("HOME")
                .filter(|home| !home.is_empty())
                .map_or_else(|| PathBuf::from(DEFAULT_SANDBOX_HOME), PathBuf::from),
        }
    }
}

#[cfg(test)]
impl SandboxHost {
    /// A fixed host layout that finds binaries on `path_env`.
    pub(crate) fn fixture(path_env: Option<OsString>) -> Self {
        use self::bwrap::RootEntry;
        Self {
            bwrap: "/usr/bin/bwrap".into(),
            root: HostRoot {
                entries: [
                    RootEntry::Symlink("usr/bin".into()),
                    RootEntry::Dir,
                    RootEntry::Absent,
                    RootEntry::Absent,
                ],
                resolv_conf_outside_etc: None,
            },
            path_env,
            home: "/home/gino".into(),
        }
    }
}

/// Resolve the run's isolation and build its launch: `direct` as-is when off,
/// wrapped in `bwrap` when sandboxed. Fails closed when the agent runs as root
/// or `bwrap` is unusable.
pub(crate) fn launch(direct: Launch, ctx: &LaunchContext<'_>) -> Result<Launch, EngineError> {
    launch_as(direct, ctx, nix::unistd::geteuid().as_raw())
}

fn launch_as(direct: Launch, ctx: &LaunchContext<'_>, euid: u32) -> Result<Launch, EngineError> {
    let base_url = llm_base_url(&direct.env);
    let isolation = resolve(
        ctx.isolation,
        ctx.task_override,
        ctx.workspace_dir,
        base_url.as_deref(),
    )?;
    match isolation.mode {
        IsolationMode::Off => Ok(direct),
        IsolationMode::Sandbox => {
            check_not_root(euid)?;
            let bwrap = bwrap::ensure_available()?;
            wrap(direct, ctx, &isolation, &SandboxHost::current(bwrap))
        }
        IsolationMode::Vm => Err(IsolationError::NotSupported("isolation.mode: vm".into()).into()),
    }
}

/// Refuse to sandbox from a root daemon: the engine's user namespace maps it
/// onto the daemon's uid, so it would own root's files.
fn check_not_root(euid: u32) -> Result<(), IsolationError> {
    if euid == 0 {
        return Err(IsolationError::Unavailable(
            "the agent runs as root; run it as an unprivileged user".into(),
        ));
    }
    Ok(())
}

/// Wrap `direct` in `bwrap` + `sandbox-init` and create the engine home.
pub(crate) fn wrap(
    direct: Launch,
    ctx: &LaunchContext<'_>,
    isolation: &Isolation,
    host: &SandboxHost,
) -> Result<Launch, EngineError> {
    let work_dir = ctx
        .work_dir
        .ok_or(IsolationError::Missing("the thread's work dir"))?;
    let agent_exe = ctx
        .self_exe
        .ok_or(IsolationError::Missing("the gino-agent binary path"))?;
    let engine_binary =
        resolve_binary(&direct.program, host.path_env.as_deref()).ok_or_else(|| {
            EngineError::Spawn(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("engine binary `{}` not found", direct.program.display()),
            ))
        })?;
    let cwd = if direct.cwd.as_os_str().is_empty() {
        work_dir.to_path_buf()
    } else {
        direct.cwd.clone()
    };
    let net = match (isolation.network, &isolation.allowlist) {
        (NetworkMode::Allowlist, Some(rules)) => {
            let mut exit = ExitConfig::new(ctx.thread_id.unwrap_or("-"), Arc::clone(rules));
            exit.gateway = isolation.llm_gateway;
            Some(NetHandover::prepare(exit).map_err(EngineError::Spawn)?)
        }
        (NetworkMode::Allowlist, None) => {
            return Err(IsolationError::Missing("the allowlist rules").into());
        }
        (NetworkMode::None | NetworkMode::Full, _) => None,
    };
    let layout = SandboxLayout {
        work_dir: work_dir.to_path_buf(),
        cwd,
        engine_home: engine_home(work_dir),
        global_config_dir: ctx.global_config_dir.map(Path::to_path_buf),
        globals: ProvisionedGlobals::inspect(ctx.global_config_dir),
        engine_binary,
        agent_exe: agent_exe.to_path_buf(),
        home: host.home.clone(),
        network: isolation.network,
        read_paths: isolation.read_paths.clone(),
        write_paths: isolation.write_paths.clone(),
        net_dir: net.as_ref().map(|net| net.dir.0.clone()),
    };
    prepare_engine_home(&layout).map_err(EngineError::Spawn)?;

    let mut inner: Vec<OsString> = vec![layout.agent_exe.clone().into(), "sandbox-init".into()];
    if net.is_some() {
        inner.extend(["--ctl-sock".into(), CTL_SOCK.into()]);
    }
    inner.extend(["--".into(), layout.engine_binary.clone().into()]);
    inner.extend(direct.args);

    // A loopback LLM endpoint is reached through the gateway.
    let llm_url = match isolation.llm_gateway {
        Some(_) => Some(
            llm_base_url(&direct.env)
                .as_deref()
                .and_then(gateway_url)
                .ok_or(IsolationError::InvalidBaseUrl)?,
        ),
        None => None,
    };

    // The host workspace root is not mounted, so the engine must not see it.
    let mut env = direct.env;
    if let Some(url) = llm_url {
        env.retain(|(key, _)| key != LLM_BASE_URL_ENV);
        env.push((LLM_BASE_URL_ENV.to_owned(), url.into()));
    }
    env.retain(|(key, _)| {
        key != "GINO_WORKSPACE_DIR" && !ctx.config_dir_keys.contains(&key.as_str())
    });
    env.extend(ctx.config_dir_keys.iter().map(|key| {
        (
            (*key).to_owned(),
            layout.engine_home.clone().into_os_string(),
        )
    }));
    env.extend([
        ("HOME".to_owned(), layout.home.clone().into_os_string()),
        ("TMPDIR".to_owned(), "/tmp".into()),
        (
            "GINO_ARCHIVE_DIR".to_owned(),
            work_dir.join("conversations").into_os_string(),
        ),
        ("GINO_SANDBOX".to_owned(), "1".into()),
    ]);

    Ok(Launch {
        program: host.bwrap.clone(),
        args: bwrap_args(&layout, &host.root, &inner),
        env,
        cwd: direct.cwd,
        sandboxed: true,
        net: net.map(Arc::new),
    })
}

/// Pid of the first process inside a sandbox (`sandbox-init`), the direct
/// child of `bwrap_pid`.
pub(crate) fn sandbox_init_pid(bwrap_pid: i32) -> Option<i32> {
    let children = format!("/proc/{bwrap_pid}/task/{bwrap_pid}/children");
    if let Some(pid) = std::fs::read_to_string(children)
        .ok()
        .and_then(|list| first_child(&list))
    {
        return Some(pid);
    }
    std::fs::read_dir("/proc")
        .ok()?
        .flatten()
        .find_map(|entry| {
            let pid: i32 = entry.file_name().to_str()?.parse().ok()?;
            let stat = std::fs::read_to_string(entry.path().join("stat")).ok()?;
            (stat_ppid(&stat)? == bwrap_pid).then_some(pid)
        })
}

/// First pid in a `/proc/<pid>/task/<tid>/children` list.
fn first_child(list: &str) -> Option<i32> {
    list.split_whitespace().next()?.parse().ok()
}

/// Parent pid from a `/proc/<pid>/stat` line; `comm` may hold spaces and `)`.
fn stat_ppid(stat: &str) -> Option<i32> {
    let (_, rest) = stat.rsplit_once(')')?;
    rest.split_whitespace().nth(1)?.parse().ok()
}

/// Lexically normalize a path: drops `.` components and trailing slashes.
fn normalize(path: &Path) -> PathBuf {
    path.components()
        .filter(|c| !matches!(c, Component::CurDir))
        .collect()
}

fn validate_paths(
    paths: Option<&[PathBuf]>,
    workspace: &Path,
) -> Result<Vec<PathBuf>, IsolationError> {
    paths
        .unwrap_or_default()
        .iter()
        .map(|path| {
            let invalid = |reason| IsolationError::InvalidPath {
                path: path.clone(),
                reason,
            };
            if !path.is_absolute() {
                return Err(invalid("not absolute"));
            }
            if path.components().any(|c| matches!(c, Component::ParentDir)) {
                return Err(invalid("contains `..`"));
            }
            let normalized = normalize(path);
            if normalized == workspace {
                return Err(invalid("is the workspace root"));
            }
            if workspace.starts_with(&normalized) {
                return Err(invalid("is a parent of the workspace root"));
            }
            Ok(normalized)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};

    use super::{
        Isolation, IsolationError, IsolationMode, IsolationSpec, Launch, LaunchContext,
        NetworkMode, NetworkSpec, SandboxHost, check_not_root, first_child, launch, launch_as,
        llm_endpoint, override_from_meta, resolve, sandbox_init_pid, stat_ppid, wrap,
    };
    use crate::engine::EngineError;
    use crate::net::rules::{AlwaysAllow, Decision, Proto, RuleError, RuleSet};

    const WORKSPACE: &str = "/var/lib/gino-agent";

    fn spec(mode: Option<&str>, network: Option<&str>) -> IsolationSpec {
        IsolationSpec {
            mode: mode.map(str::to_owned),
            network: network.map(|n| NetworkSpec {
                mode: Some(n.to_owned()),
                ..NetworkSpec::default()
            }),
            ..IsolationSpec::default()
        }
    }

    fn with_paths(mut spec: IsolationSpec, read: &[&str], write: &[&str]) -> IsolationSpec {
        spec.read_paths = Some(read.iter().map(PathBuf::from).collect());
        spec.write_paths = Some(write.iter().map(PathBuf::from).collect());
        spec
    }

    fn allowing(mut spec: IsolationSpec) -> IsolationSpec {
        spec.allow_task_override = Some(true);
        spec
    }

    fn run(
        default: &IsolationSpec,
        task: Option<&IsolationSpec>,
    ) -> Result<Isolation, IsolationError> {
        resolve(default, task, Path::new(WORKSPACE), None)
    }

    fn path_error(path: &str) -> IsolationError {
        run(&with_paths(spec(Some("sandbox"), None), &[path], &[]), None).unwrap_err()
    }

    #[test]
    fn absent_mode_resolves_off() {
        assert_eq!(
            run(&IsolationSpec::default(), None).unwrap(),
            Isolation::off()
        );
        assert_eq!(
            run(&spec(None, Some("full")), None).unwrap(),
            Isolation::off()
        );
    }

    #[test]
    fn sandbox_defaults_to_no_network() {
        let iso = run(&spec(Some("sandbox"), None), None).unwrap();
        assert_eq!(iso.mode, IsolationMode::Sandbox);
        assert_eq!(iso.network, NetworkMode::None);

        let iso = run(&spec(Some("sandbox"), Some("none")), None).unwrap();
        assert_eq!(iso.network, NetworkMode::None);
    }

    #[test]
    fn sandbox_with_full_network() {
        let iso = run(&spec(Some("sandbox"), Some("full")), None).unwrap();
        assert_eq!(iso.mode, IsolationMode::Sandbox);
        assert_eq!(iso.network, NetworkMode::Full);
    }

    #[test]
    fn override_beats_default_for_mode_and_network() {
        let default = allowing(spec(Some("sandbox"), Some("none")));
        let iso = run(&default, Some(&spec(None, Some("full")))).unwrap();
        assert_eq!(iso.mode, IsolationMode::Sandbox);
        assert_eq!(iso.network, NetworkMode::Full);

        assert_eq!(
            run(&default, Some(&spec(Some("off"), None))).unwrap(),
            Isolation::off()
        );

        let iso = run(
            &IsolationSpec::default(),
            Some(&spec(Some("sandbox"), None)),
        )
        .unwrap();
        assert_eq!(iso.mode, IsolationMode::Sandbox);
    }

    /// The resolved `(mode, network)`, or the error, of `default` + `task`.
    fn outcome(
        default: &IsolationSpec,
        task: &IsolationSpec,
    ) -> Result<(IsolationMode, NetworkMode), IsolationError> {
        run(default, Some(task)).map(|iso| (iso.mode, iso.network))
    }

    fn assert_loosens(
        default: &IsolationSpec,
        task: &IsolationSpec,
        setting: &str,
        from: &str,
        to: &str,
    ) {
        let err = run(default, Some(task)).unwrap_err();
        assert!(
            matches!(&err, IsolationError::OverrideLoosens { setting: s, from: f, to: t }
                if *s == setting && f == from && t == to),
            "{default:?} + {task:?}: {err:?}"
        );
    }

    fn assert_not_supported(result: Result<(IsolationMode, NetworkMode), IsolationError>) {
        let err = result.unwrap_err();
        assert!(matches!(err, IsolationError::NotSupported(_)), "{err:?}");
    }

    #[test]
    fn mode_override_may_only_tighten() {
        use IsolationMode::Sandbox;
        use NetworkMode::None as NoNet;

        // off -> sandbox tightens, also from an absent default.
        for default in [spec(Some("off"), None), IsolationSpec::default()] {
            assert_eq!(
                outcome(&default, &spec(Some("sandbox"), None)).unwrap(),
                (Sandbox, NoNet)
            );
            assert_not_supported(outcome(&default, &spec(Some("vm"), None)));
            assert_eq!(
                run(&default, Some(&spec(Some("off"), None))).unwrap(),
                Isolation::off()
            );
        }
        // sandbox -> vm tightens, then fails as not supported yet.
        assert_not_supported(outcome(
            &spec(Some("sandbox"), None),
            &spec(Some("vm"), None),
        ));
        assert_eq!(
            outcome(&spec(Some("sandbox"), None), &spec(Some("sandbox"), None)).unwrap(),
            (Sandbox, NoNet)
        );

        assert_loosens(
            &spec(Some("sandbox"), None),
            &spec(Some("off"), None),
            "isolation.mode",
            "sandbox",
            "off",
        );
        assert_loosens(
            &spec(Some("vm"), None),
            &spec(Some("off"), None),
            "isolation.mode",
            "vm",
            "off",
        );
        assert_loosens(
            &spec(Some("vm"), None),
            &spec(Some("sandbox"), None),
            "isolation.mode",
            "vm",
            "sandbox",
        );
    }

    #[test]
    fn mode_override_loosens_with_allow_task_override() {
        let sandbox = allowing(spec(Some("sandbox"), None));
        assert_eq!(
            run(&sandbox, Some(&spec(Some("off"), None))).unwrap(),
            Isolation::off()
        );
        let vm = allowing(spec(Some("vm"), None));
        assert_eq!(
            run(&vm, Some(&spec(Some("off"), None))).unwrap(),
            Isolation::off()
        );
        assert_eq!(
            outcome(&vm, &spec(Some("sandbox"), None)).unwrap(),
            (IsolationMode::Sandbox, NetworkMode::None)
        );
        let off = allowing(spec(Some("off"), None));
        assert_eq!(
            outcome(&off, &spec(Some("sandbox"), None)).unwrap(),
            (IsolationMode::Sandbox, NetworkMode::None)
        );
        assert_not_supported(outcome(&sandbox, &spec(Some("vm"), None)));
    }

    #[test]
    fn network_override_may_only_tighten() {
        use IsolationMode::Sandbox;
        use NetworkMode::{Allowlist, Full, None as NoNet};

        let sandbox = |network| spec(Some("sandbox"), network);
        let net = |network| spec(None, Some(network));

        // Tightening.
        assert_eq!(
            outcome(&sandbox(Some("full")), &net("allowlist")).unwrap(),
            (Sandbox, Allowlist)
        );
        assert_eq!(
            outcome(&sandbox(Some("allowlist")), &net("allowlist")).unwrap(),
            (Sandbox, Allowlist)
        );
        assert_eq!(
            outcome(&sandbox(Some("full")), &net("none")).unwrap(),
            (Sandbox, NoNet)
        );
        assert_eq!(
            outcome(&sandbox(Some("allowlist")), &net("none")).unwrap(),
            (Sandbox, NoNet)
        );
        assert_eq!(
            outcome(&sandbox(Some("full")), &net("full")).unwrap(),
            (Sandbox, Full)
        );

        // Loosening, including from an absent network (= none).
        for default in [sandbox(None), sandbox(Some("none"))] {
            for to in ["full", "allowlist"] {
                assert_loosens(&default, &net(to), "isolation.network.mode", "none", to);
            }
        }
        assert_loosens(
            &sandbox(Some("allowlist")),
            &net("full"),
            "isolation.network.mode",
            "allowlist",
            "full",
        );
        // The mode is checked first.
        assert_loosens(
            &sandbox(None),
            &spec(Some("off"), Some("full")),
            "isolation.mode",
            "sandbox",
            "off",
        );
    }

    #[test]
    fn network_override_loosens_with_allow_task_override() {
        let allow = |network| allowing(spec(Some("sandbox"), Some(network)));
        let net = |network| spec(None, Some(network));
        assert_eq!(
            outcome(&allow("none"), &net("full")).unwrap(),
            (IsolationMode::Sandbox, NetworkMode::Full)
        );
        assert_eq!(
            outcome(&allow("none"), &net("allowlist")).unwrap(),
            (IsolationMode::Sandbox, NetworkMode::Allowlist)
        );
        assert_eq!(
            outcome(&allow("allowlist"), &net("full")).unwrap(),
            (IsolationMode::Sandbox, NetworkMode::Full)
        );
        assert_eq!(
            outcome(&allow("full"), &net("none")).unwrap(),
            (IsolationMode::Sandbox, NetworkMode::None)
        );
    }

    #[test]
    fn an_off_default_accepts_any_override_network() {
        for default in [IsolationSpec::default(), spec(Some("off"), Some("none"))] {
            for (network, expected) in [
                (None, NetworkMode::None),
                (Some("none"), NetworkMode::None),
                (Some("full"), NetworkMode::Full),
                (Some("allowlist"), NetworkMode::Allowlist),
            ] {
                let result = outcome(&default, &spec(Some("sandbox"), network));
                assert_eq!(result.unwrap(), (IsolationMode::Sandbox, expected));
            }
        }
    }

    #[test]
    fn absent_override_keeps_the_default() {
        let iso = run(&spec(Some("sandbox"), Some("full")), None).unwrap();
        assert_eq!(
            (iso.mode, iso.network),
            (IsolationMode::Sandbox, NetworkMode::Full)
        );
        // An empty override changes nothing either.
        let iso = run(
            &spec(Some("sandbox"), Some("full")),
            Some(&IsolationSpec::default()),
        )
        .unwrap();
        assert_eq!(
            (iso.mode, iso.network),
            (IsolationMode::Sandbox, NetworkMode::Full)
        );
    }

    #[test]
    fn allow_task_override_inside_the_override_is_ignored() {
        let task = allowing(spec(Some("off"), None));
        assert_loosens(
            &spec(Some("sandbox"), None),
            &task,
            "isolation.mode",
            "sandbox",
            "off",
        );
        let default = IsolationSpec {
            allow_task_override: Some(false),
            ..spec(Some("sandbox"), None)
        };
        assert_loosens(&default, &task, "isolation.mode", "sandbox", "off");
    }

    #[test]
    fn invalid_values_never_pass_as_tighter() {
        let err = run(
            &spec(Some("jail"), None),
            Some(&spec(Some("sandbox"), None)),
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::UnknownMode(m) if m == "jail"));
        let err = run(
            &spec(Some("sandbox"), None),
            Some(&spec(Some("jail"), None)),
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::UnknownMode(m) if m == "jail"));
        let err = run(
            &spec(Some("sandbox"), Some("partial")),
            Some(&spec(None, Some("none"))),
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::UnknownNetworkMode(m) if m == "partial"));
        let err = run(
            &spec(Some("sandbox"), None),
            Some(&spec(None, Some("partial"))),
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::UnknownNetworkMode(m) if m == "partial"));
    }

    #[test]
    fn loosen_error_names_the_setting_and_the_opt_in() {
        let err = run(
            &spec(Some("sandbox"), None),
            Some(&spec(None, Some("full"))),
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "task meta.isolation would loosen isolation.network.mode from `none` to `full`; \
             set engine.isolation.allowTaskOverride to allow this"
        );
        let err = run(&spec(Some("sandbox"), None), Some(&spec(Some("off"), None))).unwrap_err();
        assert!(
            err.to_string()
                .contains("isolation.mode from `sandbox` to `off`"),
            "{err}"
        );
    }

    #[test]
    fn override_paths_are_ignored() {
        let default = with_paths(spec(Some("sandbox"), None), &["/data/ro"], &["/data/rw"]);
        let task = with_paths(IsolationSpec::default(), &["/etc"], &["relative"]);
        let iso = run(&default, Some(&task)).unwrap();
        assert_eq!(iso.read_paths, [PathBuf::from("/data/ro")]);
        assert_eq!(iso.write_paths, [PathBuf::from("/data/rw")]);
    }

    #[test]
    fn vm_is_not_supported_yet() {
        let err = run(&spec(Some("vm"), None), None).unwrap_err();
        assert!(matches!(err, IsolationError::NotSupported(_)), "{err:?}");
        assert!(err.to_string().ends_with("is not supported yet"), "{err}");
    }

    /// `spec` with `network.allow` set from a JSON rule list.
    fn with_rules(mut spec: IsolationSpec, rules: &str) -> IsolationSpec {
        spec.network.get_or_insert_with(NetworkSpec::default).allow =
            Some(serde_json::from_str(rules).unwrap());
        spec
    }

    fn allowlist_of(iso: &Isolation) -> &RuleSet {
        assert_eq!(iso.network, NetworkMode::Allowlist);
        iso.allowlist.as_deref().unwrap()
    }

    const ANY_IP: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(203, 0, 113, 7));

    fn allows(rules: &RuleSet, name: &str, port: u16) -> bool {
        matches!(
            rules.decide(Some(name), ANY_IP, port, Proto::Tcp),
            Decision::Allow { .. }
        )
    }

    #[test]
    fn meta_allowlist_tightens_an_off_default() {
        let task = override_from_meta(Some(
            r#"{ "isolation": { "mode": "sandbox", "network": { "mode": "allowlist" } } }"#,
        ))
        .unwrap()
        .unwrap();
        for default in [IsolationSpec::default(), spec(Some("off"), Some("none"))] {
            let iso = run(&default, Some(&task)).unwrap();
            assert_eq!(iso.mode, IsolationMode::Sandbox);
            assert!(allows(allowlist_of(&iso), "api.anthropic.com", 443));
        }
    }

    #[test]
    fn meta_allow_rules_are_ignored() {
        for allow in [
            r#"[{ "host": "evil.example", "ports": "*" }]"#,
            r#"[{ "host": "bad host", "ports": [] }]"#,
            "5",
        ] {
            let meta = format!(
                r#"{{ "isolation": {{ "mode": "sandbox",
                    "network": {{ "mode": "allowlist", "allow": {allow} }} }} }}"#
            );
            let task = override_from_meta(Some(&meta)).unwrap().unwrap();
            assert_eq!(task.network.as_ref().unwrap().allow, None, "{meta}");
            let iso = run(&IsolationSpec::default(), Some(&task)).unwrap();
            assert!(!allowlist_of(&iso).name_allowed("evil.example"), "{meta}");
        }
        // An override built in memory with rules still gets none of them.
        let task = with_rules(
            spec(Some("sandbox"), Some("allowlist")),
            r#"[{ "host": "evil.example", "ports": "*" }]"#,
        );
        let iso = run(&IsolationSpec::default(), Some(&task)).unwrap();
        assert!(!allowlist_of(&iso).name_allowed("evil.example"));
    }

    #[test]
    fn allowlist_holds_only_the_llm_endpoint_without_config_rules() {
        let iso = run(&spec(Some("sandbox"), Some("allowlist")), None).unwrap();
        let rules = allowlist_of(&iso);
        assert_eq!(
            rules.decide(Some("api.anthropic.com"), ANY_IP, 443, Proto::Tcp),
            Decision::Allow { rule_index: None }
        );
        assert!(!allows(rules, "api.anthropic.com", 80));
        assert!(!allows(rules, "example.com", 443));
        assert!(!rules.name_allowed("example.com"));
        assert_eq!(rules.cidr_covers(ANY_IP, 443, Proto::Tcp), None);
    }

    #[test]
    fn allowlist_adds_the_config_rules() {
        let default = with_rules(
            spec(Some("sandbox"), Some("allowlist")),
            r#"[{ "host": "*.pypi.org", "ports": [443] }]"#,
        );
        let iso = run(&default, Some(&spec(None, Some("allowlist")))).unwrap();
        let rules = allowlist_of(&iso);
        assert!(allows(rules, "files.pypi.org", 443));
        assert!(allows(rules, "api.anthropic.com", 443));
        assert!(!allows(rules, "example.com", 443));
    }

    #[test]
    fn config_rules_only_matter_for_allowlist() {
        let bad = r#"[{ "host": "bad host", "ports": "*" }]"#;
        for network in [None, Some("none"), Some("full")] {
            let default = with_rules(spec(Some("sandbox"), network), bad);
            let iso = run(&default, None).unwrap();
            assert!(iso.allowlist.is_none());
        }
    }

    #[test]
    fn invalid_config_rule_is_rejected_with_its_index() {
        let default = with_rules(
            spec(Some("sandbox"), Some("allowlist")),
            r#"[{ "host": "ok.example", "ports": "*" }, { "host": "bad host", "ports": "*" }]"#,
        );
        let err = run(&default, None).unwrap_err();
        assert!(
            matches!(
                &err,
                IsolationError::InvalidAllowlist(RuleError::Host { index: 1, .. })
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("rule 1"), "{err}");
    }

    #[test]
    fn llm_endpoint_follows_the_base_url() {
        let endpoint = |url| llm_endpoint(url).unwrap();
        let expect = |host: &str, port| AlwaysAllow {
            host: host.to_owned(),
            port,
        };
        assert_eq!(endpoint(None), expect("api.anthropic.com", 443));
        assert_eq!(
            endpoint(Some("https://proxy.example")),
            expect("proxy.example", 443)
        );
        assert_eq!(
            endpoint(Some("https://proxy.example:8443/v1")),
            expect("proxy.example", 8443)
        );
        assert_eq!(
            endpoint(Some("http://proxy.example/anthropic")),
            expect("proxy.example", 80)
        );
        assert_eq!(
            endpoint(Some("HTTP://user:pw@proxy.example:8080?x=1")),
            expect("proxy.example", 8080)
        );
        for url in [
            "proxy.example",
            "ftp://proxy.example",
            "https://",
            "https://proxy.example:",
            "https://proxy.example:99999",
        ] {
            let err = llm_endpoint(Some(url)).unwrap_err();
            assert!(matches!(err, IsolationError::InvalidBaseUrl), "{url}");
            assert!(!err.to_string().contains(url), "{err}");
        }
    }

    #[test]
    fn allowlist_admits_the_base_url_endpoint() {
        let default = spec(Some("sandbox"), Some("allowlist"));
        let iso = resolve(
            &default,
            None,
            Path::new(WORKSPACE),
            Some("http://proxy.example:8080"),
        )
        .unwrap();
        let rules = allowlist_of(&iso);
        assert!(allows(rules, "proxy.example", 8080));
        assert!(!allows(rules, "api.anthropic.com", 443));
    }

    #[test]
    fn isolation_spec_reads_network_allow() {
        let spec: IsolationSpec = serde_json::from_str(
            r#"{ "mode": "sandbox", "network": { "mode": "allowlist",
                 "allow": [{ "host": "pypi.org", "ports": [443] }] } }"#,
        )
        .unwrap();
        let allow = spec.network.unwrap().allow.unwrap();
        assert_eq!(allow.len(), 1);
        assert_eq!(allow[0].host.as_deref(), Some("pypi.org"));
    }

    #[test]
    fn unknown_values_are_errors() {
        let err = run(&spec(Some("jail"), None), None).unwrap_err();
        assert!(matches!(err, IsolationError::UnknownMode(m) if m == "jail"));

        let err = run(&spec(Some("sandbox"), Some("partial")), None).unwrap_err();
        assert!(matches!(err, IsolationError::UnknownNetworkMode(m) if m == "partial"));

        let err = run(
            &IsolationSpec::default(),
            Some(&spec(Some("Sandbox"), None)),
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::UnknownMode(_)));
    }

    #[test]
    fn invalid_paths_are_errors() {
        for path in [
            "data/cache",
            WORKSPACE,
            "/var/lib/gino-agent/",
            "/var/lib/./gino-agent",
            "/var/lib",
            "/",
            "/data/../etc",
        ] {
            let err = path_error(path);
            assert!(
                matches!(&err, IsolationError::InvalidPath { path: p, .. } if p == Path::new(path)),
                "{path}: {err:?}"
            );
        }
        // Write paths are validated too.
        let err = run(
            &with_paths(spec(Some("sandbox"), None), &[], &["/var"]),
            None,
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::InvalidPath { .. }));
    }

    #[test]
    fn sibling_and_nested_paths_are_ok() {
        let default = with_paths(
            spec(Some("sandbox"), None),
            &["/data/cache", "/var/lib/gino-agent-other"],
            &["/var/lib/gino-agent/sub/", "/srv/./out"],
        );
        let iso = run(&default, None).unwrap();
        assert_eq!(
            iso.read_paths,
            [
                PathBuf::from("/data/cache"),
                PathBuf::from("/var/lib/gino-agent-other")
            ]
        );
        assert_eq!(
            iso.write_paths,
            [
                PathBuf::from("/var/lib/gino-agent/sub"),
                PathBuf::from("/srv/out")
            ]
        );
    }

    #[test]
    fn off_skips_path_validation() {
        let default = with_paths(IsolationSpec::default(), &["relative", "/"], &[WORKSPACE]);
        assert_eq!(run(&default, None).unwrap(), Isolation::off());

        let default = allowing(with_paths(
            spec(Some("sandbox"), Some("bogus")),
            &["relative"],
            &[],
        ));
        assert_eq!(
            run(&default, Some(&spec(Some("off"), None))).unwrap(),
            Isolation::off()
        );
    }

    #[test]
    fn override_from_meta_without_isolation_is_none() {
        for meta in [
            None,
            Some(""),
            Some("not json"),
            Some("[1, 2]"),
            Some("\"text\""),
            Some("{}"),
            Some(r#"{ "other": 1 }"#),
        ] {
            assert_eq!(override_from_meta(meta).unwrap(), None, "{meta:?}");
        }
    }

    #[test]
    fn override_from_meta_parses_the_isolation_block() {
        let spec = override_from_meta(Some(
            r#"{ "label": "x", "isolation": { "mode": "sandbox", "network": { "mode": "full" } } }"#,
        ))
        .unwrap()
        .unwrap();
        assert_eq!(spec.mode.as_deref(), Some("sandbox"));
        assert_eq!(spec.network.unwrap().mode.as_deref(), Some("full"));
        assert!(spec.read_paths.is_none());
    }

    #[test]
    fn override_from_meta_rejects_a_malformed_isolation_value() {
        for meta in [
            r#"{ "isolation": "sandbox" }"#,
            r#"{ "isolation": { "mode": 1 } }"#,
            r#"{ "isolation": { "network": "full" } }"#,
            r#"{ "isolation": null }"#,
        ] {
            let err = override_from_meta(Some(meta)).unwrap_err();
            assert!(matches!(err, IsolationError::InvalidTaskMeta(_)), "{meta}");
        }
    }

    const KEYS: [&str; 2] = ["ENGINE_CONFIG_DIR", "ENGINE_OWN_DIR"];

    fn direct(cwd: &Path) -> Launch {
        Launch::direct(
            "engine".into(),
            vec!["-p".to_owned(), "--flag".to_owned()],
            vec![
                ("ENGINE_TOKEN".to_owned(), "secret".into()),
                ("ENGINE_CONFIG_DIR".to_owned(), "/ws/engine-config".into()),
                ("ENGINE_OWN_DIR".to_owned(), "/ws/engine-config".into()),
                ("GINO_TASK_ID".to_owned(), "task-1".into()),
            ],
            cwd.to_path_buf(),
        )
    }

    fn ctx<'a>(
        default: &'a IsolationSpec,
        task: Option<&'a IsolationSpec>,
        work_dir: Option<&'a Path>,
        self_exe: Option<&'a Path>,
    ) -> LaunchContext<'a> {
        LaunchContext {
            isolation: default,
            task_override: task,
            workspace_dir: Path::new(WORKSPACE),
            thread_id: Some("th-1"),
            work_dir,
            self_exe,
            global_config_dir: None,
            config_dir_keys: &KEYS,
        }
    }

    /// A temp dir holding an executable `engine` stub, and a host whose `PATH`
    /// finds it.
    fn stub_host() -> (tempfile::TempDir, PathBuf, SandboxHost) {
        let tmp = tempfile::tempdir().unwrap();
        let bin_dir = std::fs::canonicalize(tmp.path()).unwrap().join("bin");
        std::fs::create_dir(&bin_dir).unwrap();
        let engine = bin_dir.join("engine");
        std::fs::write(&engine, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&engine, std::fs::Permissions::from_mode(0o755)).unwrap();
        let host = SandboxHost::fixture(Some(bin_dir.into_os_string()));
        (tmp, engine, host)
    }

    fn sandboxed() -> Isolation {
        run(&spec(Some("sandbox"), None), None).unwrap()
    }

    fn env_values<'a>(launch: &'a Launch, key: &str) -> Vec<&'a OsString> {
        launch
            .env
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v)
            .collect()
    }

    #[test]
    fn off_launch_is_the_direct_launch() {
        let d = direct(Path::new("/ws/threads/t1/project"));
        let off = IsolationSpec::default();
        assert_eq!(launch(d.clone(), &ctx(&off, None, None, None)).unwrap(), d);

        let default = allowing(spec(Some("sandbox"), None));
        let task = spec(Some("off"), None);
        assert_eq!(
            launch(d.clone(), &ctx(&default, Some(&task), None, None)).unwrap(),
            d
        );
        assert!(!d.sandboxed);
    }

    #[test]
    fn root_euid_is_refused() {
        let err = check_not_root(0).unwrap_err();
        assert!(matches!(err, IsolationError::Unavailable(_)), "{err:?}");
        assert_eq!(
            err.to_string(),
            "engine sandbox is unavailable: the agent runs as root; run it as an unprivileged user"
        );
        check_not_root(1000).unwrap();
    }

    #[test]
    fn allowlist_launch_is_refused_only_for_a_root_agent() {
        let d = direct(Path::new("/ws/threads/t1/project"));
        let task = spec(Some("sandbox"), Some("allowlist"));
        let off = IsolationSpec::default();
        let ctx = ctx(&off, Some(&task), None, None);
        let err = launch_as(d.clone(), &ctx, 0).unwrap_err();
        assert!(
            matches!(&err, EngineError::Isolation(IsolationError::Unavailable(m)) if m.contains("root")),
            "{err:?}"
        );
        // Past the root check only host facts can fail it, never the mode.
        if let Err(err) = launch_as(d, &ctx, 1000) {
            assert!(
                !matches!(
                    &err,
                    EngineError::Isolation(IsolationError::NotSupported(_))
                ),
                "{err:?}"
            );
        }
    }

    fn allowlisted(base_url: Option<&str>) -> Isolation {
        resolve(
            &spec(Some("sandbox"), Some("allowlist")),
            None,
            Path::new(WORKSPACE),
            base_url,
        )
        .unwrap()
    }

    #[test]
    fn loopback_llm_endpoint_becomes_the_gateway_forward() {
        for (url, target) in [
            ("http://127.0.0.1:8080/api/llm", "127.0.0.1:8080"),
            ("http://127.0.0.5:8080", "127.0.0.5:8080"),
            ("https://[::1]:9443/v1", "[::1]:9443"),
            ("http://LOCALHOST.:8080/x", "127.0.0.1:8080"),
            ("http://localhost/x", "127.0.0.1:80"),
        ] {
            let iso = allowlisted(Some(url));
            let target: std::net::SocketAddr = target.parse().unwrap();
            let gateway = iso.llm_gateway.unwrap();
            assert_eq!(gateway.port, target.port(), "{url}");
            if url.contains("localhost") || url.contains("LOCALHOST") {
                assert!(gateway.target.ip().is_loopback(), "{url}");
            } else {
                assert_eq!(gateway.target, target, "{url}");
            }
            // Loopback is never a rule.
            let rules = allowlist_of(&iso);
            assert!(!rules.name_allowed("localhost"), "{url}");
            assert!(matches!(
                rules.decide(None, target.ip(), target.port(), Proto::Tcp),
                Decision::Deny { .. }
            ));
        }
        assert!(allowlisted(None).llm_gateway.is_none());
        assert!(
            allowlisted(Some("https://proxy.example"))
                .llm_gateway
                .is_none()
        );
        let err = resolve(
            &spec(Some("sandbox"), Some("allowlist")),
            None,
            Path::new(WORKSPACE),
            Some("http://user:pw@127.0.0.1:0/x"),
        )
        .unwrap_err();
        assert!(matches!(err, IsolationError::InvalidBaseUrl), "{err:?}");
        assert!(!err.to_string().contains("pw"), "{err}");

        // Full and none have no gateway.
        for network in ["full", "none"] {
            let iso = resolve(
                &spec(Some("sandbox"), Some(network)),
                None,
                Path::new(WORKSPACE),
                Some("http://127.0.0.1:8080"),
            )
            .unwrap();
            assert!(iso.llm_gateway.is_none(), "{network}");
        }
    }

    #[test]
    fn ip_literal_llm_endpoint_is_an_always_allowed_address() {
        for (url, ip, port) in [
            ("http://172.17.0.1:8080/llm", "172.17.0.1", 8080),
            ("https://[fd00::1]/v1", "fd00::1", 443),
        ] {
            let iso = allowlisted(Some(url));
            assert!(iso.llm_gateway.is_none());
            let rules = allowlist_of(&iso);
            let ip = ip.parse().unwrap();
            assert_eq!(
                rules.decide(None, ip, port, Proto::Tcp),
                Decision::Allow { rule_index: None },
                "{url}"
            );
            assert!(matches!(
                rules.decide(None, ip, port + 1, Proto::Tcp),
                Decision::Deny { .. }
            ));
        }
    }

    #[test]
    fn gateway_url_swaps_only_the_host() {
        for (url, want) in [
            (
                "http://127.0.0.1:8080/api/llm",
                "http://10.0.2.2:8080/api/llm",
            ),
            (
                "http://localhost:3000/x?y=1#z",
                "http://10.0.2.2:3000/x?y=1#z",
            ),
            ("https://[::1]:9443", "https://10.0.2.2:9443"),
            ("http://[::1]/v1", "http://10.0.2.2/v1"),
            ("http://localhost?q", "http://10.0.2.2?q"),
            (
                "http://user:p%40ss@127.0.0.1:8080/llm",
                "http://user:p%40ss@10.0.2.2:8080/llm",
            ),
        ] {
            assert_eq!(super::gateway_url(url).as_deref(), Some(want), "{url}");
        }
        assert_eq!(super::gateway_url("localhost:8080"), None);
    }

    fn launch_with_llm_url(base_url: &str, network: &str) -> (tempfile::TempDir, Launch) {
        let (tmp, _, host) = stub_host();
        let work_dir = tmp.path().join("threads/t1");
        let mut d = direct(&work_dir.join("project"));
        d.env
            .push(("ANTHROPIC_BASE_URL".to_owned(), base_url.into()));
        let isolation = resolve(
            &spec(Some("sandbox"), Some(network)),
            None,
            Path::new(WORKSPACE),
            Some(base_url),
        )
        .unwrap();
        let off = IsolationSpec::default();
        let agent = Path::new("/opt/gino/gino-agent");
        let wrapped = wrap(
            d,
            &ctx(&off, None, Some(&work_dir), Some(agent)),
            &isolation,
            &host,
        )
        .unwrap();
        (tmp, wrapped)
    }

    #[test]
    fn allowlist_launch_hands_over_the_network_and_rewrites_a_loopback_llm_url() {
        let (_tmp, wrapped) =
            launch_with_llm_url("http://u:pw@127.0.0.1:8080/api/llm?k=v", "allowlist");
        assert_eq!(
            env_values(&wrapped, "ANTHROPIC_BASE_URL"),
            ["http://u:pw@10.0.2.2:8080/api/llm?k=v"]
        );
        let args: Vec<&str> = wrapped.args.iter().map(|a| a.to_str().unwrap()).collect();
        let init = args.iter().position(|a| *a == "sandbox-init").unwrap();
        assert_eq!(
            args[init + 1..init + 4],
            ["--ctl-sock", "/run/gino/ctl.sock", "--"]
        );
        assert!(args.contains(&"CAP_NET_ADMIN"));

        let net = wrapped.net.as_deref().unwrap();
        let dir = net.dir.0.clone();
        let meta = std::fs::metadata(&dir).unwrap();
        assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        assert_eq!(
            std::fs::read_to_string(dir.join("resolv.conf")).unwrap(),
            "nameserver 10.0.2.2\n"
        );
        assert!(dir.join("ctl.sock").exists());
        assert_eq!(net.exit.thread_id, "th-1");
        assert_eq!(
            net.exit.gateway.map(|gw| gw.target),
            Some("127.0.0.1:8080".parse().unwrap())
        );
        let ctl = format!("{}", dir.join("ctl.sock").display());
        let bind = args.iter().position(|a| *a == ctl).unwrap();
        assert_eq!(args[bind + 1], "/run/gino/ctl.sock");
        // The dir goes with the launch.
        drop(wrapped);
        assert!(!dir.exists());
    }

    fn handover() -> (std::path::PathBuf, std::sync::Arc<super::NetHandover>) {
        let rules = std::sync::Arc::new(RuleSet::compile(&[], &[]).unwrap());
        let net =
            super::NetHandover::prepare(crate::net::exit::ExitConfig::new("th-1", rules)).unwrap();
        (net.dir.0.join("ctl.sock"), std::sync::Arc::new(net))
    }

    fn sandbox_stub(script: &str) -> tokio::process::Child {
        tokio::process::Command::new("sh")
            .args(["-c", script])
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }

    #[tokio::test]
    async fn handover_without_a_tun_device_kills_the_sandbox() {
        let (sock, net) = handover();
        let mut child = sandbox_stub("sleep 30");
        let connect = tokio::spawn(async move {
            // Connect and close without sending a descriptor.
            drop(tokio::net::UnixStream::connect(sock).await.unwrap());
        });
        let err = super::start_network(Some(net), &mut child)
            .await
            .unwrap_err();
        connect.await.unwrap();
        assert!(
            matches!(&err, EngineError::Isolation(IsolationError::Network(m)) if m.contains("TUN")),
            "{err:?}"
        );
        let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
    }

    #[tokio::test]
    async fn handover_reports_an_early_sandbox_exit_with_its_stderr() {
        let (_, net) = handover();
        let mut child = sandbox_stub("echo 'sandbox-init: creating the TUN device' >&2; exit 125");
        let err = super::start_network(Some(net), &mut child)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("exited"), "{msg}");
        assert!(msg.contains("creating the TUN device"), "{msg}");
        assert!(
            super::start_network(None, &mut child)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn allowlist_launch_keeps_a_non_loopback_llm_url() {
        let url = "https://proxy.example:8443/v1";
        let (_tmp, wrapped) = launch_with_llm_url(url, "allowlist");
        assert_eq!(env_values(&wrapped, "ANTHROPIC_BASE_URL"), [url]);
        assert_eq!(wrapped.net.as_deref().unwrap().exit.gateway, None);
    }

    #[test]
    fn full_and_none_launches_keep_the_llm_url_and_have_no_handover() {
        let url = "http://127.0.0.1:8080/api/llm";
        for network in ["full", "none"] {
            let (_tmp, wrapped) = launch_with_llm_url(url, network);
            assert_eq!(
                env_values(&wrapped, "ANTHROPIC_BASE_URL"),
                [url],
                "{network}"
            );
            assert!(wrapped.net.is_none());
            assert!(
                !wrapped
                    .args
                    .iter()
                    .any(|a| a == "--ctl-sock" || a == "CAP_NET_ADMIN")
            );
        }
    }

    #[test]
    fn root_euid_fails_only_sandboxed_launches() {
        let d = direct(Path::new("/ws/threads/t1/project"));
        let off = IsolationSpec::default();
        assert_eq!(
            launch_as(d.clone(), &ctx(&off, None, None, None), 0).unwrap(),
            d
        );

        let sandbox = spec(Some("sandbox"), None);
        let err = launch_as(d, &ctx(&sandbox, None, None, None), 0).unwrap_err();
        assert!(
            matches!(&err, EngineError::Isolation(IsolationError::Unavailable(m)) if m.contains("root")),
            "{err:?}"
        );
    }

    #[test]
    fn unsupported_or_unknown_isolation_fails_the_launch() {
        let d = direct(Path::new(""));
        for (default, task) in [
            (spec(Some("vm"), None), None),
            (IsolationSpec::default(), Some(spec(Some("jail"), None))),
            (IsolationSpec::default(), Some(spec(Some("vm"), None))),
            (spec(Some("sandbox"), None), Some(spec(Some("off"), None))),
        ] {
            let err = launch(d.clone(), &ctx(&default, task.as_ref(), None, None)).unwrap_err();
            assert!(matches!(err, EngineError::Isolation(_)), "{err:?}");
        }
    }

    #[test]
    fn sandboxed_launch_wraps_the_engine_in_bwrap_and_sandbox_init() {
        let (tmp, engine, host) = stub_host();
        let work_dir = tmp.path().join("threads/t1");
        let cwd = work_dir.join("project");
        let agent = Path::new("/opt/gino/gino-agent");
        let off = IsolationSpec::default();
        let wrapped = wrap(
            direct(&cwd),
            &ctx(&off, None, Some(&work_dir), Some(agent)),
            &sandboxed(),
            &host,
        )
        .unwrap();

        assert!(wrapped.sandboxed);
        assert_eq!(wrapped.program, Path::new("/usr/bin/bwrap"));
        assert_eq!(wrapped.cwd, cwd);
        let tail: Vec<OsString> = [
            agent.as_os_str(),
            "sandbox-init".as_ref(),
            "--".as_ref(),
            engine.as_os_str(),
            "-p".as_ref(),
            "--flag".as_ref(),
        ]
        .map(OsString::from)
        .to_vec();
        assert_eq!(wrapped.args[wrapped.args.len() - tail.len() - 1], "--");
        assert_eq!(wrapped.args[wrapped.args.len() - tail.len()..], tail[..]);
        let chdir = wrapped.args.iter().position(|a| a == "--chdir").unwrap();
        assert_eq!(wrapped.args[chdir + 1], cwd.as_os_str());

        let home = work_dir.join(".engine-home");
        assert!(home.is_dir());
        for key in KEYS {
            assert_eq!(env_values(&wrapped, key), [home.as_os_str()], "{key}");
        }
        assert_eq!(env_values(&wrapped, "ENGINE_TOKEN"), ["secret"]);
        assert_eq!(env_values(&wrapped, "GINO_TASK_ID"), ["task-1"]);
        assert_eq!(env_values(&wrapped, "HOME"), ["/home/gino"]);
        assert_eq!(env_values(&wrapped, "TMPDIR"), ["/tmp"]);
        assert_eq!(env_values(&wrapped, "GINO_SANDBOX"), ["1"]);
        assert_eq!(
            env_values(&wrapped, "GINO_ARCHIVE_DIR"),
            [work_dir.join("conversations").as_os_str()]
        );
    }

    #[test]
    fn sandboxed_launch_drops_the_workspace_dir() {
        let (tmp, _, host) = stub_host();
        let work_dir = tmp.path().join("threads/t1");
        let mut d = direct(&work_dir.join("project"));
        d.env.push((
            "GINO_WORKSPACE_DIR".to_owned(),
            "/var/lib/gino-agent".into(),
        ));
        let off = IsolationSpec::default();
        let agent = Path::new("/opt/gino/gino-agent");

        let unwrapped = launch(d.clone(), &ctx(&off, None, Some(&work_dir), Some(agent))).unwrap();
        assert_eq!(
            env_values(&unwrapped, "GINO_WORKSPACE_DIR"),
            ["/var/lib/gino-agent"]
        );

        let wrapped = wrap(
            d,
            &ctx(&off, None, Some(&work_dir), Some(agent)),
            &sandboxed(),
            &host,
        )
        .unwrap();
        assert!(env_values(&wrapped, "GINO_WORKSPACE_DIR").is_empty());
        assert_eq!(env_values(&wrapped, "GINO_TASK_ID"), ["task-1"]);
    }

    #[test]
    fn sandboxed_launch_without_a_cwd_runs_in_the_work_dir() {
        let (tmp, _, host) = stub_host();
        let work_dir = tmp.path().join("t1");
        let off = IsolationSpec::default();
        let wrapped = wrap(
            direct(Path::new("")),
            &ctx(
                &off,
                None,
                Some(&work_dir),
                Some(Path::new("/opt/gino-agent")),
            ),
            &sandboxed(),
            &host,
        )
        .unwrap();
        let chdir = wrapped.args.iter().position(|a| a == "--chdir").unwrap();
        assert_eq!(wrapped.args[chdir + 1], work_dir.as_os_str());
        assert!(wrapped.cwd.as_os_str().is_empty());
    }

    #[test]
    fn sandboxed_launch_needs_work_dir_agent_and_engine_binary() {
        let (tmp, _, mut host) = stub_host();
        let work_dir = tmp.path().join("t1");
        let agent = Path::new("/opt/gino-agent");
        let off = IsolationSpec::default();
        let err = wrap(
            direct(&work_dir),
            &ctx(&off, None, None, Some(agent)),
            &sandboxed(),
            &host,
        )
        .unwrap_err();
        assert!(
            matches!(err, EngineError::Isolation(IsolationError::Missing(_))),
            "{err:?}"
        );

        let err = wrap(
            direct(&work_dir),
            &ctx(&off, None, Some(&work_dir), None),
            &sandboxed(),
            &host,
        )
        .unwrap_err();
        assert!(
            matches!(err, EngineError::Isolation(IsolationError::Missing(_))),
            "{err:?}"
        );

        host.path_env = None;
        let err = wrap(
            direct(&work_dir),
            &ctx(&off, None, Some(&work_dir), Some(agent)),
            &sandboxed(),
            &host,
        )
        .unwrap_err();
        assert!(
            matches!(&err, EngineError::Spawn(e) if e.kind() == std::io::ErrorKind::NotFound),
            "{err:?}"
        );
        assert!(!work_dir.exists());
    }

    #[test]
    fn children_list_parsing() {
        assert_eq!(first_child("4242 4243 \n"), Some(4242));
        assert_eq!(first_child("17"), Some(17));
        assert_eq!(first_child(""), None);
        assert_eq!(first_child("x 1"), None);
    }

    #[test]
    fn stat_parsing_skips_odd_command_names() {
        assert_eq!(stat_ppid("123 (bwrap) S 100 123 123 0 -1"), Some(100));
        assert_eq!(stat_ppid("124 (a b) c) R 123 1 1"), Some(123));
        assert_eq!(stat_ppid("125 (x)"), None);
        assert_eq!(stat_ppid("garbage"), None);
    }

    #[test]
    fn sandbox_init_pid_finds_the_direct_child() {
        let mut parent = std::process::Command::new("sh")
            .args(["-c", "sleep 30 & wait"])
            .spawn()
            .unwrap();
        let parent_pid = i32::try_from(parent.id()).unwrap();
        let mut child = None;
        for _ in 0..200 {
            child = sandbox_init_pid(parent_pid);
            if child.is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let child = child.unwrap();
        let stat = std::fs::read_to_string(format!("/proc/{child}/stat")).unwrap();
        assert_eq!(stat_ppid(&stat), Some(parent_pid));
        let _ = std::process::Command::new("kill")
            .arg(child.to_string())
            .status();
        let _ = parent.kill();
        let _ = parent.wait();

        assert_eq!(sandbox_init_pid(i32::MAX), None);
    }
}
