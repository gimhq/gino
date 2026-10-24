//! Engine isolation: config types and per-run resolution.
//!
//! The config default (`engine.isolation`) and an optional task override
//! (`tasks.meta.isolation`) are kept as raw strings and resolved per run into an
//! [`Isolation`]. Unknown or unsupported values fail the run at resolve time
//! instead of breaking parsing of the whole config overlay.
//!
//! [`launch`] turns an engine's direct launch into the one a run actually
//! spawns: unchanged when off, wrapped in `bwrap` + `sandbox-init` when
//! sandboxed.

pub(crate) mod bwrap;
pub(crate) mod init;

use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;

use serde::Deserialize;
use thiserror::Error;

use self::bwrap::{
    HostRoot, ProvisionedGlobals, SandboxLayout, bwrap_args, engine_home, host_root,
    prepare_engine_home, resolve_binary,
};
use crate::engine::EngineError;

/// Sandbox `$HOME` when the daemon has none.
const DEFAULT_SANDBOX_HOME: &str = "/home/gino";

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
}

/// Resolved isolation for one run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Isolation {
    pub(crate) mode: IsolationMode,
    pub(crate) network: NetworkMode,
    /// Extra host paths mounted read-only.
    pub(crate) read_paths: Vec<PathBuf>,
    /// Extra host paths mounted read-write.
    pub(crate) write_paths: Vec<PathBuf>,
}

impl Isolation {
    /// No isolation: the engine runs directly on the host.
    pub(crate) fn off() -> Self {
        Self {
            mode: IsolationMode::Off,
            network: NetworkMode::None,
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
    #[error("invalid isolation in task meta: {0}")]
    InvalidTaskMeta(#[source] serde_json::Error),
    #[error("engine sandbox is unavailable: {0}")]
    Unavailable(String),
    #[error("engine sandbox needs {0}")]
    Missing(&'static str),
}

/// Resolve the effective isolation for a run.
///
/// The override's `mode` and `network.mode` win over the default's; extra paths
/// come from the config default only. Unless the default sets
/// `allowTaskOverride`, an override may only tighten the default. When the
/// resolved mode is off nothing else is validated.
pub(crate) fn resolve(
    default: &IsolationSpec,
    task_override: Option<&IsolationSpec>,
    workspace_dir: &Path,
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
        Some("allowlist") => {
            return Err(IsolationError::NotSupported(
                "isolation.network.mode: allowlist".into(),
            ));
        }
        Some(other) => return Err(IsolationError::UnknownNetworkMode(other.to_owned())),
    };

    let workspace = normalize(workspace_dir);
    let read_paths = validate_paths(default.read_paths.as_deref(), &workspace)?;
    let write_paths = validate_paths(default.write_paths.as_deref(), &workspace)?;

    Ok(Isolation {
        mode,
        network,
        read_paths,
        write_paths,
    })
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
pub(crate) fn override_from_meta(
    meta: Option<&str>,
) -> Result<Option<IsolationSpec>, IsolationError> {
    let Some(Ok(serde_json::Value::Object(mut map))) = meta.map(serde_json::from_str) else {
        return Ok(None);
    };
    let Some(value) = map.remove("isolation") else {
        return Ok(None);
    };
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
    let isolation = resolve(ctx.isolation, ctx.task_override, ctx.workspace_dir)?;
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
    };
    prepare_engine_home(&layout).map_err(EngineError::Spawn)?;

    let mut inner: Vec<OsString> = vec![
        layout.agent_exe.clone().into(),
        "sandbox-init".into(),
        "--".into(),
        layout.engine_binary.clone().into(),
    ];
    inner.extend(direct.args);

    // The host workspace root is not mounted, so the engine must not see it.
    let mut env = direct.env;
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
        override_from_meta, resolve, sandbox_init_pid, stat_ppid, wrap,
    };
    use crate::engine::EngineError;

    const WORKSPACE: &str = "/var/lib/gino-agent";

    fn spec(mode: Option<&str>, network: Option<&str>) -> IsolationSpec {
        IsolationSpec {
            mode: mode.map(str::to_owned),
            network: network.map(|n| NetworkSpec {
                mode: Some(n.to_owned()),
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
        resolve(default, task, Path::new(WORKSPACE))
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
        use NetworkMode::{Full, None as NoNet};

        let sandbox = |network| spec(Some("sandbox"), network);
        let net = |network| spec(None, Some(network));

        // Tightening.
        assert_not_supported(outcome(&sandbox(Some("full")), &net("allowlist")));
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
        assert_not_supported(outcome(&allow("none"), &net("allowlist")));
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
                (None, Ok(NetworkMode::None)),
                (Some("none"), Ok(NetworkMode::None)),
                (Some("full"), Ok(NetworkMode::Full)),
                (Some("allowlist"), Err(())),
            ] {
                let result = outcome(&default, &spec(Some("sandbox"), network));
                match expected {
                    Ok(net) => assert_eq!(result.unwrap(), (IsolationMode::Sandbox, net)),
                    Err(()) => assert_not_supported(result),
                }
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
    fn vm_and_allowlist_are_not_supported_yet() {
        let err = run(&spec(Some("vm"), None), None).unwrap_err();
        assert!(matches!(err, IsolationError::NotSupported(_)), "{err:?}");
        assert!(err.to_string().ends_with("is not supported yet"), "{err}");

        let err = run(&spec(Some("sandbox"), Some("allowlist")), None).unwrap_err();
        assert!(matches!(err, IsolationError::NotSupported(_)), "{err:?}");
        assert!(err.to_string().ends_with("is not supported yet"), "{err}");
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
            (spec(Some("sandbox"), Some("allowlist")), None),
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
