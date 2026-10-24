//! Bubblewrap launcher for sandboxed engine runs.
//!
//! [`bwrap_args`] is a pure argv builder over a [`SandboxLayout`] and a
//! [`HostRoot`] snapshot; [`host_root`] and [`ensure_available`] are the only
//! parts that look at the real host.

use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use super::{IsolationError, NetworkMode};

/// Host root entries that are mirrored into the sandbox, in argv order.
const ROOT_DIRS: [&str; 4] = ["/bin", "/lib", "/lib64", "/sbin"];

/// How long the availability probe may run before it counts as failed.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Bytes of probe stderr kept in the error detail.
const STDERR_TAIL: usize = 512;

/// Shape of one host root entry such as `/bin`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RootEntry {
    /// A symlink, with its raw (unresolved) target.
    Symlink(PathBuf),
    /// A real directory.
    Dir,
    /// Missing, or something else.
    Absent,
}

/// Snapshot of the host's root layout that the sandbox mirrors.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HostRoot {
    /// `/bin`, `/lib`, `/lib64`, `/sbin`, in [`ROOT_DIRS`] order.
    pub(crate) entries: [RootEntry; 4],
    /// Where `/etc/resolv.conf` resolves to, when that is outside `/etc`.
    pub(crate) resolv_conf_outside_etc: Option<PathBuf>,
}

/// Which provisioning globals exist under the global config dir.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProvisionedGlobals {
    /// `<global>/CLAUDE.md` is a file.
    pub(crate) claude_md: bool,
    /// `<global>/skills` is a directory.
    pub(crate) skills: bool,
}

impl ProvisionedGlobals {
    /// Inspect `global_config_dir` on the host.
    pub(crate) fn inspect(global_config_dir: Option<&Path>) -> Self {
        global_config_dir.map_or_else(Self::default, |dir| Self {
            claude_md: dir.join("CLAUDE.md").is_file(),
            skills: dir.join("skills").is_dir(),
        })
    }
}

/// Everything the sandbox mounts for one engine run (all paths absolute).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SandboxLayout {
    /// This thread's work dir, mounted read-write.
    pub(crate) work_dir: PathBuf,
    /// Engine cwd, inside `work_dir`.
    pub(crate) cwd: PathBuf,
    /// Per-thread engine home, `engine_home(work_dir)`.
    pub(crate) engine_home: PathBuf,
    /// Provisioning dir holding the global `CLAUDE.md` and `skills/`.
    pub(crate) global_config_dir: Option<PathBuf>,
    /// Which globals exist under `global_config_dir`.
    pub(crate) globals: ProvisionedGlobals,
    /// Canonical engine binary.
    pub(crate) engine_binary: PathBuf,
    /// The gino-agent binary, run inside as `sandbox-init`.
    pub(crate) agent_exe: PathBuf,
    /// Sandbox `$HOME` (a tmpfs).
    pub(crate) home: PathBuf,
    pub(crate) network: NetworkMode,
    /// Extra host paths mounted read-only.
    pub(crate) read_paths: Vec<PathBuf>,
    /// Extra host paths mounted read-write.
    pub(crate) write_paths: Vec<PathBuf>,
}

impl SandboxLayout {
    /// Global `CLAUDE.md` bind source, when it will be mounted.
    fn global_claude_md(&self) -> Option<PathBuf> {
        self.global_config_dir
            .as_ref()
            .filter(|_| self.globals.claude_md)
            .map(|dir| dir.join("CLAUDE.md"))
    }

    /// Global `skills/` bind source, when it will be mounted.
    fn global_skills(&self) -> Option<PathBuf> {
        self.global_config_dir
            .as_ref()
            .filter(|_| self.globals.skills)
            .map(|dir| dir.join("skills"))
    }
}

/// Per-thread engine home: `<work_dir>/.engine-home`.
pub(crate) fn engine_home(work_dir: &Path) -> PathBuf {
    work_dir.join(".engine-home")
}

/// Inspect the host's `/bin` `/lib` `/lib64` `/sbin` and `/etc/resolv.conf`.
pub(crate) fn host_root() -> HostRoot {
    let entries = ROOT_DIRS.map(|dir| match fs::symlink_metadata(dir) {
        Ok(meta) if meta.file_type().is_symlink() => {
            fs::read_link(dir).map_or(RootEntry::Absent, RootEntry::Symlink)
        }
        Ok(meta) if meta.is_dir() => RootEntry::Dir,
        _ => RootEntry::Absent,
    });
    let resolv = Path::new("/etc/resolv.conf");
    let resolv_conf_outside_etc = fs::symlink_metadata(resolv)
        .ok()
        .filter(|meta| meta.file_type().is_symlink())
        .and_then(|_| fs::canonicalize(resolv).ok())
        .filter(|target| !target.starts_with("/etc"));
    HostRoot {
        entries,
        resolv_conf_outside_etc,
    }
}

fn push(args: &mut Vec<OsString>, items: &[&OsStr]) {
    args.extend(items.iter().map(|s| (*s).to_owned()));
}

fn ro_bind(args: &mut Vec<OsString>, path: &Path) {
    push(
        args,
        &["--ro-bind".as_ref(), path.as_os_str(), path.as_os_str()],
    );
}

fn rw_bind(args: &mut Vec<OsString>, path: &Path) {
    push(
        args,
        &["--bind".as_ref(), path.as_os_str(), path.as_os_str()],
    );
}

/// Mirror `/usr` and the `/bin`-style root entries.
fn push_root(args: &mut Vec<OsString>, host: &HostRoot) {
    ro_bind(args, Path::new("/usr"));
    for (dir, entry) in ROOT_DIRS.iter().zip(&host.entries) {
        match entry {
            RootEntry::Symlink(target) => {
                push(
                    args,
                    &["--symlink".as_ref(), target.as_os_str(), dir.as_ref()],
                );
            }
            RootEntry::Dir => ro_bind(args, Path::new(dir)),
            RootEntry::Absent => {}
        }
    }
}

/// Build the full `bwrap` argv (without the program) that runs `inner`.
pub(crate) fn bwrap_args(
    layout: &SandboxLayout,
    host: &HostRoot,
    inner: &[OsString],
) -> Vec<OsString> {
    let mut args = Vec::new();
    push(&mut args, &["--unshare-all".as_ref()]);
    if layout.network == NetworkMode::Full {
        push(&mut args, &["--share-net".as_ref()]);
    }
    push(
        &mut args,
        &[
            "--uid".as_ref(),
            "0".as_ref(),
            "--gid".as_ref(),
            "0".as_ref(),
            "--die-with-parent".as_ref(),
            "--new-session".as_ref(),
            "--as-pid-1".as_ref(),
            "--proc".as_ref(),
            "/proc".as_ref(),
            "--dev".as_ref(),
            "/dev".as_ref(),
            "--tmpfs".as_ref(),
            "/tmp".as_ref(),
            "--tmpfs".as_ref(),
            layout.home.as_os_str(),
        ],
    );

    push_root(&mut args, host);
    ro_bind(&mut args, Path::new("/etc"));
    if layout.network == NetworkMode::Full
        && let Some(resolved) = &host.resolv_conf_outside_etc
    {
        ro_bind(&mut args, resolved);
    }

    if let Some(install_dir) = layout.engine_binary.parent()
        && !install_dir.starts_with("/usr")
    {
        ro_bind(&mut args, install_dir);
    }
    if !layout.agent_exe.starts_with("/usr") {
        ro_bind(&mut args, &layout.agent_exe);
    }

    for path in &layout.read_paths {
        ro_bind(&mut args, path);
    }
    rw_bind(&mut args, &layout.work_dir);
    for path in &layout.write_paths {
        rw_bind(&mut args, path);
    }

    if let Some(src) = layout.global_claude_md() {
        let dest = layout.engine_home.join("CLAUDE.md");
        push(
            &mut args,
            &["--ro-bind".as_ref(), src.as_os_str(), dest.as_os_str()],
        );
    }
    if let Some(src) = layout.global_skills() {
        let dest = layout.engine_home.join("skills");
        push(
            &mut args,
            &["--ro-bind".as_ref(), src.as_os_str(), dest.as_os_str()],
        );
    }

    push(
        &mut args,
        &["--chdir".as_ref(), layout.cwd.as_os_str(), "--".as_ref()],
    );
    args.extend(inner.iter().cloned());
    args
}

/// Create the engine home and the mount targets for the globals bound into it.
pub(crate) fn prepare_engine_home(layout: &SandboxLayout) -> std::io::Result<()> {
    fs::create_dir_all(&layout.engine_home)?;
    if layout.global_claude_md().is_some() {
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(layout.engine_home.join("CLAUDE.md"))?;
    }
    if layout.global_skills().is_some() {
        fs::create_dir_all(layout.engine_home.join("skills"))?;
    }
    Ok(())
}

fn is_executable_file(path: &Path) -> bool {
    fs::metadata(path).is_ok_and(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
}

/// Resolve `binary` to a canonical path: paths with a slash directly, bare
/// names via the first executable match on `path_env`.
pub(crate) fn resolve_binary(binary: &Path, path_env: Option<&OsStr>) -> Option<PathBuf> {
    if binary.components().count() > 1 || binary.is_absolute() {
        return fs::canonicalize(binary).ok();
    }
    std::env::split_paths(path_env?)
        .filter(|dir| !dir.as_os_str().is_empty())
        .map(|dir| dir.join(binary))
        .find(|candidate| is_executable_file(candidate))
        .and_then(|found| fs::canonicalize(found).ok())
}

/// Argv (without the program) of the availability probe.
fn probe_args(host: &HostRoot) -> Vec<OsString> {
    let mut args = Vec::new();
    push(
        &mut args,
        &["--unshare-all".as_ref(), "--die-with-parent".as_ref()],
    );
    push_root(&mut args, host);
    push(
        &mut args,
        &[
            "--proc".as_ref(),
            "/proc".as_ref(),
            "--dev".as_ref(),
            "/dev".as_ref(),
            "--".as_ref(),
            "/usr/bin/true".as_ref(),
        ],
    );
    args
}

/// Last [`STDERR_TAIL`] bytes of `stderr`, lossily decoded and trimmed.
fn stderr_tail(stderr: &[u8]) -> String {
    let start = stderr.len().saturating_sub(STDERR_TAIL);
    String::from_utf8_lossy(&stderr[start..]).trim().to_owned()
}

/// Run the probe once; resolves `bwrap` on `path_env`.
fn probe(path_env: Option<&OsStr>, host: &HostRoot) -> Result<PathBuf, String> {
    let bwrap = resolve_binary(Path::new("bwrap"), path_env)
        .ok_or_else(|| "bwrap not found on PATH".to_owned())?;
    let mut child = Command::new(&bwrap)
        .args(probe_args(host))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("failed to run {}: {e}", bwrap.display()))?;

    let deadline = Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(format!(
                    "bwrap probe timed out after {}s",
                    PROBE_TIMEOUT.as_secs()
                ));
            }
            Err(e) => return Err(format!("failed to wait for bwrap probe: {e}")),
        }
    };
    if status.success() {
        return Ok(bwrap);
    }
    let mut stderr = Vec::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_end(&mut stderr);
    }
    Err(format!(
        "bwrap probe failed ({status}): {}",
        stderr_tail(&stderr)
    ))
}

/// Resolve and probe `bwrap` once per process; the outcome is cached.
pub(crate) fn ensure_available() -> Result<PathBuf, IsolationError> {
    static PROBE: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    PROBE
        .get_or_init(|| probe(std::env::var_os("PATH").as_deref(), &host_root()))
        .clone()
        .map_err(IsolationError::Unavailable)
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::fs;
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::{Path, PathBuf};

    use super::{
        HostRoot, ProvisionedGlobals, ROOT_DIRS, RootEntry, SandboxLayout, bwrap_args, engine_home,
        ensure_available, host_root, prepare_engine_home, probe, probe_args, resolve_binary,
    };
    use crate::sandbox::NetworkMode;

    fn host() -> HostRoot {
        HostRoot {
            entries: [
                RootEntry::Symlink("usr/bin".into()),
                RootEntry::Dir,
                RootEntry::Absent,
                RootEntry::Symlink("usr/sbin".into()),
            ],
            resolv_conf_outside_etc: Some("/run/systemd/resolve/stub-resolv.conf".into()),
        }
    }

    fn layout() -> SandboxLayout {
        let work_dir = PathBuf::from("/ws/threads/t1");
        SandboxLayout {
            cwd: work_dir.join("repo"),
            engine_home: engine_home(&work_dir),
            work_dir,
            global_config_dir: Some("/ws/global".into()),
            globals: ProvisionedGlobals {
                claude_md: true,
                skills: true,
            },
            engine_binary: "/opt/claude/bin/claude".into(),
            agent_exe: "/opt/gino/gino-agent".into(),
            home: "/home/sandbox".into(),
            network: NetworkMode::None,
            read_paths: vec!["/data/ro1".into(), "/data/ro2".into()],
            write_paths: vec!["/data/rw1".into()],
        }
    }

    fn inner() -> Vec<OsString> {
        ["/opt/gino/gino-agent", "sandbox-init", "--", "claude", "-p"]
            .map(OsString::from)
            .to_vec()
    }

    fn args(layout: &SandboxLayout, host: &HostRoot) -> Vec<String> {
        bwrap_args(layout, host, &inner())
            .into_iter()
            .map(|a| a.into_string().unwrap())
            .collect()
    }

    /// Index of the first occurrence of the `window` sequence.
    fn find(args: &[String], window: &[&str]) -> Option<usize> {
        args.windows(window.len())
            .position(|w| w.iter().zip(window).all(|(a, b)| a == b))
    }

    fn pos(args: &[String], window: &[&str]) -> usize {
        find(args, window).unwrap_or_else(|| panic!("{window:?} not in {args:?}"))
    }

    #[test]
    fn engine_home_is_under_the_work_dir() {
        assert_eq!(
            engine_home(Path::new("/ws/threads/t1")),
            Path::new("/ws/threads/t1/.engine-home")
        );
    }

    #[test]
    fn process_and_namespace_flags_lead() {
        let a = args(&layout(), &host());
        assert_eq!(
            a[..13],
            [
                "--unshare-all",
                "--uid",
                "0",
                "--gid",
                "0",
                "--die-with-parent",
                "--new-session",
                "--as-pid-1",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--tmpfs",
            ]
        );
        assert!(!a.iter().any(|x| x == "--share-net" || x == "--unshare-net"));
    }

    #[test]
    fn no_network_skips_share_net_and_resolv_conf() {
        let a = args(&layout(), &host());
        assert!(!a.iter().any(|x| x == "--share-net"));
        assert!(!a.iter().any(|x| x.contains("resolv")));
    }

    #[test]
    fn full_network_shares_net_and_binds_resolv_conf_outside_etc() {
        let mut l = layout();
        l.network = NetworkMode::Full;
        let a = args(&l, &host());
        assert_eq!(a[..2], ["--unshare-all", "--share-net"]);
        assert!(!a.iter().any(|x| x == "--unshare-net"));
        let resolv = "/run/systemd/resolve/stub-resolv.conf";
        pos(&a, &["--ro-bind", resolv, resolv]);
        // The resolved file is the only thing under /run.
        assert_eq!(a.iter().filter(|x| x.starts_with("/run")).count(), 2);

        let mut h = host();
        h.resolv_conf_outside_etc = None;
        let a = args(&l, &h);
        assert!(!a.iter().any(|x| x.contains("resolv")));
    }

    #[test]
    fn root_entries_follow_the_host_shape() {
        let a = args(&layout(), &host());
        pos(&a, &["--symlink", "usr/bin", "/bin"]);
        pos(&a, &["--ro-bind", "/lib", "/lib"]);
        assert!(!a.iter().any(|x| x == "/lib64"));
        pos(&a, &["--symlink", "usr/sbin", "/sbin"]);
        pos(&a, &["--ro-bind", "/usr", "/usr"]);
        pos(&a, &["--ro-bind", "/etc", "/etc"]);

        let h = HostRoot {
            entries: [
                RootEntry::Dir,
                RootEntry::Symlink("usr/lib".into()),
                RootEntry::Symlink("usr/lib64".into()),
                RootEntry::Absent,
            ],
            resolv_conf_outside_etc: None,
        };
        let a = args(&layout(), &h);
        pos(&a, &["--ro-bind", "/bin", "/bin"]);
        pos(&a, &["--symlink", "usr/lib", "/lib"]);
        pos(&a, &["--symlink", "usr/lib64", "/lib64"]);
        assert!(!a.iter().any(|x| x == "/sbin"));
    }

    #[test]
    fn engine_install_dir_and_agent_exe_are_bound_outside_usr() {
        let a = args(&layout(), &host());
        pos(&a, &["--ro-bind", "/opt/claude/bin", "/opt/claude/bin"]);
        pos(
            &a,
            &["--ro-bind", "/opt/gino/gino-agent", "/opt/gino/gino-agent"],
        );

        let mut l = layout();
        l.engine_binary = "/usr/local/bin/claude".into();
        l.agent_exe = "/usr/bin/gino-agent".into();
        let a = args(&l, &host());
        let binds: Vec<_> = a
            .iter()
            .enumerate()
            .filter(|(_, x)| *x == "--ro-bind")
            .map(|(i, _)| a[i + 1].as_str())
            .collect();
        assert!(!binds.iter().any(|b| *b != "/usr" && b.starts_with("/usr")));
        assert!(!binds.contains(&"/opt/claude/bin"));
    }

    #[test]
    fn mounts_are_in_order_and_flavoured() {
        let a = args(&layout(), &host());
        let tmp = pos(&a, &["--tmpfs", "/tmp"]);
        let home = pos(&a, &["--tmpfs", "/home/sandbox"]);
        let first_bind = a
            .iter()
            .position(|x| x == "--ro-bind" || x == "--bind" || x == "--symlink")
            .unwrap();
        assert!(tmp < home && home < first_bind);

        let ro1 = pos(&a, &["--ro-bind", "/data/ro1", "/data/ro1"]);
        let ro2 = pos(&a, &["--ro-bind", "/data/ro2", "/data/ro2"]);
        let work = pos(&a, &["--bind", "/ws/threads/t1", "/ws/threads/t1"]);
        let rw1 = pos(&a, &["--bind", "/data/rw1", "/data/rw1"]);
        let md = pos(
            &a,
            &[
                "--ro-bind",
                "/ws/global/CLAUDE.md",
                "/ws/threads/t1/.engine-home/CLAUDE.md",
            ],
        );
        let skills = pos(
            &a,
            &[
                "--ro-bind",
                "/ws/global/skills",
                "/ws/threads/t1/.engine-home/skills",
            ],
        );
        assert!(ro1 < ro2 && ro2 < work && work < rw1 && rw1 < md && md < skills);
        assert!(find(&a, &["--bind", "/data/ro1"]).is_none());
        assert!(find(&a, &["--ro-bind", "/data/rw1"]).is_none());
    }

    #[test]
    fn globals_are_bound_only_when_present() {
        let mut l = layout();
        l.globals.claude_md = false;
        let a = args(&l, &host());
        assert!(!a.iter().any(|x| x.ends_with("CLAUDE.md")));
        pos(&a, &["--ro-bind", "/ws/global/skills"]);

        l.globals = ProvisionedGlobals {
            claude_md: true,
            skills: false,
        };
        let a = args(&l, &host());
        assert!(!a.iter().any(|x| x.ends_with("skills")));
        pos(&a, &["--ro-bind", "/ws/global/CLAUDE.md"]);

        l.globals.skills = true;
        l.global_config_dir = None;
        let a = args(&l, &host());
        assert!(!a.iter().any(|x| x.contains(".engine-home")));
    }

    #[test]
    fn chdir_separator_and_inner_come_last() {
        let a = args(&layout(), &host());
        let tail = ["--chdir", "/ws/threads/t1/repo", "--"]
            .into_iter()
            .map(str::to_owned)
            .chain(inner().into_iter().map(|s| s.into_string().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(a[a.len() - tail.len()..], tail[..]);
        assert_eq!(a.iter().filter(|x| *x == "--chdir").count(), 1);
    }

    #[test]
    fn nothing_else_of_the_host_is_mounted() {
        let a = args(&layout(), &host());
        for forbidden in ["/run", "/var", "/ws", "/ws/threads", "/ws/global", "/root"] {
            assert!(!a.iter().any(|x| x == forbidden), "{forbidden} in {a:?}");
        }
        assert!(
            !a.iter()
                .any(|x| x.starts_with("/run") || x.starts_with("/var"))
        );
        assert!(!a.iter().any(|x| x.contains("agent.db")));
    }

    #[test]
    fn prepare_engine_home_creates_only_needed_targets() {
        let tmp = tempfile::tempdir().unwrap();
        let mut l = layout();
        l.work_dir = tmp.path().join("t1");
        l.engine_home = engine_home(&l.work_dir);
        prepare_engine_home(&l).unwrap();
        let md = l.engine_home.join("CLAUDE.md");
        assert!(md.is_file());
        assert_eq!(fs::metadata(&md).unwrap().len(), 0);
        assert!(l.engine_home.join("skills").is_dir());
        // Idempotent and non-truncating.
        fs::write(&md, "x").unwrap();
        prepare_engine_home(&l).unwrap();
        assert_eq!(fs::read_to_string(&md).unwrap(), "x");

        let tmp = tempfile::tempdir().unwrap();
        l.work_dir = tmp.path().join("t2");
        l.engine_home = engine_home(&l.work_dir);
        l.globals = ProvisionedGlobals::default();
        prepare_engine_home(&l).unwrap();
        assert!(l.engine_home.is_dir());
        assert_eq!(fs::read_dir(&l.engine_home).unwrap().count(), 0);
    }

    #[test]
    fn provisioned_globals_inspects_the_dir() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(
            ProvisionedGlobals::inspect(Some(tmp.path())),
            ProvisionedGlobals::default()
        );
        fs::write(tmp.path().join("CLAUDE.md"), "").unwrap();
        fs::create_dir(tmp.path().join("skills")).unwrap();
        assert_eq!(
            ProvisionedGlobals::inspect(Some(tmp.path())),
            ProvisionedGlobals {
                claude_md: true,
                skills: true
            }
        );
        assert_eq!(
            ProvisionedGlobals::inspect(None),
            ProvisionedGlobals::default()
        );
    }

    #[test]
    fn resolve_binary_searches_path_and_canonicalizes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap();
        let real_dir = root.join("real");
        let link_dir = root.join("links");
        let empty_dir = root.join("empty");
        for d in [&real_dir, &link_dir, &empty_dir] {
            fs::create_dir(d).unwrap();
        }
        let tool = real_dir.join("tool");
        fs::write(&tool, "#!/bin/sh\n").unwrap();
        fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
        symlink(&tool, link_dir.join("tool")).unwrap();
        // Not executable: skipped.
        fs::write(empty_dir.join("tool"), "").unwrap();

        let path = std::env::join_paths([&empty_dir, &link_dir]).unwrap();
        assert_eq!(
            resolve_binary(Path::new("tool"), Some(&path)),
            Some(tool.clone())
        );
        assert_eq!(
            resolve_binary(&link_dir.join("tool"), None),
            Some(tool.clone())
        );
        assert_eq!(resolve_binary(Path::new("tool"), None), None);
        assert_eq!(resolve_binary(Path::new("missing"), Some(&path)), None);
        assert_eq!(
            resolve_binary(Path::new("tool"), Some(OsStr::new(""))),
            None
        );
        assert_eq!(resolve_binary(&root.join("nope/tool"), None), None);
    }

    #[test]
    fn probe_argv_shape() {
        let a: Vec<String> = probe_args(&host())
            .into_iter()
            .map(|s| s.into_string().unwrap())
            .collect();
        assert_eq!(
            a,
            [
                "--unshare-all",
                "--die-with-parent",
                "--ro-bind",
                "/usr",
                "/usr",
                "--symlink",
                "usr/bin",
                "/bin",
                "--ro-bind",
                "/lib",
                "/lib",
                "--symlink",
                "usr/sbin",
                "/sbin",
                "--proc",
                "/proc",
                "--dev",
                "/dev",
                "--",
                "/usr/bin/true",
            ]
        );
    }

    #[test]
    fn missing_bwrap_is_unavailable() {
        let err = probe(Some(OsStr::new("")), &host()).unwrap_err();
        assert_eq!(err, "bwrap not found on PATH");
        let err = probe(None, &host()).unwrap_err();
        assert_eq!(err, "bwrap not found on PATH");
        let err = crate::sandbox::IsolationError::Unavailable(err);
        assert!(err.to_string().contains("not found on PATH"), "{err}");
    }

    #[test]
    fn host_root_matches_the_filesystem() {
        let h = host_root();
        for (dir, entry) in ROOT_DIRS.iter().zip(&h.entries) {
            let meta = fs::symlink_metadata(dir);
            match entry {
                RootEntry::Symlink(target) => {
                    assert_eq!(&fs::read_link(dir).unwrap(), target, "{dir}");
                }
                RootEntry::Dir => assert!(meta.unwrap().is_dir(), "{dir}"),
                RootEntry::Absent => assert!(
                    !meta.is_ok_and(|m| m.is_dir() || m.file_type().is_symlink()),
                    "{dir}"
                ),
            }
        }
        if let Some(resolved) = h.resolv_conf_outside_etc {
            assert!(!resolved.starts_with("/etc"));
        }
    }

    #[test]
    fn ensure_available_is_cached_and_fails_as_unavailable() {
        let first = ensure_available();
        let second = ensure_available();
        match (&first, &second) {
            (Ok(a), Ok(b)) => assert_eq!(a, b),
            (Err(a), Err(b)) => {
                assert!(matches!(a, crate::sandbox::IsolationError::Unavailable(_)));
                assert_eq!(a.to_string(), b.to_string());
            }
            _ => panic!("cached probe changed: {first:?} vs {second:?}"),
        }
    }
}
