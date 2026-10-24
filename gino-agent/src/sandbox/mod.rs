//! Engine isolation: config types and per-run resolution.
//!
//! The config default (`engine.isolation`) and an optional task override
//! (`tasks.meta.isolation`) are kept as raw strings and resolved per run into an
//! [`Isolation`]. Unknown or unsupported values fail the run at resolve time
//! instead of breaking parsing of the whole config overlay.

// Not wired into engine spawning yet.
#![cfg_attr(not(test), allow(dead_code))]

pub(crate) mod bwrap;
pub(crate) mod init;

use std::path::{Component, Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

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
    #[error("invalid isolation path `{}`: {reason}", path.display())]
    InvalidPath { path: PathBuf, reason: &'static str },
    #[error("invalid isolation in task meta: {0}")]
    InvalidTaskMeta(#[source] serde_json::Error),
    #[error("engine sandbox is unavailable: {0}")]
    // Raised by the sandbox availability probe, not by resolution.
    #[allow(dead_code)]
    Unavailable(String),
}

/// Resolve the effective isolation for a run.
///
/// The override's `mode` and `network.mode` win over the default's; extra paths
/// come from the config default only. When the resolved mode is off nothing
/// else is validated.
pub(crate) fn resolve(
    default: &IsolationSpec,
    task_override: Option<&IsolationSpec>,
    workspace_dir: &Path,
) -> Result<Isolation, IsolationError> {
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
    use std::path::{Path, PathBuf};

    use super::{
        Isolation, IsolationError, IsolationMode, IsolationSpec, NetworkMode, NetworkSpec,
        override_from_meta, resolve,
    };

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
        let default = spec(Some("sandbox"), Some("none"));
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

        let default = with_paths(spec(Some("sandbox"), Some("bogus")), &["relative"], &[]);
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
}
