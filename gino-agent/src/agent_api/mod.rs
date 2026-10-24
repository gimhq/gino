//! Local agent API: a newline-delimited JSON protocol on a Unix socket under the
//! workspace (`run/agent.sock`). Same-user, same-namespace peers get operator
//! access; an engine run gets a per-run token scoped to its own task.

#[allow(dead_code)] // used by the CLI client
pub(crate) mod client;
mod methods;
pub(crate) mod protocol;
mod scope;
mod server;
#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

pub(crate) use methods::ApiContext;
#[allow(unused_imports)] // used by the CLI client / executor
pub(crate) use scope::{RunScope, RunToken, TokenRegistry};
pub(crate) use server::AgentApiServer;

/// The API socket under `workspace_dir`.
pub(crate) fn socket_path(workspace_dir: &Path) -> PathBuf {
    workspace_dir.join("run").join("agent.sock")
}
