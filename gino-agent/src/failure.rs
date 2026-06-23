//! Failure taxonomy, classification, retry policy and resume-safety
//! (agent-architecture §6; modelled on Multica `pkg/taskfailure`).
//!
//! Every non-success thread execution is tagged with a [`FailureReason`] that
//! lands in `threads.failure_reason` and rides the wire on
//! [`thread_result.failure_reason`](crate::protocol::v2::ThreadResultData). The
//! taxonomy splits into two groups whose distinction drives policy:
//!
//! * **`runner.*`** — the runner/platform side is at fault (a timeout, an offline
//!   runtime, a malformed request the runner built). These are the **only**
//!   retryable reasons (§6: "retry only `runner.*`").
//! * **`agent.*`** — the engine/provider side failed (auth, quota, a crashed
//!   process, unparseable output). The runner cannot fix these by retrying, so a
//!   fresh attempt would just fail the same way.
//!
//! Two orthogonal policy bits hang off a reason:
//!
//! * [`FailureReason::is_retryable`] — `runner.*` while `attempt < max_attempts`.
//! * [`FailureReason::resume_safe`] — whether the engine session may be resumed on
//!   the next attempt/turn. A *resume-unsafe* reason (e.g. `runner.invalid_request`
//!   or `agent.context_overflow`) poisons the session, so the executor drops
//!   `engine_session_id` and the next turn starts fresh (§6).
//!
//! [`classify`] maps a backend's free-text error into a reason. It is deliberately
//! best-effort (substring heuristics, most-specific-first); callers that *know* the
//! cause (the watchdog setting `runner.timeout`) construct the reason directly
//! rather than round-tripping through text.
//!
//! ## Strangler coexistence
//!
//! Added for lane D (execution) but not yet wired by the runner (lane G); several
//! reasons/variants are constructed only by tests until then, so the whole module
//! is `allow(dead_code)`, mirroring [`crate::protocol::v2`].
#![allow(dead_code)]

/// A `runner.*` failure — the runner/platform side is at fault. Retryable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RunnerFailure {
    /// The thread sat queued past its dispatch deadline.
    QueuedExpired,
    /// The runtime/engine host was offline when the thread was due.
    RuntimeOffline,
    /// A dispatch/run watchdog tripped (`dispatched > Td` / `running > Tr`) or the
    /// engine went inactive.
    Timeout,
    /// The run hit the runner-imposed turn/iteration cap.
    IterationLimit,
    /// The runner refused the work (policy / safety gate).
    AgentBlocked,
    /// The runner built a request the backend rejected as malformed.
    InvalidRequest,
}

/// An `agent.*` failure — the engine/provider side failed. Not retryable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentFailure {
    /// Authentication rejected (bad/expired API key, 401/403).
    ProviderAuth,
    /// Out of quota / credit / billing rejected the call.
    ProviderQuota,
    /// Rate limited (429 / too many requests).
    ProviderRateLimit,
    /// Provider 5xx / overloaded (500/502/503/529).
    ProviderServerError,
    /// Network/transport failure reaching the provider (DNS, refused, reset, TLS).
    ProviderNetwork,
    /// The engine process crashed / exited abnormally / could not be spawned.
    ProcessFailure,
    /// The engine produced empty or unparseable output (no usable result).
    EmptyOrUnparseableOutput,
    /// The conversation exceeded the model's context window.
    ContextOverflow,
    /// The requested model is unknown / unavailable.
    ModelUnavailable,
    /// Required engine configuration is missing.
    MissingConfig,
    /// Unclassified — the catch-all when nothing else matched.
    Unknown,
}

/// The full `threads.failure_reason` taxonomy (agent-architecture §6), split into
/// the retryable `runner.*` group and the non-retryable `agent.*` group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FailureReason {
    /// A `runner.*` reason — runner/platform fault, retryable.
    Runner(RunnerFailure),
    /// An `agent.*` reason — engine/provider fault, not retryable.
    Agent(AgentFailure),
}

impl FailureReason {
    /// The dotted wire string (`runner.timeout`, `agent.provider_auth`, …) stored
    /// in `threads.failure_reason` and sent on `thread_result.failure_reason`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Runner(r) => match r {
                RunnerFailure::QueuedExpired => "runner.queued_expired",
                RunnerFailure::RuntimeOffline => "runner.runtime_offline",
                RunnerFailure::Timeout => "runner.timeout",
                RunnerFailure::IterationLimit => "runner.iteration_limit",
                RunnerFailure::AgentBlocked => "runner.agent_blocked",
                RunnerFailure::InvalidRequest => "runner.invalid_request",
            },
            Self::Agent(a) => match a {
                AgentFailure::ProviderAuth => "agent.provider_auth",
                AgentFailure::ProviderQuota => "agent.provider_quota",
                AgentFailure::ProviderRateLimit => "agent.provider_rate_limit",
                AgentFailure::ProviderServerError => "agent.provider_server_error",
                AgentFailure::ProviderNetwork => "agent.provider_network",
                AgentFailure::ProcessFailure => "agent.process_failure",
                AgentFailure::EmptyOrUnparseableOutput => "agent.empty_or_unparseable_output",
                AgentFailure::ContextOverflow => "agent.context_overflow",
                AgentFailure::ModelUnavailable => "agent.model_unavailable",
                AgentFailure::MissingConfig => "agent.missing_config",
                AgentFailure::Unknown => "agent.unknown",
            },
        }
    }

    /// Parse a dotted wire string back into a reason (inverse of [`Self::as_str`]),
    /// or `None` if it is not a known reason.
    pub(crate) fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "runner.queued_expired" => Self::Runner(RunnerFailure::QueuedExpired),
            "runner.runtime_offline" => Self::Runner(RunnerFailure::RuntimeOffline),
            "runner.timeout" => Self::Runner(RunnerFailure::Timeout),
            "runner.iteration_limit" => Self::Runner(RunnerFailure::IterationLimit),
            "runner.agent_blocked" => Self::Runner(RunnerFailure::AgentBlocked),
            "runner.invalid_request" => Self::Runner(RunnerFailure::InvalidRequest),
            "agent.provider_auth" => Self::Agent(AgentFailure::ProviderAuth),
            "agent.provider_quota" => Self::Agent(AgentFailure::ProviderQuota),
            "agent.provider_rate_limit" => Self::Agent(AgentFailure::ProviderRateLimit),
            "agent.provider_server_error" => Self::Agent(AgentFailure::ProviderServerError),
            "agent.provider_network" => Self::Agent(AgentFailure::ProviderNetwork),
            "agent.process_failure" => Self::Agent(AgentFailure::ProcessFailure),
            "agent.empty_or_unparseable_output" => {
                Self::Agent(AgentFailure::EmptyOrUnparseableOutput)
            }
            "agent.context_overflow" => Self::Agent(AgentFailure::ContextOverflow),
            "agent.model_unavailable" => Self::Agent(AgentFailure::ModelUnavailable),
            "agent.missing_config" => Self::Agent(AgentFailure::MissingConfig),
            "agent.unknown" => Self::Agent(AgentFailure::Unknown),
            _ => return None,
        })
    }

    /// `true` for the `runner.*` group.
    pub(crate) fn is_runner(self) -> bool {
        matches!(self, Self::Runner(_))
    }

    /// Whether a fresh attempt may help. Only `runner.*` reasons are retryable
    /// (§6); `agent.*` reasons would fail identically on retry. The caller still
    /// gates on `attempt < max_attempts`.
    pub(crate) fn is_retryable(self) -> bool {
        self.is_runner()
    }

    /// Whether the engine session may be resumed on the next attempt/turn. A
    /// resume-unsafe reason poisons the session — the malformed request or the
    /// overflowing context would recur — so the executor drops `engine_session_id`
    /// and the next turn starts fresh (§6).
    pub(crate) fn resume_safe(self) -> bool {
        !matches!(
            self,
            Self::Runner(RunnerFailure::InvalidRequest)
                | Self::Agent(AgentFailure::ContextOverflow)
        )
    }
}

impl std::fmt::Display for FailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Substring heuristics, matched **most-specific-first**: the first group whose
/// any-needle is present wins. Needles are lowercase; the haystack is lowercased
/// once before matching. Ordered so a more specific signal (a context-window
/// message, an iteration cap) is checked before a broader one.
const PATTERNS: &[(&[&str], FailureReason)] = &[
    // Auth — checked first; "forbidden"/"403" are auth, not a policy block.
    (
        &[
            "401",
            "403",
            "unauthorized",
            "forbidden",
            "authentication",
            "invalid api key",
            "invalid x-api-key",
            "authentication_error",
        ],
        FailureReason::Agent(AgentFailure::ProviderAuth),
    ),
    // Quota / billing.
    (
        &[
            "402",
            "quota",
            "billing",
            "insufficient credit",
            "out of credit",
            "payment required",
            "exceeded your current quota",
        ],
        FailureReason::Agent(AgentFailure::ProviderQuota),
    ),
    // Rate limiting.
    (
        &[
            "429",
            "rate limit",
            "rate_limit",
            "ratelimit",
            "too many requests",
        ],
        FailureReason::Agent(AgentFailure::ProviderRateLimit),
    ),
    // Provider 5xx / overloaded.
    (
        &[
            "500",
            "502",
            "503",
            "529",
            "overloaded",
            "internal server error",
            "bad gateway",
            "service unavailable",
            "server_error",
        ],
        FailureReason::Agent(AgentFailure::ProviderServerError),
    ),
    // Context window — before the generic network/timeout bucket.
    (
        &[
            "context window",
            "context length",
            "context_length_exceeded",
            "maximum context",
            "prompt is too long",
            "too many tokens",
            "context overflow",
        ],
        FailureReason::Agent(AgentFailure::ContextOverflow),
    ),
    // Model availability.
    (
        &[
            "model not found",
            "unknown model",
            "model unavailable",
            "no such model",
            "model does not exist",
            "not_found_error",
        ],
        FailureReason::Agent(AgentFailure::ModelUnavailable),
    ),
    // Missing configuration.
    (
        &[
            "missing config",
            "not configured",
            "missing required",
            "missing env",
            "no api key",
            "api key not",
        ],
        FailureReason::Agent(AgentFailure::MissingConfig),
    ),
    // Network / transport.
    (
        &[
            "connection refused",
            "connection reset",
            "econnrefused",
            "broken pipe",
            "network",
            "dns",
            "tls",
            "ssl",
            "unreachable",
            "timed out",
            "connection closed",
        ],
        FailureReason::Agent(AgentFailure::ProviderNetwork),
    ),
    // Runner-side iteration cap.
    (
        &[
            "max turns",
            "maximum turns",
            "iteration limit",
            "max iterations",
            "turn limit",
        ],
        FailureReason::Runner(RunnerFailure::IterationLimit),
    ),
    // Runner-side policy block.
    (
        &[
            "blocked",
            "refused by policy",
            "not permitted",
            "permission denied",
        ],
        FailureReason::Runner(RunnerFailure::AgentBlocked),
    ),
    // Runner-built malformed request.
    (
        &[
            "400",
            "invalid request",
            "invalid_request",
            "bad request",
            "malformed",
        ],
        FailureReason::Runner(RunnerFailure::InvalidRequest),
    ),
    // Empty / unparseable engine output.
    (
        &[
            "empty output",
            "no output",
            "unparseable",
            "failed to parse",
            "parse error",
            "unexpected end of",
            "invalid json",
        ],
        FailureReason::Agent(AgentFailure::EmptyOrUnparseableOutput),
    ),
    // Process / spawn failure.
    (
        &[
            "exit code",
            "exited with",
            "killed",
            "signal",
            "panicked",
            "segfault",
            "core dumped",
            "command not found",
            "no such file",
            "spawn",
        ],
        FailureReason::Agent(AgentFailure::ProcessFailure),
    ),
];

/// Map a backend's free-text error into a [`FailureReason`]. Best-effort,
/// most-specific-first substring matching; an unmatched message folds into
/// `agent.unknown` so the host always sees a reason.
pub(crate) fn classify(raw: &str) -> FailureReason {
    let haystack = raw.to_ascii_lowercase();
    for (needles, reason) in PATTERNS {
        if needles.iter().any(|needle| haystack.contains(needle)) {
            return *reason;
        }
    }
    FailureReason::Agent(AgentFailure::Unknown)
}

#[cfg(test)]
mod tests {
    use super::{AgentFailure, FailureReason, RunnerFailure, classify};

    #[test]
    fn wire_strings_round_trip_for_every_reason() {
        let all = [
            FailureReason::Runner(RunnerFailure::QueuedExpired),
            FailureReason::Runner(RunnerFailure::RuntimeOffline),
            FailureReason::Runner(RunnerFailure::Timeout),
            FailureReason::Runner(RunnerFailure::IterationLimit),
            FailureReason::Runner(RunnerFailure::AgentBlocked),
            FailureReason::Runner(RunnerFailure::InvalidRequest),
            FailureReason::Agent(AgentFailure::ProviderAuth),
            FailureReason::Agent(AgentFailure::ProviderQuota),
            FailureReason::Agent(AgentFailure::ProviderRateLimit),
            FailureReason::Agent(AgentFailure::ProviderServerError),
            FailureReason::Agent(AgentFailure::ProviderNetwork),
            FailureReason::Agent(AgentFailure::ProcessFailure),
            FailureReason::Agent(AgentFailure::EmptyOrUnparseableOutput),
            FailureReason::Agent(AgentFailure::ContextOverflow),
            FailureReason::Agent(AgentFailure::ModelUnavailable),
            FailureReason::Agent(AgentFailure::MissingConfig),
            FailureReason::Agent(AgentFailure::Unknown),
        ];
        for reason in all {
            let wire = reason.as_str();
            assert!(
                wire.starts_with("runner.") || wire.starts_with("agent."),
                "{wire} must be dotted-grouped"
            );
            assert_eq!(
                FailureReason::from_wire(wire),
                Some(reason),
                "round-trip {wire}"
            );
            assert_eq!(reason.to_string(), wire, "Display matches as_str");
        }
        assert_eq!(FailureReason::from_wire("nonsense.reason"), None);
    }

    #[test]
    fn only_runner_reasons_are_retryable() {
        assert!(FailureReason::Runner(RunnerFailure::Timeout).is_retryable());
        assert!(FailureReason::Runner(RunnerFailure::InvalidRequest).is_retryable());
        assert!(!FailureReason::Agent(AgentFailure::ProviderAuth).is_retryable());
        assert!(!FailureReason::Agent(AgentFailure::ContextOverflow).is_retryable());
    }

    #[test]
    fn resume_unsafe_reasons_poison_the_session() {
        // Poisoning reasons: a malformed request and an overflowing context recur
        // on resume, so the session must not be reused.
        assert!(!FailureReason::Runner(RunnerFailure::InvalidRequest).resume_safe());
        assert!(!FailureReason::Agent(AgentFailure::ContextOverflow).resume_safe());
        // Transient/external failures leave the session resumable.
        assert!(FailureReason::Runner(RunnerFailure::Timeout).resume_safe());
        assert!(FailureReason::Agent(AgentFailure::ProviderRateLimit).resume_safe());
        assert!(FailureReason::Agent(AgentFailure::ProviderNetwork).resume_safe());
    }

    #[test]
    fn classify_maps_agent_group_text() {
        assert_eq!(
            classify("HTTP 401 Unauthorized: invalid x-api-key"),
            FailureReason::Agent(AgentFailure::ProviderAuth)
        );
        assert_eq!(
            classify("You exceeded your current quota, please check billing"),
            FailureReason::Agent(AgentFailure::ProviderQuota)
        );
        assert_eq!(
            classify("rate limit exceeded (429)"),
            FailureReason::Agent(AgentFailure::ProviderRateLimit)
        );
        assert_eq!(
            classify("Error 529: the model is overloaded"),
            FailureReason::Agent(AgentFailure::ProviderServerError)
        );
        assert_eq!(
            classify("connection refused while reaching api.anthropic.com"),
            FailureReason::Agent(AgentFailure::ProviderNetwork)
        );
        assert_eq!(
            classify("claude process exited with code 1"),
            FailureReason::Agent(AgentFailure::ProcessFailure)
        );
        assert_eq!(
            classify("failed to parse engine output: unexpected end of stream"),
            FailureReason::Agent(AgentFailure::EmptyOrUnparseableOutput)
        );
        assert_eq!(
            classify("prompt is too long: exceeds the model context window"),
            FailureReason::Agent(AgentFailure::ContextOverflow)
        );
        assert_eq!(
            classify("model not found: claude-does-not-exist"),
            FailureReason::Agent(AgentFailure::ModelUnavailable)
        );
        assert_eq!(
            classify("missing required ANTHROPIC_API_KEY, not configured"),
            FailureReason::Agent(AgentFailure::MissingConfig)
        );
        assert_eq!(
            classify("something nobody anticipated"),
            FailureReason::Agent(AgentFailure::Unknown)
        );
    }

    #[test]
    fn classify_maps_runner_group_text() {
        assert_eq!(
            classify("run aborted: max turns reached"),
            FailureReason::Runner(RunnerFailure::IterationLimit)
        );
        assert_eq!(
            classify("request blocked by safety policy"),
            FailureReason::Runner(RunnerFailure::AgentBlocked)
        );
        assert_eq!(
            classify("HTTP 400 Bad Request: malformed body"),
            FailureReason::Runner(RunnerFailure::InvalidRequest)
        );
    }
}
