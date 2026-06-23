//! HTTP client (PLAN-003 §2 / §2.1, decision 5; agent-architecture §10).
//!
//! The WebSocket is the primary channel; HTTP is retained for the endpoints that
//! sit outside the persistent socket. Enrollment is **protocol-version-shared**
//! (unchanged across the v1 → v2 cut); the v2 terminal output of an execution is a
//! `thread_result` frame over the outbox/WS (see [`super::Transport::send`]), not
//! an HTTP post.
//!
//! * `POST /agent/register` — enrollment (§2.1). The bootstrap *register* token
//!   travels in the JSON body; no auth header. Returns the host-assigned
//!   `{agentId, connectToken, config}` or a `{register_error}` envelope.
//! * `GET  /agent/config/:id` — config bootstrap/refresh, authenticated with the
//!   connect token in the `x-internal-token` header.
//!
//! The **attachment proxy** client (`GET /agent/attachments/:id` /
//! `POST /agent/attachments`, agent-architecture §9 / PLAN-005, Lane F) lives
//! here too — see [`HttpClient::download_attachment`] /
//! [`HttpClient::upload_attachment`]. It is the **host-proxy** byte backend: the
//! same connect-token auth as the endpoints above, but the bytes ride the request
//! body (never a frame). The attachment *layer* (`crate::attachments`) owns
//! integrity, caching and limits; this module is just the byte transfer.
//!
//! All TLS is rustls (the manifest pins `reqwest` to rustls + OS roots); this
//! module never touches the openssl/native-tls path banned by `deny.toml`.

use reqwest::Client;
use reqwest::header::CONTENT_TYPE;
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::protocol::{RegisterRequest, RegisterResponse};

/// Auth header carrying the connect token (the host reads this exact name).
const INTERNAL_TOKEN_HEADER: &str = "x-internal-token";
/// Original file name of an uploaded attachment (host-proxy upload metadata; the
/// bytes ride the body, so the name travels as a header rather than a frame).
const ATTACHMENT_NAME_HEADER: &str = "x-attachment-name";
/// Content hash of an uploaded attachment, so the host can dedup/verify too.
const ATTACHMENT_SHA256_HEADER: &str = "x-attachment-sha256";

/// Errors raised by the HTTP client. Secrets are never included in any message.
#[derive(Debug, Error)]
pub(crate) enum HttpError {
    /// The underlying `reqwest` client could not be built.
    #[error("build http client: {0}")]
    Build(reqwest::Error),
    /// The request could not be sent (DNS/TLS/connect/timeout).
    #[error("{endpoint} request failed: {source}")]
    Request {
        endpoint: &'static str,
        source: reqwest::Error,
    },
    /// The host returned a non-success status (and no parseable error body).
    #[error("{endpoint} returned HTTP {status}")]
    Status { endpoint: &'static str, status: u16 },
    /// The response body could not be decoded into the expected type.
    #[error("decode {endpoint} response: {source}")]
    Decode {
        endpoint: &'static str,
        source: reqwest::Error,
    },
}

/// Thin wrapper over a shared `reqwest::Client` (the client is internally
/// reference-counted, so cloning is cheap).
#[derive(Clone, Debug)]
pub(crate) struct HttpClient {
    client: Client,
}

impl HttpClient {
    /// Build a client with the default (rustls) TLS stack.
    pub(crate) fn new() -> Result<Self, HttpError> {
        let client = Client::builder().build().map_err(HttpError::Build)?;
        Ok(Self { client })
    }

    /// `POST {url}` with the register request body. The host signals failure via a
    /// `{register_error}` body (possibly under a non-2xx status), so the body is
    /// parsed regardless of status and the typed [`RegisterResponse`] distinguishes
    /// success from rejection. A body that is neither shape surfaces the status.
    pub(crate) async fn register(
        &self,
        url: &str,
        body: &RegisterRequest,
    ) -> Result<RegisterResponse, HttpError> {
        let response = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|source| HttpError::Request {
                endpoint: "register",
                source,
            })?;
        let status = response.status();
        match response.json::<RegisterResponse>().await {
            Ok(parsed) => Ok(parsed),
            Err(source) if status.is_success() => Err(HttpError::Decode {
                endpoint: "register",
                source,
            }),
            Err(_) => Err(HttpError::Status {
                endpoint: "register",
                status: status.as_u16(),
            }),
        }
    }

    /// `GET {url}` for the host-assigned config overlay (raw JSON).
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "config refresh is wired by the config/engine L3")
    )]
    pub(crate) async fn fetch_config(
        &self,
        url: &str,
        connect_token: &SecretString,
    ) -> Result<Value, HttpError> {
        let response = self
            .client
            .get(url)
            .header(INTERNAL_TOKEN_HEADER, connect_token.expose_secret())
            .send()
            .await
            .map_err(|source| HttpError::Request {
                endpoint: "config",
                source,
            })?;
        let response = error_for_status(response, "config")?;
        response
            .json::<Value>()
            .await
            .map_err(|source| HttpError::Decode {
                endpoint: "config",
                source,
            })
    }

    /// `GET {url}` — download an attachment's raw bytes from the host proxy
    /// (`GET /agent/attachments/:id`, agent-architecture §9 / PLAN-005). Same
    /// connect-token auth as the other runner endpoints. Integrity (`sha256`) is
    /// verified by [`crate::attachments`], not here — this is only byte transfer.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the attachment proxy is wired into the executor by Lane G"
        )
    )]
    pub(crate) async fn download_attachment(
        &self,
        url: &str,
        connect_token: &SecretString,
    ) -> Result<Vec<u8>, HttpError> {
        let response = self
            .client
            .get(url)
            .header(INTERNAL_TOKEN_HEADER, connect_token.expose_secret())
            .send()
            .await
            .map_err(|source| HttpError::Request {
                endpoint: "attachment_download",
                source,
            })?;
        let response = error_for_status(response, "attachment_download")?;
        let bytes = response.bytes().await.map_err(|source| HttpError::Decode {
            endpoint: "attachment_download",
            source,
        })?;
        Ok(bytes.to_vec())
    }

    /// `POST {url}` — upload bytes to the host proxy (`POST /agent/attachments`).
    /// The bytes ride the request body; the name, content type and content hash
    /// ride headers (never a frame). The host owns the registry (A1) and returns
    /// the assigned `{id, ref}`.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "the attachment proxy is wired into the executor by Lane G"
        )
    )]
    pub(crate) async fn upload_attachment(
        &self,
        url: &str,
        connect_token: &SecretString,
        name: &str,
        content_type: &str,
        sha256: &str,
        bytes: Vec<u8>,
    ) -> Result<UploadedAttachment, HttpError> {
        let response = self
            .client
            .post(url)
            .header(INTERNAL_TOKEN_HEADER, connect_token.expose_secret())
            .header(ATTACHMENT_NAME_HEADER, name)
            .header(ATTACHMENT_SHA256_HEADER, sha256)
            .header(CONTENT_TYPE, content_type)
            .body(bytes)
            .send()
            .await
            .map_err(|source| HttpError::Request {
                endpoint: "attachment_upload",
                source,
            })?;
        let response = error_for_status(response, "attachment_upload")?;
        response
            .json::<UploadedAttachment>()
            .await
            .map_err(|source| HttpError::Decode {
                endpoint: "attachment_upload",
                source,
            })
    }
}

/// The host-assigned identity returned by `POST /agent/attachments`. The host
/// owns the attachment registry (agent-architecture §14 A1): it mints the stable
/// `id` and the backend-specific `ref`; the runner computes `sha256`/`size`
/// locally. `ref` is a Rust keyword, so the wire `ref` maps to `reference`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct UploadedAttachment {
    pub(crate) id: String,
    #[serde(rename = "ref")]
    pub(crate) reference: String,
}

/// Map a non-success status to [`HttpError::Status`] (keeping the endpoint label).
fn error_for_status(
    response: reqwest::Response,
    endpoint: &'static str,
) -> Result<reqwest::Response, HttpError> {
    let status = response.status();
    if status.is_success() {
        Ok(response)
    } else {
        Err(HttpError::Status {
            endpoint,
            status: status.as_u16(),
        })
    }
}

#[cfg(test)]
mod tests {
    use secrecy::SecretString;
    use serde_json::{Value, json};
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::HttpClient;

    #[tokio::test]
    async fn fetch_config_sends_token_header_and_returns_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/agent/config/agent-1"))
            .and(header("x-internal-token", "connect-xyz"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "engine": { "model": "m" } })),
            )
            .mount(&server)
            .await;

        let http = HttpClient::new().unwrap();
        let url = format!("{}/agent/config/agent-1", server.uri());
        let value: Value = http
            .fetch_config(&url, &SecretString::from("connect-xyz"))
            .await
            .unwrap();
        assert_eq!(value["engine"]["model"], json!("m"));
    }

    #[tokio::test]
    async fn download_attachment_sends_token_and_returns_bytes() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/agent/attachments/a-1"))
            .and(header("x-internal-token", "connect-xyz"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello-bytes".to_vec()))
            .mount(&server)
            .await;

        let http = HttpClient::new().unwrap();
        let url = format!("{}/agent/attachments/a-1", server.uri());
        let bytes = http
            .download_attachment(&url, &SecretString::from("connect-xyz"))
            .await
            .unwrap();
        assert_eq!(bytes, b"hello-bytes");
    }

    #[tokio::test]
    async fn upload_attachment_sends_bytes_and_metadata_headers() {
        use wiremock::matchers::body_bytes;

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/agent/attachments"))
            .and(header("x-internal-token", "connect-xyz"))
            .and(header("x-attachment-name", "report.txt"))
            .and(header("x-attachment-sha256", "abc123"))
            .and(body_bytes(b"file-body".to_vec()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "id": "att-9", "ref": "/agent/attachments/att-9" })),
            )
            .mount(&server)
            .await;

        let http = HttpClient::new().unwrap();
        let url = format!("{}/agent/attachments", server.uri());
        let uploaded = http
            .upload_attachment(
                &url,
                &SecretString::from("connect-xyz"),
                "report.txt",
                "text/plain",
                "abc123",
                b"file-body".to_vec(),
            )
            .await
            .unwrap();
        assert_eq!(uploaded.id, "att-9");
        assert_eq!(uploaded.reference, "/agent/attachments/att-9");
    }

    #[tokio::test]
    async fn download_attachment_maps_non_success_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/agent/attachments/missing"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let http = HttpClient::new().unwrap();
        let url = format!("{}/agent/attachments/missing", server.uri());
        let err = http
            .download_attachment(&url, &SecretString::from("connect-xyz"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            super::HttpError::Status {
                endpoint: "attachment_download",
                status: 404
            }
        ));
    }

    #[tokio::test]
    async fn fetch_config_maps_non_success_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/agent/config/agent-1"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let http = HttpClient::new().unwrap();
        let url = format!("{}/agent/config/agent-1", server.uri());
        let err = http
            .fetch_config(&url, &SecretString::from("bad"))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            super::HttpError::Status {
                endpoint: "config",
                status: 401
            }
        ));
    }
}
