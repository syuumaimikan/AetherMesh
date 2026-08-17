//! The plumbing every cloud adapter shares: an HTTP client and a way to prove
//! who you are to it.
//!
//! Deliberately thin. Each provider's API is reached over plain REST with the
//! credentials that platform already gives a machine running inside it — an
//! instance metadata token, a mounted service account, an access key from the
//! environment. No vendor SDK is a dependency, which is what keeps a Raspberry
//! Pi build from paying for four of them.

use std::time::Duration;

use serde::de::DeserializeOwned;

use crate::CloudError;

/// Default timeout for one provider call.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How many times a throttled or temporarily failed call is retried.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 4;

/// Wait before the first retry; doubled each time after that.
pub const DEFAULT_BACKOFF: Duration = Duration::from_millis(200);

/// How a request proves who it is.
#[derive(Debug, Clone)]
pub enum Credentials {
    /// `Authorization: Bearer <token>` — Kubernetes service accounts, GCP and
    /// Azure metadata tokens.
    Bearer(String),
    /// AWS SigV4, signed per request.
    AwsSigV4 {
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        region: String,
        service: String,
    },
    /// Nothing at all. Only useful against a local emulator or a test double.
    None,
}

/// A minimal REST client for one provider endpoint.
#[derive(Debug, Clone)]
pub struct HttpClient {
    client: reqwest::Client,
    base_url: String,
    credentials: Credentials,
    max_attempts: u32,
    backoff: Duration,
}

impl HttpClient {
    /// Builds a client for `base_url`, e.g. `https://kubernetes.default.svc`.
    ///
    /// `ca_certificate` is a PEM bundle to trust in addition to the system
    /// roots — Kubernetes in-cluster access needs it.
    pub fn new(
        base_url: impl Into<String>,
        credentials: Credentials,
        ca_certificate: Option<&[u8]>,
    ) -> Result<Self, CloudError> {
        let mut builder = reqwest::Client::builder().timeout(DEFAULT_TIMEOUT);
        if let Some(pem) = ca_certificate {
            let certificate = reqwest::Certificate::from_pem(pem)
                .map_err(|error| CloudError::Request(error.to_string()))?;
            builder = builder.add_root_certificate(certificate);
        }

        Ok(Self {
            client: builder
                .build()
                .map_err(|error| CloudError::Request(error.to_string()))?,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            credentials,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            backoff: DEFAULT_BACKOFF,
        })
    }

    /// Overrides the retry policy. Zero attempts is treated as one.
    pub fn with_retry(mut self, max_attempts: u32, backoff: Duration) -> Self {
        self.max_attempts = max_attempts.max(1);
        self.backoff = backoff;
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Follows a provider's pagination and returns every page's body.
    ///
    /// `next` pulls the continuation token out of a page; `apply` puts it into
    /// the next request. Every provider spells this differently — `continue`,
    /// `pageToken`, `nextToken`, `nextLink` — but they all work this way.
    pub async fn get_all_pages<T, N, A>(
        &self,
        first_path: &str,
        next: N,
        apply: A,
    ) -> Result<Vec<T>, CloudError>
    where
        T: DeserializeOwned,
        N: Fn(&T) -> Option<String>,
        A: Fn(&str, &str) -> String,
    {
        let mut pages = Vec::new();
        let mut path = first_path.to_string();

        // A provider that keeps handing back the same token would loop forever;
        // the cap turns that into a bounded, reported failure.
        for _ in 0..100 {
            let page: T = self.get_json(&path).await?;
            let token = next(&page);
            pages.push(page);

            match token {
                Some(token) if !token.is_empty() => path = apply(first_path, &token),
                _ => return Ok(pages),
            }
        }

        Err(CloudError::Request(
            "pagination did not terminate after 100 pages".to_string(),
        ))
    }

    /// Polls until `done` says the operation finished, or the deadline passes.
    ///
    /// GCP and Azure answer a create call with an operation, not a resource:
    /// the VM exists only once that operation completes.
    pub async fn poll_until<T, D>(
        &self,
        path: &str,
        done: D,
        interval: Duration,
        deadline: Duration,
    ) -> Result<T, CloudError>
    where
        T: DeserializeOwned,
        D: Fn(&T) -> bool,
    {
        let started = std::time::Instant::now();
        loop {
            let state: T = self.get_json(path).await?;
            if done(&state) {
                return Ok(state);
            }
            if started.elapsed() >= deadline {
                return Err(CloudError::Request(format!(
                    "{path} did not finish within {deadline:?}"
                )));
            }
            tokio::time::sleep(interval).await;
        }
    }

    /// GETs a path and parses the JSON body.
    pub async fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, CloudError> {
        self.send_json(reqwest::Method::GET, path, None).await
    }

    /// POSTs a JSON body and parses the JSON reply.
    pub async fn post_json<T: DeserializeOwned>(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<T, CloudError> {
        let body =
            serde_json::to_vec(body).map_err(|error| CloudError::Request(error.to_string()))?;
        self.send_json(reqwest::Method::POST, path, Some(body))
            .await
    }

    /// Sends a request, signing or bearing it as configured.
    async fn send_json<T: DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<T, CloudError> {
        let text = self.send(method, path, body).await?;
        serde_json::from_str(&text).map_err(|error| {
            CloudError::Request(format!(
                "{error}: {}",
                text.chars().take(200).collect::<String>()
            ))
        })
    }

    /// Sends a request and returns the body as text, retrying throttling and
    /// transient server errors.
    ///
    /// Every provider throttles, and every provider expects the client to back
    /// off rather than hammer: `429` and `5xx` are retried, `4xx` is not,
    /// because a rejected request does not become valid by repeating it.
    pub async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<String, CloudError> {
        let mut wait = self.backoff;
        let mut last_error = CloudError::Request("no attempt was made".to_string());

        for attempt in 1..=self.max_attempts {
            match self.send_once(method.clone(), path, body.clone()).await {
                Ok(text) => return Ok(text),
                Err(error) => {
                    let retryable = matches!(&error, CloudError::Throttled { .. })
                        || matches!(&error, CloudError::Unavailable { .. });
                    last_error = error;

                    if !retryable || attempt == self.max_attempts {
                        return Err(last_error);
                    }
                    tokio::time::sleep(wait).await;
                    wait *= 2;
                }
            }
        }

        Err(last_error)
    }

    /// One attempt, without retrying.
    async fn send_once(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<String, CloudError> {
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        let mut request = self.client.request(method.clone(), &url);

        if let Some(body) = body.clone() {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }

        request = match &self.credentials {
            Credentials::Bearer(token) => request.bearer_auth(token),
            Credentials::AwsSigV4 { .. } => {
                let headers = self.sigv4_headers(&method, path, body.as_deref().unwrap_or(&[]))?;
                headers.into_iter().fold(request, |request, (name, value)| {
                    request.header(name, value)
                })
            }
            Credentials::None => request,
        };

        let response = request
            .send()
            .await
            .map_err(|error| CloudError::Request(error.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| CloudError::Request(error.to_string()))?;

        if !status.is_success() {
            let detail = format!(
                "{} {}: {}",
                status.as_u16(),
                url,
                text.chars().take(300).collect::<String>()
            );
            return Err(match status.as_u16() {
                429 => CloudError::Throttled { detail },
                401 | 403 => CloudError::Unauthorized { detail },
                404 => CloudError::NotFound { detail },
                500..=599 => CloudError::Unavailable { detail },
                _ => CloudError::Request(detail),
            });
        }
        Ok(text)
    }

    /// Signs a request the way AWS wants it.
    fn sigv4_headers(
        &self,
        method: &reqwest::Method,
        path: &str,
        body: &[u8],
    ) -> Result<Vec<(String, String)>, CloudError> {
        let Credentials::AwsSigV4 {
            access_key_id,
            secret_access_key,
            session_token,
            region,
            service,
        } = &self.credentials
        else {
            return Ok(Vec::new());
        };

        let host = self
            .base_url
            .split("://")
            .nth(1)
            .unwrap_or(&self.base_url)
            .to_string();

        crate::sigv4::sign(crate::sigv4::Request {
            method: method.as_str(),
            host: &host,
            path,
            body,
            access_key_id,
            secret_access_key,
            session_token: session_token.as_deref(),
            region,
            service,
            timestamp: std::time::SystemTime::now(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockServer;

    #[tokio::test]
    async fn a_get_parses_the_json_body() {
        let server = MockServer::start(vec![(200, r#"{"items":[1,2,3]}"#.to_string())]).await;
        let client = HttpClient::new(server.base_url(), Credentials::None, None).unwrap();

        let body: serde_json::Value = client.get_json("/things").await.unwrap();

        assert_eq!(body["items"][2], 3);
        assert_eq!(server.requests().await[0].path, "/things");
    }

    #[tokio::test]
    async fn a_bearer_token_reaches_the_server() {
        let server = MockServer::start(vec![(200, "{}".to_string())]).await;
        let client = HttpClient::new(
            server.base_url(),
            Credentials::Bearer("s3cret".to_string()),
            None,
        )
        .unwrap();

        let _: serde_json::Value = client.get_json("/whoami").await.unwrap();

        let request = &server.requests().await[0];
        assert_eq!(
            request.header("authorization").as_deref(),
            Some("Bearer s3cret")
        );
    }

    #[tokio::test]
    async fn a_post_sends_the_body_as_json() {
        let server = MockServer::start(vec![(201, r#"{"ok":true}"#.to_string())]).await;
        let client = HttpClient::new(server.base_url(), Credentials::None, None).unwrap();

        let _: serde_json::Value = client
            .post_json("/create", &serde_json::json!({ "name": "worker" }))
            .await
            .unwrap();

        let request = &server.requests().await[0];
        assert_eq!(request.method, "POST");
        assert!(
            request.body.contains("\"name\":\"worker\""),
            "{}",
            request.body
        );
        assert_eq!(
            request.header("content-type").as_deref(),
            Some("application/json")
        );
    }

    #[tokio::test]
    async fn an_error_status_becomes_an_error_with_the_body() {
        let server = MockServer::start(vec![(403, r#"{"message":"denied"}"#.to_string())]).await;
        let client = HttpClient::new(server.base_url(), Credentials::None, None).unwrap();

        let error = client
            .get_json::<serde_json::Value>("/nope")
            .await
            .unwrap_err();

        let text = error.to_string();
        assert!(text.contains("403"), "{text}");
        assert!(text.contains("denied"), "{text}");
    }

    #[tokio::test]
    async fn sigv4_requests_carry_a_signature() {
        let server = MockServer::start(vec![(200, "{}".to_string())]).await;
        let client = HttpClient::new(
            server.base_url(),
            Credentials::AwsSigV4 {
                access_key_id: "AKIDEXAMPLE".to_string(),
                secret_access_key: "SECRET".to_string(),
                session_token: Some("TOKEN".to_string()),
                region: "us-east-1".to_string(),
                service: "ec2".to_string(),
            },
            None,
        )
        .unwrap();

        let _: serde_json::Value = client.get_json("/?Action=DescribeInstances").await.unwrap();

        let request = &server.requests().await[0];
        let authorization = request.header("authorization").unwrap_or_default();
        assert!(
            authorization.starts_with("AWS4-HMAC-SHA256"),
            "{authorization}"
        );
        assert!(
            authorization.contains("us-east-1/ec2/aws4_request"),
            "{authorization}"
        );
        assert!(request.header("x-amz-date").is_some());
        assert_eq!(
            request.header("x-amz-security-token").as_deref(),
            Some("TOKEN")
        );
    }
}
