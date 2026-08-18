//! The client half of the controller's JSON protocol.
//!
//! Four bytes of big-endian length, then one JSON object, both directions. The
//! request and response types come from `aether-controller` rather than being
//! restated here: a dashboard that has drifted from the thing it is watching is
//! worse than no dashboard, and sharing the types makes drift a compile error.

use std::io::ErrorKind;
use std::time::Duration;

use aether_controller::client::{ClientRequest, ClientResponse, MAX_CLIENT_FRAME_BYTES};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A connection to a controller's client API.
///
/// Replies are matched to requests by order, so one connection is used by one
/// task at a time. The dashboard polls serially, which is all it needs.
pub struct Client {
    stream: TcpStream,
    timeout: Duration,
}

impl Client {
    /// Connects and completes the handshake.
    pub async fn connect(
        addr: &str,
        token: Option<String>,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow::anyhow!("connecting to {addr} timed out"))??;

        let mut client = Self { stream, timeout };
        match client.request(&ClientRequest::Hello { token }).await? {
            ClientResponse::Welcome { .. } => Ok(client),
            ClientResponse::Error { message } => Err(anyhow::anyhow!(message)),
            other => Err(anyhow::anyhow!("unexpected reply to hello: {other:?}")),
        }
    }

    /// Everything the mesh has moved, saved, and run.
    pub async fn stats(&mut self) -> anyhow::Result<ClientResponse> {
        self.request(&ClientRequest::Stats).await
    }

    /// The nodes currently registered, with what each of them holds.
    pub async fn nodes(&mut self) -> anyhow::Result<ClientResponse> {
        self.request(&ClientRequest::Nodes).await
    }

    /// Runs one task and waits for its result.
    pub async fn submit(
        &mut self,
        kind: String,
        payload: Vec<u8>,
        constraints: Vec<String>,
        priority: String,
    ) -> anyhow::Result<ClientResponse> {
        use base64::Engine as _;
        self.request(&ClientRequest::Submit {
            kind,
            payload: base64::engine::general_purpose::STANDARD.encode(payload),
            inputs: Vec::new(),
            constraints,
            priority: Some(priority),
            module: None,
        })
        .await
    }

    /// Sends one frame and reads the reply.
    async fn request(&mut self, request: &ClientRequest) -> anyhow::Result<ClientResponse> {
        tokio::time::timeout(self.timeout, self.exchange(request))
            .await
            .map_err(|_| {
                anyhow::anyhow!("the controller did not answer within {:?}", self.timeout)
            })?
    }

    async fn exchange(&mut self, request: &ClientRequest) -> anyhow::Result<ClientResponse> {
        let payload = serde_json::to_vec(request)?;
        self.stream
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await?;
        self.stream.write_all(&payload).await?;
        self.stream.flush().await?;

        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await?;
        let length = u32::from_be_bytes(header) as usize;
        if length > MAX_CLIENT_FRAME_BYTES {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("controller announced a {length} byte frame"),
            )
            .into());
        }

        let mut body = vec![0u8; length];
        self.stream.read_exact(&mut body).await?;
        Ok(serde_json::from_slice(&body)?)
    }
}
