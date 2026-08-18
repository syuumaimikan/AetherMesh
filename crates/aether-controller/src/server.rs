//! Server: agent registrations, heartbeats, and task results.
//!
//! One long-lived connection per agent. Reads are handled inline; writes go
//! through a per-connection queue so dispatch never blocks on a slow socket.
//! The connection is generic over the stream, so a plain TCP socket and a TLS
//! session take exactly the same path.

use std::io::ErrorKind;
use std::net::SocketAddr;

use aether_protocol::{Message, NetError, PROTOCOL_VERSION, read_message, write_message};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::security::SecurityConfig;
use crate::state::MeshState;

/// Binds a listener, returning it together with the address actually bound
/// (useful when the caller asked for port 0).
pub async fn bind(addr: SocketAddr) -> std::io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    Ok((listener, local_addr))
}

/// Accepts plaintext connections until the listener fails.
pub async fn serve(
    listener: TcpListener,
    state: MeshState,
    security: SecurityConfig,
) -> std::io::Result<()> {
    if security.requires_auth() {
        warn!("authentication is enabled without TLS: the token crosses the wire in the clear");
    }

    loop {
        let (stream, peer) = listener.accept().await?;
        let state = state.clone();
        let security = security.clone();
        tokio::spawn(async move {
            report(peer, handle_connection(stream, state, security).await);
        });
    }
}

/// Logs how a connection ended.
pub(crate) fn report(peer: SocketAddr, outcome: Result<(), NetError>) {
    match outcome {
        Ok(()) => debug!(%peer, "connection closed"),
        Err(error) => warn!(%peer, %error, "connection closed with an error"),
    }
}

/// Serves one agent connection until it closes.
pub(crate) async fn handle_connection<S>(
    stream: S,
    state: MeshState,
    security: SecurityConfig,
) -> Result<(), NetError>
where
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outbound, mut outbound_rx) = mpsc::unbounded_channel::<Message>();

    let writer_task = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
            if let Err(error) = write_message(&mut writer, &message).await {
                warn!(%error, kind = message.kind(), "failed to send message to agent");
                break;
            }
        }
    });

    // Set once the agent registers, so the connection can be detached on close.
    let mut registered_id = None;
    // Set instead when this connection is one of the node's data channels.
    let mut channel_for: Option<aether_core::NodeId> = None;

    let result = loop {
        let message = match read_message(&mut reader).await {
            Ok(message) => message,
            // Both mean "the agent went away", which is normal operation.
            Err(NetError::Io(error))
                if matches!(
                    error.kind(),
                    ErrorKind::UnexpectedEof | ErrorKind::ConnectionReset
                ) =>
            {
                break Ok(());
            }
            Err(error) => break Err(error),
        };
        state.metrics.record_message();

        match message {
            Message::RegisterNode {
                protocol_version,
                info,
                token,
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    warn!(
                        peer_version = protocol_version,
                        our_version = PROTOCOL_VERSION,
                        "rejecting node with an incompatible protocol version"
                    );
                    state.metrics.record_rejection();
                    let _ = outbound.send(Message::RegisterRejected {
                        reason: format!("controller speaks protocol {PROTOCOL_VERSION}"),
                    });
                    break Ok(());
                }

                if let Err(error) = security.authorize(token.as_deref()) {
                    warn!(node_id = %info.id, %error, "rejecting node");
                    state.metrics.record_rejection();
                    let _ = outbound.send(Message::RegisterRejected {
                        reason: error.to_string(),
                    });
                    break Ok(());
                }

                let node_id = info.id;
                let hostname = info.hostname.clone();
                state
                    .registry
                    .lock()
                    .expect("registry mutex poisoned")
                    .register(info);
                // A reconnecting agent starts with an empty store, so anything
                // the catalog still credits to it is stale.
                state.catalog.forget_node(node_id);
                let channel_token = state.connections.attach(node_id, outbound.clone());
                state.metrics.record_registration();
                registered_id = Some(node_id);
                info!(%node_id, %hostname, "node registered");

                if outbound
                    .send(Message::RegisterAccepted {
                        node_id,
                        channel_token: Some(channel_token),
                        heartbeat_timeout_secs: state
                            .heartbeat_timeout()
                            .map(|timeout| timeout.as_secs())
                            .unwrap_or(0),
                    })
                    .is_err()
                {
                    break Ok(());
                }
            }
            Message::RegisterDataChannel {
                protocol_version,
                node_id,
                token,
            } => {
                // The channel token is what proves this connection belongs to
                // the node it names. The mesh token is shared by every agent,
                // so authorizing on that alone would let any member attach to
                // any node and be handed that node's data.
                if protocol_version != PROTOCOL_VERSION
                    || !state
                        .connections
                        .claim_channel_token(node_id, token.as_deref())
                {
                    warn!(%node_id, "rejecting data channel");
                    state.metrics.record_rejection();
                    break Ok(());
                }

                state
                    .connections
                    .attach_data_channel(node_id, outbound.clone());
                channel_for = Some(node_id);
                info!(
                    %node_id,
                    channels = state.connections.data_channel_count(node_id),
                    "data channel attached"
                );
            }
            Message::DataReady { data_id, .. } => {
                // Identity comes from the connection, never from the body.
                let Some(node_id) = registered_id.or(channel_for) else {
                    warn!("data confirmation before registration");
                    break Ok(());
                };
                debug!(%node_id, %data_id, "data assembled on the node");
                state.connections.complete_data(node_id, data_id);
            }
            Message::Heartbeat { metrics, .. } => {
                let Some(node_id) = registered_id else {
                    warn!("heartbeat before registration");
                    break Ok(());
                };

                let outcome = state
                    .registry
                    .lock()
                    .expect("registry mutex poisoned")
                    .record_heartbeat(node_id, metrics);
                match outcome {
                    Ok(()) => {
                        state.metrics.record_heartbeat();
                        debug!(%node_id, cpu = metrics.cpu_usage, "heartbeat");
                    }
                    Err(error) => warn!(%error, "dropping heartbeat"),
                }
            }
            Message::Pong { nonce } => {
                if let Some(node_id) = registered_id {
                    state.connections.complete_pong(node_id, nonce);
                }
            }
            Message::TaskCompleted { result } => {
                // A result is only accepted from the node it claims to be from.
                let Some(node_id) = registered_id else {
                    warn!("task result before registration");
                    break Ok(());
                };
                if result.node_id != node_id {
                    warn!(%node_id, claimed = %result.node_id, "dropping result for another node");
                    break Ok(());
                }

                debug!(task_id = %result.task_id, success = result.is_success(), "task result");
                state.metrics.record_task(result.is_success());
                state.connections.complete(result);
            }
            other => warn!(kind = other.kind(), "unexpected message from agent"),
        }
    };

    if let Some(node_id) = registered_id {
        state.connections.detach(node_id);
        state.catalog.forget_node(node_id);
        state.metrics.record_disconnect();
        info!(%node_id, "node disconnected");
    }
    drop(outbound);
    let _ = writer_task.await;

    result
}
