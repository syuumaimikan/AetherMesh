//! Registration is refused without the right credential, and works over TLS.

use std::time::Duration;

use aether_agent::{AgentClient, ClientError};
use aether_controller::{MeshState, SecurityConfig, bind, serve};
use aether_core::{NodeId, NodeInfo};

fn node(hostname: &str) -> NodeInfo {
    NodeInfo::new(NodeId::generate(), hostname, "127.0.0.1:7001", 4)
}

async fn start(security: SecurityConfig) -> (MeshState, std::net::SocketAddr) {
    let state = MeshState::new();
    let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();

    let serve_state = state.clone();
    tokio::spawn(async move {
        let _ = serve(listener, serve_state, security).await;
    });
    (state, addr)
}

async fn wait_for_registration(state: &MeshState, node_id: NodeId) {
    for _ in 0..200 {
        if state.registry.lock().unwrap().contains(node_id) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("node never registered");
}

#[tokio::test]
async fn the_right_token_is_accepted() {
    let (state, addr) = start(SecurityConfig::with_token("s3cret")).await;
    let info = node("trusted");
    let node_id = info.id;

    let client = AgentClient::connect_with_token(addr, info, Some("s3cret".to_string()))
        .await
        .unwrap();

    assert_eq!(client.node_id(), node_id);
    wait_for_registration(&state, node_id).await;
    assert_eq!(state.metrics.snapshot().nodes_registered, 1);
}

#[tokio::test]
async fn a_wrong_token_is_refused() {
    let (state, addr) = start(SecurityConfig::with_token("s3cret")).await;

    let error =
        match AgentClient::connect_with_token(addr, node("impostor"), Some("guess".to_string()))
            .await
        {
            Ok(_) => panic!("an invalid token was accepted"),
            Err(error) => error,
        };

    assert!(matches!(error, ClientError::Rejected(_)), "{error:?}");
    assert!(state.registry.lock().unwrap().is_empty());
    assert_eq!(state.metrics.snapshot().registrations_rejected, 1);
}

#[tokio::test]
async fn a_missing_token_is_refused() {
    let (_state, addr) = start(SecurityConfig::with_token("s3cret")).await;

    let error = match AgentClient::connect(addr, node("forgetful")).await {
        Ok(_) => panic!("a missing token was accepted"),
        Err(error) => error,
    };

    assert!(matches!(error, ClientError::Rejected(_)), "{error:?}");
}

#[tokio::test]
async fn an_open_controller_still_accepts_agents() {
    let (state, addr) = start(SecurityConfig::open()).await;
    let info = node("anyone");
    let node_id = info.id;

    AgentClient::connect(addr, info).await.unwrap();

    wait_for_registration(&state, node_id).await;
}

#[cfg(feature = "tls")]
mod tls {
    use aether_controller::TlsConfig;

    use super::*;

    fn certificate(name: &str) -> TlsConfig {
        let dir =
            std::env::temp_dir().join(format!("aethermesh-tls-it-{name}-{}", std::process::id()));
        let config = TlsConfig::new(dir.join("cert.pem"), dir.join("key.pem"));
        aether_controller::tls::generate_self_signed(&config, vec!["localhost".to_string()])
            .unwrap();
        config
    }

    #[tokio::test]
    async fn an_agent_registers_over_tls() {
        let tls = certificate("register");
        let state = MeshState::new();
        let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let acceptor = aether_controller::tls::acceptor(&tls).unwrap();

        let serve_state = state.clone();
        tokio::spawn(async move {
            let _ = aether_controller::serve_tls(
                listener,
                serve_state,
                SecurityConfig::with_token("s3cret"),
                acceptor,
            )
            .await;
        });

        let connector = aether_agent::tls::connector(&tls.cert_path).unwrap();
        let info = node("secure");
        let node_id = info.id;

        let client = aether_agent::tls::connect(
            &addr.to_string(),
            "localhost",
            &connector,
            info,
            Some("s3cret".to_string()),
        )
        .await
        .unwrap();

        assert_eq!(client.node_id(), node_id);
        wait_for_registration(&state, node_id).await;
    }

    #[tokio::test]
    async fn an_untrusted_certificate_is_rejected() {
        let served = certificate("served");
        let other = certificate("other");
        let state = MeshState::new();
        let (listener, addr) = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let acceptor = aether_controller::tls::acceptor(&served).unwrap();

        tokio::spawn(async move {
            let _ = aether_controller::serve_tls(listener, state, SecurityConfig::open(), acceptor)
                .await;
        });

        // Trusting a different certificate must not be enough.
        let connector = aether_agent::tls::connector(&other.cert_path).unwrap();
        let error = match aether_agent::tls::connect(
            &addr.to_string(),
            "localhost",
            &connector,
            node("suspicious"),
            None,
        )
        .await
        {
            Ok(_) => panic!("an untrusted certificate was accepted"),
            Err(error) => error,
        };

        assert!(
            matches!(error, aether_agent::tls::TlsClientError::Connect(_)),
            "{error:?}"
        );
    }
}
