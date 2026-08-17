//! TLS client side of the control connection.
//!
//! Enabled with the `tls` feature. The agent verifies the controller against a
//! CA file — which, for a self-signed deployment, is the controller's own
//! certificate. There is no "accept any certificate" switch: that would make
//! the encryption decorative.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use aether_core::NodeInfo;
use tokio::io::ReadHalf;
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};
use tokio_rustls::{TlsConnector, client::TlsStream};

use crate::client::{AgentClient, ClientError};

/// Connecting over TLS failed.
#[derive(Debug, thiserror::Error)]
pub enum TlsClientError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{0} contains no certificates")]
    NoCertificates(PathBuf),
    #[error("rustls rejected the configuration: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("{0} is not a valid server name")]
    InvalidServerName(String),
    #[error("connecting to the controller: {0}")]
    Connect(#[source] io::Error),
    #[error(transparent)]
    Client(#[from] ClientError),
}

/// Builds a connector that trusts the certificates in `ca_path`.
pub fn connector(ca_path: &Path) -> Result<TlsConnector, TlsClientError> {
    let pem = std::fs::read(ca_path).map_err(|source| TlsClientError::Io {
        path: ca_path.to_path_buf(),
        source,
    })?;

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|source| TlsClientError::Io {
            path: ca_path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsClientError::NoCertificates(ca_path.to_path_buf()));
    }

    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert)?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Connects over TLS and registers.
///
/// `server_name` must match a name in the controller's certificate.
pub async fn connect(
    addr: &str,
    server_name: &str,
    connector: &TlsConnector,
    info: NodeInfo,
    token: Option<String>,
) -> Result<AgentClient<ReadHalf<TlsStream<TcpStream>>>, TlsClientError> {
    let name = ServerName::try_from(server_name.to_string())
        .map_err(|_| TlsClientError::InvalidServerName(server_name.to_string()))?;

    let tcp = TcpStream::connect(addr)
        .await
        .map_err(TlsClientError::Connect)?;
    let stream = connector
        .connect(name, tcp)
        .await
        .map_err(TlsClientError::Connect)?;

    let (reader, writer) = tokio::io::split(stream);
    Ok(AgentClient::register(reader, writer, info, token).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TlsConnector` is not `Debug`, so `unwrap_err` is not available.
    fn expect_error(path: &Path) -> TlsClientError {
        match connector(path) {
            Ok(_) => panic!("expected loading to fail"),
            Err(error) => error,
        }
    }

    #[test]
    fn a_missing_ca_file_is_reported_with_its_path() {
        let error = expect_error(Path::new("no-such-ca.pem"));
        assert!(matches!(error, TlsClientError::Io { .. }));
    }

    #[test]
    fn a_file_without_certificates_is_rejected() {
        let path = std::env::temp_dir().join(format!("aethermesh-ca-{}.pem", std::process::id()));
        std::fs::write(&path, b"not a certificate").unwrap();

        assert!(matches!(
            expect_error(&path),
            TlsClientError::NoCertificates(_)
        ));
    }
}
