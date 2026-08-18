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
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
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
    #[error("{path} is not valid PEM: {source}")]
    Pem {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    #[error("{0} contains no certificates")]
    NoCertificates(PathBuf),
    #[error("{0} contains no private key")]
    NoPrivateKey(PathBuf),
    #[error("rustls rejected the configuration: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("{0} is not a valid server name")]
    InvalidServerName(String),
    #[error("connecting to the controller: {0}")]
    Connect(#[source] io::Error),
    #[error(transparent)]
    Client(#[from] ClientError),
}

/// Reads a PEM file's certificates.
fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsClientError> {
    let pem = std::fs::read(path).map_err(|source| TlsClientError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<_, _>>()
        .map_err(|source| TlsClientError::Pem {
            path: path.to_path_buf(),
            source,
        })?;

    if certs.is_empty() {
        return Err(TlsClientError::NoCertificates(path.to_path_buf()));
    }
    Ok(certs)
}

/// Builds a connector that trusts the certificates in `ca_path`.
pub fn connector(ca_path: &Path) -> Result<TlsConnector, TlsClientError> {
    build_connector(ca_path, None)
}

/// Same, presenting a client certificate — the agent's half of mutual TLS.
pub fn connector_with_client_cert(
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> Result<TlsConnector, TlsClientError> {
    build_connector(ca_path, Some((cert_path, key_path)))
}

fn build_connector(
    ca_path: &Path,
    client_identity: Option<(&Path, &Path)>,
) -> Result<TlsConnector, TlsClientError> {
    let mut roots = RootCertStore::empty();
    for cert in certificates(ca_path)? {
        roots.add(cert)?;
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    let config = match client_identity {
        Some((cert_path, key_path)) => {
            let certs = certificates(cert_path)?;
            let key_pem = std::fs::read(key_path).map_err(|source| TlsClientError::Io {
                path: key_path.to_path_buf(),
                source,
            })?;
            let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|source| match source {
                // "no key here" deserves its own message; anything else is the
                // file being malformed rather than empty.
                rustls::pki_types::pem::Error::NoItemsFound => {
                    TlsClientError::NoPrivateKey(key_path.to_path_buf())
                }
                source => TlsClientError::Pem {
                    path: key_path.to_path_buf(),
                    source,
                },
            })?;

            builder.with_client_auth_cert(certs, key)?
        }
        None => builder.with_no_client_auth(),
    };

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
