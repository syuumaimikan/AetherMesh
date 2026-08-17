//! TLS for agent connections.
//!
//! Enabled with the `tls` feature. The controller presents a certificate; the
//! agent verifies it against a CA (or against that certificate directly, which
//! is what a self-signed deployment does). Nothing here is optional at runtime:
//! either the whole listener is TLS or it is not.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig};
use tracing::{info, warn};

use crate::security::SecurityConfig;
use crate::server::{handle_connection, report};
use crate::state::MeshState;

/// Where the controller's certificate and key live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TlsConfig {
    /// PEM file holding the certificate chain.
    pub cert_path: PathBuf,
    /// PEM file holding the private key.
    pub key_path: PathBuf,
}

impl TlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }
}

/// TLS setup failed.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{0} contains no certificates")]
    NoCertificates(PathBuf),
    #[error("{0} contains no private key")]
    NoPrivateKey(PathBuf),
    #[error("rustls rejected the configuration: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("generating a certificate failed: {0}")]
    Generate(String),
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Loads the certificate chain and key and builds an acceptor.
pub fn acceptor(config: &TlsConfig) -> Result<TlsAcceptor, TlsError> {
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut read(&config.cert_path)?.as_slice())
            .collect::<Result<_, _>>()
            .map_err(|source| TlsError::Io {
                path: config.cert_path.clone(),
                source,
            })?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates(config.cert_path.clone()));
    }

    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut read(&config.key_path)?.as_slice())
            .map_err(|source| TlsError::Io {
                path: config.key_path.clone(),
                source,
            })?
            .ok_or_else(|| TlsError::NoPrivateKey(config.key_path.clone()))?;

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Writes a self-signed certificate and key for `subject_alt_names`.
///
/// Convenience for a lab or a home mesh. A real deployment should bring its own
/// certificate from whatever authority it already trusts.
pub fn generate_self_signed(
    config: &TlsConfig,
    subject_alt_names: Vec<String>,
) -> Result<(), TlsError> {
    let certified = rcgen::generate_simple_self_signed(subject_alt_names)
        .map_err(|error| TlsError::Generate(error.to_string()))?;

    for (path, contents) in [
        (&config.cert_path, certified.cert.pem()),
        (&config.key_path, certified.signing_key.serialize_pem()),
    ] {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent).map_err(|source| TlsError::Io {
                path: path.clone(),
                source,
            })?;
        }
        std::fs::write(path, contents).map_err(|source| TlsError::Io {
            path: path.clone(),
            source,
        })?;
    }

    info!(cert = %config.cert_path.display(), key = %config.key_path.display(), "wrote self-signed certificate");
    Ok(())
}

/// Accepts TLS connections until the listener fails.
pub async fn serve_tls(
    listener: TcpListener,
    state: MeshState,
    security: SecurityConfig,
    acceptor: TlsAcceptor,
) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let state = state.clone();
        let security = security.clone();

        tokio::spawn(async move {
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    warn!(%peer, %error, "TLS handshake failed");
                    return;
                }
            };
            report(peer, handle_connection(stream, state, security).await);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(name: &str) -> TlsConfig {
        let dir =
            std::env::temp_dir().join(format!("aethermesh-tls-{name}-{}", std::process::id()));
        TlsConfig::new(dir.join("cert.pem"), dir.join("key.pem"))
    }

    #[test]
    fn a_generated_certificate_loads_back() {
        let config = paths("roundtrip");
        generate_self_signed(&config, vec!["localhost".to_string()]).unwrap();

        assert!(acceptor(&config).is_ok());
    }

    #[test]
    fn a_missing_certificate_is_reported_with_its_path() {
        let config = TlsConfig::new("does-not-exist.pem", "does-not-exist.key");
        let error = expect_error(&config);

        assert!(matches!(error, TlsError::Io { .. }));
    }

    /// `TlsAcceptor` is not `Debug`, so `unwrap_err` is not available.
    fn expect_error(config: &TlsConfig) -> TlsError {
        match acceptor(config) {
            Ok(_) => panic!("expected loading to fail"),
            Err(error) => error,
        }
    }

    #[test]
    fn a_file_without_certificates_is_rejected() {
        let config = paths("empty");
        std::fs::create_dir_all(config.cert_path.parent().unwrap()).unwrap();
        std::fs::write(&config.cert_path, b"not a pem file").unwrap();
        std::fs::write(&config.key_path, b"neither is this").unwrap();

        assert!(matches!(expect_error(&config), TlsError::NoCertificates(_)));
    }
}
