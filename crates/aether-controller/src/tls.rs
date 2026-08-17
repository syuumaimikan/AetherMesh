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
use tokio_rustls::rustls::server::WebPkiClientVerifier;
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
    /// CA that client certificates must be signed by.
    ///
    /// Setting it turns on mutual TLS: an agent or client without a valid
    /// certificate is refused during the handshake, before it can send a token.
    pub client_ca_path: Option<PathBuf>,
}

impl TlsConfig {
    pub fn new(cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
            client_ca_path: None,
        }
    }

    /// Requires every peer to present a certificate signed by this CA.
    pub fn with_client_ca(mut self, client_ca_path: impl Into<PathBuf>) -> Self {
        self.client_ca_path = Some(client_ca_path.into());
        self
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
    #[error("client certificate verification could not be set up: {0}")]
    ClientVerifier(String),
    #[error("generating a certificate failed: {0}")]
    Generate(String),
}

fn read(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Reads a PEM file's certificates.
fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut read(path)?.as_slice())
        .collect::<Result<_, _>>()
        .map_err(|source| TlsError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    if certs.is_empty() {
        return Err(TlsError::NoCertificates(path.to_path_buf()));
    }
    Ok(certs)
}

/// Loads the certificate chain and key and builds an acceptor.
///
/// With `client_ca_path` set, the acceptor demands a client certificate signed
/// by that CA — mutual TLS, checked during the handshake.
pub fn acceptor(config: &TlsConfig) -> Result<TlsAcceptor, TlsError> {
    let certs = certificates(&config.cert_path)?;

    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut read(&config.key_path)?.as_slice())
            .map_err(|source| TlsError::Io {
                path: config.key_path.clone(),
                source,
            })?
            .ok_or_else(|| TlsError::NoPrivateKey(config.key_path.clone()))?;

    let builder = match &config.client_ca_path {
        Some(client_ca_path) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in certificates(client_ca_path)? {
                roots.add(cert)?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|error| TlsError::ClientVerifier(error.to_string()))?;
            info!(ca = %client_ca_path.display(), "requiring client certificates");
            ServerConfig::builder().with_client_cert_verifier(verifier)
        }
        None => ServerConfig::builder().with_no_client_auth(),
    };

    Ok(TlsAcceptor::from(Arc::new(
        builder.with_single_cert(certs, key)?,
    )))
}

/// Writes a CA and a certificate signed by it.
///
/// Mutual TLS needs an issuer that both sides agree on; this produces one,
/// plus the first certificate under it, for a lab or a home mesh.
pub fn generate_ca_and_cert(
    ca_cert_path: &Path,
    ca_key_path: &Path,
    config: &TlsConfig,
    subject_alt_names: Vec<String>,
) -> Result<(), TlsError> {
    let mut ca_params = rcgen::CertificateParams::new(Vec::new())
        .map_err(|error| TlsError::Generate(error.to_string()))?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "AetherMesh local CA");

    let ca_key =
        rcgen::KeyPair::generate().map_err(|error| TlsError::Generate(error.to_string()))?;
    let ca_cert = ca_params
        .self_signed(&ca_key)
        .map_err(|error| TlsError::Generate(error.to_string()))?;

    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);
    let leaf_params = rcgen::CertificateParams::new(subject_alt_names)
        .map_err(|error| TlsError::Generate(error.to_string()))?;
    let leaf_key =
        rcgen::KeyPair::generate().map_err(|error| TlsError::Generate(error.to_string()))?;
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &issuer)
        .map_err(|error| TlsError::Generate(error.to_string()))?;

    write_pem(ca_cert_path, &ca_cert.pem())?;
    write_pem(ca_key_path, &ca_key.serialize_pem())?;
    write_pem(&config.cert_path, &leaf_cert.pem())?;
    write_pem(&config.key_path, &leaf_key.serialize_pem())?;

    info!(
        ca = %ca_cert_path.display(),
        cert = %config.cert_path.display(),
        "wrote a CA and a certificate signed by it"
    );
    Ok(())
}

/// Signs another certificate with an existing CA — one per agent, so a single
/// node's credential can be revoked without touching the rest.
pub fn issue_client_cert(
    ca_cert_path: &Path,
    ca_key_path: &Path,
    cert_path: &Path,
    key_path: &Path,
    subject_alt_names: Vec<String>,
) -> Result<(), TlsError> {
    let ca_pem = String::from_utf8(read(ca_cert_path)?)
        .map_err(|error| TlsError::Generate(error.to_string()))?;
    let ca_key_pem = String::from_utf8(read(ca_key_path)?)
        .map_err(|error| TlsError::Generate(error.to_string()))?;

    let ca_key = rcgen::KeyPair::from_pem(&ca_key_pem)
        .map_err(|error| TlsError::Generate(error.to_string()))?;
    let issuer = rcgen::Issuer::from_ca_cert_pem(&ca_pem, ca_key)
        .map_err(|error| TlsError::Generate(error.to_string()))?;

    let params = rcgen::CertificateParams::new(subject_alt_names)
        .map_err(|error| TlsError::Generate(error.to_string()))?;
    let key = rcgen::KeyPair::generate().map_err(|error| TlsError::Generate(error.to_string()))?;
    let cert = params
        .signed_by(&key, &issuer)
        .map_err(|error| TlsError::Generate(error.to_string()))?;

    write_pem(cert_path, &cert.pem())?;
    write_pem(key_path, &key.serialize_pem())?;
    info!(cert = %cert_path.display(), "issued a client certificate");
    Ok(())
}

/// Writes a PEM file, creating the directory if it is missing.
fn write_pem(path: &Path, contents: &str) -> Result<(), TlsError> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|source| TlsError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }
    std::fs::write(path, contents).map_err(|source| TlsError::Io {
        path: path.to_path_buf(),
        source,
    })
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

/// Accepts TLS client-API connections until the listener fails.
///
/// The client API carries the same token as the agent protocol, so it deserves
/// the same encryption.
pub async fn serve_clients_tls(
    listener: TcpListener,
    gateway: crate::client::ClientGateway,
    security: SecurityConfig,
    acceptor: TlsAcceptor,
) -> io::Result<()> {
    loop {
        let (stream, peer) = listener.accept().await?;
        let acceptor = acceptor.clone();
        let gateway = gateway.clone();
        let security = security.clone();

        tokio::spawn(async move {
            let stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(error) => {
                    warn!(%peer, %error, "client TLS handshake failed");
                    return;
                }
            };
            match crate::client::handle_client(stream, gateway, security).await {
                Ok(()) => tracing::debug!(%peer, "client disconnected"),
                Err(error) => warn!(%peer, %error, "client connection failed"),
            }
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
