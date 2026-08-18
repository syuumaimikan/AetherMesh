//! Controller configuration, from a TOML file.
//!
//! Anything that is not in the file keeps its default, so a config can be as
//! short as the one setting you care about. CLI flags win over the file.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::security::SecurityConfig;

/// Everything the controller binary needs to start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ControllerConfig {
    pub listen: SocketAddr,
    /// Address the client API listens on (publish data, submit tasks).
    /// Omit to run without a client API.
    pub client_listen: Option<SocketAddr>,
    /// Seconds without a heartbeat before a node is evicted.
    pub heartbeat_timeout_secs: u64,
    /// Token every agent must present. Omit to accept any agent.
    pub auth_token: Option<String>,
    /// Per-node tokens, keyed by a label. Any of them is accepted, so one
    /// node's credential can be revoked without re-keying the mesh.
    pub node_tokens: BTreeMap<String, String>,
    /// Certificate and key. Omit to serve plaintext.
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    /// CA that agent and client certificates must be signed by. Setting it
    /// turns on mutual TLS, so a peer without a certificate never gets as far
    /// as presenting a token.
    pub tls_client_ca_path: Option<PathBuf>,
    /// Seconds a queued task waits before it counts as one level more urgent.
    ///
    /// Zero turns promotion off, which makes a low priority a genuine risk of
    /// never running at all. Set it only if you mean that.
    pub queue_aging_secs: u64,
    /// Seconds between metrics log lines. Zero turns them off.
    pub metrics_interval_secs: u64,
    /// Address to serve `/metrics` and `/healthz` on. Omit to serve neither.
    ///
    /// The endpoint has no authentication and reports only counters and
    /// averages — no hostnames, ids, or addresses — but bind it to localhost
    /// or a management interface anyway.
    pub metrics_listen: Option<SocketAddr>,
    /// Seconds between link measurements. Zero turns probing off, leaving the
    /// scheduler on whatever latency and bandwidth were configured by hand.
    pub probe_interval_secs: u64,
    /// Ballast in the bandwidth probe.
    pub probe_bytes: usize,
    /// How many finished results to remember, so repeated work is answered
    /// without dispatching it. Zero turns caching off, which is the default:
    /// it is only correct when tasks are deterministic.
    pub result_cache_entries: usize,
    /// Seconds before a cached result is forgotten. Zero means never.
    pub result_cache_ttl_secs: u64,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:7000".parse().expect("valid default address"),
            client_listen: Some(
                "127.0.0.1:7100"
                    .parse()
                    .expect("valid default client address"),
            ),
            heartbeat_timeout_secs: 30,
            auth_token: None,
            node_tokens: BTreeMap::new(),
            tls_cert_path: None,
            tls_key_path: None,
            tls_client_ca_path: None,
            queue_aging_secs: crate::queue::DEFAULT_AGING.as_secs(),
            metrics_interval_secs: 60,
            metrics_listen: None,
            probe_interval_secs: 60,
            probe_bytes: crate::probe::DEFAULT_PROBE_BYTES,
            result_cache_entries: 0,
            result_cache_ttl_secs: 0,
        }
    }
}

/// A configuration file could not be used.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("parsing {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("tls needs both tls_cert_path and tls_key_path")]
    IncompleteTls,
}

impl ControllerConfig {
    /// Loads a TOML file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let config: Self = toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    /// Loads the file if it exists, otherwise returns defaults.
    pub fn load_or_default(path: Option<&Path>) -> Result<Self, ConfigError> {
        match path {
            Some(path) if path.exists() => Self::load(path),
            Some(path) => Err(ConfigError::Io {
                path: path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "configuration file not found",
                ),
            }),
            None => Ok(Self::default()),
        }
    }

    fn validate(&self) -> Result<(), ConfigError> {
        match (&self.tls_cert_path, &self.tls_key_path) {
            (Some(_), None) | (None, Some(_)) => Err(ConfigError::IncompleteTls),
            _ => Ok(()),
        }
    }

    pub fn security(&self) -> SecurityConfig {
        SecurityConfig {
            auth_token: self.auth_token.clone(),
            node_tokens: self.node_tokens.clone(),
        }
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(self.heartbeat_timeout_secs)
    }

    /// Certificate and key, when both are configured.
    pub fn tls_paths(&self) -> Option<(PathBuf, PathBuf)> {
        self.tls_cert_path.clone().zip(self.tls_key_path.clone())
    }

    /// CA for client certificates, when mutual TLS is configured.
    pub fn client_ca_path(&self) -> Option<PathBuf> {
        self.tls_client_ca_path.clone()
    }

    /// The result cache this configuration asks for, if any.
    pub fn result_cache(&self) -> Option<crate::cache::ResultCache> {
        if self.result_cache_entries == 0 {
            return None;
        }

        let cache = crate::cache::ResultCache::new(self.result_cache_entries);
        Some(match self.result_cache_ttl_secs {
            0 => cache,
            seconds => cache.with_ttl(Duration::from_secs(seconds)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aethermesh-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn an_empty_file_yields_the_defaults() {
        let path = write("empty.toml", "");
        assert_eq!(
            ControllerConfig::load(&path).unwrap(),
            ControllerConfig::default()
        );
    }

    #[test]
    fn settings_are_read_and_the_rest_defaulted() {
        let path = write(
            "partial.toml",
            r#"
            listen = "0.0.0.0:9000"
            auth_token = "s3cret"
            "#,
        );

        let config = ControllerConfig::load(&path).unwrap();

        assert_eq!(config.listen.port(), 9000);
        assert_eq!(config.security().auth_token.as_deref(), Some("s3cret"));
        assert_eq!(config.heartbeat_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn half_configured_tls_is_rejected() {
        let path = write("half-tls.toml", r#"tls_cert_path = "cert.pem""#);

        assert!(matches!(
            ControllerConfig::load(&path),
            Err(ConfigError::IncompleteTls)
        ));
    }

    #[test]
    fn unknown_keys_are_an_error_rather_than_a_silent_typo() {
        let path = write("typo.toml", "lisen = \"0.0.0.0:9000\"");

        assert!(matches!(
            ControllerConfig::load(&path),
            Err(ConfigError::Parse { .. })
        ));
    }

    #[test]
    fn a_missing_file_is_only_an_error_when_it_was_asked_for() {
        assert_eq!(
            ControllerConfig::load_or_default(None).unwrap(),
            ControllerConfig::default()
        );
        assert!(ControllerConfig::load_or_default(Some(Path::new("nope.toml"))).is_err());
    }
}
