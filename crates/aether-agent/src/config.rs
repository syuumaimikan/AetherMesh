//! Agent configuration, from a TOML file. CLI flags override it.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Everything the agent binary needs to start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentConfig {
    /// Controller address to register with.
    pub controller: String,
    /// Address other nodes can reach this agent on.
    pub advertise: String,
    /// Seconds between heartbeats.
    pub heartbeat_secs: u64,
    /// Shared secret, when the controller requires one.
    pub auth_token: Option<String>,
    /// File holding this node's persistent id.
    pub identity_path: Option<PathBuf>,
    /// Certificate to verify the controller against. Set it to use TLS.
    pub tls_ca_path: Option<PathBuf>,
    /// Name the controller's certificate is issued for.
    pub tls_server_name: Option<String>,
    /// Link speed toward this node, in bytes per second, if you know it.
    pub bandwidth_bytes_per_sec: Option<u64>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            controller: "127.0.0.1:7000".to_string(),
            advertise: "127.0.0.1:7001".to_string(),
            heartbeat_secs: 5,
            auth_token: None,
            identity_path: None,
            tls_ca_path: None,
            tls_server_name: None,
            bandwidth_bytes_per_sec: None,
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
}

impl AgentConfig {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        toml::from_str(&contents).map_err(|source| ConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })
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

    /// The name to verify the controller's certificate against, defaulting to
    /// the host part of the controller address.
    pub fn server_name(&self) -> String {
        self.tls_server_name.clone().unwrap_or_else(|| {
            self.controller
                .rsplit_once(':')
                .map(|(host, _)| host.to_string())
                .unwrap_or_else(|| self.controller.clone())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aethermesh-agent-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn an_empty_file_yields_the_defaults() {
        let path = write("empty.toml", "");
        assert_eq!(AgentConfig::load(&path).unwrap(), AgentConfig::default());
    }

    #[test]
    fn settings_are_read_and_the_rest_defaulted() {
        let path = write(
            "agent.toml",
            r#"
            controller = "198.51.100.10:7000"
            auth_token = "s3cret"
            "#,
        );

        let config = AgentConfig::load(&path).unwrap();

        assert_eq!(config.controller, "198.51.100.10:7000");
        assert_eq!(config.auth_token.as_deref(), Some("s3cret"));
        assert_eq!(config.heartbeat_secs, 5);
    }

    #[test]
    fn the_server_name_defaults_to_the_controller_host() {
        let mut config = AgentConfig::default();
        config.controller = "mesh.example.com:7000".to_string();
        assert_eq!(config.server_name(), "mesh.example.com");

        config.tls_server_name = Some("other.example.com".to_string());
        assert_eq!(config.server_name(), "other.example.com");
    }

    #[test]
    fn unknown_keys_are_an_error_rather_than_a_silent_typo() {
        let path = write("typo.toml", "controler = \"127.0.0.1:7000\"");
        assert!(matches!(
            AgentConfig::load(&path),
            Err(ConfigError::Parse { .. })
        ));
    }
}
