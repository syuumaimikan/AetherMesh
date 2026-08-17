//! Who is allowed to join the mesh.
//!
//! The credential is a shared bearer token: simple, and enough to keep an
//! unknown process off the mesh. It is only meaningful over TLS — without it,
//! the token crosses the wire in the clear.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Registration credentials the controller accepts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SecurityConfig {
    /// Token shared by the whole mesh. `None` means no shared token.
    pub auth_token: Option<String>,
    /// Per-node tokens, keyed by a label you choose (hostname, team, tenant).
    ///
    /// Any of these is accepted, which is what lets one node's credential be
    /// revoked without re-keying the mesh.
    pub node_tokens: BTreeMap<String, String>,
}

impl SecurityConfig {
    /// No authentication. Only sensible on a trusted network.
    pub fn open() -> Self {
        Self::default()
    }

    /// Requires this exact token.
    pub fn with_token(token: impl Into<String>) -> Self {
        Self {
            auth_token: Some(token.into()),
            node_tokens: BTreeMap::new(),
        }
    }

    /// Issues one credential per node.
    pub fn with_node_tokens<K, V>(tokens: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            auth_token: None,
            node_tokens: tokens
                .into_iter()
                .map(|(label, token)| (label.into(), token.into()))
                .collect(),
        }
    }

    pub fn requires_auth(&self) -> bool {
        self.auth_token.is_some() || !self.node_tokens.is_empty()
    }

    /// Checks a credential, and reports which label it belonged to.
    ///
    /// Every candidate is compared, and always in constant time, so neither
    /// the answer nor its timing says which token was close.
    pub fn identify(&self, presented: Option<&str>) -> Result<Option<&str>, AuthError> {
        if !self.requires_auth() {
            return Ok(None);
        }
        let Some(presented) = presented else {
            return Err(AuthError::MissingToken);
        };

        let mut matched: Option<Option<&str>> = None;
        if let Some(shared) = self.auth_token.as_deref() {
            if constant_time_eq(shared.as_bytes(), presented.as_bytes()) {
                matched = Some(None);
            }
        }
        for (label, token) in &self.node_tokens {
            if constant_time_eq(token.as_bytes(), presented.as_bytes()) {
                matched = Some(Some(label.as_str()));
            }
        }

        matched.ok_or(AuthError::InvalidToken)
    }

    /// Checks an agent's or client's credential.
    pub fn authorize(&self, presented: Option<&str>) -> Result<(), AuthError> {
        self.identify(presented).map(|_| ())
    }
}

/// Why a registration was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("this controller requires a token")]
    MissingToken,
    #[error("invalid token")]
    InvalidToken,
}

/// Compares without leaking where the first difference is.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b)
        .fold(0u8, |difference, (x, y)| difference | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_open_controller_accepts_anyone() {
        let config = SecurityConfig::open();
        assert!(!config.requires_auth());
        assert_eq!(config.authorize(None), Ok(()));
        assert_eq!(config.authorize(Some("anything")), Ok(()));
    }

    #[test]
    fn a_token_is_required_when_configured() {
        let config = SecurityConfig::with_token("s3cret");

        assert_eq!(config.authorize(Some("s3cret")), Ok(()));
        assert_eq!(config.authorize(None), Err(AuthError::MissingToken));
        assert_eq!(
            config.authorize(Some("wrong")),
            Err(AuthError::InvalidToken)
        );
        // A prefix of the real token is still wrong.
        assert_eq!(
            config.authorize(Some("s3cre")),
            Err(AuthError::InvalidToken)
        );
    }

    #[test]
    fn per_node_tokens_are_accepted_and_named() {
        let config =
            SecurityConfig::with_node_tokens([("desktop", "token-a"), ("rpi4", "token-b")]);

        assert!(config.requires_auth());
        assert_eq!(config.identify(Some("token-a")), Ok(Some("desktop")));
        assert_eq!(config.identify(Some("token-b")), Ok(Some("rpi4")));
        assert_eq!(
            config.identify(Some("token-c")),
            Err(AuthError::InvalidToken)
        );
        assert_eq!(config.identify(None), Err(AuthError::MissingToken));
    }

    #[test]
    fn a_shared_token_and_node_tokens_can_coexist() {
        let mut config = SecurityConfig::with_token("shared");
        config
            .node_tokens
            .insert("rpi4".to_string(), "token-b".to_string());

        assert_eq!(config.identify(Some("shared")), Ok(None));
        assert_eq!(config.identify(Some("token-b")), Ok(Some("rpi4")));
        assert!(config.authorize(Some("nope")).is_err());
    }

    #[test]
    fn revoking_one_node_leaves_the_others_working() {
        let mut config =
            SecurityConfig::with_node_tokens([("desktop", "token-a"), ("rpi4", "token-b")]);
        config.node_tokens.remove("rpi4");

        assert!(config.authorize(Some("token-a")).is_ok());
        assert!(config.authorize(Some("token-b")).is_err());
    }

    #[test]
    fn comparison_ignores_content_but_not_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }
}
