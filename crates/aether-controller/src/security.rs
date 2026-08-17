//! Who is allowed to join the mesh.
//!
//! The credential is a shared bearer token: simple, and enough to keep an
//! unknown process off the mesh. It is only meaningful over TLS — without it,
//! the token crosses the wire in the clear.

use serde::{Deserialize, Serialize};

/// Registration credentials the controller accepts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Token every agent must present. `None` accepts any agent.
    pub auth_token: Option<String>,
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
        }
    }

    pub fn requires_auth(&self) -> bool {
        self.auth_token.is_some()
    }

    /// Checks an agent's credential.
    pub fn authorize(&self, presented: Option<&str>) -> Result<(), AuthError> {
        let Some(expected) = self.auth_token.as_deref() else {
            return Ok(());
        };
        let Some(presented) = presented else {
            return Err(AuthError::MissingToken);
        };

        if constant_time_eq(expected.as_bytes(), presented.as_bytes()) {
            Ok(())
        } else {
            Err(AuthError::InvalidToken)
        }
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
    fn comparison_ignores_content_but_not_length() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(constant_time_eq(b"", b""));
    }
}
