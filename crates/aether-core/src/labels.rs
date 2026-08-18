//! Saying which machines a task may run on.
//!
//! Locality and load answer "where is this cheapest". Labels answer "where is
//! this *allowed*" — the GPU box, the machine inside the right jurisdiction,
//! the node with the licence for that dataset. Cheapest is only useful among
//! the nodes that qualify.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Key/value tags describing a node: `gpu=true`, `region=eu-west`, `arch=arm64`.
pub type Labels = BTreeMap<String, String>;

/// One condition a node must satisfy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Constraint {
    /// The node carries this label with this value.
    Equals { key: String, value: String },
    /// The node carries this label with any value.
    Exists { key: String },
    /// The node does not carry this label with this value.
    NotEquals { key: String, value: String },
}

impl Constraint {
    pub fn equals(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::Equals {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn exists(key: impl Into<String>) -> Self {
        Self::Exists { key: key.into() }
    }

    pub fn not_equals(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::NotEquals {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Whether a node's labels satisfy this condition.
    pub fn is_satisfied_by(&self, labels: &Labels) -> bool {
        match self {
            Self::Equals { key, value } => labels.get(key) == Some(value),
            Self::Exists { key } => labels.contains_key(key),
            // A node without the label satisfies "not equals": the condition is
            // about the value being wrong, not about the label being present.
            Self::NotEquals { key, value } => labels.get(key) != Some(value),
        }
    }
}

/// A constraint could not be read from its text form.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("`{input}` is not a constraint (expected `key`, `key=value`, or `key!=value`)")]
pub struct ConstraintParseError {
    pub input: String,
}

impl std::str::FromStr for Constraint {
    type Err = ConstraintParseError;

    /// Reads the form a CLI flag or an SDK caller writes:
    /// `gpu` (present), `region=eu-west` (equal), `arch!=x86_64` (not equal).
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let invalid = || ConstraintParseError {
            input: input.to_string(),
        };
        let text = input.trim();

        if let Some((key, value)) = text.split_once("!=") {
            let key = key.trim();
            if key.is_empty() {
                return Err(invalid());
            }
            return Ok(Self::not_equals(key, value.trim()));
        }
        if let Some((key, value)) = text.split_once('=') {
            let key = key.trim();
            if key.is_empty() {
                return Err(invalid());
            }
            return Ok(Self::equals(key, value.trim()));
        }
        if text.is_empty() {
            return Err(invalid());
        }
        Ok(Self::exists(text))
    }
}

impl std::fmt::Display for Constraint {
    /// The inverse of [`Constraint::from_str`], so a constraint survives a
    /// round trip through a config file or a log line.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Equals { key, value } => write!(f, "{key}={value}"),
            Self::Exists { key } => write!(f, "{key}"),
            Self::NotEquals { key, value } => write!(f, "{key}!={value}"),
        }
    }
}

/// Whether a node satisfies every constraint. An empty list matches anything.
pub fn satisfies_all(labels: &Labels, constraints: &[Constraint]) -> bool {
    constraints
        .iter()
        .all(|constraint| constraint.is_satisfied_by(labels))
}

/// Parses `key=value` pairs, the form a CLI flag or an environment variable
/// gives you: `--label gpu=true --label region=eu-west`.
pub fn parse_labels<I, S>(pairs: I) -> Labels
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    pairs
        .into_iter()
        .filter_map(|pair| {
            let pair = pair.as_ref();
            pair.split_once('=')
                .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
                .filter(|(key, _)| !key.is_empty())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> Labels {
        parse_labels(["gpu=true", "region=eu-west", "arch=arm64"])
    }

    #[test]
    fn pairs_parse_and_trim() {
        let parsed = parse_labels(["gpu = true", "region=eu-west", "nonsense", "=orphan"]);

        assert_eq!(parsed.get("gpu").map(String::as_str), Some("true"));
        assert_eq!(parsed.get("region").map(String::as_str), Some("eu-west"));
        assert_eq!(parsed.len(), 2, "malformed pairs are dropped");
    }

    #[test]
    fn equals_matches_only_the_exact_value() {
        assert!(Constraint::equals("gpu", "true").is_satisfied_by(&labels()));
        assert!(!Constraint::equals("gpu", "false").is_satisfied_by(&labels()));
        assert!(!Constraint::equals("missing", "true").is_satisfied_by(&labels()));
    }

    #[test]
    fn exists_ignores_the_value() {
        assert!(Constraint::exists("region").is_satisfied_by(&labels()));
        assert!(!Constraint::exists("licence").is_satisfied_by(&labels()));
    }

    #[test]
    fn not_equals_is_satisfied_by_absence() {
        assert!(Constraint::not_equals("region", "us-east").is_satisfied_by(&labels()));
        assert!(!Constraint::not_equals("region", "eu-west").is_satisfied_by(&labels()));
        // The label is not there at all, so its value is certainly not that.
        assert!(Constraint::not_equals("licence", "expired").is_satisfied_by(&labels()));
    }

    #[test]
    fn every_constraint_has_to_hold() {
        let constraints = vec![
            Constraint::equals("gpu", "true"),
            Constraint::exists("region"),
            Constraint::not_equals("arch", "x86_64"),
        ];
        assert!(satisfies_all(&labels(), &constraints));

        let stricter = vec![
            Constraint::equals("gpu", "true"),
            Constraint::exists("nvme"),
        ];
        assert!(!satisfies_all(&labels(), &stricter));
    }

    #[test]
    fn constraints_parse_from_their_text_form() {
        assert_eq!(
            "gpu=true".parse::<Constraint>().unwrap(),
            Constraint::equals("gpu", "true")
        );
        assert_eq!(
            "arch != x86_64".parse::<Constraint>().unwrap(),
            Constraint::not_equals("arch", "x86_64")
        );
        assert_eq!(
            "region".parse::<Constraint>().unwrap(),
            Constraint::exists("region")
        );
        // `!=` is checked first, so it never reads as an equals of `arch!`.
        assert!("=orphan".parse::<Constraint>().is_err());
        assert!("".parse::<Constraint>().is_err());
    }

    #[test]
    fn constraints_survive_a_round_trip_through_text() {
        for text in ["gpu=true", "arch!=x86_64", "region"] {
            let parsed: Constraint = text.parse().unwrap();
            assert_eq!(parsed.to_string(), text);
        }
    }

    #[test]
    fn no_constraints_matches_anything() {
        assert!(satisfies_all(&Labels::new(), &[]));
        assert!(satisfies_all(&labels(), &[]));
    }
}
