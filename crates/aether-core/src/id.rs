//! Opaque identifiers for nodes and tasks.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a new random identifier.
            pub fn generate() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID.
            pub const fn from_uuid(uuid: Uuid) -> Self {
                Self(uuid)
            }

            /// Returns the underlying UUID.
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(s)?))
            }
        }
    };
}

define_id!(NodeId, "Identifies a node participating in the mesh.");
define_id!(TaskId, "Identifies a submitted task.");
define_id!(
    WorkflowId,
    "Identifies a set of tasks submitted together, with an order between them."
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_unique() {
        assert_ne!(NodeId::generate(), NodeId::generate());
        assert_ne!(TaskId::generate(), TaskId::generate());
    }

    #[test]
    fn display_and_parse_round_trip() {
        let id = NodeId::generate();
        assert_eq!(id.to_string().parse::<NodeId>().unwrap(), id);
    }

    #[test]
    fn node_and_task_ids_are_distinct_types_over_the_same_uuid() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            NodeId::from_uuid(uuid).as_uuid(),
            TaskId::from_uuid(uuid).as_uuid()
        );
    }
}
