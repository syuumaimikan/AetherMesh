//! Remembering which workflow steps finished, so a restart does not redo them.
//!
//! The journal holds *where* a step's output is, never the output itself. A
//! checkpoint file that accumulated intermediate results would be the thing
//! this project exists to avoid: data walking to a central place because the
//! system could not think of anywhere better to put it. The outputs stay on
//! the nodes that computed them, which is where the next step wants them
//! anyway.
//!
//! That is also the limit of what this can do, and it is worth being exact
//! about. A step is skipped only if its output is still in the catalog, which
//! is checked rather than assumed — a node that left the mesh takes what it
//! computed with it, and those steps run again.
//!
//! The catalog lives in memory, so a **restarted controller** resumes nothing:
//! the journal survives, but the knowledge of where anything is does not, and
//! every step runs again. What this does buy is the ordinary case — a workflow
//! that failed halfway, submitted again — where the nodes never went anywhere
//! and only the steps that did not finish are repeated. Making a restart
//! resume too needs the catalog to be rebuilt from the agents, which is a
//! protocol change and not this.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use aether_core::{DataId, NodeId, TaskId, Workflow};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// One finished step, as written to the journal.
///
/// Only steps that *succeeded* are recorded. A failed step is not something to
/// skip on the way back through: it is the reason you are back here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    /// Which run this belongs to, as named by whoever submitted it.
    pub run: String,
    /// What the workflow looked like, so a run cannot resume into a different
    /// one. See [`fingerprint`].
    pub fingerprint: String,
    pub step: usize,
    pub task_id: TaskId,
    /// The node that ran it, and therefore the node that holds the output.
    pub node_id: NodeId,
    /// Where the output is. Without it there is nothing to resume onto.
    ///
    /// Written as hex rather than as the array of 32 numbers a `DataId`
    /// serialises to by default. This is a file somebody will end up reading
    /// with `cat` on a bad morning, and it is a quarter of the size.
    #[serde(with = "hex_data_id")]
    pub output_id: DataId,
    pub duration_ms: u64,
}

mod hex_data_id {
    use aether_core::DataId;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(id: &DataId, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&id.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<DataId, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

/// A journal could not be used.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    #[error("checkpoint file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The run exists but was recorded against a different workflow.
    ///
    /// Refused rather than resumed. Skipping step 3 because *some other*
    /// workflow's step 3 finished is the one failure mode of resuming that
    /// produces a confident wrong answer instead of an error.
    #[error(
        "run {run} was recorded against a different workflow \
         (recorded {recorded}, submitted {submitted}); \
         use a new run name, or resubmit the workflow it was recorded with"
    )]
    Fingerprint {
        run: String,
        recorded: String,
        submitted: String,
    },
}

/// What a workflow is, independently of any particular submission of it.
///
/// Task ids are deliberately excluded: they are generated fresh every time a
/// `Task` is constructed, so including them would mean no run ever matched its
/// own journal. What is included is everything that decides what the work
/// actually is — each step's kind, payload, declared inputs, and dependencies,
/// in order.
///
/// Hashed with [`DataId::of`] rather than by reaching for a hasher directly:
/// it is the same BLAKE3 the rest of the project addresses data with, and a
/// workflow is small — its payloads are task arguments, while the actual data
/// travels as input ids.
pub fn fingerprint(workflow: &Workflow) -> String {
    let mut bytes = Vec::new();
    for step in &workflow.steps {
        bytes.extend_from_slice(step.task.kind.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&step.task.payload);
        bytes.push(0);
        for input in &step.task.inputs {
            bytes.extend_from_slice(input.as_bytes());
        }
        bytes.push(0);
        for dependency in &step.depends_on {
            bytes.extend_from_slice(&(*dependency as u64).to_le_bytes());
        }
        bytes.push(0);
    }
    DataId::of(&bytes).to_string()
}

/// An append-only record of finished steps, on this machine's disk.
///
/// One JSON object per line, rather than one document for the whole file. A
/// crash while writing a document loses everything already recorded; a crash
/// while writing a line loses that line, and the reader skips it.
pub struct Journal {
    path: PathBuf,
    file: Mutex<File>,
}

impl Journal {
    /// Opens the file, creating it if it is not there.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, CheckpointError> {
        let path = path.into();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|source| CheckpointError::Io {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|source| CheckpointError::Io {
                path: path.clone(),
                source,
            })?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one finished step and puts it on the disk before returning.
    ///
    /// Synced rather than buffered, because the event this exists for is the
    /// process not getting a chance to flush. A step that just ran somewhere
    /// across the network will not notice one fsync.
    pub fn append(&self, record: &Record) -> Result<(), CheckpointError> {
        let mut line = serde_json::to_string(record).expect("a Record always serialises");
        line.push('\n');

        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        file.write_all(line.as_bytes())
            .and_then(|()| file.sync_data())
            .map_err(|source| CheckpointError::Io {
                path: self.path.clone(),
                source,
            })
    }

    /// Every record in the file, oldest first.
    ///
    /// A line that does not parse is skipped with a warning rather than
    /// failing the load: the usual reason for one is a process that died
    /// mid-write, and that is exactly when this file is about to be needed.
    pub fn records(&self) -> Result<Vec<Record>, CheckpointError> {
        let file = File::open(&self.path).map_err(|source| CheckpointError::Io {
            path: self.path.clone(),
            source,
        })?;
        let mut records = Vec::new();
        for (number, line) in BufReader::new(file).lines().enumerate() {
            let line = line.map_err(|source| CheckpointError::Io {
                path: self.path.clone(),
                source,
            })?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str(&line) {
                Ok(record) => records.push(record),
                Err(error) => warn!(
                    path = %self.path.display(),
                    line = number + 1,
                    %error,
                    "skipping an unreadable checkpoint line"
                ),
            }
        }
        Ok(records)
    }

    /// The steps of `run` that finished, keyed by step index.
    ///
    /// Errors if the run was recorded against a different workflow. A later
    /// record for the same step wins, which is what you want when a step was
    /// re-run after its output was lost.
    pub fn completed(
        &self,
        run: &str,
        fingerprint: &str,
    ) -> Result<HashMap<usize, Record>, CheckpointError> {
        let mut completed = HashMap::new();
        for record in self.records()? {
            if record.run != run {
                continue;
            }
            if record.fingerprint != fingerprint {
                return Err(CheckpointError::Fingerprint {
                    run: run.to_string(),
                    recorded: record.fingerprint,
                    submitted: fingerprint.to_string(),
                });
            }
            completed.insert(record.step, record);
        }
        Ok(completed)
    }
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Journal")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_core::task::kind;
    use aether_core::{Step, Task};

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "aethermesh-checkpoint-{}-{name}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("journal.jsonl")
    }

    fn record(run: &str, fingerprint: &str, step: usize) -> Record {
        Record {
            run: run.to_string(),
            fingerprint: fingerprint.to_string(),
            step,
            task_id: TaskId::generate(),
            node_id: NodeId::generate(),
            output_id: DataId::of(format!("step-{step}").as_bytes()),
            duration_ms: 12,
        }
    }

    fn workflow(payloads: &[&str]) -> Workflow {
        let steps = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| {
                let task = Task::new(kind::HASH, payload.as_bytes().to_vec());
                if index == 0 {
                    Step::new(task)
                } else {
                    Step::after(task, vec![index - 1])
                }
            })
            .collect();
        Workflow::new(steps).unwrap()
    }

    #[test]
    fn a_fingerprint_ignores_task_ids_but_not_the_work() {
        // Two constructions of the same workflow have different task ids.
        assert_eq!(
            fingerprint(&workflow(&["a", "b"])),
            fingerprint(&workflow(&["a", "b"]))
        );
        assert_ne!(
            fingerprint(&workflow(&["a", "b"])),
            fingerprint(&workflow(&["a", "c"]))
        );
        assert_ne!(
            fingerprint(&workflow(&["a", "b"])),
            fingerprint(&workflow(&["a", "b", "c"]))
        );
    }

    #[test]
    fn a_fingerprint_separates_steps() {
        // Without the separators, ("ab", "c") and ("a", "bc") would collide.
        assert_ne!(
            fingerprint(&workflow(&["ab", "c"])),
            fingerprint(&workflow(&["a", "bc"]))
        );
    }

    #[test]
    fn records_survive_a_reopen() {
        let path = temp("reopen");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path).unwrap();
        journal.append(&record("nightly", "fp", 0)).unwrap();
        journal.append(&record("nightly", "fp", 1)).unwrap();
        drop(journal);

        let reopened = Journal::open(&path).unwrap();
        let completed = reopened.completed("nightly", "fp").unwrap();

        assert_eq!(completed.len(), 2);
        assert!(completed.contains_key(&0));
        assert!(completed.contains_key(&1));
    }

    #[test]
    fn other_runs_are_not_mine() {
        let path = temp("runs");
        let _ = std::fs::remove_file(&path);
        let journal = Journal::open(&path).unwrap();

        journal.append(&record("nightly", "fp", 0)).unwrap();
        journal.append(&record("hourly", "fp", 1)).unwrap();

        let completed = journal.completed("nightly", "fp").unwrap();
        assert_eq!(completed.keys().copied().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn a_run_cannot_resume_into_a_different_workflow() {
        let path = temp("fingerprint");
        let _ = std::fs::remove_file(&path);
        let journal = Journal::open(&path).unwrap();
        journal.append(&record("nightly", "old", 0)).unwrap();

        let error = journal.completed("nightly", "new").unwrap_err();

        assert!(matches!(error, CheckpointError::Fingerprint { .. }));
        // And the message says what to do about it, because the answer is not
        // obvious from "mismatch".
        assert!(error.to_string().contains("use a new run name"));
    }

    #[test]
    fn a_torn_last_line_costs_one_step_and_not_the_file() {
        let path = temp("torn");
        let _ = std::fs::remove_file(&path);
        let journal = Journal::open(&path).unwrap();
        journal.append(&record("nightly", "fp", 0)).unwrap();
        drop(journal);

        // A process that died halfway through writing the second line.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"run":"nightly","fingerpr"#).unwrap();
        drop(file);

        let journal = Journal::open(&path).unwrap();
        let completed = journal.completed("nightly", "fp").unwrap();

        assert_eq!(completed.keys().copied().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn a_re_run_step_replaces_the_older_record() {
        let path = temp("rerun");
        let _ = std::fs::remove_file(&path);
        let journal = Journal::open(&path).unwrap();

        let first = record("nightly", "fp", 0);
        let mut second = record("nightly", "fp", 0);
        second.output_id = DataId::of(b"recomputed");
        journal.append(&first).unwrap();
        journal.append(&second).unwrap();

        let completed = journal.completed("nightly", "fp").unwrap();
        assert_eq!(completed[&0].output_id, second.output_id);
    }

    #[test]
    fn a_missing_file_is_created_rather_than_an_error() {
        let path = temp("created").parent().unwrap().join("nested/deep.jsonl");
        let _ = std::fs::remove_file(&path);

        let journal = Journal::open(&path).unwrap();

        assert!(journal.records().unwrap().is_empty());
        assert!(path.exists());
    }
}
