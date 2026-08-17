//! Benchmark CLI: `aether-benchmark run` and `aether-benchmark compare`.

use std::path::PathBuf;

use aether_benchmark::{BenchmarkConfig, Mode, compare, run};
use aether_core::task::kind;
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(name = "aether-benchmark", about = "Measures AetherMesh performance")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Runs a benchmark in one mode and prints the report.
    Run {
        #[command(flatten)]
        options: Options,

        /// Configuration to measure.
        #[arg(long, value_enum, default_value_t = CliMode::Aethermesh)]
        mode: CliMode,
    },
    /// Runs both modes and reports the difference.
    Compare {
        #[command(flatten)]
        options: Options,
    },
}

#[derive(ClapArgs)]
struct Options {
    /// Number of tasks to submit.
    #[arg(long, default_value_t = 100)]
    tasks: usize,

    /// Number of nodes in the mesh.
    #[arg(long, default_value_t = 3)]
    nodes: usize,

    /// Built-in task to run.
    #[arg(long, default_value = kind::HASH)]
    kind: String,

    /// Payload size for echo/hash tasks.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,

    /// Iteration count for cpu tasks.
    #[arg(long, default_value_t = 100_000)]
    cpu_iterations: u64,

    /// Size of the shared dataset every task reads. Zero means no inputs.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    dataset_bytes: usize,

    /// Chunk size for large datasets.
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size: usize,

    /// Link speed of every node, in bytes per second.
    #[arg(long, default_value_t = 10 * 1024 * 1024)]
    bandwidth: u64,

    /// Output format.
    #[arg(long, value_enum, default_value_t = Format::Text)]
    format: Format,

    /// Write the report to this file instead of stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

impl Options {
    fn config(&self) -> BenchmarkConfig {
        BenchmarkConfig {
            tasks: self.tasks,
            nodes: self.nodes,
            kind: self.kind.clone(),
            payload_bytes: self.payload_bytes,
            cpu_iterations: self.cpu_iterations,
            dataset_bytes: self.dataset_bytes,
            chunk_size: self.chunk_size,
            bandwidth_bytes_per_sec: self.bandwidth,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum CliMode {
    Baseline,
    Aethermesh,
}

impl From<CliMode> for Mode {
    fn from(mode: CliMode) -> Self {
        match mode {
            CliMode::Baseline => Mode::Baseline,
            CliMode::Aethermesh => Mode::AetherMesh,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum Format {
    Text,
    Json,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (options, rendered) = match Args::parse().command {
        Command::Run { options, mode } => {
            let report = run(&options.config(), mode.into()).await?;
            let rendered = match options.format {
                Format::Text => report.to_text(),
                Format::Json => report.to_json()?,
            };
            (options, rendered)
        }
        Command::Compare { options } => {
            let report = compare(&options.config()).await?;
            let rendered = match options.format {
                Format::Text => report.to_text(),
                Format::Json => report.to_json()?,
            };
            (options, rendered)
        }
    };

    match options.output {
        Some(path) => {
            std::fs::write(&path, format!("{rendered}\n"))?;
            println!("report written to {}", path.display());
        }
        None => println!("{rendered}"),
    }
    Ok(())
}
