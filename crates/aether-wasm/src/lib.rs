//! Running WebAssembly tasks under a budget.
//!
//! This is how AetherMesh runs code written in something other than Rust —
//! TypeScript, Go, C, Python, anything with a WASM target — without giving that
//! code the run of the machine. A module gets memory, an input buffer, and a
//! fixed amount of fuel. It gets no filesystem, no network, no clock, and no
//! host functions at all: if it wants to do something, it has to compute it.
//!
//! # The ABI
//!
//! A task module exports:
//!
//! | Export | Signature | Meaning |
//! |---|---|---|
//! | `memory` | — | linear memory the host reads and writes |
//! | `alloc` | `(i32) -> i32` | reserve `len` bytes, return the offset |
//! | `run` | `(i32, i32) -> i64` | run over `(ptr, len)`, return `ptr << 32 \| len` |
//!
//! The host writes the input into `alloc(len)`, calls `run`, and reads the
//! returned slice back out of memory. Both halves of the return value are
//! unsigned; a module that returns a range outside its memory is rejected
//! rather than trusted.

use std::fmt;

#[cfg(feature = "wasmi-backend")]
mod wasmi_backend;

#[cfg(all(feature = "wasmtime-backend", not(feature = "wasmi-backend")))]
mod wasmtime_backend;

#[cfg(test)]
mod testing;

/// Name of the export the host calls.
pub const RUN_EXPORT: &str = "run";
/// Name of the allocator export.
pub const ALLOC_EXPORT: &str = "alloc";
/// Name of the memory export.
pub const MEMORY_EXPORT: &str = "memory";

/// What a module is allowed to consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WasmLimits {
    /// Fuel, roughly one unit per executed instruction. Bounds run time even
    /// for a module that loops forever.
    pub fuel: u64,
    /// Ceiling on the module's linear memory.
    pub memory_bytes: usize,
    /// Largest output the host will copy back.
    pub max_output_bytes: usize,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            fuel: 100_000_000,
            memory_bytes: 64 * 1024 * 1024,
            max_output_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Why a module did not produce a result.
#[derive(Debug, thiserror::Error)]
pub enum WasmError {
    #[error("module could not be loaded: {0}")]
    Load(String),
    #[error("module could not be instantiated: {0}")]
    Instantiate(String),
    #[error("module does not export {0}")]
    MissingExport(&'static str),
    #[error("module ran out of fuel after {0} units")]
    OutOfFuel(u64),
    #[error("module exceeded its memory limit of {0} bytes")]
    OutOfMemory(usize),
    #[error("module trapped: {0}")]
    Trap(String),
    #[error("module returned {len} bytes at offset {ptr}, which is outside its memory")]
    BadOutputRange { ptr: u64, len: u64 },
    #[error("module returned {len} bytes, over the {limit} byte limit")]
    OutputTooLarge { len: u64, limit: usize },
    #[error("no WebAssembly backend was compiled in")]
    NoBackend,
}

/// Which engine actually runs modules in this build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// `wasmi`, a pure-Rust interpreter.
    Interpreter,
    /// `wasmtime`, a Cranelift JIT.
    Jit,
    /// Neither feature was enabled.
    None,
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Interpreter => "wasmi",
            Self::Jit => "wasmtime",
            Self::None => "none",
        };
        f.write_str(name)
    }
}

/// The backend this build uses.
///
/// `wasmi` wins when both features are on: an interpreter that runs everywhere
/// is the safer default, and enabling a feature should not silently swap the
/// engine underneath a deployment.
pub const fn backend() -> Backend {
    #[cfg(feature = "wasmi-backend")]
    {
        Backend::Interpreter
    }
    #[cfg(all(feature = "wasmtime-backend", not(feature = "wasmi-backend")))]
    {
        Backend::Jit
    }
    #[cfg(not(any(feature = "wasmi-backend", feature = "wasmtime-backend")))]
    {
        Backend::None
    }
}

/// Runs `module` over `input` and returns what it wrote.
pub fn run(module: &[u8], input: &[u8], limits: &WasmLimits) -> Result<Vec<u8>, WasmError> {
    #[cfg(feature = "wasmi-backend")]
    {
        wasmi_backend::run(module, input, limits)
    }
    #[cfg(all(feature = "wasmtime-backend", not(feature = "wasmi-backend")))]
    {
        wasmtime_backend::run(module, input, limits)
    }
    #[cfg(not(any(feature = "wasmi-backend", feature = "wasmtime-backend")))]
    {
        let _ = (module, input, limits);
        Err(WasmError::NoBackend)
    }
}

/// Splits a packed `ptr << 32 | len` return value.
pub(crate) fn unpack(packed: i64) -> (u64, u64) {
    let packed = packed as u64;
    (packed >> 32, packed & 0xffff_ffff)
}

/// Checks a module's declared output range against its memory and the limits.
pub(crate) fn check_output(
    ptr: u64,
    len: u64,
    memory_len: usize,
    limits: &WasmLimits,
) -> Result<(usize, usize), WasmError> {
    if len > limits.max_output_bytes as u64 {
        return Err(WasmError::OutputTooLarge {
            len,
            limit: limits.max_output_bytes,
        });
    }
    let end = ptr
        .checked_add(len)
        .ok_or(WasmError::BadOutputRange { ptr, len })?;
    if end > memory_len as u64 {
        return Err(WasmError::BadOutputRange { ptr, len });
    }
    Ok((ptr as usize, len as usize))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_values_split_into_pointer_and_length() {
        let packed = ((1024u64 << 32) | 32) as i64;
        assert_eq!(unpack(packed), (1024, 32));
    }

    #[test]
    fn an_output_inside_memory_is_accepted() {
        let limits = WasmLimits::default();
        assert_eq!(check_output(16, 32, 1024, &limits).unwrap(), (16, 32));
    }

    #[test]
    fn an_output_past_the_end_of_memory_is_rejected() {
        let limits = WasmLimits::default();
        assert!(matches!(
            check_output(1000, 100, 1024, &limits),
            Err(WasmError::BadOutputRange { .. })
        ));
        assert!(matches!(
            check_output(u64::MAX, 1, 1024, &limits),
            Err(WasmError::BadOutputRange { .. })
        ));
    }

    #[test]
    fn an_output_over_the_limit_is_rejected() {
        let limits = WasmLimits {
            max_output_bytes: 16,
            ..WasmLimits::default()
        };
        assert!(matches!(
            check_output(0, 17, 1024, &limits),
            Err(WasmError::OutputTooLarge { .. })
        ));
    }
}
