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
//!
//! # Reading datasets
//!
//! A module may also import three functions from `aether` to read the datasets
//! the task declared as inputs. They are optional: a module that imports none
//! of them is unaffected, and there is nothing else to import — no files, no
//! sockets, no clock.
//!
//! | Import | Signature | Meaning |
//! |---|---|---|
//! | `aether.input_count` | `() -> i32` | how many datasets the task declared |
//! | `aether.input_len` | `(i32) -> i32` | size of one dataset, `-1` if there is no such input |
//! | `aether.input_read` | `(i32, i32, i32) -> i32` | copy `(index, ptr, len)` into memory, returns bytes copied or `-1` |

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(feature = "wasmi-backend")]
mod wasmi_backend;

#[cfg(all(feature = "wasmtime-backend", not(feature = "wasmi-backend")))]
mod wasmtime_backend;

#[cfg(test)]
mod testing;

/// Import namespace for the host functions a module may use.
pub const HOST_MODULE: &str = "aether";

/// Name of the export the host calls.
pub const RUN_EXPORT: &str = "run";
/// Name of the allocator export.
pub const ALLOC_EXPORT: &str = "alloc";
/// Name of the memory export.
pub const MEMORY_EXPORT: &str = "memory";

/// Host functions a module is allowed to call, beyond reading its inputs.
///
/// Everything here is off by default. A capability is granted by the operator
/// running the node, not requested by the module: a module that imports
/// something it was not granted fails to instantiate rather than silently
/// getting a stub.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WasmCapabilities {
    /// `aether.log(ptr, len)` — write a line to the node's log.
    pub log: bool,
    /// `aether.now_unix_millis() -> i64` — read the wall clock.
    ///
    /// A clock is a side channel as well as a convenience: it lets a module
    /// tell how long it has been running and behave differently when watched.
    pub clock: bool,
    /// `aether.random(ptr, len) -> i32` — fill a buffer with random bytes from
    /// the operating system's CSPRNG, so the bytes are safe for keys and nonces.
    ///
    /// Granting this makes tasks non-deterministic, which means a retry on
    /// another node can produce a different answer.
    pub random: bool,
    /// `aether.file_size(path_ptr, path_len) -> i64` and
    /// `aether.file_read(path_ptr, path_len, offset, ptr, len) -> i32` — read
    /// files under one directory, and only under it.
    ///
    /// This is the one capability that reaches outside the process. Every path
    /// is resolved and then checked to be inside the root, so `..`, absolute
    /// paths, and symlinks pointing elsewhere are all refused. Reads only:
    /// there is no write, no create, no list, and no delete.
    pub read_dir: Option<PathBuf>,
}

impl WasmCapabilities {
    /// Nothing but the task's own inputs. The default.
    pub fn none() -> Self {
        Self::default()
    }

    /// Everything except filesystem access, which needs a root to allow.
    pub fn all() -> Self {
        Self {
            log: true,
            clock: true,
            random: true,
            read_dir: None,
        }
    }

    /// Allows reading files under `root`, and nowhere else.
    pub fn with_read_dir(mut self, root: impl Into<PathBuf>) -> Self {
        self.read_dir = Some(root.into());
        self
    }

    pub fn with_log(mut self, enabled: bool) -> Self {
        self.log = enabled;
        self
    }

    pub fn with_clock(mut self, enabled: bool) -> Self {
        self.clock = enabled;
        self
    }

    pub fn with_random(mut self, enabled: bool) -> Self {
        self.random = enabled;
        self
    }
}

/// What a module is allowed to consume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmLimits {
    /// Fuel, roughly one unit per executed instruction. Bounds run time even
    /// for a module that loops forever.
    pub fuel: u64,
    /// Ceiling on the module's linear memory.
    pub memory_bytes: usize,
    /// Largest output the host will copy back.
    pub max_output_bytes: usize,
    /// Host functions this module may call.
    pub capabilities: WasmCapabilities,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            fuel: 100_000_000,
            memory_bytes: 64 * 1024 * 1024,
            max_output_bytes: 64 * 1024 * 1024,
            capabilities: WasmCapabilities::none(),
        }
    }
}

impl WasmLimits {
    /// Grants host functions to a module.
    pub fn with_capabilities(mut self, capabilities: WasmCapabilities) -> Self {
        self.capabilities = capabilities;
        self
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
    run_with_inputs(module, input, &[], limits)
}

/// Same, with datasets the module can read through the `aether` imports.
///
/// `inputs` are the task's declared datasets, in the order the task listed
/// them. They are shared, not copied: nothing enters the module's memory
/// unless the module asks for it.
pub fn run_with_inputs(
    module: &[u8],
    input: &[u8],
    inputs: &[Arc<[u8]>],
    limits: &WasmLimits,
) -> Result<Vec<u8>, WasmError> {
    #[cfg(feature = "wasmi-backend")]
    {
        wasmi_backend::run(module, input, inputs, limits)
    }
    #[cfg(all(feature = "wasmtime-backend", not(feature = "wasmi-backend")))]
    {
        wasmtime_backend::run(module, input, inputs, limits)
    }
    #[cfg(not(any(feature = "wasmi-backend", feature = "wasmtime-backend")))]
    {
        let _ = (module, input, inputs, limits);
        Err(WasmError::NoBackend)
    }
}

/// Copies part of a dataset into a module's memory.
///
/// Shared by the backends: bounds are checked against both the dataset and the
/// module's memory, and a request that does not fit answers `-1` instead of
/// trapping, so a module can probe sizes without dying.
pub(crate) fn read_input(
    inputs: &[Arc<[u8]>],
    index: i32,
    len: i32,
    memory: &mut [u8],
    ptr: i32,
) -> i32 {
    let (Ok(index), Ok(len), Ok(ptr)) = (
        usize::try_from(index),
        usize::try_from(len),
        usize::try_from(ptr),
    ) else {
        return -1;
    };

    let Some(data) = inputs.get(index) else {
        return -1;
    };
    let len = len.min(data.len());
    let Some(target) = memory.get_mut(ptr..ptr + len) else {
        return -1;
    };

    target.copy_from_slice(&data[..len]);
    len as i32
}

/// Reads a UTF-8 string out of a module's memory, for the `log` capability.
///
/// Invalid bytes are replaced rather than rejected: a module logging garbage is
/// a bug in the module, not a reason to kill the task.
pub(crate) fn read_text(memory: &[u8], ptr: i32, len: i32) -> Option<String> {
    let (Ok(ptr), Ok(len)) = (usize::try_from(ptr), usize::try_from(len)) else {
        return None;
    };
    let slice = memory.get(ptr..ptr.checked_add(len)?)?;
    Some(String::from_utf8_lossy(slice).into_owned())
}

/// Milliseconds since the Unix epoch, for the `clock` capability.
pub(crate) fn now_unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0)
}

/// Fills a module's buffer with random bytes, for the `random` capability.
///
/// The bytes come from the operating system's CSPRNG. A module cannot tell
/// what its output is used for, so a caller generating a key or a nonce has to
/// be right by default; a cheaper generator here would silently be wrong.
///
/// Returns the number of bytes written, `-1` if the range is not inside the
/// module's memory, and `-2` if the OS refused to provide entropy.
pub(crate) fn fill_random(memory: &mut [u8], ptr: i32, len: i32) -> i32 {
    let (Ok(ptr), Ok(len)) = (usize::try_from(ptr), usize::try_from(len)) else {
        return -1;
    };
    let Some(target) = memory.get_mut(ptr..ptr.saturating_add(len)) else {
        return -1;
    };

    // Failing loudly beats handing back a buffer of zeroes that looks random.
    match getrandom::fill(target) {
        Ok(()) => len as i32,
        Err(error) => {
            tracing::warn!(%error, "the OS refused entropy for a wasm random call");
            -2
        }
    }
}

/// Resolves a module-supplied path inside the granted root.
///
/// Returns `None` unless the resolved path really is under the root: `..`, an
/// absolute path, and a symlink pointing outside all fail here rather than at
/// the point of reading.
pub(crate) fn resolve_in_root(root: &Path, requested: &str) -> Option<PathBuf> {
    // A path the module supplies is never trusted as absolute.
    let relative = Path::new(requested);
    if relative.is_absolute() || requested.contains('\0') {
        return None;
    }

    let candidate = root.join(relative);
    // Canonicalising both sides is what makes symlinks and `..` harmless: the
    // comparison happens on the real location, not the spelling.
    let real_root = std::fs::canonicalize(root).ok()?;
    let real_path = std::fs::canonicalize(&candidate).ok()?;

    real_path.starts_with(&real_root).then_some(real_path)
}

/// Size of a file inside the granted root, or `-1`.
pub(crate) fn file_size(root: Option<&Path>, requested: &str) -> i64 {
    let Some(root) = root else { return -1 };
    let Some(path) = resolve_in_root(root, requested) else {
        return -1;
    };

    std::fs::metadata(&path)
        .ok()
        .filter(|metadata| metadata.is_file())
        .and_then(|metadata| i64::try_from(metadata.len()).ok())
        .unwrap_or(-1)
}

/// Copies part of a file inside the granted root into a module's memory.
///
/// Returns the number of bytes read, or `-1` for anything refused.
pub(crate) fn file_read(
    root: Option<&Path>,
    requested: &str,
    offset: i64,
    memory: &mut [u8],
    ptr: i32,
    len: i32,
) -> i32 {
    use std::io::{Read, Seek, SeekFrom};

    let Some(root) = root else { return -1 };
    let Some(path) = resolve_in_root(root, requested) else {
        return -1;
    };
    let (Ok(ptr), Ok(len), Ok(offset)) = (
        usize::try_from(ptr),
        usize::try_from(len),
        u64::try_from(offset),
    ) else {
        return -1;
    };

    let Some(target) = memory.get_mut(ptr..ptr.saturating_add(len)) else {
        return -1;
    };
    let Ok(mut file) = std::fs::File::open(&path) else {
        return -1;
    };
    if file.seek(SeekFrom::Start(offset)).is_err() {
        return -1;
    }

    let mut written = 0usize;
    while written < len {
        match file.read(&mut target[written..]) {
            Ok(0) => break,
            Ok(count) => written += count,
            Err(_) => return -1,
        }
    }
    written as i32
}

/// Size of one declared dataset, or `-1` when there is no such input.
pub(crate) fn input_len(inputs: &[Arc<[u8]>], index: i32) -> i32 {
    usize::try_from(index)
        .ok()
        .and_then(|index| inputs.get(index))
        .and_then(|data| i32::try_from(data.len()).ok())
        .unwrap_or(-1)
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

    #[test]
    fn random_fills_the_requested_range_and_nothing_else() {
        let mut memory = vec![0u8; 128];
        assert_eq!(fill_random(&mut memory, 16, 32), 32);

        assert!(
            memory[16..48].iter().any(|byte| *byte != 0),
            "32 zero bytes from a CSPRNG is not a plausible draw"
        );
        assert!(memory[..16].iter().all(|byte| *byte == 0));
        assert!(memory[48..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn two_draws_differ() {
        let mut first = vec![0u8; 32];
        let mut second = vec![0u8; 32];
        fill_random(&mut first, 0, 32);
        fill_random(&mut second, 0, 32);

        // A generator seeded once per process would fail this.
        assert_ne!(first, second);
    }

    #[test]
    fn a_range_outside_memory_is_refused() {
        let mut memory = vec![0u8; 32];
        assert_eq!(fill_random(&mut memory, 24, 16), -1);
        assert_eq!(fill_random(&mut memory, -1, 4), -1);
        assert_eq!(fill_random(&mut memory, 0, -4), -1);
        assert!(memory.iter().all(|byte| *byte == 0));
    }
}
