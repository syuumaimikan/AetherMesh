//! Pure-Rust interpreter backend.

use wasmi::{Config, Engine, Instance, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};

use crate::{ALLOC_EXPORT, MEMORY_EXPORT, RUN_EXPORT, WasmError, WasmLimits, check_output, unpack};

/// Everything the store owns on the host side. No host functions are exposed,
/// so this is only the limiter.
struct HostState {
    limits: StoreLimits,
}

pub fn run(module: &[u8], input: &[u8], limits: &WasmLimits) -> Result<Vec<u8>, WasmError> {
    let mut config = Config::default();
    config.consume_fuel(true);
    let engine = Engine::new(&config);

    let module =
        Module::new(&engine, module).map_err(|error| WasmError::Load(error.to_string()))?;

    let state = HostState {
        limits: StoreLimitsBuilder::new()
            .memory_size(limits.memory_bytes)
            .build(),
    };
    let mut store = Store::new(&engine, state);
    store.limiter(|state| &mut state.limits);
    store
        .set_fuel(limits.fuel)
        .map_err(|error| WasmError::Instantiate(error.to_string()))?;

    // An empty linker is the point: the module gets no imports at all.
    let linker = Linker::<HostState>::new(&engine);
    let instance = linker
        .instantiate(&mut store, &module)
        .and_then(|pre| pre.start(&mut store))
        .map_err(|error| classify(error, limits))?;

    let memory = instance
        .get_memory(&store, MEMORY_EXPORT)
        .ok_or(WasmError::MissingExport(MEMORY_EXPORT))?;
    let alloc = instance
        .get_typed_func::<i32, i32>(&store, ALLOC_EXPORT)
        .map_err(|_| WasmError::MissingExport(ALLOC_EXPORT))?;
    let run = instance
        .get_typed_func::<(i32, i32), i64>(&store, RUN_EXPORT)
        .map_err(|_| WasmError::MissingExport(RUN_EXPORT))?;

    let ptr = alloc
        .call(&mut store, input.len() as i32)
        .map_err(|error| classify(error, limits))?;
    memory
        .write(&mut store, ptr as usize, input)
        .map_err(|_| WasmError::BadOutputRange {
            ptr: ptr as u64,
            len: input.len() as u64,
        })?;

    let packed = run
        .call(&mut store, (ptr, input.len() as i32))
        .map_err(|error| classify(error, limits))?;

    let (out_ptr, out_len) = unpack(packed);
    let memory_len = memory.data(&store).len();
    let (out_ptr, out_len) = check_output(out_ptr, out_len, memory_len, limits)?;

    Ok(memory.data(&store)[out_ptr..out_ptr + out_len].to_vec())
}

/// Turns a wasmi error into the shared error type, keeping the two failures
/// that callers act on - fuel and memory - distinguishable from a plain trap.
fn classify(error: wasmi::Error, limits: &WasmLimits) -> WasmError {
    let text = error.to_string();
    if text.contains("fuel") {
        WasmError::OutOfFuel(limits.fuel)
    } else if text.contains("memory") && text.contains("limit") {
        WasmError::OutOfMemory(limits.memory_bytes)
    } else {
        WasmError::Trap(text)
    }
}

/// Kept so the instance type is named somewhere: instantiation returns it and
/// the rest of the function only borrows it.
#[allow(dead_code)]
type _Instance = Instance;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{ECHO_WAT, INCREMENT_WAT, INFINITE_LOOP_WAT, wasm};

    #[test]
    fn a_module_sees_its_input_and_returns_output() {
        let output = run(&wasm(ECHO_WAT), b"aethermesh", &WasmLimits::default()).unwrap();
        assert_eq!(output, b"aethermesh");
    }

    #[test]
    fn a_module_can_transform_the_input() {
        let output = run(&wasm(INCREMENT_WAT), &[1, 2, 255], &WasmLimits::default()).unwrap();
        assert_eq!(output, vec![2, 3, 0]);
    }

    #[test]
    fn an_endless_module_runs_out_of_fuel() {
        let limits = WasmLimits {
            fuel: 100_000,
            ..WasmLimits::default()
        };
        let error = run(&wasm(INFINITE_LOOP_WAT), b"", &limits).unwrap_err();

        assert!(matches!(error, WasmError::OutOfFuel(_)), "{error:?}");
    }
}
