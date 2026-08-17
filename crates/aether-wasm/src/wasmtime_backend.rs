//! Cranelift JIT backend. Same ABI, same imports, same limits, different engine.

use wasmtime::{Caller, Config, Engine, Linker, Module, Store, StoreLimits, StoreLimitsBuilder};

use tracing::info;

use crate::{
    ALLOC_EXPORT, HOST_MODULE, MEMORY_EXPORT, RUN_EXPORT, WasmError, WasmLimits, check_output,
    file_read, file_size, fill_random, input_len, now_unix_millis, read_input, read_text, unpack,
};

struct HostState {
    limits: StoreLimits,
    inputs: Vec<std::sync::Arc<[u8]>>,
    read_dir: Option<std::path::PathBuf>,
}

pub fn run(
    module: &[u8],
    input: &[u8],
    inputs: &[std::sync::Arc<[u8]>],
    limits: &WasmLimits,
) -> Result<Vec<u8>, WasmError> {
    let mut config = Config::new();
    config.consume_fuel(true);
    let engine = Engine::new(&config).map_err(|error| WasmError::Load(error.to_string()))?;

    let module =
        Module::new(&engine, module).map_err(|error| WasmError::Load(error.to_string()))?;

    let state = HostState {
        limits: StoreLimitsBuilder::new()
            .memory_size(limits.memory_bytes)
            .build(),
        inputs: inputs.to_vec(),
        read_dir: limits.capabilities.read_dir.clone(),
    };
    let mut store = Store::new(&engine, state);
    store.limiter(|state| &mut state.limits);
    store
        .set_fuel(limits.fuel)
        .map_err(|error| WasmError::Instantiate(error.to_string()))?;

    // The only imports in existence: read the task's declared datasets.
    let mut linker = Linker::<HostState>::new(&engine);
    linker
        .func_wrap(HOST_MODULE, "input_count", |caller: Caller<HostState>| {
            caller.data().inputs.len() as i32
        })
        .and_then(|linker| {
            linker.func_wrap(
                HOST_MODULE,
                "input_len",
                |caller: Caller<HostState>, index: i32| input_len(&caller.data().inputs, index),
            )
        })
        .and_then(|linker| {
            linker.func_wrap(
                HOST_MODULE,
                "input_read",
                |mut caller: Caller<HostState>, index: i32, ptr: i32, len: i32| {
                    let Some(memory) = caller
                        .get_export(MEMORY_EXPORT)
                        .and_then(|export| export.into_memory())
                    else {
                        return -1;
                    };
                    let (memory, state) = memory.data_and_store_mut(&mut caller);
                    read_input(&state.inputs, index, len, memory, ptr)
                },
            )
        })
        .map_err(|error| WasmError::Instantiate(error.to_string()))?;

    if limits.capabilities.log {
        linker
            .func_wrap(
                HOST_MODULE,
                "log",
                |mut caller: Caller<HostState>, ptr: i32, len: i32| {
                    let Some(memory) = caller
                        .get_export(MEMORY_EXPORT)
                        .and_then(|export| export.into_memory())
                    else {
                        return;
                    };
                    if let Some(text) = read_text(memory.data(&caller), ptr, len) {
                        info!(target: "wasm_task", "{text}");
                    }
                },
            )
            .map_err(|error| WasmError::Instantiate(error.to_string()))?;
    }
    if limits.capabilities.clock {
        linker
            .func_wrap(HOST_MODULE, "now_unix_millis", || now_unix_millis())
            .map_err(|error| WasmError::Instantiate(error.to_string()))?;
    }
    if limits.capabilities.random {
        linker
            .func_wrap(
                HOST_MODULE,
                "random",
                |mut caller: Caller<HostState>, ptr: i32, len: i32| {
                    let Some(memory) = caller
                        .get_export(MEMORY_EXPORT)
                        .and_then(|export| export.into_memory())
                    else {
                        return -1;
                    };
                    fill_random(memory.data_mut(&mut caller), ptr, len)
                },
            )
            .map_err(|error| WasmError::Instantiate(error.to_string()))?;
    }

    if limits.capabilities.read_dir.is_some() {
        linker
            .func_wrap(
                HOST_MODULE,
                "file_size",
                |mut caller: Caller<HostState>, path_ptr: i32, path_len: i32| -> i64 {
                    let Some(memory) = caller
                        .get_export(MEMORY_EXPORT)
                        .and_then(|export| export.into_memory())
                    else {
                        return -1;
                    };
                    let Some(path) = read_text(memory.data(&caller), path_ptr, path_len) else {
                        return -1;
                    };
                    file_size(caller.data().read_dir.as_deref(), &path)
                },
            )
            .and_then(|linker| {
                linker.func_wrap(
                    HOST_MODULE,
                    "file_read",
                    |mut caller: Caller<HostState>,
                     path_ptr: i32,
                     path_len: i32,
                     offset: i64,
                     ptr: i32,
                     len: i32|
                     -> i32 {
                        let Some(memory) = caller
                            .get_export(MEMORY_EXPORT)
                            .and_then(|export| export.into_memory())
                        else {
                            return -1;
                        };
                        let Some(path) = read_text(memory.data(&caller), path_ptr, path_len) else {
                            return -1;
                        };
                        let (memory, state) = memory.data_and_store_mut(&mut caller);
                        file_read(state.read_dir.as_deref(), &path, offset, memory, ptr, len)
                    },
                )
            })
            .map_err(|error| WasmError::Instantiate(error.to_string()))?;
    }

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|error| classify(error, limits))?;

    let memory = instance
        .get_memory(&mut store, MEMORY_EXPORT)
        .ok_or(WasmError::MissingExport(MEMORY_EXPORT))?;
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, ALLOC_EXPORT)
        .map_err(|_| WasmError::MissingExport(ALLOC_EXPORT))?;
    let run = instance
        .get_typed_func::<(i32, i32), i64>(&mut store, RUN_EXPORT)
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

fn classify(error: wasmtime::Error, limits: &WasmLimits) -> WasmError {
    if let Some(wasmtime::Trap::OutOfFuel) = error.downcast_ref::<wasmtime::Trap>() {
        return WasmError::OutOfFuel(limits.fuel);
    }

    // The interesting part is usually in the cause chain, not the top message.
    let text = format!("{error:?}");
    if text.contains("fuel") {
        WasmError::OutOfFuel(limits.fuel)
    } else if text.contains("memory") && text.contains("limit") {
        WasmError::OutOfMemory(limits.memory_bytes)
    } else {
        WasmError::Trap(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{ECHO_WAT, INCREMENT_WAT, INFINITE_LOOP_WAT, SUM_INPUTS_WAT, wasm};

    #[test]
    fn a_module_sees_its_input_and_returns_output() {
        let output = run(&wasm(ECHO_WAT), b"aethermesh", &[], &WasmLimits::default()).unwrap();
        assert_eq!(output, b"aethermesh");
    }

    #[test]
    fn a_module_can_transform_the_input() {
        let output = run(
            &wasm(INCREMENT_WAT),
            &[1, 2, 255],
            &[],
            &WasmLimits::default(),
        )
        .unwrap();
        assert_eq!(output, vec![2, 3, 0]);
    }

    #[test]
    fn an_endless_module_runs_out_of_fuel() {
        let limits = WasmLimits {
            fuel: 100_000,
            ..WasmLimits::default()
        };
        let error = run(&wasm(INFINITE_LOOP_WAT), b"", &[], &limits).unwrap_err();

        assert!(matches!(error, WasmError::OutOfFuel(_)), "{error:?}");
    }

    #[test]
    fn a_module_can_read_the_declared_datasets() {
        let first: std::sync::Arc<[u8]> = vec![1u8, 2, 3].into();
        let second: std::sync::Arc<[u8]> = vec![10u8, 20].into();
        let output = run(
            &wasm(SUM_INPUTS_WAT),
            b"",
            &[first, second],
            &WasmLimits::default(),
        )
        .unwrap();

        assert_eq!(output[0], 2);
        assert_eq!(output[1], 36);
    }
}
