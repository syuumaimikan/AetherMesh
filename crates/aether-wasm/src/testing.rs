//! Tiny hand-written modules used by the backend tests.
//!
//! They are WAT rather than compiled output so the tests stay readable and the
//! repository stays free of binary fixtures.

/// Bumps a global cursor. Enough of an allocator for one input.
const ALLOC: &str = r#"
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $len)))
    (local.get $ptr))
"#;

/// Returns the input unchanged.
pub const ECHO_WAT: &str = r#"
(module
  MEM
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))
"#;

/// Adds one to every byte, in place.
pub const INCREMENT_WAT: &str = r#"
(module
  MEM
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (local $i i32)
    (block $done
      (loop $loop
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8
          (i32.add (local.get $ptr) (local.get $i))
          (i32.add
            (i32.load8_u (i32.add (local.get $ptr) (local.get $i)))
            (i32.const 1)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $loop)))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))
"#;

/// Never returns. Only fuel stops it.
pub const INFINITE_LOOP_WAT: &str = r#"
(module
  MEM
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (loop $forever (br $forever))
    (i64.const 0)))
"#;

/// Compiles one of the fixtures above to a module.
pub fn wasm(source: &str) -> Vec<u8> {
    wat::parse_str(source.replace("MEM", ALLOC)).expect("fixture is valid wat")
}
