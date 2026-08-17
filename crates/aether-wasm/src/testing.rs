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

/// Reads every declared dataset and writes `[count, sum_of_all_bytes]`.
pub const SUM_INPUTS_WAT: &str = r#"
(module
  (import "aether" "input_count" (func $input_count (result i32)))
  (import "aether" "input_len" (func $input_len (param i32) (result i32)))
  (import "aether" "input_read" (func $input_read (param i32 i32 i32) (result i32)))
  MEM
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (local $count i32)
    (local $index i32)
    (local $size i32)
    (local $buffer i32)
    (local $i i32)
    (local $sum i32)

    (local.set $count (call $input_count))
    (local.set $buffer (i32.const 4096))

    (block $inputs_done
      (loop $inputs
        (br_if $inputs_done (i32.ge_s (local.get $index) (local.get $count)))
        (local.set $size (call $input_len (local.get $index)))
        (if (i32.gt_s (local.get $size) (i32.const 0))
          (then
            (drop (call $input_read (local.get $index) (local.get $buffer) (local.get $size)))
            (local.set $i (i32.const 0))
            (block $bytes_done
              (loop $bytes
                (br_if $bytes_done (i32.ge_s (local.get $i) (local.get $size)))
                (local.set $sum
                  (i32.add
                    (local.get $sum)
                    (i32.load8_u (i32.add (local.get $buffer) (local.get $i)))))
                (local.set $i (i32.add (local.get $i) (i32.const 1)))
                (br $bytes)))))
        (local.set $index (i32.add (local.get $index) (i32.const 1)))
        (br $inputs)))

    (i32.store8 (i32.const 2048) (local.get $count))
    (i32.store8 (i32.const 2049) (local.get $sum))
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 2048)) (i64.const 32))
      (i64.const 2))))
"#;

/// Uses the granted host functions: logs a line, reads the clock, asks for
/// random bytes, and reports what it got as `[log_ok, clock_ok, random_len]`.
pub const CAPABILITIES_WAT: &str = r#"
(module
  (import "aether" "log" (func $log (param i32 i32)))
  (import "aether" "now_unix_millis" (func $now (result i64)))
  (import "aether" "random" (func $random (param i32 i32) (result i32)))
  MEM
  (data (i32.const 3072) "hello from wasm")
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (call $log (i32.const 3072) (i32.const 15))
    (i32.store8 (i32.const 2048) (i32.const 1))
    (i32.store8 (i32.const 2049)
      (select (i32.const 1) (i32.const 0) (i64.gt_s (call $now) (i64.const 0))))
    (i32.store8 (i32.const 2050) (call $random (i32.const 4096) (i32.const 16)))
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 2048)) (i64.const 32))
      (i64.const 3))))
"#;

/// Reads `data.txt` from the granted directory and returns its first bytes.
///
/// Writes `[size_ok, bytes_read]` followed by what it read, so a test can check
/// both the metadata call and the content.
pub const READ_FILE_WAT: &str = r#"
(module
  (import "aether" "file_size" (func $file_size (param i32 i32) (result i64)))
  (import "aether" "file_read"
    (func $file_read (param i32 i32 i64 i32 i32) (result i32)))
  MEM
  (data (i32.const 3072) "data.txt")
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (local $size i64)
    (local $read i32)
    (local.set $size (call $file_size (i32.const 3072) (i32.const 8)))
    (local.set $read
      (call $file_read
        (i32.const 3072) (i32.const 8)
        (i64.const 0)
        (i32.const 2050) (i32.const 16)))

    (i32.store8 (i32.const 2048)
      (select (i32.const 1) (i32.const 0) (i64.gt_s (local.get $size) (i64.const 0))))
    (i32.store8 (i32.const 2049) (local.get $read))
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 2048)) (i64.const 32))
      (i64.const 18))))
"#;

/// Tries to escape the granted directory with `..`.
pub const ESCAPE_FILE_WAT: &str = r#"
(module
  (import "aether" "file_size" (func $file_size (param i32 i32) (result i64)))
  (import "aether" "file_read"
    (func $file_read (param i32 i32 i64 i32 i32) (result i32)))
  MEM
  (data (i32.const 3072) "../secret.txt")
  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (i32.store8 (i32.const 2048)
      (select (i32.const 1) (i32.const 0)
        (i64.gt_s (call $file_size (i32.const 3072) (i32.const 13)) (i64.const 0))))
    (i64.or
      (i64.shl (i64.extend_i32_u (i32.const 2048)) (i64.const 32))
      (i64.const 1))))
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
