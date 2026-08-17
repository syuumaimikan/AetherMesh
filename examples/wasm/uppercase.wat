;; Uppercases ASCII letters in its input.
;;
;; A minimal AetherMesh task module: it exports memory, a bump allocator, and
;; `run(ptr, len) -> ptr << 32 | len`. Nothing is imported, because a task
;; module gets no host functions at all.
;;
;; Build it with:
;;   cargo run -p aether-wasm --example wat2wasm -- examples/wasm/uppercase.wat uppercase.wasm
(module
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))

  ;; Reserves `len` bytes and returns the offset. One task, one buffer, so
  ;; bumping a cursor is all the allocation this needs.
  (func (export "alloc") (param $len i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $next))
    (global.set $next (i32.add (global.get $next) (local.get $len)))
    (local.get $ptr))

  (func (export "run") (param $ptr i32) (param $len i32) (result i64)
    (local $i i32)
    (local $byte i32)
    (block $done
      (loop $loop
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (local.set $byte
          (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))

        ;; 'a'..='z' becomes 'A'..='Z'; everything else is left alone.
        (if (i32.and
              (i32.ge_u (local.get $byte) (i32.const 97))
              (i32.le_u (local.get $byte) (i32.const 122)))
          (then
            (i32.store8
              (i32.add (local.get $ptr) (local.get $i))
              (i32.sub (local.get $byte) (i32.const 32)))))

        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $loop)))

    ;; Return the (unchanged) buffer location and length, packed into one i64.
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $len)))))
