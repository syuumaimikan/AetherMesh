//! Assembles a `.wat` file into a `.wasm` module.
//!
//! Handy for trying WASM tasks without installing a toolchain:
//!
//! ```text
//! cargo run -p aether-wasm --example wat2wasm -- examples/wasm/uppercase.wat uppercase.wasm
//! ```

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let (input, output) = match (args.next(), args.next()) {
        (Some(input), Some(output)) => (input, output),
        _ => {
            eprintln!("usage: wat2wasm <input.wat> <output.wasm>");
            std::process::exit(2);
        }
    };

    let module = wat::parse_str(std::fs::read_to_string(&input)?)?;
    std::fs::write(&output, &module)?;
    println!("wrote {} ({} bytes)", output, module.len());
    Ok(())
}
