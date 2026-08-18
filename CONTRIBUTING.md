# Contributing to AetherMesh

Thanks for looking. This file is the short version of how work happens here, so
you can spend your time on the change rather than on guessing house style.

## Before you open a pull request

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

CI runs exactly this on Linux, Windows, and macOS, plus a cross-build for
`aarch64` and `armv7`, plus `cargo audit`. If it passes locally on one OS it
usually passes everywhere; the exceptions are timing-sensitive tests, which is
why there aren't many of them.

## What a good change looks like

**Small and finished.** A pull request that does one thing, with the tests for
that thing, gets reviewed in an evening. One that does four things waits.

**Tested at the boundary that matters.** Prefer a test that pins observable
behaviour ("a node that fails a constraint is not selected") over one that
pins an implementation detail ("`eligible` calls `satisfies_all`"). Test names
are sentences: `a_constraint_rules_out_a_node_the_load_would_have_picked`.

**Comments explain why.** The code already says what it does. A comment earns
its place by recording the reason a reader would otherwise have to reconstruct
— why a value is clamped, why an error is not retried, why the cheap approach
was rejected.

**No `unwrap()` outside tests.** Errors are `thiserror` enums that say what
failed and what it was doing. `expect()` is acceptable only for invariants that
would already be a bug, and then the message says which invariant.

**No `unsafe`.** The workspace denies it. If you think you need it, open an
issue first and describe what you are trying to do — there has always been
another way so far.

**Dependencies are a cost.** Each new one is weight in every binary, another
crate in the audit surface, and another thing that can break the MSRV. Adding
one is fine; adding one to save twenty lines is not.

## Commit messages

Conventional-commit prefix, then a sentence someone can read in `git log`
without opening the diff:

```
fix(wasm): draw the `random` capability from the OS CSPRNG
```

The body is for the reasoning: what was wrong, why it mattered, what you chose
instead. If the change is obvious, the body can be empty. If it is not, the
body is the most valuable part of the commit.

## Things that are especially welcome

- **Running it on real hardware and reporting what broke.** Numbers from
  loopback are the weakest part of this repository. A Raspberry Pi, a flaky
  Wi-Fi link, or a machine on the other side of an ocean teaches more than
  another unit test does.
- **Cloud adapter verification.** The AWS, GCP, Azure, and Kubernetes adapters
  are tested against their HTTP contract with stub servers. Nobody has run them
  against a live account from this repository. If you have one, that is the
  highest-value contribution available right now.
- **Another SDK.** The client protocol is a 4-byte big-endian length and one
  JSON object. Ruby, Java, C#, and Elixir would each be a few hundred lines;
  see `sdk/python` for the smallest existing implementation.

## Security

Do not open a public issue for a vulnerability. [`SECURITY.md`](SECURITY.md)
says where to send it and what to expect.

## Licensing

By contributing you agree that your work is dual-licensed under Apache-2.0 and
MIT, matching the rest of the project.
