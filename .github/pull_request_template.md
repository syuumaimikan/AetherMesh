<!--
Thanks for the change. The questions below are the ones a reviewer would ask
anyway; answering them here saves a round trip.
-->

## What this changes

<!-- One or two sentences. The commit body is the place for detail. -->

## Why

<!--
The problem, not the patch. If it fixes an issue, link it: "Fixes #12".
If the obvious simpler approach does not work, this is where to say so.
-->

## How it was verified

<!--
Which tests you added and what they pin. If you ran it on real hardware or
across a real network, say what — that evidence is scarce here.
-->

## Checklist

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace --all-features`
- [ ] New behaviour has a test that would fail without the change
- [ ] Docs updated if the change is visible to a user (README, `docs/`, SDK docstrings)
- [ ] No new dependency, or the PR explains why one was worth it
