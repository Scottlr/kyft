# Spanfold Rust

Rust 1.95.0 / Rust 2024 scaffold for Spanfold's production high-throughput CLI
and systems implementation.

This package is intentionally early. The current code establishes the workspace,
toolchain baseline, crate boundaries, and typed core primitives that future
parity work will build on.

Private implementation planning specs live under `packages/rust/specs/` and are
ignored by Git.

## Commands

```bash
cargo test --all
cargo run -p spanfold-cli -- --help
```
