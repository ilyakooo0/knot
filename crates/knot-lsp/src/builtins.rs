//! Centralized builtin name lists shared with the compiler.
//!
//! These were duplicated in `main.rs` and `effects.rs`; if a new effectful
//! builtin is added in the compiler, the LSP completion filter must agree.
//! The lists here are the single source of truth that both the LSP and the
//! compiler reference (the compiler also re-exports them via
//! `knot_compiler::builtins`).

pub use knot_compiler::builtins::{ATOMIC_DISALLOWED_BUILTINS, EFFECTFUL_BUILTINS};
