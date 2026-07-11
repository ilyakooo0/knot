//! Runs the compiler's recursive passes on a thread with a large stack.
//!
//! Desugaring turns a *flat* `do` block of N statements into an N-deep chain
//! of nested `__bind` applications, so a few hundred sequential statements
//! produce an AST far deeper than the source looks. Every pass after desugar
//! walks that AST recursively, and the parser's nesting guard never fires
//! because the source itself is not nested — it is a statement list, not a
//! nested expression. On the default 8 MiB main-thread stack, inference
//! overflows at roughly 500 statements; libtest's worker threads get 2 MiB
//! and overflow sooner.
//!
//! The passes stay recursive — inference and codegen interleave unification
//! and scope handling across ~100 expression forms, and an explicit work
//! stack would obscure all of it. Instead they get a stack big enough that
//! the depth the parser already admits cannot exhaust it.

use std::cell::Cell;
use std::sync::Mutex;
use std::thread;

/// Stack reserved for pass execution. The kernel commits stack pages lazily,
/// so this is reserved address space rather than resident memory: a
/// `hello world` compile still touches only a few pages of it.
///
/// Inference is the heaviest walker, at roughly 64 KiB of stack per `do`
/// statement in a debug build (its `infer_expr` frame is large and the
/// desugared chain nests several expressions per statement). 1 GiB therefore
/// clears well past ten thousand statements, which is far beyond the point
/// where inference's superlinear running time makes a `do` block that large
/// impractical to compile at all — so the stack is no longer what fails first
/// at any depth that can actually be compiled.
///
/// 32-bit targets can't spare a gigabyte of address space; they get a smaller
/// reservation, which still clears the few-hundred-statement blocks that
/// motivated this.
#[cfg(target_pointer_width = "64")]
const STACK_SIZE: usize = 1024 * 1024 * 1024;
#[cfg(not(target_pointer_width = "64"))]
const STACK_SIZE: usize = 64 * 1024 * 1024;

thread_local! {
    /// Set on threads spawned by [`grow`]. Passes call `grow` individually so
    /// that library consumers (the LSP, tests) are covered too, but the CLI
    /// also wraps the whole pipeline; this flag keeps that from spawning a
    /// fresh thread per pass.
    static ON_GROWN_STACK: Cell<bool> = const { Cell::new(false) };
}

/// Run `f` on a thread with a large stack and return its result.
///
/// Re-entrant: when the caller is already on a grown stack, `f` runs inline.
/// If the thread cannot be spawned, `f` also runs inline — a pass that might
/// overflow beats a compiler that refuses to start.
///
/// Panics propagate to the caller, so `catch_unwind` and test `#[should_panic]`
/// behave as they would without the thread hop.
pub fn grow<T, F>(f: F) -> T
where
    T: Send,
    F: FnOnce() -> T + Send,
{
    if ON_GROWN_STACK.with(Cell::get) {
        return f();
    }

    // Held in a slot rather than moved straight into the closure so that a
    // failed spawn can hand `f` back instead of dropping it.
    let slot = Mutex::new(Some(f));
    let take = || {
        (slot.lock().unwrap_or_else(|e| e.into_inner()).take())
            .expect("grow closure is taken exactly once")
    };

    thread::scope(|scope| {
        let spawned = thread::Builder::new()
            .stack_size(STACK_SIZE)
            .spawn_scoped(scope, || {
                ON_GROWN_STACK.with(|on| on.set(true));
                take()()
            });

        match spawned {
            Ok(handle) => match handle.join() {
                Ok(value) => value,
                Err(payload) => std::panic::resume_unwind(payload),
            },
            Err(_) => take()(),
        }
    })
}
