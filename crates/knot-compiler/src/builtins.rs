//! Centralized builtin name tables.
//!
//! These lists are the single source of truth for which builtins perform IO
//! and which are forbidden inside `atomic` blocks. Both the effect inferencer
//! (`effects.rs`) and the LSP (`knot_lsp::builtins`) reference this module so
//! the lists never drift apart.

/// Builtins that perform a console effect (`println`, `print`, `logInfo`, ...).
pub const CONSOLE_BUILTINS: &[&str] = &[
    "println", "putLine", "print", "readLine",
    "logInfo", "logWarn", "logError", "logDebug",
];

/// Builtins that read/manipulate the wall clock or sleep.
pub const CLOCK_BUILTINS: &[&str] = &["now", "sleep"];

/// Builtins that consume randomness.
pub const RANDOM_BUILTINS: &[&str] = &[
    "randomInt", "randomFloat", "randomUuid",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
];

/// Builtins that perform network IO.
pub const NETWORK_BUILTINS: &[&str] = &["listen", "listenOn", "fetch", "fetchWith"];

/// Builtins that touch the filesystem.
pub const FS_BUILTINS: &[&str] = &[
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
];

/// Concurrency builtins (`fork`, `retry`, `race`). These do not contribute IO
/// effects the same way as console/fs/etc.: `fork` propagates the effect row of
/// its spawned IO through its own result (`IO r {} -> IO r {}`), `retry` is the
/// STM primitive used inside atomic to trigger a retry, and `race` propagates
/// the effect rows of its arguments through its result.
pub const CONCURRENCY_BUILTINS: &[&str] = &["fork", "retry", "race"];

/// Pure builtins. These are listed so the LSP can recognize them as user-callable
/// and so they sort sensibly in completion lists. They have no effects.
pub const PURE_BUILTINS: &[&str] = &[
    "show", "union", "count", "filter", "match", "map",
    "fold", "single", "any", "all", "diff", "inter", "sum", "avg",
    "toUpper", "toLower", "take", "drop",
    "length", "trim", "contains", "elem", "reverse", "chars",
    "id", "not", "toJson", "parseJson",
    "decrypt", "sign", "verify",
];

/// Set of builtin function names that perform some IO effect. Used by the LSP
/// to mark them with the `effectful` semantic-token modifier and to filter
/// them out of atomic-context completions.
pub const EFFECTFUL_BUILTINS: &[&str] = &[
    // console
    "println", "putLine", "print", "readLine",
    "logInfo", "logWarn", "logError", "logDebug",
    // clock
    "now", "sleep",
    // random
    "randomInt", "randomFloat", "randomUuid",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
    // network
    "listen", "listenOn", "fetch", "fetchWith",
    // fs
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
    // concurrency
    "fork", "retry", "race",
];

/// Builtins whose effects cannot be rolled back by the savepoint-based atomic
/// machinery. The effect checker rejects any of these inside an `atomic` block;
/// the LSP also filters them from atomic-context completion lists.
///
/// `fork` and `retry` are intentionally absent: `fork`'s spawned IO runs in an
/// independent transaction (its effects propagate through the type but the work
/// happens on its own connection), and `retry` is the STM primitive used inside
/// atomic to trigger a retry.
pub const ATOMIC_DISALLOWED_BUILTINS: &[&str] = &[
    "println", "putLine", "print", "readLine",
    "logInfo", "logWarn", "logError", "logDebug",
    "now", "sleep",
    "randomInt", "randomFloat", "randomUuid",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
    "listen", "listenOn", "fetch", "fetchWith",
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
    "race",
];

/// Built-in trait method names (Eq.eq, Ord.compare, Num.add, etc.). Resolved
/// at codegen time via runtime tag dispatch.
pub const TRAIT_METHOD_BUILTINS: &[&str] = &[
    "eq", "compare", "ap", "bind", "alt", "empty",
    "add", "sub", "mul", "div", "negate", "append",
    "yield", "display",
];

/// Bytes builtins. Pure but kept separate so the LSP/codegen can recognize
/// them as user-callable and they sort sensibly in completion lists.
pub const BYTES_BUILTINS: &[&str] = &[
    "bytesLength", "bytesSlice", "bytesConcat",
    "textToBytes", "bytesToText", "bytesToHex",
    "bytesFromHex", "hexDecode", "bytesGet",
    "hash",
];

/// Internal desugaring helpers emitted by the desugarer. Not user-facing.
pub const INTERNAL_BUILTINS: &[&str] = &["__bind", "__yield", "__empty"];

/// Single source of truth: every name the compiler treats as a builtin.
/// Used by `codegen::is_builtin_name` (free-variable filtering) and the LSP
/// completion suggestions. Adding a new builtin should only require editing
/// this file and the codegen registration site.
pub const ALL_BUILTINS: &[&[&str]] = &[
    CONSOLE_BUILTINS,
    CLOCK_BUILTINS,
    RANDOM_BUILTINS,
    NETWORK_BUILTINS,
    FS_BUILTINS,
    CONCURRENCY_BUILTINS,
    PURE_BUILTINS,
    TRAIT_METHOD_BUILTINS,
    BYTES_BUILTINS,
    INTERNAL_BUILTINS,
];

/// True if `name` is a known builtin (any category).
pub fn is_builtin(name: &str) -> bool {
    ALL_BUILTINS.iter().any(|list| list.contains(&name))
}

/// True if `name` is an effectful builtin — i.e. calling it produces an IO
/// value. Excludes `retry` (no IO) and `fork` (returns `IO {} {}`, but its
/// argument's effects are decoupled from the caller).  `race` is included
/// because the result IO inherits the effect row of its arguments.
pub fn is_io_builtin(name: &str) -> bool {
    EFFECTFUL_BUILTINS.contains(&name) && !matches!(name, "retry")
}
