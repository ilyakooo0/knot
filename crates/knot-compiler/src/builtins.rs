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
    "randomInt", "randomFloat",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
];

/// Builtins that perform network IO.
pub const NETWORK_BUILTINS: &[&str] = &["listen", "fetch", "fetchWith"];

/// Builtins that touch the filesystem.
pub const FS_BUILTINS: &[&str] = &[
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
];

/// Concurrency builtins (`fork`, `retry`). These do not contribute IO effects
/// the same way as console/fs/etc.: `fork` returns `IO {} {}` (the spawned IO
/// is not part of the surrounding transaction) and `retry` is the STM
/// primitive used inside atomic to trigger a retry.
pub const CONCURRENCY_BUILTINS: &[&str] = &["fork", "retry"];

/// Pure builtins. These are listed so the LSP can recognize them as user-callable
/// and so they sort sensibly in completion lists. They have no effects.
pub const PURE_BUILTINS: &[&str] = &[
    "show", "union", "count", "filter", "match", "map",
    "fold", "single", "diff", "inter", "sum", "avg",
    "toUpper", "toLower", "take", "drop",
    "length", "trim", "contains", "reverse", "chars",
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
    "randomInt", "randomFloat",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
    // network
    "listen", "fetch", "fetchWith",
    // fs
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
    // concurrency
    "fork", "retry",
];

/// Builtins whose effects cannot be rolled back by the savepoint-based atomic
/// machinery. The effect checker rejects any of these inside an `atomic` block;
/// the LSP also filters them from atomic-context completion lists.
///
/// `fork` and `retry` are intentionally absent: `fork` returns `IO {} {}`
/// (spawned IO is independent of the transaction) and `retry` is the STM
/// primitive used inside atomic to trigger a retry.
pub const ATOMIC_DISALLOWED_BUILTINS: &[&str] = &[
    "println", "putLine", "print", "readLine",
    "logInfo", "logWarn", "logError", "logDebug",
    "now", "sleep",
    "randomInt", "randomFloat",
    "generateKeyPair", "generateSigningKeyPair", "encrypt",
    "listen", "fetch", "fetchWith",
    "readFile", "writeFile", "appendFile",
    "fileExists", "removeFile", "listDir",
];
