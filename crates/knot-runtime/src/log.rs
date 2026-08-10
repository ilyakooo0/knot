use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};

static DEBUG: AtomicBool = AtomicBool::new(false);

pub fn debug_enabled() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

/// Scan `--debug` in process arguments and enable debug logging.
#[unsafe(no_mangle)]
pub extern "C-unwind" fn knot_debug_init() {
    // Use `args_os()`: `args()` panics mid-iteration on any non-UTF-8 argv
    // entry, which would crash every compiled program at startup over an
    // argument irrelevant to the `--debug` check.
    for arg in std::env::args_os() {
        if arg.to_str() == Some("--debug") {
            DEBUG.store(true, Ordering::Relaxed);
            return;
        }
    }
}

#[derive(Clone, Copy)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
    }

    fn label(self) -> &'static str {
        match self {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        }
    }

    fn color(self) -> &'static str {
        match self {
            LogLevel::Debug => "\x1b[36m", // cyan
            LogLevel::Info => "\x1b[34m",  // blue
            LogLevel::Warn => "\x1b[33m",  // yellow
            LogLevel::Error => "\x1b[31m", // red
        }
    }
}

/// `HH:MM:SS±HH:MM` in the user's own machine time zone, for the terminal log
/// prefix. `localtime_r` resolves the zone from the system (`TZ` env /
/// `/etc/localtime`); `tm_gmtoff` reports that zone's UTC offset, so the prefix
/// is self-describing (e.g. `23:18:22+03:00`). Falls back to UTC (with a `Z`)
/// on non-unix targets or if the conversion fails.
#[cfg(unix)]
fn local_time_string() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as libc::time_t;
    // SAFETY: `localtime_r` writes into `tm` and returns it on success; the
    // `tm` struct is plain data with no Drop, so an uninit-then-assume-init
    // read after a successful call is sound.
    unsafe {
        let mut tm: libc::tm = std::mem::zeroed();
        if !libc::localtime_r(&secs, &mut tm).is_null() {
            let off = tm.tm_gmtoff; // seconds east of UTC (the machine's zone)
            let sign = if off < 0 { '-' } else { '+' };
            let a = off.abs();
            format!(
                "{:02}:{:02}:{:02}{}{:02}:{:02}",
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec,
                sign,
                a / 3600,
                (a % 3600) / 60
            )
        } else {
            utc_time_string(secs as u64)
        }
    }
}

/// `HH:MM:SS` UTC, used when local time is unavailable (non-unix, or
/// `localtime_r` failure).
#[cfg(unix)]
fn utc_time_string(secs: u64) -> String {
    let s = secs % 86_400;
    format!("{:02}:{:02}:{:02}Z", s / 3600, (s / 60) % 60, s % 60)
}

/// `HH:MM:SS` on non-unix targets (no `localtime_r`) — UTC with a `Z` suffix.
#[cfg(not(unix))]
fn local_time_string() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let s = secs % 86_400;
    format!("{:02}:{:02}:{:02}Z", s / 3600, (s / 60) % 60, s % 60)
}

pub fn log(level: LogLevel, message: &str) {
    let stderr = std::io::stderr();
    let mut handle = stderr.lock();

    if stderr.is_terminal() {
        let _ = writeln!(
            handle,
            "{} {}{}\x1b[0m {}",
            local_time_string(),
            level.color(),
            level.label(),
            message,
        );
    } else {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let json = serde_json::json!({
            "level": level.as_str(),
            "message": message,
            "timestamp": ts,
        });
        let _ = writeln!(handle, "{json}");
    }
}

/// A single context field, pre-rendered for both output shapes.
/// `terminal` is the `value` rendered for the `k: v` splat; `json` is the
/// value rendered as a JSON fragment (used verbatim inside the object).
pub struct CtxField {
    pub name: String,
    pub terminal: String,
    pub json: String,
}

/// Emit one structured log line in the unified shape:
///   terminal: `LEVEL msg k: v, k: v`   (ctx splat only when non-empty)
///   JSON:     `{"level","msg",...ctx,"timestamp"}`  (ctx merged into the object)
///
/// `level_tag` is the constructor leaf ("Debug"/"Info"/"Warn"/"Error").
/// `debug`-level lines are gated on `--debug`. The whole line is built in a
/// single buffer and written with ONE locked `writeln!`, so concurrent
/// threads never interleave bytes *within* a line.
pub fn emit(level_tag: &str, msg: &str, ctx: Vec<CtxField>) {
    let level = match level_tag {
        "Debug" => LogLevel::Debug,
        "Warn" => LogLevel::Warn,
        "Error" => LogLevel::Error,
        _ => LogLevel::Info,
    };
    if matches!(level, LogLevel::Debug) && !debug_enabled() {
        return;
    }

    let stderr = std::io::stderr();
    let mut handle = stderr.lock();
    let mut line = String::new();

    if stderr.is_terminal() {
        use std::fmt::Write;
        let _ = write!(
            line,
            "{} {}{}\x1b[0m {}",
            local_time_string(),
            level.color(),
            level.label(),
            msg
        );
        if !ctx.is_empty() {
            let splat = ctx
                .iter()
                .map(|f| format!("{}: {}", f.name, f.terminal))
                .collect::<Vec<_>>()
                .join(", ");
            let _ = write!(line, " {{{splat}}}");
        }
    } else {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        let mut obj = serde_json::Map::new();
        obj.insert("level".into(), serde_json::Value::String(level.as_str().into()));
        obj.insert("msg".into(), serde_json::Value::String(msg.into()));
        for f in &ctx {
            // The json fragment was rendered by write_value_json; parse it back
            // into a Value so the object is well-formed.
            let v = serde_json::from_str(&f.json).unwrap_or(serde_json::Value::Null);
            obj.insert(f.name.clone(), v);
        }
        obj.insert("timestamp".into(), serde_json::json!(ts));
        line = serde_json::Value::Object(obj).to_string();
    }

    let _ = writeln!(handle, "{line}");
}

pub fn log_debug(message: &str) {
    if debug_enabled() {
        log(LogLevel::Debug, message);
    }
}

pub fn log_info(message: &str) {
    log(LogLevel::Info, message);
}

pub fn log_warn(message: &str) {
    log(LogLevel::Warn, message);
}

pub fn log_error(message: &str) {
    log(LogLevel::Error, message);
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        if $crate::log::debug_enabled() {
            $crate::log::log_debug(&format!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        $crate::log::log_info(&format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        $crate::log::log_warn(&format!($($arg)*))
    };
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        $crate::log::log_error(&format!($($arg)*))
    };
}
