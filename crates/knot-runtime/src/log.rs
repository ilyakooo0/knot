use std::io::{IsTerminal, Write};
use std::sync::atomic::{AtomicBool, Ordering};

static DEBUG: AtomicBool = AtomicBool::new(false);

pub fn debug_enabled() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

/// Scan `--debug` in process arguments and enable debug logging.
#[unsafe(no_mangle)]
pub extern "C" fn knot_debug_init() {
    for arg in std::env::args() {
        if arg == "--debug" {
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

pub fn log(level: LogLevel, message: &str) {
    let stderr = std::io::stderr();
    let mut handle = stderr.lock();

    if stderr.is_terminal() {
        let _ = writeln!(
            handle,
            "{}{}\x1b[0m {}",
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
