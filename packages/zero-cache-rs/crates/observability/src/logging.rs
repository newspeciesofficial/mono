//! Logging setup using `tracing_subscriber` with JSON formatting.
//!
//! Produces structured logs matching the zero-cache format:
//! ```json
//! {"timestamp":"...","level":"info","pid":12345,"worker":"syncer","message":"..."}
//! ```

use tracing::Level;
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, time::SystemTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Initialize the global tracing subscriber with JSON formatting.
///
/// `log_level` should be one of: "trace", "debug", "info", "warn", "error".
/// Falls back to "info" on invalid input.
pub fn init_logging(log_level: &str) {
    let level = parse_level(log_level);
    let filter = EnvFilter::new(level.as_str());

    let json_layer = fmt::layer()
        .json()
        .with_timer(SystemTime)
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_current_span(true)
        .with_span_list(false);

    tracing_subscriber::registry()
        .with(filter)
        .with(json_layer)
        .init();
}

fn parse_level(s: &str) -> Level {
    match s.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_level() {
        assert_eq!(parse_level("trace"), Level::TRACE);
        assert_eq!(parse_level("DEBUG"), Level::DEBUG);
        assert_eq!(parse_level("Info"), Level::INFO);
        assert_eq!(parse_level("WARN"), Level::WARN);
        assert_eq!(parse_level("error"), Level::ERROR);
        assert_eq!(parse_level("garbage"), Level::INFO);
    }
}
