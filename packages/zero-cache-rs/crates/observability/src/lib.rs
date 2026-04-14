#![doc = "Observability for zero-cache-rs: structured logging and OTEL metrics."]
#![deny(unsafe_code)]

pub mod logging;
pub mod metrics;

pub use logging::init_logging;
pub use metrics::Metrics;
