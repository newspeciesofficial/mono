//! OTEL metric instruments for zero-cache.
//!
//! All metric names follow the pattern `zero.{category}.{name}` matching the
//! TypeScript implementation in `packages/zero-cache/src/observability/metrics.ts`.

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

/// Lazily-initialized collection of all zero-cache OTEL metric instruments.
pub struct Metrics {
    meter: Meter,
}

impl Metrics {
    /// Create a new `Metrics` instance using the global meter provider.
    pub fn new() -> Self {
        let meter = opentelemetry::global::meter("zero");
        Self { meter }
    }

    // ── Sync histograms (seconds) ──────────────────────────────────────

    /// Time spent advancing the view syncer (wall-clock, per transaction).
    pub fn sync_advance_time(&self) -> Histogram<f64> {
        self.meter
            .f64_histogram("zero.sync.advance-time")
            .with_description("Time to advance the view syncer")
            .with_unit("s")
            .build()
    }

    /// Time spent hydrating queries.
    pub fn sync_hydration_time(&self) -> Histogram<f64> {
        self.meter
            .f64_histogram("zero.sync.hydration-time")
            .with_description("Time to hydrate queries")
            .with_unit("s")
            .build()
    }

    /// Time spent flushing CVR rows.
    pub fn sync_cvr_flush_time(&self) -> Histogram<f64> {
        self.meter
            .f64_histogram("zero.sync.cvr.flush-time")
            .with_description("Time to flush CVR rows")
            .with_unit("s")
            .build()
    }

    /// Time spent sending pokes to clients.
    pub fn sync_poke_time(&self) -> Histogram<f64> {
        self.meter
            .f64_histogram("zero.sync.poke.time")
            .with_description("Time to send pokes to clients")
            .with_unit("s")
            .build()
    }

    /// Record a poke time observation with a `poke_type` attribute.
    ///
    /// `poke_type` should be one of: `"hydration"`, `"catchup"`, `"advancement"`.
    pub fn record_poke_time(&self, seconds: f64, poke_type: &str) {
        self.sync_poke_time().record(
            seconds,
            &[KeyValue::new("poke_type", poke_type.to_string())],
        );
    }

    /// Number of QUERY_PLAN diagnostic warnings emitted.
    pub fn sync_query_plan_warnings(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.query-plan.warnings")
            .with_description("Number of QUERY_PLAN diagnostic warnings")
            .build()
    }

    /// Time spent in IVM advance per pipeline.
    pub fn sync_ivm_advance_time(&self) -> Histogram<f64> {
        self.meter
            .f64_histogram("zero.sync.ivm.advance-time")
            .with_description("Time spent in IVM advance")
            .with_unit("s")
            .build()
    }

    /// Time spent transforming queries.
    pub fn sync_query_transformation_time(&self) -> Histogram<f64> {
        self.meter
            .f64_histogram("zero.sync.query.transformation-time")
            .with_description("Time to transform queries")
            .with_unit("s")
            .build()
    }

    // ── Sync counters ──────────────────────────────────────────────────

    /// Number of hydrations performed.
    pub fn sync_hydration(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.hydration")
            .with_description("Number of query hydrations")
            .build()
    }

    /// Number of CVR rows flushed.
    pub fn sync_cvr_rows_flushed(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.cvr.rows-flushed")
            .with_description("Number of CVR rows flushed")
            .build()
    }

    /// Number of poke transactions sent.
    pub fn sync_poke_transactions(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.poke.transactions")
            .with_description("Number of poke transactions sent")
            .build()
    }

    /// Number of rows poked.
    pub fn sync_poke_rows(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.poke.rows")
            .with_description("Number of rows sent in pokes")
            .build()
    }

    /// Number of IVM conflict rows deleted.
    pub fn sync_ivm_conflict_rows_deleted(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.ivm.conflict-rows-deleted")
            .with_description("Number of IVM conflict rows deleted")
            .build()
    }

    /// Number of query transformations.
    pub fn sync_query_transformations(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.sync.query.transformations")
            .with_description("Number of query transformations")
            .build()
    }

    // ── Sync up-down counters ──────────────────────────────────────────

    /// Number of currently active sync clients.
    pub fn sync_active_clients(&self) -> UpDownCounter<i64> {
        self.meter
            .i64_up_down_counter("zero.sync.active-clients")
            .with_description("Number of active sync clients")
            .build()
    }

    // ── Replication counters ───────────────────────────────────────────

    /// Number of replication events received.
    pub fn replication_events(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.replication.events")
            .with_description("Number of replication events received")
            .build()
    }

    /// Number of replication transactions processed.
    pub fn replication_transactions(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.replication.transactions")
            .with_description("Number of replication transactions processed")
            .build()
    }

    // ── Mutation counters ──────────────────────────────────────────────

    /// Number of CRUD mutations processed.
    pub fn mutation_crud(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.mutation.crud")
            .with_description("Number of CRUD mutations")
            .build()
    }

    /// Number of custom mutations processed.
    pub fn mutation_custom(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.mutation.custom")
            .with_description("Number of custom mutations")
            .build()
    }

    /// Number of mutation pushes.
    pub fn mutation_pushes(&self) -> Counter<u64> {
        self.meter
            .u64_counter("zero.mutation.pushes")
            .with_description("Number of mutation pushes")
            .build()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = Metrics::new();

        // Verify all instruments can be created without panicking.
        let _ = metrics.sync_advance_time();
        let _ = metrics.sync_hydration_time();
        let _ = metrics.sync_hydration();
        let _ = metrics.sync_cvr_flush_time();
        let _ = metrics.sync_cvr_rows_flushed();
        let _ = metrics.sync_poke_time();
        let _ = metrics.sync_poke_transactions();
        let _ = metrics.sync_poke_rows();
        let _ = metrics.sync_active_clients();
        let _ = metrics.sync_ivm_advance_time();
        let _ = metrics.sync_ivm_conflict_rows_deleted();
        let _ = metrics.sync_query_transformations();
        let _ = metrics.sync_query_transformation_time();
        let _ = metrics.sync_query_plan_warnings();
        let _ = metrics.replication_events();
        let _ = metrics.replication_transactions();
        let _ = metrics.mutation_crud();
        let _ = metrics.mutation_custom();
        let _ = metrics.mutation_pushes();
    }

    #[test]
    fn test_record_poke_time() {
        let metrics = Metrics::new();
        // Verify that recording with each poke_type does not panic.
        metrics.record_poke_time(0.1, "hydration");
        metrics.record_poke_time(0.2, "catchup");
        metrics.record_poke_time(0.3, "advancement");
    }

    #[test]
    fn test_metrics_default() {
        let metrics = Metrics::default();
        // Should work the same as ::new().
        let _ = metrics.sync_advance_time();
    }
}
