// Copyright (c) MangoNet Labs Ltd.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use prometheus::{IntCounter, IntGauge, Histogram, Registry, HistogramOpts};

/// Cold start metrics
#[derive(Debug)]
pub struct ColdStartMetrics {
    /// Total cold start operations
    pub cold_start_operations_total: IntCounter,
    /// Successful cold start count
    pub cold_start_success_total: IntCounter,
    /// Failed cold start count
    pub cold_start_failure_total: IntCounter,
    /// Cold start duration
    pub cold_start_duration: Histogram,
    /// Number of healthy nodes discovered
    pub healthy_nodes_discovered: IntGauge,
    /// Current cold start status
    pub cold_start_in_progress: IntGauge,
}

impl ColdStartMetrics {
    pub fn new(registry: &Registry) -> Arc<Self> {
        let cold_start_operations_total = IntCounter::new(
            "cold_start_operations_total",
            "Total number of cold start operations",
        )
        .unwrap();
        registry.register(Box::new(cold_start_operations_total.clone())).unwrap();

        let cold_start_success_total = IntCounter::new(
            "cold_start_success_total",
            "Total number of successful cold starts",
        )
        .unwrap();
        registry.register(Box::new(cold_start_success_total.clone())).unwrap();

        let cold_start_failure_total = IntCounter::new(
            "cold_start_failure_total",
            "Total number of failed cold starts",
        )
        .unwrap();
        registry.register(Box::new(cold_start_failure_total.clone())).unwrap();

        let cold_start_duration = Histogram::with_opts(
            HistogramOpts::new(
                "cold_start_duration_seconds",
                "Duration of cold start operations in seconds",
            )
        )
        .unwrap();
        registry.register(Box::new(cold_start_duration.clone())).unwrap();

        let healthy_nodes_discovered = IntGauge::new(
            "healthy_nodes_discovered",
            "Number of healthy nodes discovered",
        )
        .unwrap();
        registry.register(Box::new(healthy_nodes_discovered.clone())).unwrap();

        let cold_start_in_progress = IntGauge::new(
            "cold_start_in_progress",
            "Whether a cold start operation is currently in progress",
        )
        .unwrap();
        registry.register(Box::new(cold_start_in_progress.clone())).unwrap();

        Arc::new(Self {
            cold_start_operations_total,
            cold_start_success_total,
            cold_start_failure_total,
            cold_start_duration,
            healthy_nodes_discovered,
            cold_start_in_progress,
        })
    }
}
