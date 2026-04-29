// Package observability provides runtime metrics primitives for the rsql service.
// It exposes middleware and handlers for Prometheus-compatible instrumentation.
//
// The package focuses on:
//   - HTTP request counters and latency histograms
//   - Dedicated Prometheus registry management
//   - Metrics handler wiring for HTTP servers
package observability
