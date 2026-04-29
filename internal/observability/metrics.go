package observability

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics contains metric collectors and helpers.
type Metrics struct {
	registry *prometheus.Registry
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewMetrics creates and registers metrics for the HTTP service.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	requests := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "rsql",
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	duration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "rsql",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	registry.MustRegister(requests, duration)

	return &Metrics{
		registry: registry,
		requests: requests,
		duration: duration,
	}
}

// Handler exposes Prometheus metrics for scraping.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry returns the underlying Prometheus registry.
func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

// Middleware records per-request count and latency.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		path := pathLabelForMetrics(r)
		status := strconv.Itoa(rec.status)
		labels := []string{r.Method, path, status}

		m.requests.WithLabelValues(labels...).Inc()
		m.duration.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	})
}

func pathLabelForMetrics(r *http.Request) string {
	if r.Pattern != "" {
		return r.Pattern
	}
	return normalizePathForMetrics(r.URL.Path)
}

func normalizePathForMetrics(path string) string {
	if path == "" {
		return "/"
	}
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "/"
	}

	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return "/"
	}
	if parts[0] != "v1" {
		return path
	}
	if len(parts) == 1 {
		return "/v1"
	}

	if parts[1] == "namespaces" {
		switch len(parts) {
		case 2:
			return "/v1/namespaces"
		case 3:
			return "/v1/namespaces/{ns}"
		case 4:
			switch parts[3] {
			case "duplicate", "export", "import":
				return "/v1/namespaces/{ns}/" + parts[3]
			default:
				return "/v1/namespaces/{ns}/..."
			}
		default:
			return "/v1/namespaces/{ns}/..."
		}
	}

	if len(parts) == 3 {
		switch parts[2] {
		case "query", "subscribe", "changelog", "stats", "tables":
			return "/v1/{ns}/" + parts[2]
		default:
			return "/v1/{ns}/..."
		}
	}

	if len(parts) >= 4 && parts[2] == "tables" {
		switch len(parts) {
		case 4:
			return "/v1/{ns}/tables/{table}"
		case 5:
			switch parts[4] {
			case "indexes", "rows":
				return "/v1/{ns}/tables/{table}/" + parts[4]
			default:
				return "/v1/{ns}/tables/{table}/..."
			}
		case 6:
			switch parts[4] {
			case "indexes":
				return "/v1/{ns}/tables/{table}/indexes/{index}"
			case "rows":
				return "/v1/{ns}/tables/{table}/rows/{id}"
			default:
				return "/v1/{ns}/tables/{table}/..."
			}
		default:
			return "/v1/{ns}/tables/{table}/..."
		}
	}

	return "/v1/{ns}/..."
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
