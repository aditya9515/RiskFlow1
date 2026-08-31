package observability

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/correlation"
	"github.com/prometheus/client_golang/prometheus"
)

const RequestIDHeader = "X-Request-ID"

// RequestID returns the validated UUID attached by HTTPMiddleware.
func RequestID(ctx context.Context) string {
	return correlation.RequestID(ctx)
}

// WithRequestID returns a context carrying a validated UUID request ID. Invalid
// values are ignored so downstream trace contracts cannot be polluted.
func WithRequestID(ctx context.Context, requestID string) context.Context {
	return correlation.WithRequestID(ctx, requestID)
}

// HTTPMetrics contains bounded-label API request instruments.
type HTTPMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	inFlight prometheus.Gauge
}

func NewHTTPMetrics(registerer prometheus.Registerer) *HTTPMetrics {
	metrics := &HTTPMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "riskflow",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Completed HTTP requests by method, normalized route, and status code.",
		}, []string{"method", "route", "status_code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "riskflow",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request duration by method and normalized route.",
			Buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		}, []string{"method", "route"}),
		inFlight: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "riskflow",
			Subsystem: "http",
			Name:      "in_flight_requests",
			Help:      "Number of HTTP requests currently executing.",
		}),
	}
	registerer.MustRegister(metrics.requests, metrics.duration, metrics.inFlight)
	return metrics
}

// Middleware attaches one UUID request ID, records request metrics, and emits
// one structured completion log. Arbitrary caller values are not accepted:
// this prevents log injection and keeps the identifier compatible with the
// existing UUID trace contract.
func (m *HTTPMetrics) Middleware(next http.Handler, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := strings.ToLower(strings.TrimSpace(r.Header.Get(RequestIDHeader)))
		if !validUUID(requestID) {
			requestID = newUUID()
		}
		ctx := WithRequestID(r.Context(), requestID)
		r = r.WithContext(ctx)
		w.Header().Set(RequestIDHeader, requestID)

		startedAt := time.Now()
		m.inFlight.Inc()
		defer m.inFlight.Dec()

		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)

		route := normalizedRoute(r.Pattern)
		duration := time.Since(startedAt)
		method := normalizedMethod(r.Method)
		m.requests.WithLabelValues(method, route, strconv.Itoa(recorder.status)).Inc()
		m.duration.WithLabelValues(method, route).Observe(duration.Seconds())
		logger.InfoContext(ctx, "HTTP request completed",
			slog.String("request_id", requestID),
			slog.String("method", r.Method),
			slog.String("route", route),
			slog.Int("status_code", recorder.status),
			slog.Int64("duration_ms", duration.Milliseconds()),
		)
	})
}

func normalizedMethod(method string) string {
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodHead, http.MethodOptions:
		return method
	default:
		return "OTHER"
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func normalizedRoute(pattern string) string {
	if pattern == "" {
		return "unmatched"
	}
	if _, route, found := strings.Cut(pattern, " "); found {
		return route
	}
	return pattern
}

func validUUID(value string) bool {
	_, valid := correlation.NormalizeRequestID(value)
	return valid
}

func newUUID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		// crypto/rand failures are exceptional. A timestamp fallback preserves
		// availability while remaining within the UUID-shaped trace contract.
		return fmt.Sprintf("00000000-0000-4000-8000-%012x", time.Now().UnixNano()&0xffffffffffff)
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", bytes[0:4], bytes[4:6], bytes[6:8], bytes[8:10], bytes[10:16])
}
