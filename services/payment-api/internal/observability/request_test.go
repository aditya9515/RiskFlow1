package observability

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHTTPMiddlewareEchoesValidatedRequestIDAndNormalizesRoute(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := NewHTTPMetrics(registry)
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) != "10000000-0000-4000-8000-000000000001" {
			t.Fatalf("request ID in context = %q", RequestID(r.Context()))
		}
		w.WriteHeader(http.StatusCreated)
	})

	request := httptest.NewRequest(http.MethodGet, "/v1/payments/abc", nil)
	request.Header.Set(RequestIDHeader, "10000000-0000-4000-8000-000000000001")
	response := httptest.NewRecorder()
	metrics.Middleware(mux, logger).ServeHTTP(response, request)

	if response.Header().Get(RequestIDHeader) != "10000000-0000-4000-8000-000000000001" {
		t.Fatalf("response request ID = %q", response.Header().Get(RequestIDHeader))
	}
	assertMetricValue(t, registry, "riskflow_http_requests_total", map[string]string{
		"method": "GET", "route": "/v1/payments/{id}", "status_code": "201",
	}, 1)
	if !strings.Contains(logOutput.String(), `"request_id":"10000000-0000-4000-8000-000000000001"`) {
		t.Fatalf("structured log did not contain request ID: %s", logOutput.String())
	}
}

func TestHTTPMiddlewareReplacesUntrustedRequestID(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	metrics := NewHTTPMetrics(registry)
	request := httptest.NewRequest(http.MethodGet, "/missing", nil)
	request.Header.Set(RequestIDHeader, "not-a-uuid\nforged-log-line")
	response := httptest.NewRecorder()
	metrics.Middleware(http.NewServeMux(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))).ServeHTTP(response, request)

	requestID := response.Header().Get(RequestIDHeader)
	if !validUUID(requestID) {
		t.Fatalf("generated request ID = %q, want UUIDv4", requestID)
	}
	if strings.Contains(requestID, "forged") {
		t.Fatalf("untrusted request ID was reflected: %q", requestID)
	}
	assertMetricValue(t, registry, "riskflow_http_requests_total", map[string]string{
		"method": "GET", "route": "unmatched", "status_code": "404",
	}, 1)
}

func TestNormalizedMethodBoundsUntrustedValues(t *testing.T) {
	t.Parallel()

	if normalizedMethod(http.MethodPost) != http.MethodPost {
		t.Fatal("known method was not preserved")
	}
	if normalizedMethod("ATTACKER-SUPPLIED-METHOD") != "OTHER" {
		t.Fatal("untrusted method was not collapsed to OTHER")
	}
}

func assertMetricValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matched := len(metric.GetLabel()) == len(labels)
			for _, label := range metric.GetLabel() {
				if labels[label.GetName()] != label.GetValue() {
					matched = false
				}
			}
			if !matched {
				continue
			}
			got := metric.GetCounter().GetValue()
			if metric.Gauge != nil {
				got = metric.GetGauge().GetValue()
			}
			if got != want {
				t.Fatalf("metric %s = %v, want %v", name, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
}
