package observability

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkerHandlerExposesLivenessAndMetrics(t *testing.T) {
	t.Parallel()

	registry := NewRegistry("test-worker")
	handler := WorkerHandler(registry)

	health := httptest.NewRecorder()
	handler.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK || strings.TrimSpace(health.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("health response = %d %q", health.Code, health.Body.String())
	}

	metrics := httptest.NewRecorder()
	handler.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK || !strings.Contains(metrics.Body.String(), `riskflow_build_info{service="test-worker",version="dev"} 1`) {
		t.Fatalf("metrics response did not contain build info: %d", metrics.Code)
	}
}
