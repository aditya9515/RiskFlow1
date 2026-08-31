package observability

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
)

// WorkerHandler exposes only liveness and metrics for a background process.
// Readiness remains an API contract because worker availability also depends
// on per-record PostgreSQL and Kafka operations.
func WorkerHandler(gatherer prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", Handler(gatherer))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(struct {
			Status string `json:"status"`
		}{Status: "ok"})
	})
	return mux
}
