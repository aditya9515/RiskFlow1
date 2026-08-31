package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRegistry creates an isolated process registry instead of mutating the
// package-global Prometheus registry. This keeps tests and multiple binaries
// deterministic.
func NewRegistry(service string) *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "riskflow",
			Name:      "build_info",
			Help:      "Static build information for a running RiskFlow process.",
			ConstLabels: prometheus.Labels{
				"service": service,
				"version": "dev",
			},
		}, func() float64 { return 1 }),
	)
	return registry
}

// Handler returns a Prometheus/OpenMetrics-compatible scrape handler.
func Handler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}
