package apiservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
)

// logWideTrust reports TRUSTED_PROXIES entries wider than a /28. Where the chart says the request ports are
// fenced to the ingress controller pods, the wide range cannot be reached by another pod and the line is
// informational; everywhere else it is a warning.
func logWideTrust(logger *slog.Logger) {
	wide := clientip.WidePrefixes()
	if wide == 0 {
		return
	}
	if clientip.NetworkFenced() {
		logger.Info("api: the trusted proxy list holds prefixes wider than a /28; the request ports are fenced to the ingress controller", "count", wide)
		return
	}
	logger.Warn("api: the trusted proxy list holds prefixes wider than a /28", "count", wide)
}
