package apiservice

import (
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/api/clientip"
)

// logWideTrust reports TRUSTED_PROXIES entries wider than a /28. Where the chart says the request ports are
// restricted by a NetworkPolicy to the ingress controller pods and the peers it names, another pod of the
// range cannot reach them and the line is informational; everywhere else it is a warning.
func logWideTrust(logger *slog.Logger) {
	wide := clientip.WidePrefixes()
	if wide == 0 {
		return
	}
	if clientip.NetworkFenced() {
		logger.Info("api: the trusted proxy list holds prefixes wider than a /28; the chart restricts the request ports to the ingress controller pods and the peers it names", "count", wide)
		return
	}
	logger.Warn("api: the trusted proxy list holds prefixes wider than a /28", "count", wide)
}
