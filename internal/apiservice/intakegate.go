package apiservice

import (
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/pyheaders"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// NewIntakeGate is the gate the api listener is built with: it counts in the
// rate-limit store the routes count in (the shared Valkey store when the api
// has a client, else the in-process one), and records that store in deps so
// Routes counts through the same instance.
func NewIntakeGate(deps *Deps, logger *slog.Logger) *webhookintake.Gate {
	return newIntakeGate(deps, logger, limitStore)
}

// newIntakeGate builds the gate over the store build returns. A store that
// cannot be built is logged once, by class only, and the gate counts in the
// in-process store: the limits then hold per process, not across replicas.
func newIntakeGate(deps *Deps, logger *slog.Logger, build func(Deps) (httpapi.CounterStore, error)) *webhookintake.Gate {
	limits, err := build(*deps)
	if err != nil {
		logger.Error("api: the shared rate limiter is unavailable; intake deliveries count in the in-process limiter", logging.ErrorAttr(err))
		limits = httpapi.NewMemoryCounters(deps.Now)
	}
	deps.Limits = limits
	return webhookintake.NewGate(limits, deps.Now, logger)
}

// intakeGate counts a webhook intake delivery before the scope middlewares see
// it. A refusal is written through the origin check, security headers and
// CORS layers, so it carries what a refusal from the intake handler carried.
func intakeGate(gate *webhookintake.Gate, allowedOrigins []string) func(http.Handler) http.Handler {
	layers := func(refuse http.Handler) http.Handler {
		return NewOriginValidation(allowedOrigins).Wrap(pyheaders.SecurityHeaders(pyheaders.NewCORS(allowedOrigins).Wrap(refuse)))
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provider, intake := webhookintake.Provider(r.Method, r.URL.Path)
			if !intake {
				next.ServeHTTP(w, r)
				return
			}
			if refuse := gate.Admit(r, provider); refuse != nil {
				layers(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { refuse(w) })).ServeHTTP(w, r)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
