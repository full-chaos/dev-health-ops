package apiservice

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/pyheaders"
	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
)

// intakeGate counts a webhook intake delivery before the scope middlewares see
// it. A refusal is written through the origin check, security headers and
// CORS layers, so it carries what a refusal from the intake handler carried.
func intakeGate(gate *webhookintake.Gate, allowedOrigins []string) func(http.Handler) http.Handler {
	layers := func(refuse http.Handler) http.Handler {
		return NewOriginValidation(allowedOrigins).Wrap(pyheaders.SecurityHeaders(pyheaders.NewCORS(allowedOrigins).Wrap(refuse)))
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provider, intake := webhookintake.Provider(r.Method, r.URL.EscapedPath())
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
