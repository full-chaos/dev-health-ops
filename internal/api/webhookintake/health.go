package webhookintake

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// handleHealth ports router.py's webhooks_health. "celery_available" is
// always false: Celery is retired (CHAOS-4026), and the Python route itself
// only ever reports whether the import succeeds -- the Go api has no such
// import to attempt, so the honest answer is always false, not omitted.
// The Python body also named which provider secrets are set; this route is
// public, so it answers liveness only and names no server-side secret state.
// *pyjson.Object keeps the field order (status, celery_available) through
// policy.WriteModel via its own MarshalJSON.
func (d Deps) handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := pyjson.NewObject()
		body.Set("status", "ok")
		body.Set("celery_available", false)
		policy.WriteModel(w, http.StatusOK, body, nil)
	}
}
