package webhookintake

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// handleHealth ports router.py's webhooks_health. "celery_available" is
// always false: Celery is retired (CHAOS-4026), and the Python route itself
// only ever reports whether the import succeeds -- the Go api has no such
// import to attempt, so the honest answer is always false, not omitted.
func (d Deps) handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secrets := d.secrets()
		policy.WriteJSON(w, http.StatusOK, map[string]any{
			"status": "ok",
			"secrets_configured": map[string]any{
				"github": secrets.GitHub != "",
				"gitlab": secrets.GitLab != "",
				"jira":   secrets.Jira != "",
			},
			"celery_available": false,
		}, nil)
	}
}
