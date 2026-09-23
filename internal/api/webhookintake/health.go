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
// Field order matters here, not just presence: a plain map[string]any
// through encoding/json (what policy.WriteJSON uses generically) sorts keys
// alphabetically, which happened to diverge from Python's dict insertion
// order (status, secrets_configured, celery_available) -- caught by the
// venue oracle. *pyjson.Object preserves Set() order through
// policy.WriteJSON via its own MarshalJSON.
func (d Deps) handleHealth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secrets := d.secrets()
		secretsConfigured := pyjson.NewObject()
		secretsConfigured.Set("github", secrets.GitHub != "")
		secretsConfigured.Set("gitlab", secrets.GitLab != "")
		secretsConfigured.Set("jira", secrets.Jira != "")
		body := pyjson.NewObject()
		body.Set("status", "ok")
		body.Set("secrets_configured", secretsConfigured)
		body.Set("celery_available", false)
		policy.WriteJSON(w, http.StatusOK, body, nil)
	}
}
