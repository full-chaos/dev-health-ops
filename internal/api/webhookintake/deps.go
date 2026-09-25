// Package webhookintake is the `dho api` route area for
// /api/v1/webhooks/*: GitHub, GitLab, Jira, and PagerDuty webhook intake
// (Python src/dev_health_ops/api/webhooks/). GitHub/GitLab/Jira authenticate
// by a shared provider secret (HMAC signature or a static token) read from
// the environment; PagerDuty authenticates per-binding, against an
// encrypted secret stored on the addressed pagerduty_webhook_bindings row.
// Every route here answers with the app-wide {"detail": ...} shape
// (internal/api/policy) -- these are exactly the FastAPI-dependency-body,
// plain-HTTPException routes that shape targets, unlike external-ingest's
// own bespoke envelope (CHAOS-6246).
package webhookintake

import (
	"log/slog"
	"os"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// Secrets are the provider-shared webhook secrets (Python's
// _get_github_webhook_secret/_get_gitlab_webhook_token/_get_jira_webhook_secret,
// GITHUB_WEBHOOK_SECRET/GITLAB_WEBHOOK_TOKEN/JIRA_WEBHOOK_SECRET), read live
// per request -- an unset secret is a 500 (router.py's own behavior), not a
// route that silently never verifies.
type Secrets struct {
	GitHub string
	GitLab string
	Jira   string
}

func envSecrets() Secrets {
	return Secrets{
		GitHub: os.Getenv("GITHUB_WEBHOOK_SECRET"),
		GitLab: os.Getenv("GITLAB_WEBHOOK_TOKEN"),
		Jira:   os.Getenv("JIRA_WEBHOOK_SECRET"),
	}
}

// Deps are this area's dependencies, handed down from apiservice.
type Deps struct {
	Pool   *pgxpool.Pool
	Valkey valkeygo.Client
	// Producer publishes to the job outbox (GitHub/GitLab/Jira durable
	// deliveries -- KindWebhookDelivery). Nil is legal (env not configured
	// yet); every route needing it answers 500 rather than dereferencing.
	Producer *joboutbox.Producer
	// Decryptor decrypts pagerduty_webhook_bindings.signing_secret_encrypted
	// (providerfoundation.FernetDecryptor, wire-compatible with
	// core/encryption.py's encrypt_value/decrypt_value).
	Decryptor providerfoundation.FernetDecryptor
	// PagerDuty's canonical_incident_ingestion decision is built inline from
	// Pool at each call site (licensing.PostgresStore{Pool: d.Pool}.Decide),
	// matching internal/api/externalingest's own pattern -- the one shared
	// feature-decision engine, consumed the same way everywhere, never a
	// second hand-rolled implementation and never a separate injected field.
	// Secrets overrides envSecrets() for tests.
	Secrets Secrets
	Now     func() time.Time
	Logger  *slog.Logger
	// Counters is the store the PagerDuty limiter counts in: the shared
	// Valkey-backed store in a real deployment (so the limit holds across api
	// replicas), nil for an in-process store on Now (development and tests).
	Counters httpapi.CounterStore

	limiters *rateLimiters
}

func (d Deps) secrets() Secrets {
	if d.Secrets.GitHub != "" || d.Secrets.GitLab != "" || d.Secrets.Jira != "" {
		return d.Secrets
	}
	return envSecrets()
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d Deps) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

// Routes builds the 5 /api/v1/webhooks/* routes (router.py + pagerduty.py).
// PagerDuty carries its own 60/minute-per-caller rate limit (rate_limit.py's
// @limiter.limit("60/minute") on pagerduty_webhook); GitHub/GitLab/Jira/health
// carry none in Python either.
func Routes(deps Deps) []httpapi.Route {
	if deps.limiters == nil {
		deps.limiters = newRateLimiters(deps.Counters, deps.Now)
	}
	const prefix = "/api/v1/webhooks"
	return []httpapi.Route{
		{Method: "POST", Pattern: prefix + "/github", Handler: deps.handleGitHubWebhook()},
		{Method: "POST", Pattern: prefix + "/gitlab", Handler: deps.handleGitLabWebhook()},
		{Method: "POST", Pattern: prefix + "/jira", Handler: deps.handleJiraWebhook()},
		{Method: "POST", Pattern: prefix + "/pagerduty/{binding_id}", Handler: deps.handlePagerDutyWebhook()},
		{Method: "GET", Pattern: prefix + "/health", Handler: deps.handleHealth()},
	}
}

// LoadJobRegistry is configure's seam for constructing the Producer this
// package needs (internal/apiservice wires it once at startup, the same way
// internal/schedulerservice and dev-health-workerctl already load it). root
// is jobruntime.Load's checked-in manifest directory, staged into the dho
// api image (docker/go-worker.Dockerfile's dho target) at
// "contracts/jobs/v1"; a caller two directories below the repo root (a Go
// test package, matching internal/schedulerservice's own testContractRoot
// convention) passes "../../contracts/jobs/v1" instead.
func LoadJobRegistry(root string) (*jobruntime.Registry, error) {
	return jobruntime.Load(root)
}
