// Package session serves the Python api's session routes from dho api:
// login, social login, token refresh, validation, logout, /me, the caller's
// organizations and the organization switch (api/auth/routers/login.py,
// oauth.py, refresh.py, session.py). The Python bodies are the spec; every
// response, row and token matches them (TestSessionVenueOracle).
//
// Tokens are minted and verified by internal/auth/edgetoken, the one HS256
// implementation; the caller of a protected route is decided by
// internal/api/policy. Passwords, provider tokens and minted tokens never
// reach a log line or an error text.
package session

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Deps are the session routes' dependencies.
type Deps struct {
	Pool     *pgxpool.Pool
	Guard    *policy.Guard
	Auth     *policy.Authenticator
	Verifier *edgetoken.Verifier
	Signer   *edgetoken.Signer
	// ClickHouse is the api's own ClickHouse login, used for organization
	// activity; nil means no activity (Python: no CLICKHOUSE_URI).
	ClickHouse driver.Conn
	// Limits is where the rate limits count: the api's shared counter
	// store (Valkey when configured); nil means an in-process one on Now.
	Limits httpapi.CounterStore
	// Write renders the limits' refusal (httpapi.CodeRateLimited); nil
	// means httpapi.WriteError.
	Write httpapi.ErrorWriter
	OAuth *oauthprovider.Client
	// Getenv reads the social-login client settings on every request, as
	// the Python route does (os.environ.get); nil means os.Getenv.
	Getenv  func(string) string
	Audit   audit.Writer
	Now     func() time.Time
	NewUUID func() uuid.UUID
	Logger  *slog.Logger
}

type handlers struct {
	Deps
	loginLimiter, refreshLimiter, validateLimiter *httpapi.KeyedLimiter
}

func (d Deps) withDefaults() Deps {
	if d.Getenv == nil {
		d.Getenv = os.Getenv
	}
	if d.Audit == nil {
		d.Audit = audit.PGWriter{Now: d.Now}
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.NewUUID == nil {
		d.NewUUID = uuid.New
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.OAuth == nil {
		d.OAuth = oauthprovider.NewClient()
	}
	if d.Limits == nil {
		d.Limits = httpapi.NewMemoryCounters(d.Now)
	}
	if d.Write == nil {
		d.Write = httpapi.WriteError
	}
	return d
}

// Routes mounts the eight session routes. It returns nothing unless the
// protected-route runtime and the token keys are configured.
func Routes(deps Deps) []httpapi.Route {
	if deps.Pool == nil || deps.Guard == nil || deps.Auth == nil || deps.Verifier == nil || deps.Signer == nil {
		return nil
	}
	defaults := deps.withDefaults()
	h := handlers{Deps: defaults,
		loginLimiter:    httpapi.NewKeyedLimiter(defaults.Limits, loginLimit),
		refreshLimiter:  httpapi.NewKeyedLimiter(defaults.Limits, refreshLimit),
		validateLimiter: httpapi.NewKeyedLimiter(defaults.Limits, validateLimit),
	}
	guard := h.Guard
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: "/api/v1/auth/login", Handler: guard.BodyFirst(policy.Public,
			httpapi.ValidateThenLimit(validateLogin, h.loginLimiter, forwardedIPKey, h.Write)(http.HandlerFunc(h.login)))},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/social-login", Handler: guard.BodyFirst(policy.Public, http.HandlerFunc(h.socialLogin))},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/refresh", Handler: guard.BodyFirst(policy.Public,
			httpapi.ValidateThenLimit(validateRefresh, h.refreshLimiter, forwardedIPKey, h.Write)(http.HandlerFunc(h.refresh)))},
		{Method: http.MethodGet, Pattern: "/api/v1/auth/me", Handler: guard.Wrap(policy.Authenticated, http.HandlerFunc(h.me))},
		{Method: http.MethodGet, Pattern: "/api/v1/auth/me/organizations", Handler: guard.Wrap(policy.Authenticated, http.HandlerFunc(h.myOrganizations))},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/switch-org", Handler: guard.BodyFirst(policy.Authenticated, http.HandlerFunc(h.switchOrg))},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/validate", Handler: guard.BodyFirst(policy.Public,
			httpapi.ValidateThenLimit(h.validateTokenBody, h.validateLimiter, validateLimitKey, h.Write)(http.HandlerFunc(h.validate)))},
		{Method: http.MethodPost, Pattern: "/api/v1/auth/logout", Handler: guard.BodyFirst(policy.Public, http.HandlerFunc(h.logout))},
	}
}

// fail logs a failure with its step and writes the Python api's bare 500.
// err must never carry a credential.
func (h handlers) fail(w http.ResponseWriter, r *http.Request, step string, err error) {
	attrs := []any{slog.String("path", r.URL.Path), slog.String("step", step)}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	h.Logger.ErrorContext(r.Context(), "api session route failed", attrs...)
	policy.WriteInternal(w)
}

// refuse writes {"detail": {"message": message}} with status.
func refuse(w http.ResponseWriter, status int, message string) {
	policy.WriteDetail(w, status, policy.ErrorDetail(message), nil)
}
