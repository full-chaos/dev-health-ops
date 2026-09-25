// Package admin serves dho api's org, user, invite and impersonation admin
// routes (CHAOS-6250, split into CHAOS-6303 impersonation, CHAOS-6304
// users, CHAOS-6305 orgs): api/admin/impersonation.py and
// api/admin/routers/{orgs,users}.py, ported route for route. Every route
// here authenticates and authorizes through internal/api/policy (Guard,
// Scope); this package never re-checks a bearer token or re-derives the
// request's org itself.
//
// This file mounts all three areas: impersonation (CHAOS-6303), users
// (CHAOS-6304), orgs (CHAOS-6305).
package admin

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Deps is this area's dependency set, built from the api Service's shared
// Postgres pool, Valkey client and protected-route runtime -- see
// apiservice.Deps's own doc comment for why an area package never opens its
// own pool or client.
type Deps struct {
	Pool   *pgxpool.Pool
	Valkey valkeygo.Client
	Guard  *policy.Guard
	Logger *slog.Logger
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
	// ClickHouseDSN is the org-deletion route's analytics-table purge
	// target ("" = not configured; every ClickHouse count/delete is
	// skipped with a warning, matching org_deletion.py's own behavior).
	ClickHouseDSN string
	// ClickHouse is the api's own dedicated login (nil when not
	// configured): the LLM spend route reads llm_token_usage and
	// work_unit_investments with it.
	ClickHouse driver.Conn
	// Decryptor reads provider_oauth_credentials/provider_oauth_revocations'
	// encrypted PagerDuty tokens before the org-deletion route revokes
	// them -- keyed by the same SETTINGS_ENCRYPTION_KEY/SALT every other
	// encrypted-value reader in this repo uses.
	Decryptor providerfoundation.FernetDecryptor
	// PagerDuty is the registered app's OAuth client identity the
	// org-deletion route revokes grants with. A venue test overrides
	// PagerDuty.RevokeURL to point both planes at one fake endpoint.
	PagerDuty providerfoundation.PagerDutyRevokeConfig
	// HTTPDoer is the client the org-deletion route's PagerDuty revoke
	// call uses; nil means http.DefaultClient.
	HTTPDoer providerfoundation.HTTPDoer
	// Write renders every error this area's own middleware writes directly
	// (currently: the keyed rate limiter's 429, CHAOS-6357) -- every other
	// error already renders through httpapi's route-level ErrorWriter, set
	// once at Server construction (apiservice.NewServer). Nil means
	// httpapi.WriteError, which is NOT what a real deployment uses (that is
	// apiservice.WriteError, the Python-wire-shape renderer) -- callers
	// building a real Service always pass it; nil is a test-only default
	// for a case that does not care about the exact body.
	Write httpapi.ErrorWriter
	// Limits is the rate-limit store every limited route in this area counts
	// through: the shared Valkey-backed store in a real deployment (so a limit
	// holds across api replicas), the in-process one otherwise. Nil means an
	// in-process store on Now, for a test that does not care.
	Limits httpapi.CounterStore
	// Invites configures create_org_invite's token signing, accept link and
	// email sender; see InviteConfig. The zero value works (no email sent).
	Invites InviteConfig
}

// Routes is the admin area's route set.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	store := pgStore{Pool: deps.Pool, Now: deps.Now}
	auditWriter := audit.PGWriter{Now: deps.Now}
	cache := newImpersonationCache(deps.Valkey, store, logger)
	if deps.Now != nil {
		cache.now = deps.Now
	}
	httpDoer := deps.HTTPDoer
	if httpDoer == nil {
		// PagerDuty's revoke carries a bearer token in its body: httpx does
		// not follow redirects and has a 10s timeout, so neither may this
		// client (http.DefaultClient would replay a 307's body, token
		// included, to the redirect target).
		httpDoer = &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	limits := deps.Limits
	if limits == nil {
		limits = httpapi.NewMemoryCounters(deps.Now)
	}
	write := deps.Write
	if write == nil {
		write = httpapi.WriteError
	}
	area := &handlers{
		store:           store,
		audit:           auditWriter,
		cache:           cache,
		guard:           deps.Guard,
		logger:          logger,
		clickHouseDSN:   deps.ClickHouseDSN,
		decryptor:       deps.Decryptor,
		pagerDuty:       deps.PagerDuty,
		httpDoer:        httpDoer,
		upstreamDoer:    deps.HTTPDoer,
		write:           write,
		passwordLimiter: httpapi.NewKeyedLimiter(limits, passwordLimit),
		inviteLimiter:   httpapi.NewKeyedLimiter(limits, inviteLimit),
		invites:         deps.Invites,
	}
	if deps.ClickHouse != nil {
		area.spend = clickhouseSpend{conn: deps.ClickHouse}
	}
	return area.routes()
}

// routes is this area's full route set: impersonation, users, orgs.
func (h *handlers) routes() []httpapi.Route {
	var out []httpapi.Route
	out = append(out, h.impersonationRoutes()...)
	out = append(out, h.userRoutes()...)
	out = append(out, h.orgRoutes()...)
	out = append(out, h.governanceRoutes()...)
	return out
}

// handlers holds the built dependencies every admin route handler closes
// over.
type handlers struct {
	store  pgStore
	audit  audit.Writer
	cache  *impersonationCache
	guard  *policy.Guard
	logger *slog.Logger
	// clickHouseDSN, decryptor, pagerDuty and httpDoer are the org-deletion
	// route's own dependencies (CHAOS-6306); every other handler in this
	// package ignores them.
	clickHouseDSN string
	// spend is the LLM spend route's ClickHouse reader; nil when the api has no
	// ClickHouse login.
	spend     spendReader
	decryptor providerfoundation.FernetDecryptor
	pagerDuty providerfoundation.PagerDutyRevokeConfig
	httpDoer  providerfoundation.HTTPDoer
	// upstreamDoer is Deps.HTTPDoer as given: nil means the PagerDuty
	// callback and credential-validation calls build their own client with
	// the reference's timeout and redirect policy, which the defaulted
	// httpDoer above would bypass.
	upstreamDoer providerfoundation.HTTPDoer
	// write renders this area's own directly-written errors (the keyed
	// rate limiter's 429) in the same wire shape every other error uses.
	write httpapi.ErrorWriter
	// One KeyedLimiter per limited route, all over Deps.Limits: the limits
	// themselves are passwordLimit and inviteLimit.
	passwordLimiter *httpapi.KeyedLimiter
	inviteLimiter   *httpapi.KeyedLimiter
	invites         InviteConfig
}

// passwordLimit is users.py's `@limiter.limit(ADMIN_PASSWORD_LIMIT =
// "5/hour", key_func=get_admin_user_key)` on set_user_password, and
// inviteLimit orgs.py's `@limiter.limit("10/hour", key_func=
// get_admin_user_key)` on create_org_invite: fixed window per (admin, exact
// path). The IDs are the shared store's namespace; do not rename them once
// deployed (a rename resets every counter).
var (
	passwordLimit = httpapi.Limit{ID: "admin_password", Count: 5, Window: time.Hour}
	inviteLimit   = httpapi.Limit{ID: "admin_org_invite", Count: 10, Window: time.Hour}
)
