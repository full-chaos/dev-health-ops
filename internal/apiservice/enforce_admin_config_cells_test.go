package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// adminConfigRoute selects the settings, integrations, sync and teams rows (leaf 6.8) of the go-api profile.
var adminConfigRoute = regexp.MustCompile(`^/api/v1/admin/(settings|integrations|sync-configs|sync-runs|sync-targets|backfill-jobs|teams)(/|$)`)

// adminConfigActions is today's action of every 6.8 row ("" = no action, so the Enforcer never asks for it). A
// profile change to any of these rows must change this table in the same PR.
var adminConfigActions = map[string]string{
	"GET /api/v1/admin/backfill-jobs":                                                  "integrations.read",
	"GET /api/v1/admin/backfill-jobs/{job_id}":                                         "integrations.read",
	"GET /api/v1/admin/integrations":                                                   "integrations.read",
	"POST /api/v1/admin/integrations":                                                  "integrations.write",
	"POST /api/v1/admin/integrations/github/install-callback":                          "integrations.write",
	"POST /api/v1/admin/integrations/github/install-url":                               "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/api-token":                              "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/authorize":                              "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/callback":                               "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/client-credentials":                     "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/disconnect":                             "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/preflight":                              "integrations.write",
	"GET /api/v1/admin/integrations/pagerduty/services":                                "integrations.read",
	"GET /api/v1/admin/integrations/pagerduty/status":                                  "integrations.read",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings":                       "integrations.write",
	"GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}":           "integrations.read",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate": "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke":   "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate":   "integrations.write",
	"GET /api/v1/admin/integrations/{integration_id}":                                  "integrations.read",
	"PATCH /api/v1/admin/integrations/{integration_id}":                                "integrations.write",
	"POST /api/v1/admin/integrations/{integration_id}/backfill":                        "integrations.write",
	"GET /api/v1/admin/integrations/{integration_id}/datasets":                         "integrations.read",
	"PATCH /api/v1/admin/integrations/{integration_id}/datasets":                       "integrations.write",
	"POST /api/v1/admin/integrations/{integration_id}/discover":                        "integrations.read",
	"GET /api/v1/admin/integrations/{integration_id}/sources":                          "integrations.read",
	"PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}":            "integrations.write",
	"POST /api/v1/admin/integrations/{integration_id}/sync":                            "integrations.write",
	"POST /api/v1/admin/settings":                                                      "settings.write",
	"GET /api/v1/admin/settings/{category}":                                            "settings.write",
	"DELETE /api/v1/admin/settings/{category}/{key}":                                   "settings.write",
	"GET /api/v1/admin/settings/{category}/{key}":                                      "settings.write",
	"PUT /api/v1/admin/settings/{category}/{key}":                                      "settings.write",
	"GET /api/v1/admin/sync-configs":                                                   "integrations.read",
	"POST /api/v1/admin/sync-configs":                                                  "integrations.write",
	"GET /api/v1/admin/sync-configs/auto-import-capabilities":                          "integrations.read",
	"POST /api/v1/admin/sync-configs/batch":                                            "integrations.write",
	"DELETE /api/v1/admin/sync-configs/{config_id}":                                    "integrations.write",
	"GET /api/v1/admin/sync-configs/{config_id}":                                       "integrations.read",
	"PATCH /api/v1/admin/sync-configs/{config_id}":                                     "integrations.write",
	"POST /api/v1/admin/sync-configs/{config_id}/backfill":                             "integrations.write",
	"GET /api/v1/admin/sync-configs/{config_id}/coverage":                              "integrations.read",
	"GET /api/v1/admin/sync-configs/{config_id}/jobs":                                  "integrations.read",
	"GET /api/v1/admin/sync-configs/{config_id}/repositories":                          "integrations.read",
	"PUT /api/v1/admin/sync-configs/{config_id}/repositories":                          "integrations.write",
	"POST /api/v1/admin/sync-configs/{config_id}/trigger":                              "integrations.write",
	"GET /api/v1/admin/sync-runs/{run_id}":                                             "integrations.read",
	"GET /api/v1/admin/sync-runs/{run_id}/units":                                       "integrations.read",
	"GET /api/v1/admin/sync-targets":                                                   "integrations.read",
	"GET /api/v1/admin/teams":                                                          "teams.write",
	"POST /api/v1/admin/teams":                                                         "teams.write",
	"DELETE /api/v1/admin/teams/{team_id}":                                             "teams.write",
	"GET /api/v1/admin/teams/{team_id}":                                                "teams.write",
	"PATCH /api/v1/admin/teams/{team_id}":                                              "teams.write",
	"POST /api/v1/admin/teams/{team_id}":                                               "teams.write",
	"POST /api/v1/admin/teams/{team_id}/approve-changes":                               "teams.write",
	"POST /api/v1/admin/teams/{team_id}/confirm-inferred-members":                      "teams.write",
	"POST /api/v1/admin/teams/{team_id}/confirm-members":                               "teams.write",
	"GET /api/v1/admin/teams/{team_id}/discover-members":                               "teams.write",
	"POST /api/v1/admin/teams/{team_id}/dismiss-changes":                               "teams.write",
	"GET /api/v1/admin/teams/{team_id}/infer-members":                                  "teams.write",
}

// adminConfigNamespaces are the cells 6.8 turns on.
const adminConfigNamespaces = "settings,integrations,teams"

// adminConfigProfileRows reads the 6.8 rows of the go-api profile as "METHOD /route" -> action.
func adminConfigProfileRows(t *testing.T) map[string]string {
	t.Helper()
	var document struct {
		Rows []struct {
			Service string  `json:"service"`
			Method  string  `json:"method"`
			Route   string  `json:"route"`
			Action  *string `json:"action"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, row := range document.Rows {
		if row.Service != goAPIAccessService || !adminConfigRoute.MatchString(row.Route) {
			continue
		}
		action := ""
		if row.Action != nil {
			action = *row.Action
		}
		out[row.Method+" "+row.Route] = action
	}
	return out
}

func TestAdminConfigRowsMatchTheProfile(t *testing.T) {
	got := adminConfigProfileRows(t)
	for key, action := range got {
		want, ok := adminConfigActions[key]
		if !ok {
			t.Errorf("%s: a profile row the table does not list (action %q)", key, action)
			continue
		}
		if action != want {
			t.Errorf("%s: profile action %q, table %q", key, action, want)
		}
	}
	for key := range adminConfigActions {
		if _, ok := got[key]; !ok {
			t.Errorf("%s: listed but not a profile row", key)
		}
	}
	unmapped := 0
	for _, action := range adminConfigActions {
		if action == "" {
			unmapped++
		}
	}
	if len(adminConfigActions) != 61 || unmapped != 0 {
		t.Errorf("table has %d rows, %d without an action; want 61 and 0", len(adminConfigActions), unmapped)
	}
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	for key, action := range adminConfigActions {
		row, ok := rows[key]
		switch {
		case action == "" && ok:
			t.Errorf("%s: no action in the table but an Enforcer row %+v", key, row)
		case action != "" && (!ok || row.Action != action || row.ResourceType != "organization"):
			t.Errorf("%s: Enforcer row %+v, want action %s on the organization", key, row, action)
		}
	}
}

// cellPDP is a decision point that answers every call the same way and records the requests the Enforcer sent.
type cellPDP struct {
	srv    *httptest.Server
	client *pep.Client
	mu     sync.Mutex
	mode   string // allow, deny, fail, evaluate
	seen   []pep.Request
	// callers maps a bearer token to the state the evaluate mode decides over.
	callers map[string]cellSnapshot
	// reasons are the internal reasons of the evaluate mode, one per call.
	reasons []authorization.Reason
}

func newCellPDP(t *testing.T) *cellPDP {
	t.Helper()
	p := &cellPDP{mode: "deny"}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		mode := p.mode
		p.mu.Unlock()
		if mode == "fail" {
			http.Error(w, "down", http.StatusInternalServerError)
			return
		}
		var body struct {
			PrincipalReference string `json:"principal_reference"`
			Action             string `json:"action"`
			Resource           struct {
				Type string  `json:"type"`
				Key  string  `json:"key"`
				Org  *string `json:"organization_id"`
			} `json:"resource"`
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		var org any
		if body.Resource.Org != nil {
			org = *body.Resource.Org
		}
		allowed, reason := mode == "allow", "role_lacks_action"
		if allowed {
			reason = "allowed_by_role"
		}
		if mode == "evaluate" {
			p.mu.Lock()
			snapshot, known := p.callers[body.PrincipalReference]
			p.mu.Unlock()
			if !known {
				http.Error(w, "unknown caller", http.StatusInternalServerError)
				return
			}
			orgID := ""
			if body.Resource.Org != nil {
				orgID = *body.Resource.Org
			}
			request := authorization.Request{Action: body.Action, ResourceKind: body.Resource.Type, OrganizationID: orgID}
			if body.Resource.Type == "repository" {
				request.ResourceKey = body.Resource.Key
			}
			d := authorization.NewEvaluator().Evaluate(request, snapshot)
			allowed, reason = d.Allowed, string(d.Reason)
			p.mu.Lock()
			p.reasons = append(p.reasons, d.Reason)
			p.mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schema_version": "authorization_decision.v1", "decision_id": "dec_" + strings.Repeat("A", 26), "allowed": allowed,
			"reason_code": reason, "principal_id": "prn_TESTPRINCIPAL", "organization_id": org, "policy_revision": 1,
			"expires_at": time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z"),
		})
	}))
	t.Cleanup(p.srv.Close)
	client, err := pep.New(pep.Config{BaseURL: p.srv.URL, AllowPlainHTTP: true, HTTPClient: p.srv.Client(), ClientService: goAPIAccessService, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	p.client = client
	return p
}

func (p *cellPDP) Decide(ctx context.Context, req pep.Request) pep.Decision {
	p.mu.Lock()
	p.seen = append(p.seen, req)
	p.mu.Unlock()
	return p.client.Decide(ctx, req)
}

func (p *cellPDP) DecideBatch(ctx context.Context, reqs []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(reqs))
	for i, req := range reqs {
		out[i] = p.Decide(ctx, req)
	}
	return out
}

// reset sets the answer and forgets the calls.
func (p *cellPDP) reset(mode string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode, p.seen, p.reasons = mode, nil, nil
}

func (p *cellPDP) calls() []pep.Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]pep.Request(nil), p.seen...)
}

// adminConfigServer is the api listener over the real go-api routes, with the EdDSA user-token path, the probe
// store's users and memberships, and enforcer (nil = none) on the guard. Pool and ClickHouse dial a closed port,
// so a handler that is reached fails at its first read and never at the guard.
func adminConfigServer(t *testing.T, enforcer func(*testing.T) *policy.Enforcer) (http.Handler, *eddsaMinter) {
	t.Helper()
	return adminConfigServerWith(t, enforcer, nil)
}

// adminConfigServerWith is adminConfigServer over a probe store that edit has changed first.
func adminConfigServerWith(t *testing.T, enforcer func(*testing.T) *policy.Enforcer, edit func(*probeStore)) (http.Handler, *eddsaMinter) {
	t.Helper()
	return adminConfigServerOver(t, enforcer, edit, func(_ *testing.T, deps Deps) []httpapi.Route { return Routes(deps, quietLogger()) })
}

// adminConfigServerOver is adminConfigServerWith over the routes build returns: the same authenticator, routed
// access verify, Enforcer and scope layers, whatever the routes are.
func adminConfigServerOver(t *testing.T, enforcer func(*testing.T) *policy.Enforcer, edit func(*probeStore), build func(*testing.T, Deps) []httpapi.Route) (http.Handler, *eddsaMinter) {
	t.Helper()
	return adminConfigServerScoped(t, enforcer, edit, build, nil)
}

// adminConfigServerScoped is adminConfigServerOver with scoped (nil = none) applied to the scope before it serves.
func adminConfigServerScoped(t *testing.T, enforcer func(*testing.T) *policy.Enforcer, edit func(*probeStore), build func(*testing.T, Deps) []httpapi.Route, scoped func(*policy.Scope)) (http.Handler, *eddsaMinter) {
	t.Helper()
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	if edit != nil {
		edit(store)
	}
	auth, err := policy.NewAuthenticator(store, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	delegated, err := goAPIDelegatedRows()
	if err != nil {
		t.Fatal(err)
	}
	auth.DelegatedRows(delegated)
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows: realRows(t), EdDSA: eddsa,
		Meter:  sdkmetric.NewMeterProvider().Meter("test"),
		Logger: quietLogger(), Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(routedAccessVerify{validator}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	guard := policy.NewGuard(auth, quietLogger())
	if enforcer != nil {
		guard.SetEnforcer(enforcer(t))
	}
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: guard}
	scope := policy.NewScope(auth, quietLogger())
	if scoped != nil {
		scoped(scope)
	}
	server, err := NewServer(apiTestConfig(), quietLogger(), build(t, deps), scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler(), minter
}

// principalEcho answers 200 with exactly what an admin-config handler acts on: the guarded user, the effective org, the
// role, the admin and admin.orgs answers, the superuser flag and the impersonation context.
func principalEcho(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{}
	if user := policy.UserFrom(r.Context()); user != nil {
		out["user_id"] = user.UserID
		out["org"] = policy.EffectiveOrgID(r.Context(), user)
		out["org_id"] = user.OrgID
		out["role"] = user.Role
		out["is_admin"] = user.IsAdmin()
		out["holds_admin_orgs"] = user.Holds(policy.ActionAdminOrgs)
		out["is_superuser"] = user.IsSuperuser
	}
	if session := policy.ImpersonationFrom(r.Context()); session != nil {
		out["impersonating"] = session.TargetUserID.String()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// adminConfigEchoRoutes mounts the admin-config rows with the principal echo behind the guard level each row's
// registration names in the source (scanRegistrations, the same scan the profile gate uses), over the mounted
// route table. The rows come from the table, never from a typed list.
func adminConfigEchoRoutes(t *testing.T, deps Deps) []httpapi.Route {
	t.Helper()
	regs := scanRegistrations(t, "../api", ".")
	echo := http.HandlerFunc(principalEcho)
	var routes []httpapi.Route
	for _, m := range mountedRoutes(t) {
		if _, ok := adminConfigActions[m.Method+" "+m.Pattern]; !ok || m.Listener != "api" {
			continue
		}
		reg, ok := registrationFor(regs, m)
		if !ok {
			t.Fatalf("%s: no registration found in the source", m.key())
		}
		level := reg.Level
		if level == "" {
			level = dispatchedLevels[m.key()]
		}
		var authz policy.Authz
		switch level {
		case "Admin":
			authz = policy.Admin
		case "AdminOrg":
			authz = policy.AdminOrg
		default:
			t.Fatalf("%s: guard level %q, the echo wraps Admin and AdminOrg only", m.key(), level)
		}
		routes = append(routes, httpapi.Route{Method: m.Method, Pattern: m.Pattern, Handler: deps.Guard.Wrap(authz, echo)})
	}
	return routes
}

// adminConfigEchoServer is adminConfigServerWith with every row's handler replaced by the principal echo.
func adminConfigEchoServer(t *testing.T, enforcer func(*testing.T) *policy.Enforcer, edit func(*probeStore)) (http.Handler, *eddsaMinter) {
	t.Helper()
	return adminConfigEchoServerScoped(t, enforcer, edit, nil)
}

// adminConfigEchoServerScoped is adminConfigEchoServer with scoped applied to the scope (adminConfigServerScoped).
func adminConfigEchoServerScoped(t *testing.T, enforcer func(*testing.T) *policy.Enforcer, edit func(*probeStore), scoped func(*policy.Scope)) (http.Handler, *eddsaMinter) {
	t.Helper()
	routes := 0
	handler, minter := adminConfigServerScoped(t, enforcer, edit, func(t *testing.T, deps Deps) []httpapi.Route {
		built := adminConfigEchoRoutes(t, deps)
		routes = len(built)
		got := make([]string, 0, len(built))
		for _, r := range built {
			got = append(got, r.Method+" "+r.Pattern)
		}
		sort.Strings(got)
		if want := sortedAdminConfigKeys(); !slices.Equal(got, want) {
			t.Fatalf("echo routes %d, want the %d rows of sortedAdminConfigKeys\ngot  %v\nwant %v", len(got), len(want), got, want)
		}
		return built
	}, scoped)
	if routes != 61 {
		t.Fatalf("%d echo routes, want 61", routes)
	}
	return handler, minter
}

func newCellEnforcer(t *testing.T, decider pep.Decider, namespaces string, unconfigured bool) *policy.Enforcer {
	t.Helper()
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	cfg := policy.EnforceConfig{Rows: rows, Switch: authzenforce.Parse(authzenforce.GoAPI, namespaces), Timeout: 2 * time.Second, Logger: quietLogger()}
	if unconfigured {
		cfg.Unconfigured = true
	} else {
		cfg.Decider = decider
	}
	enf, err := policy.NewEnforcer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return enf
}

var cellRouteParam = regexp.MustCompile(`\{([a-z_]+)\}`)

// cellPath fills the route's parameters with values each handler can parse. POST /api/v1/admin/teams/{team_id}
// serves only the fixed mode "import" (teamsidentity postTeamRoute); any other value is a 405 before the guard.
func cellPath(key string) string {
	if key == "POST /api/v1/admin/teams/{team_id}" {
		return "/api/v1/admin/teams/import"
	}
	_, route, _ := strings.Cut(key, " ")
	return cellRouteParam.ReplaceAllStringFunc(route, func(param string) string {
		switch param {
		case "{category}":
			return "general"
		case "{key}":
			return "k"
		case "{team_id}":
			return "team-a"
		}
		return "00000000-0000-4000-8000-0000000000e1"
	})
}

type cellAnswer struct {
	status int
	body   string
	header http.Header
}

func cellDo(handler http.Handler, key, token string, headers map[string]string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	var body io.Reader
	if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
		body = strings.NewReader("{}")
	}
	request := httptest.NewRequest(method, cellPath(key), body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

func sortedAdminConfigKeys() []string {
	keys := make([]string, 0, len(adminConfigActions))
	for key := range adminConfigActions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

const adminRequiredBody = `{"detail":"Admin access required"}`

func memberToken(minter *eddsaMinter) string {
	return minter.mint(probeMember, map[string]any{"sid": sessionid.Wire(probeOtherSID)})
}

// isGuardRefusal reports a refusal the guard or the Enforcer writes.
func isGuardRefusal(a cellAnswer) bool {
	return a.status == http.StatusUnauthorized || a.body == adminRequiredBody ||
		strings.Contains(a.body, "Authorization service temporarily unavailable") ||
		strings.Contains(a.body, "Organization context required")
}

// With every 6.8 cell off, a member gets the legacy refusal on each row and the decision point is never asked.
func TestAdminConfigCellsOffKeepTheLegacyAnswer(t *testing.T) {
	pdp := newCellPDP(t)
	for name, enforcer := range map[string]func(*testing.T) *policy.Enforcer{
		"no enforcer": nil,
		"other namespaces on": func(t *testing.T) *policy.Enforcer {
			return newCellEnforcer(t, pdp, "billing,members,org,admin", false)
		},
	} {
		t.Run(name, func(t *testing.T) {
			handler, minter := adminConfigServer(t, enforcer)
			pdp.reset("allow")
			for _, key := range sortedAdminConfigKeys() {
				if got := cellDo(handler, key, memberToken(minter), nil); got.status != http.StatusForbidden || got.body != adminRequiredBody {
					t.Errorf("%s: %d %s, want the legacy 403", key, got.status, got.body)
				}
			}
			if calls := pdp.calls(); len(calls) != 0 {
				t.Errorf("%d decision point calls with the cells off: %+v", len(calls), calls)
			}
		})
	}
}

// With the 6.8 cells on, a row with an action is decided by the decision point: its deny is the legacy 403 body,
// its allow lets a member through to the handler, and its failure is a 503. The request names the row's action and
// the token's organization. A row with no action keeps the legacy answer and is never asked.
func TestAdminConfigCellsOnDecideEachRow(t *testing.T) {
	pdp := newCellPDP(t)
	handler, minter := adminConfigServer(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, adminConfigNamespaces, false)
	})
	for _, key := range sortedAdminConfigKeys() {
		action := adminConfigActions[key]
		t.Run(key, func(t *testing.T) {
			for _, mode := range []string{"deny", "allow", "fail"} {
				pdp.reset(mode)
				got := cellDo(handler, key, memberToken(minter), nil)
				calls := pdp.calls()
				if action == "" {
					if got.status != http.StatusForbidden || got.body != adminRequiredBody || len(calls) != 0 {
						t.Errorf("%s: %d %s with %d calls, want the legacy 403 and no call", mode, got.status, got.body, len(calls))
					}
					continue
				}
				if len(calls) != 1 || calls[0].Action != action || calls[0].ResourceType != "organization" ||
					calls[0].OrganizationID != probeOrg.String() || calls[0].ResourceKey != probeOrg.String() {
					t.Fatalf("%s: calls %+v, want one %s call on organization %s", mode, calls, action, probeOrg)
				}
				switch mode {
				case "deny":
					if got.status != http.StatusForbidden || got.body != adminRequiredBody {
						t.Errorf("deny: %d %s, want the legacy 403 body", got.status, got.body)
					}
				case "allow":
					if isGuardRefusal(got) {
						t.Errorf("allow: %d %s, want the handler's answer", got.status, got.body)
					}
				case "fail":
					if got.status != http.StatusServiceUnavailable || got.header.Get("Retry-After") != "5" {
						t.Errorf("fail: %d %s Retry-After %q, want 503 and 5", got.status, got.body, got.header.Get("Retry-After"))
					}
				}
			}
		})
	}
}

// X-Org-Id selects only: with a member org in the header the decision point is still asked about the token's org.
func TestAdminConfigCellsAskAboutTheTokenOrgNotTheHeader(t *testing.T) {
	pdp := newCellPDP(t)
	handler, minter := adminConfigServer(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, adminConfigNamespaces, false)
	})
	admin := minter.mint(probeUser, nil)
	for _, key := range sortedAdminConfigKeys() {
		if adminConfigActions[key] == "" {
			continue
		}
		pdp.reset("deny")
		got := cellDo(handler, key, admin, map[string]string{"X-Org-Id": probeOtherOrg.String()})
		calls := pdp.calls()
		if len(calls) != 1 || calls[0].OrganizationID != probeOrg.String() {
			t.Errorf("%s: calls %+v, want one call on the token org %s", key, calls, probeOrg)
		}
		if got.status != http.StatusForbidden || got.body != adminRequiredBody {
			t.Errorf("%s: %d %s, want the deny", key, got.status, got.body)
		}
	}
}

// A cell switched on with no decision point client refuses each row that has an action with a 503 and the reason
// code, and keeps the legacy answer on a row with none.
func TestAdminConfigCellsOnWithoutAClientFailClosed(t *testing.T) {
	handler, minter := adminConfigServer(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, nil, adminConfigNamespaces, true)
	})
	admin := minter.mint(probeUser, nil)
	for _, key := range sortedAdminConfigKeys() {
		if adminConfigActions[key] == "" {
			if got := cellDo(handler, key, memberToken(minter), nil); got.status != http.StatusForbidden || got.body != adminRequiredBody {
				t.Errorf("%s: %d %s, want the legacy 403", key, got.status, got.body)
			}
			continue
		}
		got := cellDo(handler, key, admin, nil)
		if got.status != http.StatusServiceUnavailable || !strings.Contains(got.body, `"reason_code":"authz_enforce_no_decision_point"`) {
			t.Errorf("%s: %d %s, want the 503 with the reason code", key, got.status, got.body)
		}
	}
}

// cellSnapshot is the state the evaluate mode decides over: the caller's platform role and its role in the token org.
type cellSnapshot struct {
	platformRole string
	orgRole      string // "" = no membership row
}

func (cellSnapshot) PrincipalStatus() authorization.Status { return authorization.StatusActive }
func (c cellSnapshot) PlatformRole() string                { return c.platformRole }
func (cellSnapshot) OrganizationStatus(string) authorization.Status {
	return authorization.StatusActive
}
func (c cellSnapshot) Membership(org string) (authorization.Membership, bool) {
	if c.orgRole == "" {
		return authorization.Membership{}, false
	}
	return authorization.Membership{OrgID: org, Role: c.orgRole, Status: authorization.StatusActive}, true
}

var (
	cellPlatformMember = uuid.MustParse("00000000-0000-4000-8000-0000000000f1")
	cellPlatformNoRow  = uuid.MustParse("00000000-0000-4000-8000-0000000000f2")
	cellPlatformSID    = uuid.MustParse("00000000-0000-4000-8000-0000000000f3")
	cellNoRowSID       = uuid.MustParse("00000000-0000-4000-8000-0000000000f4")
)

// withPlatformAdmins adds two platform admins (users.is_superuser): one whose org role is member, one with no
// membership row in the token org.
func withPlatformAdmins(store *probeStore) {
	store.users[cellPlatformMember] = probeUserRow{state: policy.UserState{IsActive: true, IsSuperuser: true}, email: "pm@example.test"}
	store.users[cellPlatformNoRow] = probeUserRow{state: policy.UserState{IsActive: true, IsSuperuser: true}, email: "pn@example.test"}
	store.members[[2]uuid.UUID{cellPlatformMember, probeOrg}] = "member"
	store.sessions[cellPlatformSID] = policy.SessionRow{PrincipalID: cellPlatformMember, LegacyTokenVersion: i64ptr(0)}
	store.sessions[cellNoRowSID] = policy.SessionRow{PrincipalID: cellPlatformNoRow, LegacyTokenVersion: i64ptr(0)}
}

type cellCaller struct {
	name  string
	token string
	snap  cellSnapshot
}

func cellCallers(minter *eddsaMinter) map[string]cellCaller {
	mk := func(name string, user uuid.UUID, sid uuid.UUID, snap cellSnapshot) cellCaller {
		return cellCaller{name: name, token: minter.mint(user, map[string]any{"sid": sessionid.Wire(sid)}), snap: snap}
	}
	return map[string]cellCaller{
		"org admin":                      mk("org admin", probeUser, probeLiveSID, cellSnapshot{orgRole: "admin"}),
		"platform admin + org member":    mk("platform admin + org member", cellPlatformMember, cellPlatformSID, cellSnapshot{platformRole: "platform_admin", orgRole: "member"}),
		"platform admin + no membership": mk("platform admin + no membership", cellPlatformNoRow, cellNoRowSID, cellSnapshot{platformRole: "platform_admin"}),
		"org member":                     mk("org member", probeMember, probeOtherSID, cellSnapshot{orgRole: "member"}),
	}
}

// register lets the evaluate mode decide for the callers' tokens.
func (p *cellPDP) register(callers map[string]cellCaller) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.callers = map[string]cellSnapshot{}
	for _, c := range callers {
		p.callers[c.token] = c.snap
	}
}

func (p *cellPDP) reasonsSeen() []authorization.Reason {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]authorization.Reason(nil), p.reasons...)
}

// With the 6.8 cells on and the real evaluator behind the decision point, on every row of the table: an org admin
// and a platform admin with an org role of member are let through to the handler (the platform admin by
// allowed_by_platform_admin); an org member without admin.orgs is refused with the legacy 403 body. member holds
// settings.read and teams.read only, so it holds none of the four actions the table asks (integrations.read,
// integrations.write, settings.write, teams.write).
func TestAdminConfigCellsOnPlatformAdminIsDecidedByTheEvaluator(t *testing.T) {
	pdp := newCellPDP(t)
	handler, minter := adminConfigServerWith(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, adminConfigNamespaces, false)
	}, withPlatformAdmins)
	callers := cellCallers(minter)
	pdp.register(callers)
	rows := 0
	for _, key := range sortedAdminConfigKeys() {
		action := adminConfigActions[key]
		rows++
		for _, name := range []string{"org admin", "platform admin + org member", "org member"} {
			c := callers[name]
			pdp.reset("evaluate")
			got := cellDo(handler, key, c.token, nil)
			calls := pdp.calls()
			if len(calls) != 1 || calls[0].Action != action || calls[0].OrganizationID != probeOrg.String() {
				t.Fatalf("%s / %s: calls %+v, want one %s call on %s", key, name, calls, action, probeOrg)
			}
			if name == "org member" {
				if got.status != http.StatusForbidden || got.body != adminRequiredBody {
					t.Errorf("%s / %s: %d %s, want the 403 %s", key, name, got.status, got.body, adminRequiredBody)
				}
				continue
			}
			if isGuardRefusal(got) {
				t.Errorf("%s / %s: %d %s, want the handler's answer", key, name, got.status, got.body)
			}
			want := authorization.ReasonAllowedByRole
			if name == "platform admin + org member" {
				want = authorization.ReasonAllowedByPlatformAdmin
			}
			if r := pdp.reasonsSeen(); len(r) != 1 || r[0] != want {
				t.Errorf("%s / %s: reasons %v, want %s", key, name, r, want)
			}
		}
	}
	if rows != 61 {
		t.Errorf("%d rows, want 61", rows)
	}
}

// A platform admin with no membership row in the token org reaches the handler on every row, on or off (D6773).
// Before CHAOS-13126 this caller came on the HS256 token, which no row accepts now; the yardstick is the answer main
// f496ee5d gave that HS256 caller (handler, never a guard refusal), now asked of the same caller's user access token.
// Off, the decision point is not asked; on, the Enforcer judges the user access token once per row.
func TestAdminConfigCellsOnPlatformAdminWithoutMembershipRow(t *testing.T) {
	pdp := newCellPDP(t)
	for _, on := range []bool{false, true} {
		var enforcer func(*testing.T) *policy.Enforcer
		if on {
			enforcer = func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, adminConfigNamespaces, false) }
		}
		handler, minter := adminConfigServerWith(t, enforcer, withPlatformAdmins)
		callers := cellCallers(minter)
		pdp.register(callers)
		token := callers["platform admin + no membership"].token
		for _, key := range sortedAdminConfigKeys() {
			pdp.reset("evaluate")
			got := cellDo(handler, key, token, nil)
			if isGuardRefusal(got) {
				t.Errorf("on=%v %s user access token: %d %s, want the handler's answer", on, key, got.status, got.body)
			}
			want := 0
			if on {
				want = 1
			}
			if calls := pdp.calls(); len(calls) != want {
				t.Errorf("on=%v %s: %d decision point calls, want %d", on, key, len(calls), want)
			}
		}
	}
}

// D6773 (chris 2026-11-06, 13:12Z): a platform admin never needs a membership row, and every token class gives the
// same answer as the legacy path today, not stricter and not wider. With a user access token and no membership row
// in the token org, each row must answer exactly what the legacy path answered that caller on main f496ee5d (the
// HS256 token, retired by CHAOS-13126): the 200 principal echo, on or off.
func TestAdminConfigCellsPlatformAdminWithoutMembershipRowAnswersAsLegacy(t *testing.T) {
	echoParityWithLegacy(t, cellPlatformNoRow, "platform admin + no membership")
}

// The control for the cell above: the platform admin WITH an org role of member gets the echo the legacy path gave it
// on main f496ee5d, on or off, so the echo compare is not vacuous.
func TestAdminConfigCellsPlatformAdminWithMembershipRowEchoesAsLegacy(t *testing.T) {
	echoParityWithLegacy(t, cellPlatformMember, "platform admin + org member")
}

// legacyEcho is the principal echo the HS256 token of user (org claim probeOrg, role member) got on every admin-config
// row on main f496ee5d, on or off: the yardstick recorded before CHAOS-13126 retired that token class.
func legacyEcho(user uuid.UUID) string {
	return `{"holds_admin_orgs":true,"is_admin":true,"is_superuser":true,"org":"` + probeOrg.String() + `","org_id":"` +
		probeOrg.String() + `","role":"member","user_id":"` + user.String() + `"}` + "\n"
}

// echoParityWithLegacy compares, on every row and on or off, the answer of the caller's user access token with the
// legacy path's answer to the same caller (legacyEcho), over the principal echo behind the production guard.
func echoParityWithLegacy(t *testing.T, user uuid.UUID, caller string) {
	t.Helper()
	pdp := newCellPDP(t)
	want := legacyEcho(user)
	for _, on := range []bool{false, true} {
		var enforcer func(*testing.T) *policy.Enforcer
		if on {
			enforcer = func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, adminConfigNamespaces, false) }
		}
		handler, minter := adminConfigEchoServer(t, enforcer, withPlatformAdmins)
		callers := cellCallers(minter)
		pdp.register(callers)
		rows := 0
		for _, key := range sortedAdminConfigKeys() {
			rows++
			pdp.reset("evaluate")
			got := cellDo(handler, key, callers[caller].token, nil)
			if got.status != http.StatusOK || got.body != want {
				t.Errorf("on=%v %s: %d %s, want the legacy answer 200 %s (D6773)", on, key, got.status, got.body, want)
			}
		}
		if rows != 61 {
			t.Errorf("on=%v: %d rows, want 61", on, rows)
		}
	}
}

// CHAOS-13126: ops_access_token_hs256 is retired. The HS256 token of the platform admin with no membership row, the
// caller the cells above took on before, is the generic 401 on every admin-config row, on or off, and the decision
// point is not asked.
func TestAdminConfigCellsRefuseTheRetiredHS256Token(t *testing.T) {
	pdp := newCellPDP(t)
	for _, on := range []bool{false, true} {
		var enforcer func(*testing.T) *policy.Enforcer
		if on {
			enforcer = func(t *testing.T) *policy.Enforcer { return newCellEnforcer(t, pdp, adminConfigNamespaces, false) }
		}
		handler, minter := adminConfigServerWith(t, enforcer, withPlatformAdmins)
		pdp.register(cellCallers(minter))
		hs256 := probeHS256(t, probeKey, map[string]any{"sub": cellPlatformNoRow.String(), "org_id": probeOrg.String(), "role": "member", "tv": 0})
		rows := 0
		for _, key := range sortedAdminConfigKeys() {
			rows++
			pdp.reset("evaluate")
			got := cellDo(handler, key, hs256, nil)
			if got.status != http.StatusUnauthorized || strings.TrimSpace(got.body) != genericRefusal {
				t.Errorf("on=%v %s HS256 token: %d %s, want 401 %s", on, key, got.status, got.body, genericRefusal)
			}
			if calls := pdp.calls(); len(calls) != 0 {
				t.Errorf("on=%v %s: %d decision point calls, want 0", on, key, len(calls))
			}
		}
		if rows != 61 {
			t.Errorf("on=%v: %d rows, want 61", on, rows)
		}
	}
}

// With the 6.8 cells off, the answer to each caller on each row is the same with no Enforcer and with an Enforcer
// whose namespaces are other ones, and it is the legacy answer: the org admin, the platform admin with an org role
// of member and the platform admin with no membership row (D6773) reach the handler, the org member gets the 403.
// The no-membership platform admin's own cell, on the HS256 token on main f496ee5d (handler answer, byte-equal on
// both servers), now runs on that caller's user access token (CHAOS-13126 retired the HS256 class). The decision
// point is never asked.
func TestAdminConfigCellsOffPlatformAdminKeepsTheLegacyAnswer(t *testing.T) {
	pdp := newCellPDP(t)
	plain, plainMinter := adminConfigServerWith(t, nil, withPlatformAdmins)
	other, otherMinter := adminConfigServerWith(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, "billing,members,org,admin", false)
	}, withPlatformAdmins)
	plainCallers, otherCallers := cellCallers(plainMinter), cellCallers(otherMinter)
	pdp.register(otherCallers)
	pdp.reset("evaluate")
	legacyAnswer := map[string]func(cellAnswer) bool{
		"org admin":                      func(a cellAnswer) bool { return !isGuardRefusal(a) },
		"platform admin + org member":    func(a cellAnswer) bool { return !isGuardRefusal(a) },
		"platform admin + no membership": func(a cellAnswer) bool { return !isGuardRefusal(a) },
		"org member":                     func(a cellAnswer) bool { return a.status == http.StatusForbidden && a.body == adminRequiredBody },
	}
	rows := 0
	for _, key := range sortedAdminConfigKeys() {
		rows++
		for name, ok := range legacyAnswer {
			a := cellDo(plain, key, plainCallers[name].token, nil)
			b := cellDo(other, key, otherCallers[name].token, nil)
			if a.status != b.status || a.body != b.body {
				t.Errorf("%s / %s: no enforcer %d %s, other namespaces %d %s, want byte-equal", key, name, a.status, a.body, b.status, b.body)
			}
			if !ok(a) {
				t.Errorf("%s / %s: %d %s is not the legacy answer", key, name, a.status, a.body)
			}
		}
		a := cellDo(plain, key, plainCallers["platform admin + no membership"].token, nil)
		b := cellDo(other, key, otherCallers["platform admin + no membership"].token, nil)
		if a.status != b.status || a.body != b.body || isGuardRefusal(a) {
			t.Errorf("%s / no-membership platform admin, user access token: %d %s vs %d %s, want byte-equal handler answers", key, a.status, a.body, b.status, b.body)
		}
	}
	if rows != 61 {
		t.Errorf("%d rows, want 61", rows)
	}
	if calls := pdp.calls(); len(calls) != 0 {
		t.Errorf("%d decision point calls with the cells off", len(calls))
	}
}
