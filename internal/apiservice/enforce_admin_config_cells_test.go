package apiservice

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-go/pep"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// adminConfigRoute selects the settings, integrations, sync and teams rows (leaf 6.8) of the go-api profile.
var adminConfigRoute = regexp.MustCompile(`^/api/v1/admin/(settings|integrations|sync-configs|sync-runs|sync-targets|backfill-jobs|teams)(/|$)`)

// adminConfigActions is today's action of every 6.8 row ("" = no action, so the Enforcer never asks for it). A
// profile change to any of these rows must change this table in the same PR.
var adminConfigActions = map[string]string{
	"GET /api/v1/admin/backfill-jobs":                                                  "",
	"GET /api/v1/admin/backfill-jobs/{job_id}":                                         "",
	"GET /api/v1/admin/integrations":                                                   "integrations.read",
	"POST /api/v1/admin/integrations":                                                  "integrations.write",
	"POST /api/v1/admin/integrations/github/install-callback":                          "integrations.write",
	"POST /api/v1/admin/integrations/github/install-url":                               "",
	"POST /api/v1/admin/integrations/pagerduty/api-token":                              "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/authorize":                              "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/callback":                               "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/client-credentials":                     "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/disconnect":                             "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/preflight":                              "",
	"GET /api/v1/admin/integrations/pagerduty/services":                                "integrations.read",
	"GET /api/v1/admin/integrations/pagerduty/status":                                  "integrations.read",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings":                       "integrations.write",
	"GET /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}":           "integrations.read",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/activate": "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/revoke":   "integrations.write",
	"POST /api/v1/admin/integrations/pagerduty/webhook-bindings/{binding_id}/rotate":   "integrations.write",
	"GET /api/v1/admin/integrations/{integration_id}":                                  "integrations.read",
	"PATCH /api/v1/admin/integrations/{integration_id}":                                "integrations.write",
	"POST /api/v1/admin/integrations/{integration_id}/backfill":                        "",
	"GET /api/v1/admin/integrations/{integration_id}/datasets":                         "integrations.read",
	"PATCH /api/v1/admin/integrations/{integration_id}/datasets":                       "integrations.write",
	"POST /api/v1/admin/integrations/{integration_id}/discover":                        "",
	"GET /api/v1/admin/integrations/{integration_id}/sources":                          "integrations.read",
	"PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}":            "integrations.write",
	"POST /api/v1/admin/integrations/{integration_id}/sync":                            "",
	"POST /api/v1/admin/settings":                                                      "settings.write",
	"GET /api/v1/admin/settings/{category}":                                            "settings.read",
	"DELETE /api/v1/admin/settings/{category}/{key}":                                   "settings.write",
	"GET /api/v1/admin/settings/{category}/{key}":                                      "settings.read",
	"PUT /api/v1/admin/settings/{category}/{key}":                                      "settings.write",
	"GET /api/v1/admin/sync-configs":                                                   "",
	"POST /api/v1/admin/sync-configs":                                                  "",
	"GET /api/v1/admin/sync-configs/auto-import-capabilities":                          "",
	"POST /api/v1/admin/sync-configs/batch":                                            "",
	"DELETE /api/v1/admin/sync-configs/{config_id}":                                    "",
	"GET /api/v1/admin/sync-configs/{config_id}":                                       "",
	"PATCH /api/v1/admin/sync-configs/{config_id}":                                     "",
	"POST /api/v1/admin/sync-configs/{config_id}/backfill":                             "",
	"GET /api/v1/admin/sync-configs/{config_id}/coverage":                              "",
	"GET /api/v1/admin/sync-configs/{config_id}/jobs":                                  "",
	"GET /api/v1/admin/sync-configs/{config_id}/repositories":                          "",
	"PUT /api/v1/admin/sync-configs/{config_id}/repositories":                          "",
	"POST /api/v1/admin/sync-configs/{config_id}/trigger":                              "",
	"GET /api/v1/admin/sync-runs/{run_id}":                                             "",
	"GET /api/v1/admin/sync-runs/{run_id}/units":                                       "",
	"GET /api/v1/admin/sync-targets":                                                   "",
	"GET /api/v1/admin/teams":                                                          "teams.read",
	"POST /api/v1/admin/teams":                                                         "teams.write",
	"DELETE /api/v1/admin/teams/{team_id}":                                             "teams.write",
	"GET /api/v1/admin/teams/{team_id}":                                                "teams.read",
	"PATCH /api/v1/admin/teams/{team_id}":                                              "teams.write",
	"POST /api/v1/admin/teams/{team_id}":                                               "",
	"POST /api/v1/admin/teams/{team_id}/approve-changes":                               "",
	"POST /api/v1/admin/teams/{team_id}/confirm-inferred-members":                      "",
	"POST /api/v1/admin/teams/{team_id}/confirm-members":                               "",
	"GET /api/v1/admin/teams/{team_id}/discover-members":                               "",
	"POST /api/v1/admin/teams/{team_id}/dismiss-changes":                               "",
	"GET /api/v1/admin/teams/{team_id}/infer-members":                                  "",
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
	if len(adminConfigActions) != 61 || unmapped != 30 {
		t.Errorf("table has %d rows, %d without an action; want 61 and 30", len(adminConfigActions), unmapped)
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
	mode   string // allow, deny, fail
	seen   []pep.Request
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
			Resource struct {
				Org *string `json:"organization_id"`
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
	p.mode, p.seen = mode, nil
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
	minter := newEdDSAMinter(t)
	store := newProbeStore()
	verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, store, quietLogger())
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
		Rows: realRows(t), EdDSA: eddsa, Legacy: legacyVerifier(auth.AuthenticateLegacy),
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
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: guard, Verifier: verifier}
	scope := policy.NewScope(auth, quietLogger())
	server, err := NewServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()), scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	return server.Handler(), minter
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
