package authapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The routes are built only from a binary whose embedded endpoint-profile
// document has a row, accepting the session issuer credential, for each of
// them.
func TestProvisioningRoutesNeedTheirProfileRows(t *testing.T) {
	if err := provisioningRowsPresent(authv1.AuthServiceEndpointProfiles); err != nil {
		t.Fatalf("the embedded document: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	plant := func(edit func(row map[string]any) (map[string]any, bool)) []byte {
		t.Helper()
		var kept []any
		for _, item := range rows {
			row := map[string]any{}
			for k, v := range item.(map[string]any) {
				row[k] = v
			}
			if changed, keep := edit(row); keep {
				kept = append(kept, changed)
			}
		}
		out := map[string]any{}
		for k, v := range document {
			out[k] = v
		}
		out["rows"] = kept
		raw, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for _, pattern := range provisioningPatterns {
		for name, edit := range map[string]func(map[string]any) (map[string]any, bool){
			"row removed": func(row map[string]any) (map[string]any, bool) { return row, row["route"] != pattern },
			"class removed": func(row map[string]any) (map[string]any, bool) {
				if row["route"] == pattern {
					row["accepted_credential_classes"] = []any{"workload_token"}
				}
				return row, true
			},
			"another service": func(row map[string]any) (map[string]any, bool) {
				if row["route"] == pattern {
					row["service"] = "dev-health-ops-api"
				}
				return row, true
			},
		} {
			err := provisioningRowsPresent(plant(edit))
			if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+pattern+" ") {
				t.Fatalf("%s, %s: %v, want a refusal naming the route", pattern, name, err)
			}
		}
	}
	if err := provisioningRowsPresent([]byte("{")); !errors.Is(err, errProvisioningRows) {
		t.Fatalf("an unreadable document: %v, want a refusal", err)
	}
	// ProvisioningRoutes runs the check: a planted document without one route's
	// row builds no route at all.
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate,
		ProfileRows: plant(func(row map[string]any) (map[string]any, bool) { return row, row["route"] != PatternInviteAccept })})
	if !errors.Is(err, errProvisioningRows) || routes != nil {
		t.Fatalf("ProvisioningRoutes over a document without a row: %d routes, %v; want none and a refusal", len(routes), err)
	}
}

// The provisioning routes accept only the session issuer credential holding
// memberships:write. Every kind of user token, as the Authorization bearer, is
// refused 401 invalid_credentials at the credential gate on every route: no
// credential-row lookup (counted on the real servicecred checker's pool), no
// actor or store read, no organization projection, no echo of the token in the
// body or the logs.
func TestProvisioningRoutesRefuseUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	pool, connects := usertokens.CountingPool(t)
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: servicecred.PG{Pool: pool}, Users: f.users, Reader: noRows{},
		Organizations: f.orgs, Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ProfileRows: docWithRow(t, PatternOrgSuspend),
		Logger: slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	for _, c := range routeCases() {
		body := encode(c.body(f, f.super))
		for _, tok := range tokens {
			request := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer "+tok.Value)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
				t.Errorf("%s %s: %d %s, want 401 %s", c.op, tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
			}
			usertokens.NoLeak(t, c.op+" "+tok.Kind+" body", response.Body.String(), tokens)
		}
	}
	for _, tok := range tokens {
		request := httptest.NewRequest(http.MethodPost, PatternOrgState, strings.NewReader(f.stateBody()))
		request.Header.Set("Authorization", "Bearer "+tok.Value)
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || !strings.Contains(response.Body.String(), reasonInvalidCredentials) {
			t.Errorf("%s %s: %d %s, want 401 %s", opOrgState, tok.Kind, response.Code, response.Body, reasonInvalidCredentials)
		}
		usertokens.NoLeak(t, opOrgState+" "+tok.Kind+" body", response.Body.String(), tokens)
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the credential table: %d connection attempt(s), want 0", got)
	}
	if f.storeCalls() != 0 || f.users.reads != 0 {
		t.Errorf("a user token reached the stores: %d calls, %d reads", f.storeCalls(), f.users.reads)
	}
	usertokens.NoLeak(t, "the logs", f.logs.String(), tokens)
}

// The reactivate route is one of the routes the startup gate covers: it is in provisioningPatterns, the embedded
// document holds its row, and the default build serves it (no planted document).
func TestTheReactivateRouteIsInTheStartupGate(t *testing.T) {
	if !slices.Contains(provisioningPatterns, PatternOrgReactivate) {
		t.Fatalf("provisioningPatterns %v does not hold %s", provisioningPatterns, PatternOrgReactivate)
	}
	if err := provisioningRowsPresent(authv1.AuthServiceEndpointProfiles); err != nil {
		t.Fatalf("the embedded document: %v", err)
	}
	f := newProvFixture(t)
	routes := 0
	for _, c := range routeCases() {
		if got := f.post(c.path, encode(c.body(f, f.super)), nil); got.Code != http.StatusOK {
			t.Fatalf("%s on the embedded document: %d %s, want 200", c.op, got.Code, got.Body)
		}
		routes++
	}
	if routes != 11 {
		t.Fatalf("%d provisioning routes served, want 11", routes)
	}
	for _, line := range strings.Split(f.logs.String(), "\n") {
		if strings.Contains(line, "not mounted") && !strings.Contains(line, PatternOrgState) {
			t.Fatalf("a not-mounted line was logged for a route other than the dark state route: %s", line)
		}
	}
}

// A document without the reactivate row refuses the whole switch (all nine routes), like any other missing row.
func TestAMissingReactivateRowRefusesTheWholeSwitch(t *testing.T) {
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		if item.(map[string]any)["route"] != PatternOrgReactivate {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	err = provisioningRowsPresent(raw)
	if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+PatternOrgReactivate+" ") {
		t.Fatalf("a document without the reactivate row: %v, want a refusal naming the route", err)
	}
}

// The rows gate needs the invite revoke route's row, named by its literal
// route (not through provisioningPatterns): without it the switch builds no
// route.
func TestTheRowsGateNeedsTheInviteRevokeRow(t *testing.T) {
	const route = "/internal/v1/invites/revoke"
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	found := false
	for _, item := range document["rows"].([]any) {
		if item.(map[string]any)["route"] == route {
			found = true
			continue
		}
		kept = append(kept, item)
	}
	if !found {
		t.Fatalf("the embedded document has no %s row", route)
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if err := provisioningRowsPresent(raw); !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+route+" ") {
		t.Fatalf("without the revoke row: %v, want a refusal naming the route", err)
	}
}

// A route mounts only on a row that lists every credential class its handler
// admits. With the go-api workload binding configured, the nine workload
// operations admit the workload token, so a row that lists only the session
// issuer class under-describes the route: ProvisioningRoutes refuses the whole
// set (the gated routes), and the state route is not mounted. Invite revoke and
// organization suspend never take a workload token, so a row without it is
// enough for them; without the binding no route admits it, and a row without it
// is enough for every route.
func TestProvisioningRoutesNeedRowsListingEveryAdmittedClass(t *testing.T) {
	issuerOnly := func(t *testing.T, route string) []byte {
		t.Helper()
		var document map[string]any
		if err := json.Unmarshal(docWithRow(t, PatternOrgSuspend), &document); err != nil {
			t.Fatal(err)
		}
		for _, item := range document["rows"].([]any) {
			row := item.(map[string]any)
			if row["route"] == route && row["service"] == AccessService {
				row["accepted_credential_classes"] = []any{serviceSessionIssuerClass}
			}
		}
		raw, err := json.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	build := func(rows []byte, workload *SessionIssueWorkload) ([]httpapi.Route, error) {
		f := newProvFixture(t)
		return ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
			Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ProfileRows: rows,
			Workload: workload, Logger: slog.New(slog.DiscardHandler)})
	}
	mounted := func(routes []httpapi.Route, pattern string) bool {
		return slices.ContainsFunc(routes, func(r httpapi.Route) bool { return r.Pattern == pattern })
	}
	workloadPatterns := map[string]bool{PatternMembershipAdd: true, PatternMembershipRole: true, PatternMembershipRemove: true,
		PatternMembershipTransfer: true, PatternInviteCreate: true, PatternInviteAccept: true, PatternOrgDeprovision: true,
		PatternOrgReactivate: true, PatternOrgState: true}
	for _, pattern := range append(slices.Clone(provisioningPatterns), PatternOrgState) {
		rows := issuerOnly(t, pattern)
		t.Run(pattern+" with the workload binding", func(t *testing.T) {
			routes, err := build(rows, &SessionIssueWorkload{})
			switch {
			case !workloadPatterns[pattern]:
				if err != nil || !mounted(routes, pattern) {
					t.Fatalf("a route that takes no workload token: %v, mounted %v; want mounted", err, mounted(routes, pattern))
				}
			case pattern == PatternOrgState:
				if err != nil || mounted(routes, pattern) {
					t.Fatalf("the state route on a row without workload_token: %v, mounted %v; want not mounted", err, mounted(routes, pattern))
				}
			default:
				if !errors.Is(err, errProvisioningRows) || routes != nil || !strings.Contains(err.Error(), "POST "+pattern+" ") {
					t.Fatalf("a row without workload_token: %d routes, %v; want none and a refusal naming the route", len(routes), err)
				}
			}
		})
		t.Run(pattern+" without the workload binding", func(t *testing.T) {
			if routes, err := build(rows, nil); err != nil || !mounted(routes, pattern) {
				t.Fatalf("no route admits a workload token: %v, mounted %v; want mounted", err, mounted(routes, pattern))
			}
		})
	}
}

// provisioningPatternOps decides which classes the startup gate requires, so
// each entry must be the operation the mounted handler really runs: a bearer
// that is no credential is refused on every route, and the refusal line names
// the handler's operation.
func TestProvisioningPatternOpsNameEachHandlersOperation(t *testing.T) {
	if len(provisioningPatternOps) != len(provisioningPatterns) {
		t.Fatalf("%d operations for %d routes", len(provisioningPatternOps), len(provisioningPatterns))
	}
	f := newProvFixture(t)
	for _, pattern := range provisioningPatterns {
		f.logs.Reset()
		if code := f.post(pattern, `{}`, map[string]string{"Authorization": "Bearer not-a-credential"}).Code; code != http.StatusUnauthorized {
			t.Fatalf("POST %s with a bearer that is no credential: %d, want 401", pattern, code)
		}
		want := `"operation":"` + provisioningPatternOps[pattern] + `"`
		if !strings.Contains(f.logs.String(), want) {
			t.Fatalf("POST %s: the refusal names no %s: %s", pattern, want, f.logs)
		}
	}
}

// A route with no entry in provisioningPatternOps fails the gate closed: the
// gate cannot know which classes the route admits.
func TestTheRowsGateRefusesARouteWithNoOperation(t *testing.T) {
	saved := provisioningPatternOps
	t.Cleanup(func() { provisioningPatternOps = saved })
	provisioningPatternOps = map[string]string{}
	for pattern, op := range saved {
		if pattern != PatternInviteAccept {
			provisioningPatternOps[pattern] = op
		}
	}
	err := provisioningRowsAccept(authv1.AuthServiceEndpointProfiles, false)
	if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+PatternInviteAccept+" names no operation") {
		t.Fatalf("a route with no operation: %v, want a refusal naming it", err)
	}
}
