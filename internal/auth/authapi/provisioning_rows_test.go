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
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The routes are built only from a binary whose embedded endpoint-profile
// document has a row, accepting the session issuer credential, for each of
// them.
func TestProvisioningRoutesNeedTheirProfileRows(t *testing.T) {
	t.Parallel()
	if err := provisioningRowsPresent(authv1.AuthServiceEndpointProfiles); err != nil {
		t.Fatalf("the embedded document: %v", err)
	}
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	plant := func(t *testing.T, edit func(row map[string]any) (map[string]any, bool)) []byte {
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
			t.Run(pattern+" "+name, func(t *testing.T) {
				t.Parallel()
				err := provisioningRowsPresent(plant(t, edit))
				if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+pattern+" ") {
					t.Fatalf("%s, %s: %v, want a refusal naming the route", pattern, name, err)
				}
			})
		}
	}
	if err := provisioningRowsPresent([]byte("{")); !errors.Is(err, errProvisioningRows) {
		t.Fatalf("an unreadable document: %v, want a refusal", err)
	}
	// ProvisioningRoutes runs the check: a planted document without one route's
	// row builds no route at all.
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants,
		ProfileRows: plant(t, func(row map[string]any) (map[string]any, bool) { return row, row["route"] != PatternInviteAccept })})
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
	t.Parallel()
	tokens := userTokensWithIssuer(t)
	pool, connects := usertokens.CountingPool(t)
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: servicecred.PG{Pool: pool}, Users: f.users, Reader: noRows{},
		Organizations: f.orgs, Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants, ProfileRows: docWithRow(t, PatternOrgSuspend),
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
	t.Parallel()
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
	if routes != 25 {
		t.Fatalf("%d provisioning routes served, want 25", routes)
	}
	for _, line := range strings.Split(f.logs.String(), "\n") {
		if strings.Contains(line, "not mounted") && !strings.Contains(line, PatternOrgState) && !strings.Contains(line, "/internal/v1/grants/") {
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

// plantClasses is the embedded endpoint-profile document with the accepted
// credential classes of one auth-service POST route replaced.
func plantClasses(t *testing.T, route string, classes ...string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(docWithRows(t, PatternOrgRename), &document); err != nil {
		t.Fatal(err)
	}
	planted := false
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		if row["route"] == route && row["service"] == AccessService && row["method"] == "POST" {
			list := make([]any, 0, len(classes))
			for _, class := range classes {
				list = append(list, class)
			}
			row["accepted_credential_classes"] = list
			planted = true
		}
	}
	if !planted {
		t.Fatalf("the embedded document has no POST %s row", route)
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func provisioningRoutesOver(t *testing.T, rows []byte, workload *SessionIssueWorkload) ([]string, error) {
	t.Helper()
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants, Workload: workload, ProfileRows: rows})
	var patterns []string
	for _, route := range routes {
		patterns = append(patterns, route.Pattern)
	}
	return patterns, err
}

// A provisioning route builds only on a row that lists exactly the credential classes its operation admits: the session
// issuer class, and the go-api workload token on a workload operation. The set is the document's, so it holds in a
// build with or without the workload path.
func TestProvisioningRoutesBuildOnlyOnRowsListingExactlyTheAdmittedClasses(t *testing.T) {
	for op, pattern := range provisioningRouteOps {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			workload := newWorkloadIssueFixture(t).workload
			exact := plantClasses(t, pattern, fullClassSet(op)...)
			for _, w := range []*SessionIssueWorkload{nil, workload} {
				patterns, err := provisioningRoutesOver(t, exact, w)
				if err != nil || len(patterns) != 25 {
					t.Errorf("%s on its exact row: %d routes, %v; want all 25", op, len(patterns), err)
				}
				for _, wrong := range [][]string{{serviceSessionIssuerClass}, {serviceSessionIssuerClass, workloadTokenClass}} {
					if slices.Equal(wrong, fullClassSet(op)) {
						continue
					}
					if _, err := provisioningRoutesOver(t, plantClasses(t, pattern, wrong...), w); !errors.Is(err, errProvisioningRows) ||
						!strings.Contains(err.Error(), "POST "+pattern+" ") {
						t.Errorf("%s on the row %v: %v, want a refusal naming the route", op, wrong, err)
					}
				}
			}
		})
	}
}

// provisioningRouteOps names every provisioning route's operation: the rows
// gate derives each route's admitted classes from it.
func TestProvisioningRouteOpsCoverTheRoutes(t *testing.T) {
	var patterns []string
	for _, pattern := range provisioningPatterns {
		patterns = append(patterns, pattern)
	}
	var mapped []string
	var ops []string
	for op, pattern := range provisioningRouteOps {
		mapped = append(mapped, pattern)
		ops = append(ops, op)
	}
	slices.Sort(patterns)
	slices.Sort(mapped)
	slices.Sort(ops)
	want := slices.Clone(provisioningOperations)
	slices.Sort(want)
	if !slices.Equal(patterns, mapped) || !slices.Equal(ops, want) {
		t.Fatalf("provisioningRouteOps patterns %v ops %v; want patterns %v ops %v", mapped, ops, patterns, want)
	}
}

// The rows gate's refusal names, for the start log, the route and every class its row must list, as a typed error with
// a stable reason: the document's full set, with or without the workload path.
func TestTheRowsGateRefusalNamesTheRouteAndClasses(t *testing.T) {
	t.Parallel()
	workload := newWorkloadIssueFixture(t).workload
	classes := []string{serviceSessionIssuerClass, workloadTokenClass}
	for name, w := range map[string]*SessionIssueWorkload{"without the workload path": nil, "with the workload path": workload} {
		t.Run(name, func(t *testing.T) {
			rows := plantClasses(t, PatternMembershipAdd, "no_such_class")
			_, err := provisioningRoutesOver(t, rows, w)
			var refusal *ProvisioningRowError
			if !errors.As(err, &refusal) || !errors.Is(err, errProvisioningRows) {
				t.Fatalf("err = %v, want a ProvisioningRowError that is errProvisioningRows", err)
			}
			if refusal.Route != "POST "+PatternMembershipAdd || !slices.Equal(refusal.Classes, classes) {
				t.Fatalf("refusal route=%q classes=%v, want POST %s and %v", refusal.Route, refusal.Classes, PatternMembershipAdd, classes)
			}
			if refusal.DependencyReason() != ProvisioningRowReason {
				t.Fatalf("reason = %q", refusal.DependencyReason())
			}
			attrs := refusal.DependencyAttrs()
			want := []any{"route", "POST " + PatternMembershipAdd, "required_classes", classes}
			if len(attrs) != len(want) || attrs[0] != want[0] || attrs[1] != want[1] || attrs[2] != want[2] ||
				!slices.Equal(attrs[3].([]string), classes) {
				t.Fatalf("attrs = %v, want %v", attrs, want)
			}
		})
	}
}

// fullClassSet is the credential class set a provisioning route's endpoint-profile row must list, exactly.
func fullClassSet(op string) []string {
	if workloadOperations[op] {
		return []string{serviceSessionIssuerClass, workloadTokenClass}
	}
	return []string{serviceSessionIssuerClass}
}

// The startup gate covers all 25 routes, the org state read and the rename among them: the embedded document passes,
// and a document without one route's row is refused naming the route. (The two routes shipped dark until both rows were
// embedded on main; D6617 holds for future routes, which join provisioningPatterns in their PR-2.)
func TestRowsGateCoversStateAndRename(t *testing.T) {
	t.Parallel()
	if err := provisioningRowsPresent(authv1.AuthServiceEndpointProfiles); err != nil {
		t.Fatalf("the embedded document: %v", err)
	}
	for _, pattern := range []string{PatternOrgState, PatternOrgRename} {
		err := provisioningRowsPresent(embeddedDocWithoutRow(t, pattern))
		if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+pattern+" ") {
			t.Errorf("%s without its row: %v, want a refusal naming the route", pattern, err)
		}
		routes, err := ProvisioningRoutes(provisioningDepsOver(t, embeddedDocWithoutRow(t, pattern), nil))
		if !errors.Is(err, errProvisioningRows) || routes != nil {
			t.Errorf("%s without its row: %d routes, %v; want none and a start-up refusal", pattern, len(routes), err)
		}
	}
}

// A row lists exactly the classes its route admits: one extra class, on any of the 25 routes, is refused naming the
// route.
func TestRowsGateRefusesAnExtraClass(t *testing.T) {
	for op, pattern := range provisioningRouteOps {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			raw := plantClasses(t, pattern, append(fullClassSet(op), "extra_class")...)
			err := provisioningRowsPresent(raw)
			if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+pattern+" ") {
				t.Errorf("%s with an extra class: %v, want a refusal naming the route", op, err)
			}
			if _, err := ProvisioningRoutes(provisioningDepsOver(t, raw, nil)); !errors.Is(err, errProvisioningRows) {
				t.Errorf("%s with an extra class: ProvisioningRoutes %v, want a start-up refusal", op, err)
			}
		})
	}
}

// The set is the document's, not the build's: a route that takes no workload token is refused on a row that lists the
// workload class, and a workload route on an issuer-only row, whether or not the build has the workload path.
func TestRowsGateWantsTheDocumentsFullSet(t *testing.T) {
	t.Parallel()
	for op, pattern := range provisioningRouteOps {
		other := []string{serviceSessionIssuerClass, workloadTokenClass}
		if workloadOperations[op] {
			other = []string{serviceSessionIssuerClass}
		}
		err := provisioningRowsPresent(plantClasses(t, pattern, other...))
		if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+pattern+" ") {
			t.Errorf("%s on the set %v: %v, want a refusal naming the route", op, other, err)
		}
	}
}

func provisioningDepsOver(t *testing.T, rows []byte, workload *SessionIssueWorkload) ProvisioningDeps {
	t.Helper()
	f := newProvFixture(t)
	return ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs, Memberships: f.members,
		Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants, Workload: workload, ProfileRows: rows}
}
