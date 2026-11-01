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
	if err := provisioningRowsPresent(authv1.AuthServiceEndpointProfiles, true); err != nil {
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
			err := provisioningRowsPresent(plant(edit), false)
			if !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+pattern+" ") {
				t.Fatalf("%s, %s: %v, want a refusal naming the route", pattern, name, err)
			}
		}
	}
	if err := provisioningRowsPresent([]byte("{"), false); !errors.Is(err, errProvisioningRows) {
		t.Fatalf("an unreadable document: %v, want a refusal", err)
	}
	// ProvisioningRoutes runs the check: a planted document without one route's
	// row builds no route at all.
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa,
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
		Organizations: f.orgs, Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, ProfileRows: docWithRow(t, PatternOrgSuspend),
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
	if err := provisioningRowsPresent(authv1.AuthServiceEndpointProfiles, true); err != nil {
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
	if routes != 17 {
		t.Fatalf("%d provisioning routes served, want 17", routes)
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
	err = provisioningRowsPresent(raw, false)
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
	if err := provisioningRowsPresent(raw, false); !errors.Is(err, errProvisioningRows) || !strings.Contains(err.Error(), "POST "+route+" ") {
		t.Fatalf("without the revoke row: %v, want a refusal naming the route", err)
	}
}

// plantClasses is the embedded endpoint-profile document with the accepted
// credential classes of one auth-service POST route replaced.
func plantClasses(t *testing.T, route string, classes ...string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
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
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Workload: workload, ProfileRows: rows})
	var patterns []string
	for _, route := range routes {
		patterns = append(patterns, route.Pattern)
	}
	return patterns, err
}

// A provisioning route mounts only on a row that lists every credential class
// its operation admits: the session issuer class, and the go-api workload
// token when the operation is a workload operation and the workload path is
// configured. A row that lists the issuer class alone under-describes a
// workload route, so the switch (and the org state route) refuse it; without
// the workload path, or on a non-workload operation, the issuer class alone
// is the whole admitted set.
func TestProvisioningRoutesMountOnlyOnRowsAcceptingEveryAdmittedClass(t *testing.T) {
	workload := newWorkloadIssueFixture(t).workload
	// The org state read is dark: not on the startup gate, mounted only on a row.
	dark := []string{PatternOrgState}
	for op, pattern := range provisioningRouteOps {
		if slices.Contains(grantPatterns, pattern) {
			continue // no row in the embedded document yet; provisioning_grant_test.go plants its own
		}
		issuerOnly := plantClasses(t, pattern, serviceSessionIssuerClass)
		both := plantClasses(t, pattern, serviceSessionIssuerClass, workloadTokenClass)
		if workloadOperations[op] && !slices.Contains(dark, pattern) {
			if _, err := provisioningRoutesOver(t, issuerOnly, workload); !errors.Is(err, errProvisioningRows) ||
				!strings.Contains(err.Error(), "POST "+pattern+" ") {
				t.Errorf("%s: an issuer-only row with the workload path configured: %v, want a refusal naming the route", op, err)
			}
		} else if !slices.Contains(dark, pattern) {
			if _, err := provisioningRoutesOver(t, issuerOnly, workload); err != nil {
				t.Errorf("%s: a non-workload route on an issuer-only row: %v, want it mounted", op, err)
			}
		}
		if _, err := provisioningRoutesOver(t, issuerOnly, nil); err != nil {
			t.Errorf("%s: an issuer-only row without the workload path: %v, want it mounted", op, err)
		}
		if _, err := provisioningRoutesOver(t, both, workload); err != nil {
			t.Errorf("%s: a row listing both classes: %v, want it mounted", op, err)
		}
	}
	for _, pattern := range dark {
		mounted := func(rows []byte, workload *SessionIssueWorkload) bool {
			patterns, err := provisioningRoutesOver(t, rows, workload)
			if err != nil {
				t.Fatalf("%s row: %v", pattern, err)
			}
			return slices.Contains(patterns, pattern)
		}
		issuerOnly := plantClasses(t, pattern, serviceSessionIssuerClass)
		if mounted(issuerOnly, workload) {
			t.Errorf("%s mounted on an issuer-only row with the workload path configured", pattern)
		}
		if !mounted(issuerOnly, nil) {
			t.Errorf("%s did not mount on an issuer-only row without the workload path", pattern)
		}
		if !mounted(plantClasses(t, pattern, serviceSessionIssuerClass, workloadTokenClass), workload) {
			t.Errorf("%s did not mount on a row listing both classes", pattern)
		}
	}
}

// provisioningRouteOps names every provisioning route's operation: the rows
// gate derives each route's admitted classes from it.
func TestProvisioningRouteOpsCoverTheRoutes(t *testing.T) {
	var patterns []string
	for _, pattern := range provisioningPatterns {
		patterns = append(patterns, pattern)
	}
	patterns = append(patterns, PatternOrgState)
	patterns = append(patterns, grantPatterns...)
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

// The rows gate's refusal names, for the start log, the route and every class
// its row must list, as a typed error with a stable reason: the workload class
// too when the binding is configured.
func TestTheRowsGateRefusalNamesTheRouteAndClasses(t *testing.T) {
	workload := newWorkloadIssueFixture(t).workload
	for name, tc := range map[string]struct {
		workload *SessionIssueWorkload
		classes  []string
	}{
		"without the workload path": {nil, []string{serviceSessionIssuerClass}},
		"with the workload path":    {workload, []string{serviceSessionIssuerClass, workloadTokenClass}},
	} {
		t.Run(name, func(t *testing.T) {
			rows := plantClasses(t, PatternMembershipAdd, "no_such_class")
			_, err := provisioningRoutesOver(t, rows, tc.workload)
			var refusal *ProvisioningRowError
			if !errors.As(err, &refusal) || !errors.Is(err, errProvisioningRows) {
				t.Fatalf("err = %v, want a ProvisioningRowError that is errProvisioningRows", err)
			}
			if refusal.Route != "POST "+PatternMembershipAdd || !slices.Equal(refusal.Classes, tc.classes) {
				t.Fatalf("refusal route=%q classes=%v, want POST %s and %v", refusal.Route, refusal.Classes, PatternMembershipAdd, tc.classes)
			}
			if refusal.DependencyReason() != ProvisioningRowReason {
				t.Fatalf("reason = %q", refusal.DependencyReason())
			}
			attrs := refusal.DependencyAttrs()
			want := []any{"route", "POST " + PatternMembershipAdd, "required_classes", tc.classes}
			if len(attrs) != len(want) || attrs[0] != want[0] || attrs[1] != want[1] || attrs[2] != want[2] ||
				!slices.Equal(attrs[3].([]string), tc.classes) {
				t.Fatalf("attrs = %v, want %v", attrs, want)
			}
		})
	}
}
