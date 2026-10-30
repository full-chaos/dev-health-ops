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
	for _, c := range routeCases()[:8] {
		if got := f.post(c.path, encode(c.body(f, f.super)), nil); got.Code != http.StatusOK {
			t.Fatalf("%s on the embedded document: %d %s, want 200", c.op, got.Code, got.Body)
		}
		routes++
	}
	if routes != 8 {
		t.Fatalf("%d provisioning routes served, want 8", routes)
	}
	if strings.Contains(f.logs.String(), "not mounted") {
		t.Fatalf("a not-mounted line was logged: %s", f.logs.String())
	}
}

// A document without the reactivate row refuses the whole switch (all eight routes), like any other missing row.
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
