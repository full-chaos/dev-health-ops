package authapi

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// bearerRowOperations are the rows authenticateWith gates, by operation.
var bearerRowOperations = map[string]string{
	opSessionCurrent:   "GET /v1/sessions/current",
	opSessionRevoke:    "POST /v1/sessions/revoke",
	opSessionRevokeAll: "POST /v1/sessions/revoke-all",
	opChangePassword:   "POST /api/v1/auth/change-password",
	opSocialLink:       "POST /v1/identities/social",
	opSocialUnlink:     "DELETE /v1/identities/social/{id}",
}

type profileRow struct {
	Method          string   `json:"method"`
	Route           string   `json:"route"`
	Accepted        []string `json:"accepted_credential_classes"`
	DelegatedBearer *string  `json:"delegated_bearer"`
}

func embeddedRows(t *testing.T) []profileRow {
	t.Helper()
	var document struct {
		Rows []profileRow `json:"rows"`
	}
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	return document.Rows
}

func TestDelegatedBearerRowsEqualTheGoAllowlist(t *testing.T) {
	operationOf := map[string]string{}
	for op, row := range bearerRowOperations {
		operationOf[row] = op
	}
	for op := range delegatedBearerOperations {
		if _, listed := bearerRowOperations[op]; !listed {
			t.Errorf("the allowlist holds %s, which names no bearer row", op)
		}
	}
	seen := map[string]bool{}
	for _, row := range embeddedRows(t) {
		key := row.Method + " " + row.Route
		op, reached := operationOf[key]
		userBearer := slices.Contains(row.Accepted, "user_access_token") || slices.Contains(row.Accepted, "ops_access_token_hs256")
		switch {
		case userBearer != reached:
			t.Errorf("%s: takes a user bearer = %v, in the table of authenticateWith rows = %v", key, userBearer, reached)
		case !reached && row.DelegatedBearer != nil:
			t.Errorf("%s: authenticateWith does not reach this row, and it has delegated_bearer %q", key, *row.DelegatedBearer)
		case reached && row.DelegatedBearer == nil:
			t.Errorf("%s: no delegated_bearer", key)
		case reached:
			want := "denied"
			if delegatedBearerOperations[op] {
				want = "allowed"
			}
			if *row.DelegatedBearer != want {
				t.Errorf("%s: delegated_bearer %q, the Go allowlist says %q", key, *row.DelegatedBearer, want)
			}
			seen[op] = true
		}
	}
	for op, row := range bearerRowOperations {
		if !seen[op] {
			t.Errorf("%s: no profile row %s", op, row)
		}
	}
}

func TestDelegatedRowsListExactlyTheCallerClasses(t *testing.T) {
	want := []string{"internal_svc_authsess_token", "workload_token"}
	found := 0
	for _, row := range embeddedRows(t) {
		if row.Route != PatternDelegatedSession && row.Route != PatternDelegatedSessionEnd {
			continue
		}
		found++
		if !slices.Equal(row.Accepted, want) {
			t.Errorf("%s: classes %v, want %v", row.Route, row.Accepted, want)
		}
	}
	if found != 2 {
		t.Fatalf("delegated rows = %d, want 2", found)
	}
}

func TestDelegatedRoutesTakeTheCallerClassesOnly(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	record := delegatedRecord(w.fixture, time.Now().Add(10*time.Minute).UTC())
	w.sessions.delegated, w.sessions.records[record.ID] = record, record
	endBody := `{"session_id":"` + record.ID.String() + `"}`
	workload, _ := w.mint("workload+jwt", nil)
	for name, header := range map[string]map[string]string{"session issuer": issuerBearer, "go-api workload": workload} {
		if r := w.do(http.MethodPost, PatternDelegatedSession, delegatedBody(w.compatFixture, w.record.ID), header); r.Code != http.StatusCreated {
			t.Errorf("%s start: %d %s", name, r.Code, r.Body)
		}
		if r := w.do(http.MethodPost, PatternDelegatedSessionEnd, endBody, header); r.Code != http.StatusNoContent {
			t.Errorf("%s end: %d %s", name, r.Code, r.Body)
		}
	}
	started, ended := len(w.sessions.started), len(w.sessions.ended)
	wrongAudience, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
	for name, header := range map[string]map[string]string{
		"no bearer":                 nil,
		"another token prefix":      {"Authorization": "Bearer svc_other_" + strings.Repeat("A", 43)},
		"another workload audience": wrongAudience,
	} {
		expectError(t, w.do(http.MethodPost, PatternDelegatedSession, delegatedBody(w.compatFixture, w.record.ID), header), http.StatusUnauthorized, "invalid_credentials")
		expectError(t, w.do(http.MethodPost, PatternDelegatedSessionEnd, endBody, header), http.StatusUnauthorized, "invalid_credentials")
		if len(w.sessions.started) != started || len(w.sessions.ended) != ended {
			t.Fatalf("%s reached the store", name)
		}
	}
}

func TestRevokeAllDeclaresTheDelegatedRefusal(t *testing.T) {
	f := newFixture(t)
	markDelegated(f)
	status := f.do(http.MethodPost, PatternRevokeAll, "", f.bearer(f.grant())).Code
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "contracts/auth/v1/openapi/auth-v1.openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Paths map[string]map[string]struct {
			Responses map[string]any `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if _, declared := document.Paths[PatternRevokeAll]["post"].Responses[strconv.Itoa(status)]; !declared {
		t.Fatalf("a delegated bearer gets %d, which the operation does not declare", status)
	}
}
