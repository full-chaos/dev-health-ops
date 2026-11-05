package authapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func TestEmbeddedProfileHoldsABothClassRowForEveryReadRoute(t *testing.T) {
	var document struct {
		Rows []struct {
			ID      string   `json:"id"`
			Service string   `json:"service"`
			Method  string   `json:"method"`
			Route   string   `json:"route"`
			Classes []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	for _, c := range readCases() {
		found := 0
		for _, row := range document.Rows {
			if row.Service != AccessService || row.Method != "POST" || row.Route != c.path {
				continue
			}
			found++
			if row.ID != "POST "+c.path+" [dev-health-auth-service @api]" {
				t.Errorf("%s: row id %q is not an API listener row", c.op, row.ID)
			}
			slices.Sort(row.Classes)
			if !slices.Equal(row.Classes, []string{serviceSessionIssuerClass, workloadTokenClass}) {
				t.Errorf("%s: row classes %v, want the session issuer credential and the workload token", c.op, row.Classes)
			}
		}
		if found != 1 {
			t.Errorf("%s: %d embedded rows, want 1", c.op, found)
		}
	}
}

func newEmbeddedReadFixture(t *testing.T) *readWorkloadFixture {
	t.Helper()
	return newReadWorkloadFixture(t, authv1.AuthServiceEndpointProfiles)
}

func TestEmbeddedProfileServesTheReadRoutesToTheirCredentials(t *testing.T) {
	for _, c := range readCases() {
		f := newEmbeddedReadFixture(t)
		header, _ := f.w.mint(contracts.WorkloadTokenTyp, nil)
		if got := f.post(c.path, c.body(f.provFixture), header); got.Code != http.StatusOK {
			t.Errorf("%s: go-api workload token %d, want 200", c.op, got.Code)
		}

		held := newEmbeddedReadFixture(t)
		held.creds.held = []string{c.scope}
		if got := held.post(c.path, c.body(held.provFixture), nil); got.Code != http.StatusOK {
			t.Errorf("%s: svc_authsess_ with %s %d, want 200", c.op, c.scope, got.Code)
		}

		wrong := newEmbeddedReadFixture(t)
		wrong.creds.held = []string{"memberships:write", "sessions:issue"}
		for _, scope := range []string{"principals:read", "organizations:read", "memberships:read", "grants:read"} {
			if scope != c.scope {
				wrong.creds.held = append(wrong.creds.held, scope)
			}
		}
		expectRefusal(t, wrong.post(c.path, c.body(wrong.provFixture), nil), http.StatusUnauthorized, reasonInvalidCredentials)

		for _, typ := range []string{"at+jwt", "jwt", contracts.ServiceAccountTokenTyp} {
			other := newEmbeddedReadFixture(t)
			otherHeader, _ := other.w.mint(typ, nil)
			expectRefusal(t, other.post(c.path, c.body(other.provFixture), otherHeader), http.StatusUnauthorized, reasonInvalidCredentials)
		}
		bare := newEmbeddedReadFixture(t)
		expectRefusal(t, bare.post(c.path, c.body(bare.provFixture), map[string]string{"Authorization": "Bearer nonsense"}), http.StatusUnauthorized, reasonInvalidCredentials)
	}
}

func readRowsWithExtraClass(t *testing.T, route string, extra ...string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	changed := 0
	for _, item := range document["rows"].([]any) {
		row := item.(map[string]any)
		if row["service"] == AccessService && row["method"] == "POST" && row["route"] == route {
			classes := row["accepted_credential_classes"].([]any)
			for _, e := range extra {
				classes = append(classes, e)
			}
			row["accepted_credential_classes"] = classes
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("%s: %d rows changed, want 1", route, changed)
	}
	out, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mountedReadPatterns(t *testing.T, rows []byte) []string {
	t.Helper()
	f := newProvFixtureWith(t, nil)
	h := readHandlers{ReadDeps: ReadDeps{Reads: f.reads, GrantReads: f.grantReads, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}}
	var patterns []string
	for _, route := range h.withReadRoutes(nil, rows) {
		patterns = append(patterns, route.Pattern)
	}
	return patterns
}

func TestReadRoutesMountOnlyOnAnExactClassRow(t *testing.T) {
	if got := mountedReadPatterns(t, authv1.AuthServiceEndpointProfiles); len(got) != len(readCases()) {
		t.Errorf("exact two-class rows mount %d routes, want %d", len(got), len(readCases()))
	}
	for _, c := range readCases() {
		rows := readRowsWithExtraClass(t, c.path, "user_access_token")
		if slices.Contains(mountedReadPatterns(t, rows), c.path) {
			t.Errorf("%s: a row that also lists user_access_token still mounts the route", c.op)
		}
	}
}
