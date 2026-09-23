//go:build integration

package teamsidentity

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// callWithBody drives a handler the way decodeFirst would, minus the Guard
// wrap (Guard's own auth/authz behavior is covered by internal/api/policy's
// own tests): a *policy.User is placed on the context exactly as
// guard.Wrap(policy.AdminOrg, ...) leaves it, and the JSON body is decoded
// into a pybody.Body exactly as pybody.Read would for a real request.
func callWithBody(t *testing.T, h handlers, handler http.HandlerFunc, method, path, orgID string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	body := pybody.Body{Value: value}
	request := httptest.NewRequest(method, path, strings.NewReader(string(raw)))
	ctx := context.WithValue(request.Context(), bodyKey{}, body)
	ctx = policy.WithUser(ctx, &policy.User{OrgID: orgID, Role: "admin"})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request.WithContext(ctx))
	return recorder
}

func newTestHandlers(store Store) handlers {
	return handlers{store: store, logger: slog.Default()}
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode response body %q: %v", recorder.Body.String(), err)
	}
	return decoded
}

// TestCreateOrUpdateIdentityRejectsUnknownTeamID is the 404 preflight check:
// team_ids must all exist BEFORE any write (atomic validate-then-write).
func TestCreateOrUpdateIdentityRejectsUnknownTeamID(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	h := newTestHandlers(store)

	recorder := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "org-1",
		map[string]any{"canonical_id": "carol", "team_ids": []string{"does-not-exist"}})
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=404 body=%s", recorder.Code, recorder.Body.String())
	}

	identity, err := store.GetIdentity(ctx, "org-1", "carol")
	if err != nil {
		t.Fatal(err)
	}
	if identity != nil {
		t.Fatalf("identity must not have been written on a 404: %+v", identity)
	}
}

// TestCreateOrUpdateIdentityRejectsConflictingProviderIdentity is the 409
// check: a provider identity already owned by a DIFFERENT canonical_id is
// refused.
func TestCreateOrUpdateIdentityRejectsConflictingProviderIdentity(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	h := newTestHandlers(store)

	first := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "org-1",
		map[string]any{"canonical_id": "dave", "provider_identities": map[string][]string{"github": {"dave-gh"}}})
	if first.Code != http.StatusOK {
		t.Fatalf("first create status=%d body=%s", first.Code, first.Body.String())
	}

	second := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "org-1",
		map[string]any{"canonical_id": "erin", "provider_identities": map[string][]string{"github": {"dave-gh"}}})
	if second.Code != http.StatusConflict {
		t.Fatalf("status=%d want=409 body=%s", second.Code, second.Body.String())
	}
	body := decodeBody(t, second)
	detail, _ := body["detail"].(string)
	if !strings.Contains(detail, "dave") || !strings.Contains(detail, "github:dave-gh") {
		t.Fatalf("409 detail missing expected content: %q", detail)
	}

	erin, err := store.GetIdentity(ctx, "org-1", "erin")
	if err != nil {
		t.Fatal(err)
	}
	if erin != nil {
		t.Fatalf("identity must not have been written on a 409: %+v", erin)
	}
}

// TestCreateOrUpdateIdentityReconcilesTeamMembership is the core facet-
// reconciliation path: creating an identity with team_ids adds its facets
// to those teams; updating it to drop a team removes its facets from that
// team while leaving other members untouched; updating its email changes
// which facet is tracked.
func TestCreateOrUpdateIdentityReconcilesTeamMembership(t *testing.T) {
	store, ctx := startTeamsIdentitiesStore(t)
	h := newTestHandlers(store)

	if _, err := store.CreateOrUpdateTeam(ctx, "org-1", TeamWrite{TeamID: "team-x", Name: "Team X"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateOrUpdateTeam(ctx, "org-1", TeamWrite{TeamID: "team-y", Name: "Team Y"}); err != nil {
		t.Fatal(err)
	}
	// A pre-existing, unrelated member of team-x that reconciliation must
	// never touch.
	if _, err := store.AddMembers(ctx, "org-1", "team-x", []string{"preexisting@example.com"}); err != nil {
		t.Fatal(err)
	}

	created := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "org-1",
		map[string]any{"canonical_id": "frank", "email": "frank@example.com", "team_ids": []string{"team-x"}})
	if created.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", created.Code, created.Body.String())
	}

	teamX, err := store.GetTeam(ctx, "org-1", "team-x")
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(teamX.Members, "frank@example.com") {
		t.Fatalf("team-x must gain frank's facet: %+v", teamX.Members)
	}
	if !containsString(teamX.Members, "preexisting@example.com") {
		t.Fatalf("team-x must keep the pre-existing member: %+v", teamX.Members)
	}

	// Move frank from team-x to team-y, and change his email (the facet
	// tracked). Both the old team AND the old facet must be cleaned up.
	updated := callWithBody(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "org-1",
		map[string]any{"canonical_id": "frank", "email": "frank-new@example.com", "team_ids": []string{"team-y"}})
	if updated.Code != http.StatusOK {
		t.Fatalf("update status=%d body=%s", updated.Code, updated.Body.String())
	}

	teamXAfter, err := store.GetTeam(ctx, "org-1", "team-x")
	if err != nil {
		t.Fatal(err)
	}
	if containsString(teamXAfter.Members, "frank@example.com") || containsString(teamXAfter.Members, "frank-new@example.com") {
		t.Fatalf("team-x must lose every one of frank's facets after he left it: %+v", teamXAfter.Members)
	}
	if !containsString(teamXAfter.Members, "preexisting@example.com") {
		t.Fatalf("team-x must still keep the pre-existing member: %+v", teamXAfter.Members)
	}

	teamY, err := store.GetTeam(ctx, "org-1", "team-y")
	if err != nil {
		t.Fatal(err)
	}
	if !containsString(teamY.Members, "frank-new@example.com") {
		t.Fatalf("team-y must gain frank's NEW facet: %+v", teamY.Members)
	}
	if containsString(teamY.Members, "frank@example.com") {
		t.Fatalf("team-y must never have seen frank's OLD facet: %+v", teamY.Members)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
