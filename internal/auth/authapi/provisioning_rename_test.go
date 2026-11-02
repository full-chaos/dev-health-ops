package authapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

func renameBody(f *provFixture, actor uuid.UUID, name string) string {
	return encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": actor.String(), "name": name})
}

// Dark: the embedded document has no rename row, so the route answers 404, the store is not reached, one startup line
// names it, and every other provisioning route is still built.
func TestOrgRenameIsDarkWithoutItsProfileRow(t *testing.T) {
	f := newProvFixtureWith(t, authv1.AuthServiceEndpointProfiles)
	if got := f.post(PatternOrgRename, renameBody(f, f.super, "New"), nil); got.Code != http.StatusNotFound {
		t.Fatalf("rename with the embedded document: %d %s, want 404", got.Code, got.Body)
	}
	if f.reactivate.renames != 0 {
		t.Fatal("the store ran for a route that is not mounted")
	}
	lines := 0
	for _, line := range strings.Split(strings.TrimSpace(f.logs.String()), "\n") {
		if strings.Contains(line, PatternOrgRename) {
			lines++
			if !strings.Contains(line, "not mounted") {
				t.Fatalf("the startup line does not say the route is not mounted: %s", line)
			}
		}
	}
	if lines != 1 {
		t.Fatalf("%d startup lines name the rename route, want 1: %s", lines, f.logs.String())
	}
	if got := f.post(PatternOrgSuspend, encode(routeCases()[8].body(f, f.super)), nil); got.Code != http.StatusOK {
		t.Fatalf("suspend beside the dark rename route: %d %s, want 200", got.Code, got.Body)
	}
}

// The route is not on the startup gate: a document without its row builds every other route.
func TestOrgRenameIsNotOnTheStartupGate(t *testing.T) {
	if slices.Contains(provisioningPatterns, PatternOrgRename) {
		t.Fatal("the rename route is on the startup gate; it ships dark (D6617)")
	}
	if !workloadOperations[opOrgRename] || provisioningRouteOps[opOrgRename] != PatternOrgRename {
		t.Fatal("the rename route is not a workload operation mapped to its pattern")
	}
}

// With its row the route mounts and gives the verb the org, the normalized name, the actor and a request id.
func TestOrgRenameMountsWithItsRow(t *testing.T) {
	f := newProvFixture(t)
	revision := encode(map[string]any{"organization_id": f.org.String(), "actor_principal_id": f.super.String(), "name": "  Acme Two ",
		"if_revision": 7})
	response := f.post(PatternOrgRename, revision, nil)
	if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("rename: %d (Cache-Control %q) %s", response.Code, response.Header().Get("Cache-Control"), response.Body)
	}
	var doc map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc) != 3 || doc["organization_id"] != f.org.String() || doc["name"] != "Acme Two" || doc["renamed"] != true {
		t.Fatalf("rename answer = %v", doc)
	}
	if f.reactivate.renames != 1 || f.reactivate.rid != f.org || f.reactivate.rname != "Acme Two" || f.reactivate.rIfRevision == nil ||
		*f.reactivate.rIfRevision != 7 || f.reactivate.ractor.PrincipalID == nil || *f.reactivate.ractor.PrincipalID != f.super ||
		f.reactivate.ractor.RequestID == nil {
		t.Fatalf("the rename verb was given %+v", f.reactivate)
	}
	f.reactivate.noop = true
	response = f.post(PatternOrgRename, renameBody(f, f.super, "Acme Two"), nil)
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil || response.Code != http.StatusOK || doc["renamed"] != false {
		t.Fatalf("rename to the current name: %d %s, want 200 with renamed false", response.Code, response.Body)
	}
}

// go-api's two rename paths: a superuser, or an owner or admin of the organization; nobody else reaches the verb.
func TestOrgRenameActorRule(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	for name, tc := range map[string]struct {
		actor  uuid.UUID
		reason string
	}{
		"viewer of the org":  {f.viewer, reasonAdminRequired},
		"member of no org":   {f.target, reasonAdminRequired},
		"inactive superuser": {f.x, reasonDeactivated},
		"unknown actor":      {uuid.New(), reasonDeactivated},
	} {
		expectRefusal(t, f.post(PatternOrgRename, renameBody(f, tc.actor, "New"), nil), http.StatusForbidden, tc.reason)
		if f.reactivate.renames != 0 {
			t.Fatalf("%s: a refused actor reached the verb", name)
		}
	}
	for name, actor := range map[string]uuid.UUID{"superuser": f.super, "owner": f.owner, "admin": f.admin} {
		if got := f.post(PatternOrgRename, renameBody(f, actor, "New"), nil); got.Code != http.StatusOK {
			t.Fatalf("rename by the %s: %d %s", name, got.Code, got.Body)
		}
	}
}

func TestOrgRenameRunsTheActorRuleAgainUnderTheLock(t *testing.T) {
	f := newProvFixture(t)
	f.users.inTx = &fakeProvUsers{states: map[uuid.UUID]UserState{f.admin: {IsActive: true}}}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.admin, "New"), nil), http.StatusForbidden, reasonAdminRequired)
	f.users.inTx = &fakeProvUsers{err: errors.New("read failed")}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.admin, "New"), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestOrgRenameRefusalsAreNamedReasonCodes(t *testing.T) {
	f := newProvFixture(t)
	for name, tc := range map[string]struct {
		err    error
		status int
		reason string
	}{
		"missing organization":  {org.ErrNotFound, http.StatusNotFound, reasonOrganizationNotFound},
		"suspended":             {org.ErrSuspended, http.StatusConflict, reasonOrganizationInactive},
		"stale revision":        {org.ErrRevisionConflict, http.StatusConflict, reasonStateChanged},
		"denied under the lock": {org.ErrDenied, http.StatusForbidden, reasonAdminRequired},
		"name refused":          {org.ErrNameInvalid, http.StatusUnprocessableEntity, reasonNameInvalid},
		"store down":            {fmt.Errorf("%w: rename: down", org.ErrStore), http.StatusServiceUnavailable, reasonStoreUnavailable},
		"unclassified":          {errors.New("unclassified"), http.StatusServiceUnavailable, reasonStoreUnavailable},
	} {
		f.reactivate.err = tc.err
		response := f.post(PatternOrgRename, renameBody(f, f.super, "New"), nil)
		expectRefusal(t, response, tc.status, tc.reason)
		if strings.Contains(response.Body.String(), "SQLSTATE") {
			t.Fatalf("%s: the answer carries database text: %s", name, response.Body)
		}
	}
}

// A name that is blank after the strip or longer than 100 characters is refused before any read.
func TestOrgRenameRefusesABadNameBeforeTheStore(t *testing.T) {
	f := newProvFixture(t)
	for _, name := range []string{"   ", strings.Repeat("n", 101)} {
		expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.super, name), nil), http.StatusUnprocessableEntity, reasonNameInvalid)
	}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.super, ""), nil), http.StatusBadRequest, reasonMalformed)
	if f.reactivate.renames != 0 || f.users.roleReads != 0 {
		t.Fatalf("a refused name reached the store (%d renames, %d role reads)", f.reactivate.renames, f.users.roleReads)
	}
}
