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

// A document without the rename row refuses the service start, naming the route, and builds no route.
func TestOrgRenameNeedsItsProfileRowToStart(t *testing.T) {
	t.Parallel()
	routes, err := ProvisioningRoutes(provisioningDepsOver(t, docWithoutRow(t, nil, PatternOrgRename), nil))
	var refusal *ProvisioningRowError
	if !errors.As(err, &refusal) || refusal.Route != "POST "+PatternOrgRename || routes != nil {
		t.Fatalf("%d routes, %v; want none and a refusal naming the rename route", len(routes), err)
	}
}

// The embedded document holds the rename row: the route is served, and no startup line calls it unmounted.
func TestOrgRenameIsServedWithTheEmbeddedDocument(t *testing.T) {
	t.Parallel()
	f := newProvFixtureWith(t, authv1.AuthServiceEndpointProfiles)
	if got := f.post(PatternOrgRename, renameBody(f, f.super, "New"), nil); got.Code != http.StatusOK {
		t.Fatalf("rename with the embedded document: %d %s, want 200", got.Code, got.Body)
	}
	if f.reactivate.renames != 1 {
		t.Fatalf("%d renames, want 1", f.reactivate.renames)
	}
	for _, line := range strings.Split(f.logs.String(), "\n") {
		if strings.Contains(line, "not mounted") && strings.Contains(line, PatternOrgRename) {
			t.Fatalf("a not-mounted line for the rename route: %s", line)
		}
	}
}

// The route is on the startup gate: its row is embedded on main, so the dark phase D6617 describes for it is over (a
// new route still joins provisioningPatterns only in its PR-2).
func TestOrgRenameIsOnTheStartupGate(t *testing.T) {
	if !slices.Contains(provisioningPatterns, PatternOrgRename) {
		t.Fatal("the rename route is not on the startup gate")
	}
	if !workloadOperations[opOrgRename] || provisioningRouteOps[opOrgRename] != PatternOrgRename {
		t.Fatal("the rename route is not a workload operation mapped to its pattern")
	}
}

// With its row the route mounts and gives the verb the org, the normalized name, the actor and a request id.
func TestOrgRenameMountsWithItsRow(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	f := newProvFixture(t)
	f.users.inTx = &fakeProvUsers{states: map[uuid.UUID]UserState{f.admin: {IsActive: true}}}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.admin, "New"), nil), http.StatusForbidden, reasonAdminRequired)
	f.users.inTx = &fakeProvUsers{err: errors.New("read failed")}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.admin, "New"), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestOrgRenameRefusalsAreNamedReasonCodes(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	f := newProvFixture(t)
	for _, name := range []string{"   ", strings.Repeat("n", 101)} {
		expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.super, name), nil), http.StatusUnprocessableEntity, reasonNameInvalid)
	}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.super, ""), nil), http.StatusBadRequest, reasonMalformed)
	if f.reactivate.renames != 0 || f.users.roleReads != 0 {
		t.Fatalf("a refused name reached the store (%d renames, %d role reads)", f.reactivate.renames, f.users.roleReads)
	}
}

// The name is bounded after the strip, by runes: 100 four-byte runes inside surrounding spaces are a valid name.
func TestOrgRenameBoundsTheNameAfterTheStrip(t *testing.T) {
	t.Parallel()
	f := newProvFixture(t)
	wide := strings.Repeat("\U0001F600", 100)
	if got := f.post(PatternOrgRename, renameBody(f, f.super, "  "+wide+"  "), nil); got.Code != http.StatusOK {
		t.Fatalf("100 four-byte runes with surrounding spaces: %d %s, want 200", got.Code, got.Body)
	}
	if f.reactivate.rname != wide {
		t.Fatalf("the verb was given %q, want the stripped name", f.reactivate.rname)
	}
	expectRefusal(t, f.post(PatternOrgRename, renameBody(f, f.super, wide+"x"), nil), http.StatusUnprocessableEntity, reasonNameInvalid)
}
