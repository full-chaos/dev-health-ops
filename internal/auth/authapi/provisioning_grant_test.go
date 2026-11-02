package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
)

var grantPatterns = []string{PatternGrantCreate, PatternGrantUpdate, PatternGrantRevoke}

var grantOps = []string{opGrantCreate, opGrantUpdate, opGrantRevoke}

// fakeGrants records the input of each verb and runs its Authorize on an empty transaction with the locked state a store
// would give it, as the store does after its locks.
type fakeGrants struct {
	calls     int
	verb      string
	create    grant.CreateInput
	update    grant.UpdateInput
	revoke    grant.RevokeInput
	principal uuid.UUID
	err       error
	out       grant.Grant
}

func (f *fakeGrants) run(verb string, org uuid.UUID, principal uuid.UUID, a grant.Actor, authorize grant.Authorize) (*grant.Grant, error) {
	f.calls++
	f.verb = verb
	if authorize == nil {
		return nil, grant.ErrDenied
	}
	if err := authorize(context.Background(), audit.TxOps{}, grant.Locked{OrgID: org, Principals: []uuid.UUID{principal}}); err != nil {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	out := f.out
	return &out, nil
}

func (f *fakeGrants) Create(ctx context.Context, in grant.CreateInput) (*grant.Grant, error) {
	f.create = in
	return f.run("create", in.OrgID, in.PrincipalID, in.Actor, in.Authorize)
}

func (f *fakeGrants) Update(ctx context.Context, in grant.UpdateInput) (*grant.Grant, error) {
	f.update = in
	return f.run("update", in.OrgID, f.principal, in.Actor, in.Authorize)
}

func (f *fakeGrants) Revoke(ctx context.Context, in grant.RevokeInput) (*grant.Grant, error) {
	f.revoke = in
	return f.run("revoke", in.OrgID, f.principal, in.Actor, in.Authorize)
}

func newGrantFixture(t *testing.T) *provFixture {
	t.Helper()
	f := newProvFixtureFor(t, nil, nil)
	f.grants.principal = f.target
	f.grants.out = grant.Grant{ID: uuid.New(), OrgID: f.org, PrincipalID: f.target, Role: "viewer", Key: "acme/api", Origin: grant.OriginAPI, Revision: 1}
	return f
}

func grantCases() []routeCase {
	return []routeCase{
		{opGrantCreate, PatternGrantCreate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "principal_id": f.target.String(),
				"role": "viewer", "resource_key": "acme/api"}
		}, []string{"organization_id", "actor_principal_id", "principal_id", "role", "resource_key"}},
		{opGrantUpdate, PatternGrantUpdate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "grant_id": f.grants.out.ID.String(), "role": "admin"}
		}, []string{"organization_id", "actor_principal_id", "grant_id"}},
		{opGrantRevoke, PatternGrantRevoke, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "grant_id": f.grants.out.ID.String()}
		}, []string{"organization_id", "actor_principal_id", "grant_id"}},
	}
}

// The grant routes are served with the embedded document: its rows are there, so each answers and none is a 404.
func TestGrantRoutesAreServedWithTheEmbeddedDocument(t *testing.T) {
	f := newGrantFixture(t)
	for _, c := range grantCases() {
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Errorf("%s over the embedded document: %d %s, want 200", c.path, response.Code, response.Body)
		}
	}
	if strings.Contains(f.logs.String(), "not mounted") && strings.Contains(f.logs.String(), "/internal/v1/grants/") {
		t.Fatalf("a grant route was logged as not mounted: %s", f.logs.String())
	}
}

// The grant routes are on the startup gate: a document without a row builds no route at all and names the route.
func TestGrantRoutesAreOnTheStartupGate(t *testing.T) {
	f := newProvFixtureFor(t, nil, nil)
	for _, c := range grantCases() {
		if !slices.Contains(provisioningPatterns, c.path) || provisioningRouteOps[c.op] != c.path {
			t.Fatalf("%s is not in the provisioning patterns or route operations", c.path)
		}
		routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
			Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants,
			ProfileRows: embeddedDocWithoutRow(t, c.path)})
		if !errors.Is(err, errProvisioningRows) || routes != nil || !strings.Contains(err.Error(), "POST "+c.path+" ") {
			t.Fatalf("a document without the %s row built %d routes, err %v; want none and a refusal naming it", c.path, len(routes), err)
		}
	}
	if _, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa}); err == nil {
		t.Fatal("routes were built with no grant store")
	}
}

// Each route reads a strict body and answers the store's result; the verbs get the request's members.
func TestGrantRoutesPassTheRequestAndAnswerTheGrant(t *testing.T) {
	f := newGrantFixture(t)
	expires := time.Date(2031, 5, 6, 7, 8, 9, 0, time.UTC)
	post := func(c routeCase, mutate func(map[string]any)) map[string]any {
		body := c.body(f, f.admin)
		if mutate != nil {
			mutate(body)
		}
		response := f.post(c.path, encode(body), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", c.path, response.Code, response.Body)
		}
		var out map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	cases := grantCases()
	f.grants.out.ExpiresAt = &expires
	out := post(cases[0], func(b map[string]any) { b["expires_at"] = expires.Format(time.RFC3339) })
	want := map[string]any{"grant_id": f.grants.out.ID.String(), "organization_id": f.org.String(), "principal_id": f.target.String(),
		"role": "viewer", "expires_at": "2031-05-06T07:08:09Z", "status": "active", "revision": float64(1)}
	if !sameJSON(out, want) {
		t.Fatalf("create answer %v, want %v", out, want)
	}
	in := f.grants.create
	if in.OrgID != f.org || in.PrincipalID != f.target || in.Role != "viewer" || in.Key != "acme/api" || in.ExpiresAt == nil || !in.ExpiresAt.Equal(expires) ||
		in.Actor.PrincipalID == nil || *in.Actor.PrincipalID != f.admin || in.Actor.RequestID == nil {
		t.Fatalf("create was given %+v, want the input, the actor and a request id", in)
	}
	post(cases[1], func(b map[string]any) { b["expected_revision"] = 3; b["clear_expiry"] = true })
	if u := f.grants.update; u.GrantID != f.grants.out.ID || u.Role == nil || *u.Role != "admin" || !u.ClearExpiry || u.ExpectedRevision == nil || *u.ExpectedRevision != 3 {
		t.Fatalf("update was given %+v", u)
	}
	f.grants.out.RevokedAt = &expires
	if out := post(cases[2], func(b map[string]any) { b["expected_revision"] = 2 }); out["status"] != "revoked" {
		t.Fatalf("revoke answer %v, want status revoked", out)
	}
	if r := f.grants.revoke; r.GrantID != f.grants.out.ID || r.ExpectedRevision == nil || *r.ExpectedRevision != 2 || r.Actor.PrincipalID == nil {
		t.Fatalf("revoke was given %+v", r)
	}
}

// A body that is not exactly the route's members is a 400 and reaches no store.
func TestGrantRoutesRefuseMalformedBodies(t *testing.T) {
	f := newGrantFixture(t)
	for _, c := range grantCases() {
		mutations := map[string]func(map[string]any){
			"unknown member":   func(b map[string]any) { b["extra"] = 1 },
			"bad organization": func(b map[string]any) { b["organization_id"] = "not-a-uuid" },
			"bad actor":        func(b map[string]any) { b["actor_principal_id"] = nil },
		}
		if c.op == opGrantCreate {
			mutations["no role"] = func(b map[string]any) { delete(b, "role") }
			mutations["no key"] = func(b map[string]any) { delete(b, "resource_key") }
			mutations["bad expiry"] = func(b map[string]any) { b["expires_at"] = "tomorrow" }
		} else {
			mutations["bad grant"] = func(b map[string]any) { b["grant_id"] = "x" }
			mutations["bad revision"] = func(b map[string]any) { b["expected_revision"] = 0 }
		}
		for name, mutate := range mutations {
			body := c.body(f, f.admin)
			mutate(body)
			expectRefusal(t, f.post(c.path, encode(body), nil), http.StatusBadRequest, reasonMalformed)
			if f.grants.calls != 0 {
				t.Fatalf("%s %s reached the store", c.op, name)
			}
		}
	}
}

// Every store error has its literal answer, and no answer carries the key.
func TestGrantRoutesMapEveryStoreRefusal(t *testing.T) {
	f := newGrantFixture(t)
	for _, c := range []struct {
		err    error
		status int
		reason string
	}{
		{grant.ErrNotOwned, 409, "users_not_owned"}, {grant.ErrNotFound, 404, "grant_not_found"}, {grant.ErrOrgNotFound, 404, "organization_not_found"},
		{grant.ErrOrgSuspended, 409, "organization_suspended"}, {grant.ErrPrincipalNotFound, 404, "principal_not_found"},
		{grant.ErrNotMember, 409, "grantee_not_member"}, {grant.ErrAlreadyGranted, 409, "already_granted"}, {grant.ErrRevoked, 409, "grant_revoked"},
		{grant.ErrRevisionConflict, 409, "revision_conflict"}, {grant.ErrRoleInvalid, 400, "role_invalid"}, {grant.ErrKeyInvalid, 400, "resource_key_invalid"},
		{grant.ErrExpiryPast, 400, "expiry_not_future"}, {grant.ErrNothingToSet, 400, "nothing_to_change"}, {grant.ErrStore, 503, "store_unavailable"},
		{errors.New("boom"), 503, "store_unavailable"},
	} {
		f.grants.err = c.err
		for _, gc := range grantCases() {
			response := f.post(gc.path, encode(gc.body(f, f.admin)), nil)
			expectRefusal(t, response, c.status, c.reason)
			if strings.Contains(response.Body.String(), "acme/api") {
				t.Fatalf("%s answer carries the key: %s", c.err, response.Body)
			}
		}
	}
}

// The actor rule runs before the verb and again inside it: a viewer or an unknown actor is refused with no store call, an
// actor that stops being an admin between the two is refused by the verb's own Authorize with the rule's reason, and a
// grant that is not in the organization answers 403 to an actor who is no admin of it and 404 only once the rule passes.
func TestGrantRoutesRunTheActorRuleBeforeAndInsideTheVerb(t *testing.T) {
	f := newGrantFixture(t)
	for _, c := range grantCases() {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.viewer)), nil), http.StatusForbidden, "admin_required")
		expectRefusal(t, f.post(c.path, encode(c.body(f, uuid.New())), nil), http.StatusForbidden, "principal_deactivated")
	}
	if f.grants.calls != 0 {
		t.Fatalf("a refused actor reached the store %d times", f.grants.calls)
	}
	f.users.states[f.target] = UserState{IsActive: true, IsSuperuser: true}
	expectRefusal(t, f.post(PatternGrantCreate, encode(grantCases()[0].body(f, f.admin)), nil), http.StatusForbidden, "superuser_required")
	if f.grants.calls != 0 {
		t.Fatal("an admin granting to a superuser reached the store")
	}
	f.users.states[f.target] = UserState{IsActive: true}
	f.grants.err = grant.ErrNotFound
	expectRefusal(t, f.post(PatternGrantRevoke, encode(grantCases()[2].body(f, f.viewer)), nil), http.StatusForbidden, "admin_required")
	expectRefusal(t, f.post(PatternGrantRevoke, encode(grantCases()[2].body(f, f.admin)), nil), http.StatusNotFound, "grant_not_found")
	f.grants.err = nil
	f.users.inTx = &fakeProvUsers{states: f.users.states, roles: map[[2]uuid.UUID]string{{f.admin, f.org}: "viewer"}}
	before := f.grants.calls
	expectRefusal(t, f.post(PatternGrantCreate, encode(grantCases()[0].body(f, f.admin)), nil), http.StatusForbidden, "admin_required")
	if f.grants.calls != before+1 {
		t.Fatal("an actor demoted between the first read and the verb's transaction was refused before the verb ran its own Authorize")
	}
}

// The go-api workload token is taken by the three grant routes, as by the service account routes, and the operations are on the
// workload list.
func TestGrantRoutesTakeTheWorkloadToken(t *testing.T) {
	for _, op := range grantOps {
		if !workloadOperations[op] {
			t.Fatalf("%s is not on the workload list", op)
		}
	}
	w := newWorkloadIssueFixture(t)
	f := newProvFixtureFor(t, nil, w.workload)
	f.grants.principal = f.target
	f.grants.out = grant.Grant{ID: uuid.New(), OrgID: f.org, PrincipalID: f.target, Role: "viewer", Key: "acme/api", Origin: grant.OriginAPI, Revision: 1}
	header, _ := w.mint("workload+jwt", nil)
	for _, c := range grantCases() {
		if response := f.post(c.path, encode(c.body(f, f.super)), header); response.Code != http.StatusOK {
			t.Fatalf("%s with a workload token: %d %s", c.path, response.Code, response.Body)
		}
	}
	if f.grants.calls != 3 {
		t.Fatalf("the grant store was called %d times, want 3", f.grants.calls)
	}
}
