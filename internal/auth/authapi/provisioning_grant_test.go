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

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
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

// docWithGrantRows is the embedded endpoint-profile document with a row for each grant route, copied from the deprovision row.
func docWithGrantRows(t testing.TB) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	var source map[string]any
	for _, item := range rows {
		if row := item.(map[string]any); row["route"] == PatternOrgDeprovision && row["service"] == AccessService {
			source = row
		}
	}
	if source == nil {
		t.Fatal("no deprovision row in the embedded document")
	}
	for _, route := range grantPatterns {
		clone := map[string]any{}
		for k, v := range source {
			clone[k] = v
		}
		clone["route"] = route
		clone["id"] = "POST " + route + " [dev-health-auth-service @api]"
		rows = append(rows, clone)
	}
	document["rows"] = rows
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func newGrantFixture(t *testing.T) *provFixture {
	t.Helper()
	f := newProvFixtureFor(t, docWithGrantRows(t), nil)
	f.grants.principal = f.target
	f.grants.out = grant.Grant{ID: uuid.New(), OrgID: f.org, PrincipalID: f.target, Role: "viewer", Key: "acme/api", Origin: grant.OriginAPI, Revision: 1}
	return f
}

type grantCase struct {
	op, path string
	body     func(f *provFixture, actor uuid.UUID) map[string]any
}

func grantCases() []grantCase {
	return []grantCase{
		{opGrantCreate, PatternGrantCreate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "principal_id": f.target.String(),
				"role": "viewer", "resource_key": "acme/api"}
		}},
		{opGrantUpdate, PatternGrantUpdate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "grant_id": f.grants.out.ID.String(), "role": "admin"}
		}},
		{opGrantRevoke, PatternGrantRevoke, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "grant_id": f.grants.out.ID.String()}
		}},
	}
}

// The grant routes are dark: not on the startup gate, mounted only when the profile document holds a row for each, and one
// log line names each route that is not mounted.
func TestGrantRoutesMountOnlyOnTheirRows(t *testing.T) {
	for _, pattern := range grantPatterns {
		if slices.Contains(provisioningPatterns, pattern) {
			t.Fatalf("%s is on the startup gate; the grant routes are dark until their rows ship", pattern)
		}
	}
	bare := newProvFixtureFor(t, nil, nil)
	for _, c := range grantCases() {
		if response := bare.post(c.path, encode(c.body(bare, bare.super)), nil); response.Code != http.StatusNotFound {
			t.Errorf("%s over the embedded document: %d, want 404 (no route)", c.path, response.Code)
		}
		if !strings.Contains(bare.logs.String(), `"route":"`+c.path+`"`) {
			t.Errorf("no log line names the unmounted route %s", c.path)
		}
	}
	if bare.grants.calls != 0 {
		t.Fatal("an unmounted route reached the store")
	}
	f := newGrantFixture(t)
	for _, c := range grantCases() {
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Errorf("%s over a document with its row: %d %s", c.path, response.Code, response.Body)
		}
	}
}

// Each route reads a strict body and answers the store's result; the verbs get the request's members.
func TestGrantRoutesPassTheRequestAndAnswerTheGrant(t *testing.T) {
	f := newGrantFixture(t)
	expires := time.Date(2031, 5, 6, 7, 8, 9, 0, time.UTC)
	post := func(c grantCase, mutate func(map[string]any)) map[string]any {
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

// A go-api workload token is refused on the grant routes until their operations are listed.
func TestGrantRoutesTakeNoWorkloadToken(t *testing.T) {
	for _, op := range grantOps {
		if workloadOperations[op] {
			t.Fatalf("%s is on the workload list", op)
		}
	}
	w := newWorkloadIssueFixture(t)
	f := newProvFixtureFor(t, docWithGrantRows(t), w.workload)
	header, _ := w.mint("workload+jwt", nil)
	for _, c := range grantCases() {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), header), http.StatusUnauthorized, reasonInvalidCredentials)
	}
	if f.grants.calls != 0 {
		t.Fatal("a workload token reached the grant store")
	}
}

// No answer of the three routes carries the repository key: not as a member, not anywhere in the body.
func TestGrantRoutesAnswersNeverCarryTheKey(t *testing.T) {
	f := newGrantFixture(t)
	for _, c := range grantCases() {
		response := f.post(c.path, encode(c.body(f, f.admin)), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", c.path, response.Code, response.Body)
		}
		body := response.Body.String()
		if strings.Contains(body, "acme/api") || strings.Contains(body, "resource_key") {
			t.Fatalf("%s answer carries the repository key: %s", c.path, body)
		}
	}
}

// The status of an answer is revoked first, then expired, else active: a role-only update of an expired grant answers expired,
// a revoke of an expired grant answers revoked, a grant without an expiry or with one in the future is active.
func TestGrantAnswerStatusIsRevokedThenExpiredThenActive(t *testing.T) {
	f := newGrantFixture(t)
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	cases := grantCases()
	for _, c := range []struct {
		name    string
		route   grantCase
		expires *time.Time
		revoked *time.Time
		want    string
	}{
		{"create, no expiry", cases[0], nil, nil, "active"},
		{"create, future expiry", cases[0], &future, nil, "active"},
		{"update, role only, expired grant", cases[1], &past, nil, "expired"},
		{"update, expired grant, live again", cases[1], &future, nil, "active"},
		{"revoke, expired grant", cases[2], &past, &past, "revoked"},
		{"revoke, live grant", cases[2], nil, &past, "revoked"},
	} {
		f.grants.out.ExpiresAt, f.grants.out.RevokedAt = c.expires, c.revoked
		response := f.post(c.route.path, encode(c.route.body(f, f.admin)), nil)
		var out map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil || response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", c.name, response.Code, response.Body)
		}
		if out["status"] != c.want {
			t.Errorf("%s: status %v, want %s", c.name, out["status"], c.want)
		}
	}
}
