package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
)

// fakeServiceAccounts answers Lookup from one account and runs each verb's Authorize on an empty transaction with the
// locked state a store would give it, as the store does after its locks.
type fakeServiceAccounts struct {
	info      serviceaccount.Info
	err       error
	lookupErr error
	calls     int
	lookups   int
	verb      string
	create    serviceaccount.CreateInput
	actor     serviceaccount.Actor
	newOwner  uuid.UUID
	locked    serviceaccount.Locked
	fakeKeys
}

func (f *fakeServiceAccounts) run(verb string, a serviceaccount.Actor, l serviceaccount.Locked) error {
	f.calls++
	f.verb, f.actor, f.locked = verb, a, l
	if a.Authorize == nil {
		return serviceaccount.ErrDenied
	}
	if err := a.Authorize(context.Background(), audit.TxOps{}, l); err != nil {
		return err
	}
	return f.err
}

func (f *fakeServiceAccounts) Create(_ context.Context, in serviceaccount.CreateInput, a serviceaccount.Actor) (uuid.UUID, error) {
	f.create = in
	if err := f.run("create", a, serviceaccount.Locked{OrgID: in.OrgID}); err != nil {
		return uuid.Nil, err
	}
	return f.info.ID, nil
}

func (f *fakeServiceAccounts) Disable(_ context.Context, _ uuid.UUID, a serviceaccount.Actor) error {
	return f.run("disable", a, serviceaccount.Locked{OrgID: f.info.OrgID, Owner: f.info.Owner})
}

func (f *fakeServiceAccounts) Enable(_ context.Context, _ uuid.UUID, a serviceaccount.Actor) error {
	return f.run("enable", a, serviceaccount.Locked{OrgID: f.info.OrgID, Owner: f.info.Owner})
}

func (f *fakeServiceAccounts) SetOwner(_ context.Context, _ uuid.UUID, newOwner uuid.UUID, a serviceaccount.Actor) error {
	f.newOwner = newOwner
	return f.run("owner", a, serviceaccount.Locked{OrgID: f.info.OrgID, Owner: f.info.Owner})
}

func (f *fakeServiceAccounts) Lookup(_ context.Context, id uuid.UUID) (*serviceaccount.Info, error) {
	f.lookups++
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	if id != f.info.ID {
		return nil, serviceaccount.ErrNotFound
	}
	info := f.info
	return &info, nil
}

func (f *provFixture) account() { // the account the disable, enable and owner routes name
	f.sa.info = serviceaccount.Info{ID: uuid.New(), OrgID: f.org, Name: "ci-bot", Active: true, Owner: f.owner, OwnerEnabled: true, OwnerMember: true}
	seedKey(f)
}

func serviceAccountCases() []routeCase {
	return []routeCase{
		{opServiceAccountCreate, PatternServiceAccountCreate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String(), "owner_principal_id": f.target.String(), "name": "ci-bot"}
		}, []string{"organization_id", "actor_principal_id", "owner_principal_id", "name"}},
		{opServiceAccountDisable, PatternServiceAccountDisable, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"service_account_id": f.sa.info.ID.String(), "actor_principal_id": a.String()}
		}, []string{"service_account_id", "actor_principal_id"}},
		{opServiceAccountEnable, PatternServiceAccountEnable, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"service_account_id": f.sa.info.ID.String(), "actor_principal_id": a.String()}
		}, []string{"service_account_id", "actor_principal_id"}},
		{opServiceAccountOwner, PatternServiceAccountOwner, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"service_account_id": f.sa.info.ID.String(), "actor_principal_id": a.String(), "owner_principal_id": f.target.String()}
		}, []string{"service_account_id", "actor_principal_id", "owner_principal_id"}},
	}
}

func TestServiceAccountActorRule(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	for _, c := range serviceAccountCases() {
		for name, tc := range map[string]struct {
			actor  uuid.UUID
			reason string
		}{
			"unknown actor":      {uuid.New(), reasonDeactivated},
			"inactive superuser": {f.x, reasonDeactivated},
			"viewer of the org":  {f.viewer, reasonAdminRequired},
			"member of no org":   {f.target, reasonAdminRequired},
		} {
			before := f.storeCalls()
			expectRefusal(t, f.post(c.path, encode(c.body(f, tc.actor)), nil), http.StatusForbidden, tc.reason)
			if f.storeCalls() != before {
				t.Fatalf("%s %s: a refused actor reached the verb", c.op, name)
			}
		}
		for _, actor := range []uuid.UUID{f.admin, f.owner, f.super} {
			if response := f.post(c.path, encode(c.body(f, actor)), nil); response.Code != http.StatusOK {
				t.Fatalf("%s by an allowed actor: %d %s", c.op, response.Code, response.Body)
			}
		}
	}
}

func TestServiceAccountActorRuleRunsAgainUnderTheLocks(t *testing.T) {
	f := newProvFixture(t)
	f.users.inTx = &fakeProvUsers{states: f.users.states, roles: map[[2]uuid.UUID]string{{f.admin, f.org}: "viewer"}}
	for _, c := range serviceAccountCases() {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonAdminRequired)
	}
	f.users.inTx = &fakeProvUsers{err: errors.New("read failed")}
	for _, c := range serviceAccountCases() {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
	}
}

// A non-superuser actor may not name a superuser as an owner (create, owner), nor move an account off one (owner):
// the rule reads the new owner and, for an owner change, the owner the verb locked. Disable and enable name no owner.
func TestServiceAccountTargetsRule(t *testing.T) {
	f := newProvFixture(t)
	cases := serviceAccountCases()
	f.users.states[f.target] = UserState{IsActive: true, IsSuperuser: true}
	for _, c := range []routeCase{cases[0], cases[3]} {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s naming a superuser by a superuser: %d %s", c.op, response.Code, response.Body)
		}
	}
	// The owner the verb locked can differ from the one the first read saw: the rule reads it again under the locks.
	f.users.states[f.target] = UserState{IsActive: true}
	promoted := map[uuid.UUID]UserState{}
	for k, v := range f.users.states {
		promoted[k] = v
	}
	promoted[f.owner] = UserState{IsActive: true, IsSuperuser: true}
	f.users.inTx = &fakeProvUsers{states: promoted, roles: f.users.roles}
	before := f.sa.calls
	expectRefusal(t, f.post(cases[3].path, encode(cases[3].body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
	if f.sa.calls != before+1 || f.sa.locked.Owner != f.owner {
		t.Fatalf("the owner change was refused without the verb reading the locked owner %s (locked %+v)", f.owner, f.sa.locked)
	}
	f.users.inTx = nil
	// A new owner promoted after the first read is refused by the rule under the locks.
	f.users.states[f.owner] = UserState{IsActive: true}
	promotedTarget := map[uuid.UUID]UserState{}
	for k, v := range f.users.states {
		promotedTarget[k] = v
	}
	promotedTarget[f.target] = UserState{IsActive: true, IsSuperuser: true}
	f.users.inTx = &fakeProvUsers{states: promotedTarget, roles: f.users.roles}
	for _, c := range []routeCase{cases[0], cases[3]} {
		before = f.sa.calls
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
		if f.sa.calls != before+1 {
			t.Fatalf("%s: a new owner promoted after the first read was refused before the verb's locks", c.op)
		}
	}
	f.users.inTx = nil
	// The first read refuses a superuser owner before any verb runs.
	f.users.states[f.owner] = UserState{IsActive: true, IsSuperuser: true}
	before = f.sa.calls
	expectRefusal(t, f.post(cases[3].path, encode(cases[3].body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
	if f.sa.calls != before {
		t.Fatal("a superuser-owned account reached the owner verb for a non-superuser actor")
	}
	for _, c := range []routeCase{cases[1], cases[2]} {
		if response := f.post(c.path, encode(c.body(f, f.admin)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s of a superuser-owned account by an admin: %d %s", c.op, response.Code, response.Body)
		}
	}
	if response := f.post(cases[3].path, encode(cases[3].body(f, f.super)), nil); response.Code != http.StatusOK {
		t.Fatalf("owner change off a superuser by a superuser: %d", response.Code)
	}
}

func TestServiceAccountUnknownAccountIsNamedOnlyToASuperuser(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	for _, c := range serviceAccountCases()[1:] {
		body := func(actor uuid.UUID) string {
			m := c.body(f, actor)
			m["service_account_id"] = uuid.New().String()
			return encode(m)
		}
		for name, tc := range map[string]struct {
			actor  uuid.UUID
			status int
			reason string
		}{
			"admin of the org": {f.admin, http.StatusForbidden, reasonAdminRequired},
			"owner of the org": {f.owner, http.StatusForbidden, reasonAdminRequired},
			"inactive":         {f.x, http.StatusForbidden, reasonDeactivated},
			"unknown actor":    {uuid.New(), http.StatusForbidden, reasonDeactivated},
			"superuser":        {f.super, http.StatusNotFound, reasonServiceAccountNotFound},
		} {
			before := f.sa.calls
			expectRefusal(t, f.post(c.path, body(tc.actor), nil), tc.status, tc.reason)
			if f.sa.calls != before {
				t.Fatalf("%s %s: an unknown account reached a verb", c.op, name)
			}
		}
	}
	f.sa.lookupErr = errors.New("read failed")
	for _, c := range serviceAccountCases()[1:] {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
	}
}

func TestServiceAccountAnswersAndWhatTheVerbIsGiven(t *testing.T) {
	f := newProvFixture(t)
	decode := func(c routeCase, actor uuid.UUID) map[string]any {
		t.Helper()
		response := f.post(c.path, encode(c.body(f, actor)), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", c.op, response.Code, response.Body)
		}
		var out map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	cases := serviceAccountCases()
	out := decode(cases[0], f.admin)
	want := map[string]any{"service_account_id": f.sa.info.ID.String(), "organization_id": f.org.String(), "name": "ci-bot", "status": "active",
		"owner_principal_id": f.target.String()}
	if !sameJSON(out, want) {
		t.Fatalf("create answer %v, want %v", out, want)
	}
	if f.sa.create.OrgID != f.org || f.sa.create.Name != "ci-bot" || f.sa.create.Owner != f.target || f.sa.actor.PrincipalID == nil || *f.sa.actor.PrincipalID != f.admin ||
		f.sa.actor.RequestID == nil {
		t.Fatalf("create was given %+v actor %+v, want the input, the actor and a request id", f.sa.create, f.sa.actor)
	}
	if out := decode(cases[1], f.admin); !sameJSON(out, map[string]any{"service_account_id": f.sa.info.ID.String(), "status": "disabled"}) {
		t.Fatalf("disable answer %v", out)
	}
	if out := decode(cases[2], f.admin); !sameJSON(out, map[string]any{"service_account_id": f.sa.info.ID.String(), "status": "active"}) {
		t.Fatalf("enable answer %v", out)
	}
	if out := decode(cases[3], f.admin); !sameJSON(out, map[string]any{"service_account_id": f.sa.info.ID.String(), "owner_principal_id": f.target.String()}) ||
		f.sa.newOwner != f.target || f.sa.verb != "owner" {
		t.Fatalf("owner answer %v, verb %q owner %s", out, f.sa.verb, f.sa.newOwner)
	}
}

func sameJSON(a, b map[string]any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

func TestServiceAccountRefusalAnswers(t *testing.T) {
	f := newProvFixture(t)
	for err, want := range map[error]struct {
		status int
		reason string
	}{
		serviceaccount.ErrNotOwned:       {409, reasonUsersNotOwned},
		serviceaccount.ErrNotFound:       {404, "service_account_not_found"},
		serviceaccount.ErrNotAccount:     {404, "service_account_not_found"},
		serviceaccount.ErrOrgNotFound:    {404, reasonOrganizationNotFound},
		serviceaccount.ErrOrgNotActive:   {409, reasonOrganizationInactive},
		serviceaccount.ErrNameInvalid:    {400, "name_invalid"},
		serviceaccount.ErrNameTaken:      {409, "name_taken"},
		serviceaccount.ErrOwnerNotUser:   {409, "owner_invalid"},
		serviceaccount.ErrOwnerDisabled:  {409, "owner_invalid"},
		serviceaccount.ErrOwnerNotMember: {409, "owner_not_member"},
		serviceaccount.ErrAlreadyInState: {409, "already_in_state"},
		serviceaccount.ErrStateChanged:   {409, reasonStateChanged},
		serviceaccount.ErrStore:          {503, reasonStoreUnavailable},
		errors.New("anything else"):      {503, reasonStoreUnavailable},
	} {
		f.sa.err = err
		for _, c := range serviceAccountCases() {
			expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), want.status, want.reason)
		}
	}
	f.sa.err = nil
}

// The routes take the go-api workload token (its audience and configured subject) as the other workload operations do,
// hand the verb the workload principal and the token's jti as the caller, and refuse a token of any other subject.
func TestServiceAccountRoutesTakeTheGoAPIWorkloadTokenAndNoOtherSubject(t *testing.T) {
	f := newProvWorkloadFixture(t)
	want := uuidOfWire(t, f.w.subject)
	for _, c := range serviceAccountCases() {
		if !workloadOperations[c.op] {
			t.Fatalf("%s is not a workload operation", c.op)
		}
		if classes := admittedClasses(c.path, true); !slices.Equal(slices.Sorted(slices.Values(classes)), []string{serviceSessionIssuerClass, workloadTokenClass}) {
			t.Fatalf("%s admits %v, want the session issuer credential and the workload token", c.op, classes)
		}
		header, jti := f.w.mint("workload+jwt", nil)
		if response := f.post(c.path, encode(c.body(f.provFixture, f.super)), header); response.Code != http.StatusOK {
			t.Fatalf("%s with the workload token: %d %s", c.op, response.Code, response.Body)
		}
		caller := f.sa.actor.Caller
		if caller.Workload == nil || *caller.Workload != want || caller.JTI != jti || caller.CredentialID != "" {
			t.Fatalf("%s: caller = %+v, want workload %s and jti %s", c.op, caller, want, jti)
		}
		before := f.sa.calls
		wrong := issuePreKeyRefusals()["wrong subject"](f.w)
		expectRefusal(t, f.post(c.path, encode(c.body(f.provFixture, f.super)), wrong), http.StatusUnauthorized, reasonInvalidCredentials)
		if f.sa.calls != before {
			t.Fatalf("%s: a token of another subject reached the verb", c.op)
		}
	}
}

// The routes are dark until their rows ship: not on the startup gate, mounted only with a row listing every class they
// admit, each left out with one log line naming it, and the service starts either way.
func TestServiceAccountRoutesMountOnlyWithTheirRow(t *testing.T) {
	for _, c := range serviceAccountCases() {
		if slices.Contains(provisioningPatterns, c.path) {
			t.Fatalf("%s is on the startup gate before its row ships", c.path)
		}
		if provisioningRouteOps[c.op] != c.path {
			t.Fatalf("%s is not in the route operations", c.op)
		}
	}
	// No row: every route answers 404, one line names each, and the other routes still serve.
	f := newProvFixtureWith(t, docWithRows(t, PatternOrgSuspend))
	for _, c := range serviceAccountCases() {
		before := f.sa.calls
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusNotFound {
			t.Fatalf("%s with no row: %d, want 404", c.path, response.Code)
		}
		if f.sa.calls != before || !strings.Contains(f.logs.String(), `"route":"`+c.path+`"`) {
			t.Fatalf("%s with no row reached the verb or logged no not-mounted line", c.path)
		}
	}
	if response := f.post(PatternOrgSuspend, encode(routeCases()[8].body(f, f.super)), nil); response.Code != http.StatusOK {
		t.Fatalf("another provisioning route with the service account rows missing: %d", response.Code)
	}
	// One row: only that route is served.
	one := newProvFixtureWith(t, docWithRows(t, PatternOrgSuspend, PatternServiceAccountEnable))
	for _, c := range serviceAccountCases() {
		want := http.StatusNotFound
		if c.path == PatternServiceAccountEnable {
			want = http.StatusOK
		}
		if response := one.post(c.path, encode(c.body(one, one.super)), nil); response.Code != want {
			t.Fatalf("%s with only the enable row: %d, want %d", c.path, response.Code, want)
		}
	}
	// With the go-api workload path on, a row that lists only the session issuer class leaves the route out.
	var document map[string]any
	if err := json.Unmarshal(docWithRow(t, PatternOrgSuspend), &document); err != nil {
		t.Fatal(err)
	}
	for _, item := range document["rows"].([]any) {
		if row := item.(map[string]any); slices.Contains(serviceAccountPatterns, row["route"].(string)) {
			row["accepted_credential_classes"] = []any{serviceSessionIssuerClass}
		}
	}
	issuerOnly, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	w := newWorkloadIssueFixture(t)
	g := newProvFixtureFor(t, issuerOnly, w.workload)
	for _, c := range serviceAccountCases() {
		if response := g.post(c.path, encode(c.body(g, g.super)), nil); response.Code != http.StatusNotFound {
			t.Fatalf("%s with a row for the issuer class only and the workload path on: %d, want 404", c.path, response.Code)
		}
	}
}
