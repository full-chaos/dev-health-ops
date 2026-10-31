package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func (f *fakeEnsurer) OnboardOrganization(_ context.Context, in identity.OnboardOrganizationRequest) (*identity.BornOrganization, error) {
	f.calls++
	f.onboard = in
	if f.bornErr != nil {
		return nil, f.bornErr
	}
	id := uuid.New()
	return &identity.BornOrganization{ID: f.bornID, MembershipID: &id, Slug: "born"}, nil
}

func (f *fakeEnsurer) CreateOrganization(ctx context.Context, in identity.CreateOrganizationRequest) (*identity.BornOrganization, error) {
	f.calls++
	f.create = in
	if err := in.Authorize(ctx, audit.TxOps{}); err != nil {
		return nil, err
	}
	if f.bornErr != nil {
		return nil, f.bornErr
	}
	return &identity.BornOrganization{ID: f.bornID, Slug: in.Slug}, nil
}

// newOrgFixture serves the routes over a document that holds their rows.
func newOrgFixture(t *testing.T) *provFixture {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	rows := document["rows"].([]any)
	for _, pattern := range []string{PatternOrgOnboard, PatternOrgCreate} {
		rows = append(rows, map[string]any{"service": AccessService, "method": "POST", "route": pattern,
			"accepted_credential_classes": []any{serviceSessionIssuerClass}})
	}
	document["rows"] = rows
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return newProvFixtureWith(t, raw)
}

func onboardBody(principal uuid.UUID) string {
	return `{"principal_id":"` + principal.String() + `","organization_name":"Acme","description":"Organization created during onboarding","request_metadata":{"ip_address":"192.0.2.1"}}`
}

func createBody(actor uuid.UUID, extra string) string {
	return `{"actor_principal_id":"` + actor.String() + `","organization_name":"Acme","slug":"acme","tier":"team","managed_by":"manual"` + extra + `}`
}

func TestOrgCreateRoutesRefusals(t *testing.T) {
	failed := errors.New("database down")
	for _, c := range []struct {
		name, path string
		body       func(f *provFixture) string
		bornErr    error
		status     int
		reason     string
		store      int
	}{
		{"onboard malformed", PatternOrgOnboard, func(*provFixture) string { return `{"organization_name":"Acme"}` }, nil, 400, reasonMalformed, 0},
		{"onboard blank name", PatternOrgOnboard, func(f *provFixture) string {
			return `{"principal_id":"` + f.target.String() + `","organization_name":"   "}`
		}, nil, 422, reasonNameInvalid, 0},
		{"onboard long name", PatternOrgOnboard, func(f *provFixture) string {
			return `{"principal_id":"` + f.target.String() + `","organization_name":"` + strings.Repeat("a", 101) + `"}`
		}, nil, 422, reasonNameInvalid, 0},
		{"onboard member", PatternOrgOnboard, func(f *provFixture) string { return onboardBody(f.target) }, identity.ErrOrganizationMember, 409, reasonAlreadyMember, 1},
		{"onboard inactive", PatternOrgOnboard, func(f *provFixture) string { return onboardBody(f.target) }, identity.ErrAccountInactive, 403, reasonDeactivated, 1},
		{"onboard unknown", PatternOrgOnboard, func(f *provFixture) string { return onboardBody(f.target) }, identity.ErrUserNotFound, 403, reasonDeactivated, 1},
		{"onboard collision", PatternOrgOnboard, func(f *provFixture) string { return onboardBody(f.target) }, identity.ErrOrganizationSlugCollision, 409, reasonOrganizationConflict, 1},
		{"onboard not owned", PatternOrgOnboard, func(f *provFixture) string { return onboardBody(f.target) }, identity.ErrSignupNotAvailable, 409, reasonUsersNotOwned, 1},
		{"onboard store down", PatternOrgOnboard, func(f *provFixture) string { return onboardBody(f.target) }, failed, 503, reasonStoreUnavailable, 1},
		{"create admin refused first", PatternOrgCreate, func(f *provFixture) string { return createBody(f.admin, "") }, nil, 403, reasonSuperuserRequired, 0},
		{"create malformed", PatternOrgCreate, func(f *provFixture) string { return `{"organization_name":"Acme"}` }, nil, 400, reasonMalformed, 0},
		{"create settings not an object", PatternOrgCreate, func(f *provFixture) string { return createBody(f.super, `,"settings":[1]`) }, nil, 400, reasonMalformed, 0},
		{"create owner unknown", PatternOrgCreate, func(f *provFixture) string { return createBody(f.super, "") }, identity.ErrUserNotFound, 404, reasonPrincipalNotFound, 1},
		{"create collision", PatternOrgCreate, func(f *provFixture) string { return createBody(f.super, "") }, identity.ErrOrganizationSlugCollision, 409, reasonOrganizationConflict, 1},
		{"create store down", PatternOrgCreate, func(f *provFixture) string { return createBody(f.super, "") }, failed, 503, reasonStoreUnavailable, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newOrgFixture(t)
			f.orgs.bornErr = c.bornErr
			got := f.post(c.path, c.body(f), nil)
			if got.Code != c.status || reasonOf(t, got) != c.reason || f.orgs.calls != c.store {
				t.Fatalf("answer %d %s, store calls %d; want %d %s, %d", got.Code, got.Body.String(), f.orgs.calls, c.status, c.reason, c.store)
			}
		})
	}
}

func TestOrgCreateRoutesAnswer(t *testing.T) {
	f := newOrgFixture(t)
	f.orgs.bornID = uuid.New()
	got := f.post(PatternOrgOnboard, onboardBody(f.target), nil)
	if got.Code != http.StatusOK || !strings.Contains(got.Body.String(), f.orgs.bornID.String()) {
		t.Fatalf("onboard: %d %s", got.Code, got.Body.String())
	}
	if in := f.orgs.onboard; in.Principal != f.target || in.Name != "Acme" || in.Description == nil || !strings.Contains(string(in.RequestMetadata), "192.0.2.1") {
		t.Fatalf("onboard request %+v", in)
	}
	owner := uuid.New()
	got = f.post(PatternOrgCreate, createBody(f.super, `,"organization_description":"d","settings":{"a":1},"owner_principal_id":"`+owner.String()+`"`), nil)
	if got.Code != http.StatusOK {
		t.Fatalf("create: %d %s", got.Code, got.Body.String())
	}
	in := f.orgs.create
	if in.LockRoles == nil {
		t.Fatal("the create takes no role write lock")
	}
	if in.Actor != f.super || in.Slug != "acme" || in.Name != "Acme" || in.Tier != "team" || in.ManagedBy != "manual" ||
		in.Owner == nil || *in.Owner != owner || in.Description == nil || *in.Description != "d" || string(in.Settings) != `{"a":1}` {
		t.Fatalf("create request %+v", in)
	}
}

// The actor rule runs again on the verb's transaction: a demotion after the
// precheck is refused there.
func TestOrgCreateSuperuserRuleRunsOnTheTx(t *testing.T) {
	f := newOrgFixture(t)
	f.users.inTx = &fakeProvUsers{states: map[uuid.UUID]UserState{f.super: {IsActive: true}}}
	got := f.post(PatternOrgCreate, createBody(f.super, ""), nil)
	if got.Code != http.StatusForbidden || reasonOf(t, got) != reasonSuperuserRequired {
		t.Fatalf("answer %d %s", got.Code, got.Body.String())
	}
}

func TestOnboardRouteRefusesTierSlugSettings(t *testing.T) {
	for _, member := range []string{`"tier":"enterprise"`, `"slug":"mine"`, `"settings":{}`, `"managed_by":"manual"`, `"owner_principal_id":"` + uuid.NewString() + `"`} {
		f := newOrgFixture(t)
		got := f.post(PatternOrgOnboard, `{"principal_id":"`+f.target.String()+`","organization_name":"Acme",`+member+`}`, nil)
		if got.Code != http.StatusBadRequest || f.orgs.calls != 0 {
			t.Fatalf("%s: %d %s, store calls %d", member, got.Code, got.Body.String(), f.orgs.calls)
		}
	}
}

// The routes stay dark in a build whose document has no rows for them: a row's
// source line must exist at the document's source_commit.
func TestOrgCreateRoutesAreDarkWithoutTheirRows(t *testing.T) {
	f := newProvFixture(t)
	for _, c := range []struct{ path, body string }{{PatternOrgOnboard, onboardBody(f.target)}, {PatternOrgCreate, createBody(f.super, "")}} {
		if got := f.post(c.path, c.body, nil); got.Code != http.StatusNotFound || f.orgs.calls != 0 {
			t.Fatalf("%s: %d %s, store calls %d; want 404 and none", c.path, got.Code, got.Body.String(), f.orgs.calls)
		}
	}
}
