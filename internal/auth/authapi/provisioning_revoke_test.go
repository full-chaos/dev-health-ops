package authapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// fakeRevoker runs the revoke's Authorize on an empty transaction (the fake
// users never touch it), as the store does after its locks, then answers err.
type fakeRevoker struct {
	err   error
	calls int
	in    invite.RevokeInput
}

func (f *fakeRevoker) Revoke(_ context.Context, in invite.RevokeInput) error {
	f.calls++
	f.in = in
	if err := in.Authorize(context.Background(), audit.TxOps{}, in.OrgID); err != nil {
		return err
	}
	return f.err
}

// revoke posts body to the revoke handler on revoker, built from the
// fixture's collaborators as ProvisioningRoutes builds the mounted ones.
func (f *provFixture) revoke(revoker InviteRevoker, body map[string]any, header map[string]string) *httptest.ResponseRecorder {
	f.t.Helper()
	logger := slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewManualReader())).Meter("test")
	rec, err := newRecorder(logger, meter)
	if err != nil {
		f.t.Fatal(err)
	}
	uses, err := meter.Int64Counter("auth.provisioning.compat_credential_uses")
	if err != nil {
		f.t.Fatal(err)
	}
	h := provisioningHandlers{ProvisioningDeps: ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Logger: logger}, rec: rec, provisioningUses: uses}
	route := h.revokeRoute(revoker)
	if route.Method != http.MethodPost || route.Pattern != PatternInviteRevoke {
		f.t.Fatalf("revoke route = %s %s", route.Method, route.Pattern)
	}
	request := httptest.NewRequest(http.MethodPost, PatternInviteRevoke, strings.NewReader(encode(body)))
	if header == nil {
		header = map[string]string{"Authorization": "Bearer " + provisioningToken}
	}
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	requestid.Middleware(route.Handler).ServeHTTP(response, request)
	return response
}

func (f *provFixture) revokeBody(actor uuid.UUID, inviteID uuid.UUID) map[string]any {
	return map[string]any{"organization_id": f.org.String(), "invite_id": inviteID.String(), "actor_principal_id": actor.String()}
}

// The revoke route is mounted with the other provisioning routes and reaches
// the invite aggregate's revoke.
func TestInviteRevokeIsMounted(t *testing.T) {
	f := newProvFixture(t)
	id := uuid.New()
	response := f.post(PatternInviteRevoke, encode(f.revokeBody(f.owner, id)), nil)
	if response.Code != http.StatusOK || f.invites.revoke.InviteID != id {
		t.Fatalf("POST %s through the provisioning routes = %d %s, want 200 from the store's revoke", PatternInviteRevoke, response.Code, response.Body)
	}
	if got, ok := f.uses(opInviteRevoke); !ok || got != 1 {
		t.Fatalf("compat credential uses for %s = %d (%v), want 1", opInviteRevoke, got, ok)
	}
}

// A refused actor is refused before the invite is looked up: a missing or a
// no longer pending invite gets the same 403 as any other, and the store is
// not called.
func TestInviteRevokeRefusesARefusedActorBeforeAnyInviteLookup(t *testing.T) {
	for _, c := range []struct {
		name   string
		store  error
		reason string
	}{
		{"viewer, missing invite", invite.ErrNotFound, reasonAdminRequired},
		{"viewer, invite not pending", invite.ErrNotPending, reasonAdminRequired},
		{"inactive actor, missing invite", invite.ErrNotFound, reasonDeactivated},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newProvFixture(t)
			actor := f.viewer
			if c.reason == reasonDeactivated {
				actor = f.x
			}
			revoker := &fakeRevoker{err: c.store}
			expectRefusal(t, f.revoke(revoker, f.revokeBody(actor, uuid.New()), nil), http.StatusForbidden, c.reason)
			if revoker.calls != 0 || f.orgs.calls != 0 {
				t.Fatalf("a refused actor reached the store (%d revokes, %d projections)", revoker.calls, f.orgs.calls)
			}
		})
	}
}

// The answers: success (also a repeat, which the store reports as nil), and
// each store refusal by its code.
func TestInviteRevokeAnswers(t *testing.T) {
	f := newProvFixture(t)
	id := uuid.New()
	response := f.revoke(&fakeRevoker{}, f.revokeBody(f.owner, id), nil)
	if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != fmt.Sprintf(`{"invite_id":%q,"status":"revoked"}`, id.String()) {
		t.Fatalf("revoke: %d %s", response.Code, response.Body)
	}
	for err, want := range map[error]refusal{
		invite.ErrNotFound:                      {http.StatusNotFound, reasonInviteNotFound},
		invite.ErrNotPending:                    {http.StatusConflict, reasonInviteNotPending},
		invite.ErrDenied:                        {http.StatusForbidden, reasonAdminRequired},
		invite.ErrNotOwned:                      {http.StatusConflict, reasonUsersNotOwned},
		invite.ErrOrgNotFound:                   {http.StatusNotFound, reasonOrganizationNotFound},
		invite.ErrOrgSuspended:                  {http.StatusConflict, reasonOrganizationInactive},
		fmt.Errorf("%w: boom", invite.ErrStore): {http.StatusServiceUnavailable, reasonStoreUnavailable},
	} {
		expectRefusal(t, f.revoke(&fakeRevoker{err: err}, f.revokeBody(f.owner, uuid.New()), nil), want.status, want.reason)
	}
}

// What the store is given: the body's ids, the actor, the request id and an
// Authorize that runs the actor rule again (here it refuses a demoted actor).
func TestInviteRevokePassesTheRequestAndTheLockedActorRule(t *testing.T) {
	f := newProvFixture(t)
	revoker := &fakeRevoker{}
	id := uuid.New()
	if response := f.revoke(revoker, f.revokeBody(f.admin, id), nil); response.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", response.Code, response.Body)
	}
	in := revoker.in
	if in.OrgID != f.org || in.InviteID != id || in.Actor.PrincipalID == nil || *in.Actor.PrincipalID != f.admin || in.Actor.RequestID == nil ||
		in.Actor.Caller.CredentialID != "cred-1" || in.Actor.Caller.Workload != nil {
		t.Fatalf("the store got %+v", in)
	}
	f.users.roles[[2]uuid.UUID{f.admin, f.org}] = "viewer"
	if err := in.Authorize(context.Background(), audit.TxOps{}, f.org); !errors.Is(err, invite.ErrDenied) {
		t.Fatalf("Authorize after the demotion = %v, want ErrDenied", err)
	}
	if f.orgs.calls != 1 || f.orgs.last != f.org {
		t.Fatalf("the organization was projected %d times (%v), want once for %v", f.orgs.calls, f.orgs.last, f.org)
	}
}

// The body is strict and the bearer is checked first.
func TestInviteRevokeRefusesABadBearerOrBody(t *testing.T) {
	f := newProvFixture(t)
	revoker := &fakeRevoker{}
	expectRefusal(t, f.revoke(revoker, f.revokeBody(f.owner, uuid.New()), map[string]string{"Authorization": "Bearer nope"}),
		http.StatusUnauthorized, reasonInvalidCredentials)
	body := f.revokeBody(f.owner, uuid.New())
	body["reason"] = "x"
	expectRefusal(t, f.revoke(revoker, body, nil), http.StatusBadRequest, reasonMalformed)
	missing := f.revokeBody(f.owner, uuid.New())
	delete(missing, "invite_id")
	expectRefusal(t, f.revoke(revoker, missing, nil), http.StatusBadRequest, reasonMalformed)
	if revoker.calls != 0 {
		t.Fatal("a refused request reached the store")
	}
	if strings.Contains(f.logs.String(), provisioningToken) {
		t.Fatal("a log line carries the bearer")
	}
}
