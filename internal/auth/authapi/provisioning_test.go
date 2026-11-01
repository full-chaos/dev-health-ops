package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const provisioningToken = "svc_authsess_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

type fakeScopeCredentials struct {
	refusal servicecred.Refusal
	err     error
	scopes  []string
}

func (f *fakeScopeCredentials) CheckScope(_ context.Context, token contracts.SecretString, scope string) (servicecred.Credential, servicecred.Refusal, error) {
	f.scopes = append(f.scopes, scope)
	if f.err != nil {
		return servicecred.Credential{}, "", f.err
	}
	if f.refusal != "" || token.Reveal() != provisioningToken {
		refusal := f.refusal
		if refusal == "" {
			refusal = servicecred.RefusedUnknown
		}
		return servicecred.Credential{}, refusal, nil
	}
	return servicecred.Credential{ID: "cred-1"}, "", nil
}

// fakeProvUsers answers the actor and target reads. inTx, when set, answers
// the reads made inside a verb's transaction (an audit.TxOps queryer).
type fakeProvUsers struct {
	states map[uuid.UUID]UserState
	roles  map[[2]uuid.UUID]string
	inTx   *fakeProvUsers
	err    error
	reads  int
	// roleReads counts RoleOn alone: a superuser-only route never reads a role.
	roleReads int
}

func (f *fakeProvUsers) pick(q session.Queryer) *fakeProvUsers {
	if _, tx := q.(audit.TxOps); tx && f.inTx != nil {
		return f.inTx
	}
	return f
}

func (f *fakeProvUsers) StateOn(_ context.Context, q session.Queryer, id uuid.UUID) (UserState, bool, error) {
	u := f.pick(q)
	f.reads++
	if u.err != nil {
		return UserState{}, false, u.err
	}
	s, ok := u.states[id]
	return s, ok, nil
}

func (f *fakeProvUsers) RoleOn(_ context.Context, q session.Queryer, user, org uuid.UUID) (*string, bool, error) {
	u := f.pick(q)
	f.reads++
	f.roleReads++
	if u.err != nil {
		return nil, false, u.err
	}
	role, ok := u.roles[[2]uuid.UUID{user, org}]
	if !ok {
		return nil, false, nil
	}
	return &role, true, nil
}

type noRows struct{}

func (noRows) QueryRow(context.Context, string, ...any) pgx.Row { return nil }

type fakeEnsurer struct {
	err     error
	calls   int
	last    uuid.UUID
	bornErr error
	bornID  uuid.UUID
	onboard identity.OnboardOrganizationRequest
	create  identity.CreateOrganizationRequest
}

func (f *fakeEnsurer) EnsureOrganization(_ context.Context, orgID uuid.UUID) error {
	f.calls++
	f.last = orgID
	return f.err
}

// fakeMembers runs the change's Authorize on an empty transaction (the fake
// users never touch it), as the store does after its locks.
type fakeMembers struct {
	err      error
	calls    int
	change   membership.Change
	transfer membership.Transfer
}

func (f *fakeMembers) run(c membership.Change) (*membership.Membership, error) {
	f.calls++
	f.change = c
	if err := c.Authorize(context.Background(), audit.TxOps{}, membership.Locked{OrgID: c.OrgID, Principals: []uuid.UUID{c.PrincipalID}}); err != nil {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	role, joined := c.Role, time.Date(2026, 10, 29, 9, 0, 0, 0, time.UTC)
	return &membership.Membership{ID: uuid.New(), OrgID: c.OrgID, PrincipalID: c.PrincipalID, Role: &role, Status: membership.StatusActive,
		JoinedAt: &joined, Revision: 3}, nil
}

func (f *fakeMembers) Add(_ context.Context, c membership.Change) (*membership.Membership, error) {
	return f.run(c)
}

func (f *fakeMembers) ChangeRole(_ context.Context, c membership.Change) (*membership.Membership, error) {
	return f.run(c)
}

func (f *fakeMembers) Remove(_ context.Context, c membership.Change) (*membership.Membership, error) {
	return f.run(c)
}

func (f *fakeMembers) TransferOwnership(_ context.Context, t membership.Transfer) error {
	f.calls++
	f.transfer = t
	if err := t.Authorize(context.Background(), audit.TxOps{}, membership.Locked{OrgID: t.OrgID, Principals: []uuid.UUID{t.From, t.To}}); err != nil {
		return err
	}
	return f.err
}

// fakeTokenID is a token-shaped id built at run time: a test fixture, never a credential.
var fakeTokenID = strings.Repeat("0", 32)

var createdToken = fakeTokenID + ".SENTINEL-INVITE-TOKEN"

type fakeInvites struct {
	err    error
	calls  int
	create invite.CreateInput
	accept invite.AcceptInput
	revoke invite.RevokeInput
}

func (f *fakeInvites) Revoke(_ context.Context, in invite.RevokeInput) error {
	f.calls++
	f.revoke = in
	if err := in.Authorize(context.Background(), audit.TxOps{}, in.OrgID); err != nil {
		return err
	}
	return f.err
}

func (f *fakeInvites) Create(_ context.Context, in invite.CreateInput) (*invite.Created, error) {
	f.calls++
	f.create = in
	if err := in.Authorize(context.Background(), audit.TxOps{}, in.OrgID); err != nil {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	return &invite.Created{ID: uuid.New(), Token: createdToken, ExpiresAt: time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)}, nil
}

func (f *fakeInvites) Accept(_ context.Context, in invite.AcceptInput) (*invite.Accepted, error) {
	f.calls++
	f.accept = in
	if f.err != nil {
		return nil, f.err
	}
	return &invite.Accepted{InviteID: uuid.New(), OrgID: uuid.New(), MembershipID: uuid.New(), Role: "member"}, nil
}

type fakeDeprovision struct {
	err   error
	calls int
	in    orgdeprovision.Input
}

func (f *fakeDeprovision) Deprovision(_ context.Context, in orgdeprovision.Input) (*orgdeprovision.Result, error) {
	f.calls++
	f.in = in
	if err := in.Authorize(context.Background(), audit.TxOps{}, in.OrgID); err != nil {
		return nil, err
	}
	if f.err != nil {
		return nil, f.err
	}
	return &orgdeprovision.Result{RemovedMemberships: 2, RevokedInvites: 1, Suspended: true}, nil
}

// fakeReactivate runs the verb's Authorize on an empty transaction, as the
// store does after its row lock, then answers changed (a no-op when noop).
type fakeReactivate struct {
	err        error
	noop       bool
	calls      int
	id         uuid.UUID
	ifRevision *int64
	actor      org.Actor
	// suspends counts SuspendAuthorized calls; sid/sactor/sIfRevision are the last call's arguments.
	suspends    int
	sid         uuid.UUID
	sIfRevision *int64
	sactor      org.Actor
	// Get is the state read: gets counts calls, status/revision/revStep shape the answer (the revision grows by revStep on
	// each further read), getErr fails it.
	gets     int
	gid      uuid.UUID
	status   org.Status
	revision int64
	revStep  int64
	getErr   error
}

func (f *fakeReactivate) Get(_ context.Context, id uuid.UUID) (*org.Organization, error) {
	f.gets++
	f.gid = id
	if f.getErr != nil {
		return nil, f.getErr
	}
	return &org.Organization{ID: id, Status: f.status, Revision: f.revision + int64(f.gets-1)*f.revStep}, nil
}

func (f *fakeReactivate) SuspendAuthorized(ctx context.Context, id uuid.UUID, ifRevision *int64, a org.Actor, authz org.Authorize) (bool, error) {
	f.suspends++
	f.sid, f.sIfRevision, f.sactor = id, ifRevision, a
	if authz == nil {
		return false, org.ErrDenied
	}
	if err := authz(ctx, audit.TxOps{}, id); err != nil {
		return false, err
	}
	if f.err != nil {
		return false, f.err
	}
	return !f.noop, nil
}

func (f *fakeReactivate) ReactivateAuthorized(ctx context.Context, id uuid.UUID, ifRevision *int64, a org.Actor, authz org.Authorize) (bool, error) {
	f.calls++
	f.id, f.ifRevision, f.actor = id, ifRevision, a
	if authz == nil {
		return false, org.ErrDenied
	}
	if err := authz(ctx, audit.TxOps{}, id); err != nil {
		return false, err
	}
	if f.err != nil {
		return false, f.err
	}
	return !f.noop, nil
}

type provFixture struct {
	t                                           *testing.T
	creds                                       *fakeScopeCredentials
	users                                       *fakeProvUsers
	orgs                                        *fakeEnsurer
	members                                     *fakeMembers
	invites                                     *fakeInvites
	deprovision                                 *fakeDeprovision
	reactivate                                  *fakeReactivate
	sa                                          *fakeServiceAccounts
	grants                                      *fakeGrants
	reader                                      *sdkmetric.ManualReader
	logs                                        *bytes.Buffer
	mux                                         *http.ServeMux
	org, admin, owner, viewer, super, target, x uuid.UUID
}

func newProvFixture(t *testing.T) *provFixture {
	t.Helper()
	return newProvFixtureWith(t, docWithRow(t, PatternOrgSuspend))
}

// docWithRow is the embedded endpoint-profile document with its row for route (if any) replaced by a copy of the
// deprovision row, last: the suspend route is mounted only when its row exists.
func docWithRow(t testing.TB, route string) []byte {
	t.Helper()
	return docWithRows(t, append([]string{route}, keyPatterns...)...)
}

// embeddedDocWithoutRow is the embedded endpoint-profile document without the rows for route.
func embeddedDocWithoutRow(t testing.TB, route string) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(authv1.AuthServiceEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, item := range document["rows"].([]any) {
		if item.(map[string]any)["route"] != route {
			kept = append(kept, item)
		}
	}
	document["rows"] = kept
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// newProvFixtureWith builds the routes over the given endpoint-profile
// document (nil is the one the binary embeds).
func newProvFixtureWith(t *testing.T, profileRows []byte) *provFixture {
	t.Helper()
	return newProvFixtureFor(t, profileRows, nil)
}

// newProvFixtureFor is newProvFixtureWith with the go-api workload path
// (nil refuses every workload token).
func newProvFixtureFor(t *testing.T, profileRows []byte, workload *SessionIssueWorkload) *provFixture {
	t.Helper()
	f := &provFixture{t: t, creds: &fakeScopeCredentials{}, orgs: &fakeEnsurer{}, members: &fakeMembers{}, invites: &fakeInvites{},
		deprovision: &fakeDeprovision{}, reactivate: &fakeReactivate{}, sa: &fakeServiceAccounts{}, grants: &fakeGrants{}, reader: sdkmetric.NewManualReader(), logs: &bytes.Buffer{},
		org: uuid.New(), admin: uuid.New(), owner: uuid.New(), viewer: uuid.New(), super: uuid.New(), target: uuid.New(), x: uuid.New()}
	f.account()
	f.users = &fakeProvUsers{
		states: map[uuid.UUID]UserState{
			f.admin: {IsActive: true}, f.owner: {IsActive: true}, f.viewer: {IsActive: true},
			f.super: {IsActive: true, IsSuperuser: true}, f.target: {IsActive: true},
		},
		roles: map[[2]uuid.UUID]string{{f.admin, f.org}: "admin", {f.owner, f.org}: "owner", {f.viewer, f.org}: "viewer"},
	}
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa, Grants: f.grants, ProfileRows: profileRows,
		Workload: workload,
		Logger:   slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Meter:    sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("test")})
	if err != nil {
		t.Fatal(err)
	}
	f.mux = http.NewServeMux()
	for _, route := range routes {
		f.mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	return f
}

func (f *provFixture) post(path, body string, header map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if header == nil {
		header = map[string]string{"Authorization": "Bearer " + provisioningToken}
	}
	for k, v := range header {
		request.Header.Set(k, v)
	}
	response := httptest.NewRecorder()
	f.mux.ServeHTTP(response, request)
	return response
}

func (f *provFixture) storeCalls() int {
	return f.orgs.calls + f.members.calls + f.invites.calls + f.deprovision.calls + f.reactivate.calls + f.reactivate.suspends + f.sa.calls
}

func (f *provFixture) uses(op string) (int64, bool) {
	f.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := f.reader.Collect(context.Background(), &rm); err != nil {
		f.t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != "auth.provisioning.compat_credential_uses" || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if got, _ := point.Attributes.Value(attribute.Key("operation")); got.AsString() == op {
					return point.Value, true
				}
			}
		}
	}
	return 0, false
}

func reasonOf(t *testing.T, response *httptest.ResponseRecorder) string {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
		t.Fatalf("not JSON: %s", response.Body)
	}
	if doc["schema_version"] != contracts.ErrorSurface {
		t.Fatalf("not error.v1: %s", response.Body)
	}
	reason, _ := doc["reason_code"].(string)
	return reason
}

func expectRefusal(t *testing.T, response *httptest.ResponseRecorder, status int, reason string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d: %s", response.Code, status, response.Body)
	}
	if got := reasonOf(t, response); got != reason {
		t.Fatalf("reason_code = %q, want %q", got, reason)
	}
}

func encode(body map[string]any) string {
	raw, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}
	return string(raw)
}

// routeCase is one route with a valid body from actor and the members a
// valid body must carry.
type routeCase struct {
	op, path string
	body     func(f *provFixture, actor uuid.UUID) map[string]any
	required []string
}

func routeCases() []routeCase {
	return append([]routeCase{
		{opMembershipAdd, PatternMembershipAdd, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "principal_id": f.target.String(), "actor_principal_id": a.String(),
				"role": "member", "invited_by_principal_id": nil}
		}, []string{"organization_id", "principal_id", "actor_principal_id", "role"}},
		{opMembershipRole, PatternMembershipRole, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "principal_id": f.target.String(), "actor_principal_id": a.String(),
				"role": "admin", "expected_revision": 4}
		}, []string{"organization_id", "principal_id", "actor_principal_id", "role"}},
		{opMembershipRemove, PatternMembershipRemove, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "principal_id": f.target.String(), "actor_principal_id": a.String()}
		}, []string{"organization_id", "principal_id", "actor_principal_id"}},
		{opMembershipTransfer, PatternMembershipTransfer, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "from_principal_id": f.owner.String(), "to_principal_id": f.target.String(),
				"actor_principal_id": a.String()}
		}, []string{"organization_id", "from_principal_id", "to_principal_id", "actor_principal_id"}},
		{opInviteCreate, PatternInviteCreate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "email": "new@example.test", "role": "member", "ttl_seconds": 7200,
				"actor_principal_id": a.String(), "description": "Organization invite created", "request_metadata": map[string]any{"ip": "198.51.100.1"}}
		}, []string{"organization_id", "email", "role", "actor_principal_id"}},
		{opInviteAccept, PatternInviteAccept, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"token": fakeTokenID + "." + strings.Repeat("a", 64), "principal_id": a.String()}
		}, []string{"token", "principal_id"}},
		{opOrgDeprovision, PatternOrgDeprovision, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String()}
		}, []string{"organization_id", "actor_principal_id"}},
		{opOrgReactivate, PatternOrgReactivate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String()}
		}, []string{"organization_id", "actor_principal_id"}},
		{opOrgSuspend, PatternOrgSuspend, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "actor_principal_id": a.String()}
		}, []string{"organization_id", "actor_principal_id"}},
		{opInviteRevoke, PatternInviteRevoke, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String(), "invite_id": fakeInviteID.String(), "actor_principal_id": a.String()}
		}, []string{"organization_id", "invite_id", "actor_principal_id"}},
		{opOrgState, PatternOrgState, func(f *provFixture, _ uuid.UUID) map[string]any {
			return map[string]any{"organization_id": f.org.String()}
		}, []string{"organization_id"}},
		{opOrgOnboard, PatternOrgOnboard, func(_ *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"principal_id": a.String(), "organization_name": "Acme"}
		}, []string{"principal_id", "organization_name"}},
		{opOrgCreate, PatternOrgCreate, func(f *provFixture, a uuid.UUID) map[string]any {
			return map[string]any{"actor_principal_id": a.String(), "organization_name": "Acme", "slug": "acme", "tier": "community", "managed_by": "stripe"}
		}, []string{"actor_principal_id", "organization_name", "managed_by"}},
	}, append(serviceAccountCases(), keyCases()...)...)
}

// fakeInviteID is the invite the revoke route case names.
var fakeInviteID = uuid.MustParse("6f1b8f1e-7c43-4a7e-9b62-1d2a3c4e5f60")

// Every route serves only the session issuer credential holding
// memberships:write: any other bearer is a 401 with no read and no write, and
// a failed credential lookup is a 503. Accepted uses are counted per route;
// the counter is declared at 0 for every route.
func TestProvisioningRoutesTakeOnlyTheMembershipsWriteCredential(t *testing.T) {
	f := newProvFixture(t)
	for _, c := range routeCases() {
		if got, declared := f.uses(c.op); !declared || got != 0 {
			t.Fatalf("%s: compat uses = %d (declared %v), want declared at 0", c.op, got, declared)
		}
	}
	for _, c := range routeCases() {
		body := encode(c.body(f, f.super))
		for name, header := range map[string]map[string]string{
			"no bearer":      {},
			"basic":          {"Authorization": "Basic " + provisioningToken},
			"empty bearer":   {"Authorization": "Bearer "},
			"another token":  {"Authorization": "Bearer svc_authsess_BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
			"a user's token": {"Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.e30.x"},
		} {
			response := f.post(c.path, body, header)
			expectRefusal(t, response, http.StatusUnauthorized, reasonInvalidCredentials)
			if response.Header().Get("WWW-Authenticate") == "" {
				t.Fatalf("%s %s: no WWW-Authenticate", c.op, name)
			}
		}
		for _, refusal := range []servicecred.Refusal{servicecred.RefusedScope, servicecred.RefusedService, servicecred.RefusedRevoked,
			servicecred.RefusedExpired, servicecred.RefusedMalformed} {
			f.creds.refusal = refusal
			expectRefusal(t, f.post(c.path, body, nil), http.StatusUnauthorized, reasonInvalidCredentials)
		}
		f.creds.refusal, f.creds.err = "", errors.New("lookup failed")
		expectRefusal(t, f.post(c.path, body, nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
		f.creds.err = nil
		if got, _ := f.uses(c.op); got != 0 {
			t.Fatalf("%s: compat uses after refused credentials = %d, want 0", c.op, got)
		}
		if f.storeCalls() != 0 || f.users.reads != 0 {
			t.Fatalf("%s: a refused credential reached the stores (%d calls, %d reads)", c.op, f.storeCalls(), f.users.reads)
		}
	}
	for _, scope := range f.creds.scopes {
		if scope != servicecred.ScopeMembershipsWrite {
			t.Fatalf("a route asked for scope %q, want only %q", scope, servicecred.ScopeMembershipsWrite)
		}
	}
	for _, c := range routeCases() {
		response := f.post(c.path, encode(c.body(f, f.super)), nil)
		if response.Code != http.StatusOK {
			t.Fatalf("%s with the credential: %d %s", c.op, response.Code, response.Body)
		}
		if got, _ := f.uses(c.op); got != 1 {
			t.Fatalf("%s: compat uses = %d, want 1", c.op, got)
		}
	}
}

// Each body is one strict JSON object: an unknown, duplicate, missing or
// mistyped member, a non-canonical id, trailing data or a body over 4 KiB is
// a 400 with no read and no write.
func TestProvisioningBodiesAreStrict(t *testing.T) {
	f := newProvFixture(t)
	for _, c := range routeCases() {
		valid := c.body(f, f.super)
		id := f.super.String()
		if !strings.Contains(encode(valid), id) {
			id = f.org.String() // a body with no actor (the state read)
		}
		bad := map[string]string{
			"not an object":   `[]`,
			"empty":           ``,
			"trailing data":   encode(valid) + `{}`,
			"unknown member":  encode(merged(valid, map[string]any{"schema_version": "x"})),
			"duplicate":       strings.Replace(encode(valid), "{", `{"`+c.required[0]+`":null,`, 1),
			"over the bound":  encode(merged(valid, map[string]any{c.required[0]: strings.Repeat("a", maxProvisioningBody)})),
			"upper-case uuid": strings.Replace(encode(valid), `"`+id+`"`, `"`+strings.ToUpper(id)+`"`, 1),
		}
		for _, name := range c.required {
			bad["missing "+name] = encode(without(valid, name))
			bad["null "+name] = encode(merged(valid, map[string]any{name: nil}))
			bad["number "+name] = encode(merged(valid, map[string]any{name: 7}))
			bad["empty "+name] = encode(merged(valid, map[string]any{name: ""}))
		}
		for name, body := range bad {
			response := f.post(c.path, body, nil)
			if response.Code != http.StatusBadRequest || reasonOf(t, response) != reasonMalformed {
				t.Fatalf("%s %s: %d %s, want 400 malformed", c.op, name, response.Code, response.Body)
			}
		}
		if f.storeCalls() != 0 || f.users.reads != 0 {
			t.Fatalf("%s: a malformed body reached the stores", c.op)
		}
	}
	optional := map[string]map[string][]any{
		PatternMembershipAdd:  {"invited_by_principal_id": {"x", 1, strings.ToUpper(uuid.NewString())}},
		PatternMembershipRole: {"expected_revision": {0, -1, 1.5, "4", 9007199254740992, json.RawMessage(`1e3`)}},
		PatternInviteCreate: {"ttl_seconds": {0, 1.5, "3600", json.RawMessage(`36e2`)}, "description": {"", 7, strings.Repeat("d", maxDescriptionBytes+1)},
			"request_metadata": {"x", 7, []any{}, json.RawMessage(`{"a":1,"a":2}`), json.RawMessage(`{"n":{"b":1,"b":2}}`),
				map[string]any{"pad": strings.Repeat("m", maxRequestMetadataBytes)}}},
		PatternInviteAccept: {"description": {""}, "request_metadata": {"x"}},
	}
	for _, c := range routeCases() {
		for name, values := range optional[c.path] {
			for _, value := range values {
				response := f.post(c.path, encode(merged(c.body(f, f.super), map[string]any{name: value})), nil)
				if response.Code != http.StatusBadRequest {
					t.Fatalf("%s %s=%v: %d, want 400", c.op, name, value, response.Code)
				}
			}
		}
	}
	if f.storeCalls() != 0 {
		t.Fatal("a refused optional member reached the stores")
	}
}

func merged(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func without(a map[string]any, name string) map[string]any {
	out := merged(a, nil)
	delete(out, name)
	return out
}

// The actor rule is go-api's PEP rule, checked before anything is read for
// the request: an unknown or inactive actor, a member that is no owner or
// admin, and (for a deprovision) any actor but a superuser are refused with
// no organization projection and no store call. A non-superuser actor may not
// change a superuser member.
func TestProvisioningActorRule(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	stranger := uuid.New()
	membershipRoutes := []routeCase{routeCases()[0], routeCases()[1], routeCases()[2], routeCases()[3], routeCases()[4]}
	for _, c := range membershipRoutes {
		for name, tc := range map[string]struct {
			actor  uuid.UUID
			reason string
		}{
			"unknown actor":           {stranger, reasonDeactivated},
			"inactive superuser":      {f.x, reasonDeactivated},
			"viewer of the org":       {f.viewer, reasonAdminRequired},
			"member of no membership": {f.target, reasonAdminRequired},
		} {
			before := f.storeCalls()
			expectRefusal(t, f.post(c.path, encode(c.body(f, tc.actor)), nil), http.StatusForbidden, tc.reason)
			if f.storeCalls() != before {
				t.Fatalf("%s %s: a refused actor reached a store", c.op, name)
			}
		}
		for _, actor := range []uuid.UUID{f.admin, f.owner, f.super} {
			if response := f.post(c.path, encode(c.body(f, actor)), nil); response.Code != http.StatusOK {
				t.Fatalf("%s by an allowed actor: %d %s", c.op, response.Code, response.Body)
			}
		}
	}
	dep := routeCases()[6]
	for _, actor := range []uuid.UUID{f.admin, f.owner} {
		before := f.storeCalls()
		expectRefusal(t, f.post(dep.path, encode(dep.body(f, actor)), nil), http.StatusForbidden, reasonSuperuserRequired)
		if f.storeCalls() != before {
			t.Fatal("a non-superuser deprovision reached a store")
		}
	}
	if response := f.post(dep.path, encode(dep.body(f, f.super)), nil); response.Code != http.StatusOK {
		t.Fatalf("deprovision by a superuser: %d %s", response.Code, response.Body)
	}
	// A superuser member: an admin may not change it, a superuser may.
	f.users.states[f.target] = UserState{IsActive: true, IsSuperuser: true}
	for _, c := range membershipRoutes[:4] {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
		if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
			t.Fatalf("%s of a superuser member by a superuser: %d", c.op, response.Code)
		}
	}
	// An invite names an address, not an account: no target rule.
	if response := f.post(PatternInviteCreate, encode(routeCases()[4].body(f, f.admin)), nil); response.Code != http.StatusOK {
		t.Fatalf("invite by an admin: %d %s", response.Code, response.Body)
	}
}

// The rule runs again inside the verb's transaction, after its locks: a
// change the first read missed (the actor demoted, the target made a
// superuser) is refused by the verb.
func TestProvisioningActorRuleRunsAgainUnderTheLocks(t *testing.T) {
	f := newProvFixture(t)
	f.users.inTx = &fakeProvUsers{states: f.users.states, roles: map[[2]uuid.UUID]string{{f.admin, f.org}: "viewer"}}
	for _, c := range routeCases()[:5] {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonAdminRequired)
	}
	promoted := map[uuid.UUID]UserState{}
	for k, v := range f.users.states {
		promoted[k] = v
	}
	promoted[f.target] = UserState{IsActive: true, IsSuperuser: true}
	f.users.inTx = &fakeProvUsers{states: promoted, roles: f.users.roles}
	for _, c := range routeCases()[:4] {
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.admin)), nil), http.StatusForbidden, reasonSuperuserRequired)
	}
	f.users.inTx = &fakeProvUsers{states: map[uuid.UUID]UserState{f.super: {IsActive: true}}}
	dep := routeCases()[6]
	expectRefusal(t, f.post(dep.path, encode(dep.body(f, f.super)), nil), http.StatusForbidden, reasonSuperuserRequired)
	f.users.inTx = &fakeProvUsers{err: errors.New("read failed")}
	expectRefusal(t, f.post(dep.path, encode(dep.body(f, f.super)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

// The organization is projected (only when the auth schema has no row) after
// the actor rule and before the verb.
func TestProvisioningEnsuresTheOrganization(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		status int
		reason string
	}{
		"missing in both schemas": {identity.ErrOrganizationNotFound, http.StatusNotFound, reasonOrganizationNotFound},
		"slug collision":          {identity.ErrOrganizationSlugCollision, http.StatusConflict, reasonOrganizationConflict},
		"store failure":           {errors.New("down"), http.StatusServiceUnavailable, reasonDependency},
	} {
		t.Run(name, func(t *testing.T) {
			f := newProvFixture(t)
			f.orgs.err = tc.err
			for _, c := range append(routeCases()[:5], routeCases()[6], routeCases()[7], routeCases()[8]) {
				expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), tc.status, tc.reason)
			}
			if f.members.calls+f.invites.calls+f.deprovision.calls+f.reactivate.calls+f.reactivate.suspends != 0 {
				t.Fatal("a verb ran without its organization")
			}
		})
	}
}

// An accept needs an active account; the token and the verified address
// decide the rest inside the invite aggregate.
func TestInviteAcceptNeedsAnActiveAccount(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false}
	accept := routeCases()[5]
	for _, principal := range []uuid.UUID{f.x, uuid.New()} {
		expectRefusal(t, f.post(accept.path, encode(accept.body(f, principal)), nil), http.StatusForbidden, reasonDeactivated)
	}
	if f.invites.calls != 0 {
		t.Fatal("an inactive account reached the invite store")
	}
	if response := f.post(accept.path, encode(accept.body(f, f.target)), nil); response.Code != http.StatusOK {
		t.Fatalf("accept: %d %s", response.Code, response.Body)
	}
	if f.invites.accept.PrincipalID != f.target || *f.invites.accept.Actor.PrincipalID != f.target {
		t.Fatal("the accept is not made as the account")
	}
}

// An accept hands the invite aggregate the organization projection, which it
// runs once it knows the invite's organization: the route's own ensurer, called
// with that organization. The route itself projects nothing before the token is
// read.
func TestInviteAcceptHandsTheOrganizationProjectionToTheStore(t *testing.T) {
	f := newProvFixture(t)
	accept := routeCases()[5]
	if response := f.post(accept.path, encode(accept.body(f, f.target)), nil); response.Code != http.StatusOK {
		t.Fatalf("accept: %d %s", response.Code, response.Body)
	}
	if f.orgs.calls != 0 {
		t.Fatal("the route projected an organization before the store read the token")
	}
	ensure := f.invites.accept.EnsureOrganization
	if ensure == nil {
		t.Fatal("the accept has no organization projection")
	}
	org := uuid.New()
	if err := ensure(context.Background(), org); err != nil || f.orgs.calls != 1 || f.orgs.last != org {
		t.Fatalf("the projection did not reach the route's ensurer with the invite's organization (calls %d, err %v)", f.orgs.calls, err)
	}
	f.invites.err = invite.ErrOrgConflict
	expectRefusal(t, f.post(accept.path, encode(accept.body(f, f.target)), nil), http.StatusConflict, reasonOrganizationConflict)
}

// What each verb is given: the body's values, the actor and the request id,
// and the request part of go-api's audit_logs row.
func TestProvisioningPassesTheRequestToTheStores(t *testing.T) {
	f := newProvFixture(t)
	inviter := uuid.New()
	add := routeCases()[0]
	if response := f.post(add.path, encode(merged(add.body(f, f.admin), map[string]any{"invited_by_principal_id": inviter.String()})), nil); response.Code != http.StatusOK {
		t.Fatalf("add: %d", response.Code)
	}
	c := f.members.change
	if c.OrgID != f.org || c.PrincipalID != f.target || c.Role != "member" || c.InvitedBy == nil || *c.InvitedBy != inviter ||
		c.Actor.PrincipalID == nil || *c.Actor.PrincipalID != f.admin || c.Actor.RequestID == nil {
		t.Fatalf("add change = %+v", c)
	}
	role := routeCases()[1]
	f.post(role.path, encode(role.body(f, f.admin)), nil)
	if f.members.change.ExpectedRevision == nil || *f.members.change.ExpectedRevision != 4 || f.members.change.Role != "admin" {
		t.Fatalf("role change = %+v", f.members.change)
	}
	transfer := routeCases()[3]
	f.post(transfer.path, encode(transfer.body(f, f.owner)), nil)
	if f.members.transfer.From != f.owner || f.members.transfer.To != f.target {
		t.Fatalf("transfer = %+v", f.members.transfer)
	}
	create := routeCases()[4]
	f.post(create.path, encode(create.body(f, f.admin)), nil)
	in := f.invites.create
	if in.TTL == nil || *in.TTL != 2*time.Hour || in.InvitedBy != f.admin || in.Email != "new@example.test" || in.PublicAudit == nil ||
		in.PublicAudit.Description == nil || *in.PublicAudit.Description != "Organization invite created" ||
		string(in.PublicAudit.RequestMetadata) != `{"ip":"198.51.100.1"}` {
		t.Fatalf("create input = %+v", in)
	}
	dep := routeCases()[6]
	f.post(dep.path, encode(dep.body(f, f.super)), nil)
	if f.deprovision.in.OrgID != f.org || *f.deprovision.in.Actor.PrincipalID != f.super {
		t.Fatalf("deprovision input = %+v", f.deprovision.in)
	}
}

// The answers: every store error has one answer; a store failure is a 503.
func TestProvisioningRefusalAnswers(t *testing.T) {
	stored := fmt.Errorf("%w: boom", membership.ErrStore)
	for err, want := range map[error]refusal{
		membership.ErrDenied: {403, reasonAdminRequired}, membership.ErrSuperuserRequired: {403, reasonSuperuserRequired},
		membership.ErrNotOwned: {409, reasonUsersNotOwned}, membership.ErrRoleInvalid: {400, reasonRoleInvalid},
		membership.ErrRoleMissing: {409, reasonRoleMissing}, membership.ErrNotMember: {404, reasonMembershipNotFound},
		membership.ErrAlreadyMember: {409, reasonAlreadyMember}, membership.ErrSuspended: {409, reasonMembershipSuspended},
		membership.ErrLastOwner: {409, reasonLastOwner}, membership.ErrNotOwner: {409, reasonNotOwner},
		membership.ErrTargetNotMember: {409, reasonTargetNotMember}, membership.ErrRevisionConflict: {409, reasonRevisionStale},
		membership.ErrOrgNotFound: {404, reasonOrganizationNotFound}, membership.ErrOrgSuspended: {409, reasonOrganizationInactive},
		membership.ErrPrincipalNotFound: {404, reasonPrincipalNotFound}, membership.ErrInviterNotFound: {404, reasonInviterNotFound},
		membership.ErrStateMoved: {409, reasonStateChanged}, stored: {503, reasonStoreUnavailable},
	} {
		if got := membershipRefusal(err); got != want {
			t.Errorf("membershipRefusal(%v) = %+v, want %+v", err, got, want)
		}
	}
	for err, want := range map[error]refusal{
		invite.ErrInviteInvalid: {400, reasonInviteInvalid}, invite.ErrDenied: {403, reasonAdminRequired},
		invite.ErrAccountInactive: {403, reasonDeactivated},
		invite.ErrNotOwned:        {409, reasonUsersNotOwned}, invite.ErrEmailUnverified: {403, reasonEmailUnverified},
		invite.ErrEmailMismatch: {403, reasonEmailMismatch}, invite.ErrEmailInvalid: {400, reasonEmailInvalid},
		invite.ErrRoleInvalid: {400, reasonRoleInvalid}, invite.ErrTTLInvalid: {400, reasonTTLInvalid},
		invite.ErrPendingExists: {409, reasonInvitePending}, invite.ErrAlreadyMember: {409, reasonAlreadyMember},
		invite.ErrOrgNotFound: {404, reasonOrganizationNotFound}, invite.ErrOrgSuspended: {409, reasonOrganizationInactive},
		invite.ErrOrgConflict:     {409, reasonOrganizationConflict},
		invite.ErrInviterNotFound: {404, reasonInviterNotFound}, membership.ErrStateMoved: {409, reasonStateChanged},
		fmt.Errorf("%w: boom", invite.ErrStore): {503, reasonStoreUnavailable},
	} {
		if got := inviteRefusal(err); got != want {
			t.Errorf("inviteRefusal(%v) = %+v, want %+v", err, got, want)
		}
	}
	f := newProvFixture(t)
	dep := routeCases()[6]
	for err, want := range map[error]refusal{
		orgdeprovision.ErrNotFound: {404, reasonOrganizationNotFound}, orgdeprovision.ErrNotOwned: {409, reasonUsersNotOwned},
		orgdeprovision.ErrStateMoved: {409, reasonStateChanged}, fmt.Errorf("%w: boom", orgdeprovision.ErrStore): {503, reasonStoreUnavailable},
	} {
		f.deprovision.err = err
		expectRefusal(t, f.post(dep.path, encode(dep.body(f, f.super)), nil), want.status, want.reason)
	}
	f.members.err = membership.ErrLastOwner
	remove := routeCases()[2]
	expectRefusal(t, f.post(remove.path, encode(remove.body(f, f.owner)), nil), http.StatusConflict, reasonLastOwner)
	f.invites.err = invite.ErrInviteInvalid
	accept := routeCases()[5]
	expectRefusal(t, f.post(accept.path, encode(accept.body(f, f.target)), nil), http.StatusBadRequest, reasonInviteInvalid)
}

// The success answers carry ids only, and the invite token once.
func TestProvisioningSuccessAnswers(t *testing.T) {
	f := newProvFixture(t)
	decode := func(response *httptest.ResponseRecorder) map[string]any {
		t.Helper()
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("answer %d (Cache-Control %q): %s", response.Code, response.Header().Get("Cache-Control"), response.Body)
		}
		var doc map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	add := decode(f.post(PatternMembershipAdd, encode(routeCases()[0].body(f, f.admin)), nil))
	if add["organization_id"] != f.org.String() || add["principal_id"] != f.target.String() || add["role"] != "member" ||
		add["status"] != "active" || add["revision"] != float64(3) || add["joined_at"] != "2026-10-29T09:00:00Z" {
		t.Fatalf("add answer = %v", add)
	}
	created := decode(f.post(PatternInviteCreate, encode(routeCases()[4].body(f, f.admin)), nil))
	if created["token"] != createdToken || created["expires_at"] != "2026-11-01T00:00:00Z" {
		t.Fatalf("create answer = %v", created)
	}
	dep := decode(f.post(PatternOrgDeprovision, encode(routeCases()[6].body(f, f.super)), nil))
	if dep["removed_memberships"] != float64(2) || dep["revoked_invites"] != float64(1) || dep["suspended"] != true {
		t.Fatalf("deprovision answer = %v", dep)
	}
	transfer := decode(f.post(PatternMembershipTransfer, encode(routeCases()[3].body(f, f.owner)), nil))
	if transfer["owner_principal_id"] != f.target.String() || transfer["from_principal_id"] != f.owner.String() {
		t.Fatalf("transfer answer = %v", transfer)
	}
}

// No log line carries an invite token, an address, a description or the
// request metadata, on success or refusal.
func TestProvisioningLogsCarryNoTokenOrAuditText(t *testing.T) {
	f := newProvFixture(t)
	acceptToken := fakeTokenID + ".SENTINEL-ACCEPT-TOKEN"
	const (
		description = "SENTINEL-DESCRIPTION"
		metadata    = "SENTINEL-METADATA"
		email       = "sentinel-address@example.test"
	)
	create := merged(routeCases()[4].body(f, f.admin), map[string]any{"email": email, "description": description,
		"request_metadata": map[string]any{"ua": metadata}})
	accept := map[string]any{"token": acceptToken, "principal_id": f.target.String(), "description": description,
		"request_metadata": map[string]any{"ua": metadata}}
	f.post(PatternInviteCreate, encode(create), nil)
	f.post(PatternInviteAccept, encode(accept), nil)
	for _, err := range []error{invite.ErrInviteInvalid, invite.ErrEmailMismatch, fmt.Errorf("%w: %s", invite.ErrStore, acceptToken)} {
		f.invites.err = err
		f.post(PatternInviteCreate, encode(create), nil)
		f.post(PatternInviteAccept, encode(accept), nil)
	}
	f.post(PatternInviteAccept, encode(accept), map[string]string{"Authorization": "Bearer " + acceptToken})
	logs := f.logs.String()
	if !strings.Contains(logs, opInviteAccept) {
		t.Fatal("the routes logged nothing: the check would pass vacuously")
	}
	for _, secret := range []string{createdToken, acceptToken, "SENTINEL-INVITE-TOKEN", "SENTINEL-ACCEPT-TOKEN", description, metadata, email} {
		if strings.Contains(logs, secret) {
			t.Fatalf("a log line carries %q", secret)
		}
	}
}

// The reactivate route is superuser only (go-api's PATCH is policy.Superuser),
// reads the account through the users seam alone (StateOn, never a role), and
// refuses before the verb is reached.
func TestOrgReactivateIsSuperuserOnlyThroughTheUsersSeam(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	c := routeCases()[7]
	for name, tc := range map[string]struct {
		actor  uuid.UUID
		reason string
	}{
		"admin of the org":   {f.admin, reasonSuperuserRequired},
		"owner of the org":   {f.owner, reasonSuperuserRequired},
		"member of no org":   {f.target, reasonSuperuserRequired},
		"inactive superuser": {f.x, reasonDeactivated},
		"unknown actor":      {uuid.New(), reasonDeactivated},
	} {
		expectRefusal(t, f.post(c.path, encode(c.body(f, tc.actor)), nil), http.StatusForbidden, tc.reason)
		if f.reactivate.calls != 0 {
			t.Fatalf("%s: a refused actor reached the verb", name)
		}
	}
	if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
		t.Fatalf("reactivate by a superuser: %d %s", response.Code, response.Body)
	}
	if f.users.roleReads != 0 {
		t.Fatalf("the route read %d roles; the actor rule of a superuser-only route reads none", f.users.roleReads)
	}
	if f.reactivate.id != f.org || f.reactivate.actor.PrincipalID == nil || *f.reactivate.actor.PrincipalID != f.super ||
		f.reactivate.actor.RequestID == nil || f.reactivate.ifRevision != nil {
		t.Fatalf("the verb was given %+v for org %s, want the org, the actor, a request id and no revision", f.reactivate, f.reactivate.id)
	}
}

// The rule runs again inside the verb's transaction, after the row lock: a
// superuser demoted since the first read is refused by the verb.
func TestOrgReactivateRunsTheActorRuleAgainUnderTheLock(t *testing.T) {
	f := newProvFixture(t)
	c := routeCases()[7]
	f.users.inTx = &fakeProvUsers{states: map[uuid.UUID]UserState{f.super: {IsActive: true}}}
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusForbidden, reasonSuperuserRequired)
	f.users.inTx = &fakeProvUsers{err: errors.New("read failed")}
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestOrgReactivateAnswers(t *testing.T) {
	f := newProvFixture(t)
	c := routeCases()[7]
	decode := func() map[string]any {
		t.Helper()
		response := f.post(c.path, encode(c.body(f, f.super)), nil)
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("answer %d (Cache-Control %q): %s", response.Code, response.Header().Get("Cache-Control"), response.Body)
		}
		var doc map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	got := decode()
	if len(got) != 3 || got["organization_id"] != f.org.String() || got["status"] != "active" || got["reactivated"] != true {
		t.Fatalf("reactivate answer = %v", got)
	}
	f.reactivate.noop = true
	got = decode()
	if len(got) != 3 || got["status"] != "active" || got["reactivated"] != false {
		t.Fatalf("answer for an org that is not suspended = %v, want 200 with reactivated false", got)
	}
}

func TestOrgReactivateRefusalsAreNamedReasonCodes(t *testing.T) {
	f := newProvFixture(t)
	c := routeCases()[7]
	for _, tc := range []struct {
		err    error
		status int
		reason string
	}{
		{org.ErrNotFound, http.StatusNotFound, reasonOrganizationNotFound},
		{org.ErrDenied, http.StatusForbidden, reasonSuperuserRequired},
		{fmt.Errorf("%w: reactivate: down", org.ErrStore), http.StatusServiceUnavailable, reasonStoreUnavailable},
		{errors.New("unclassified"), http.StatusServiceUnavailable, reasonStoreUnavailable},
	} {
		f.reactivate.err = tc.err
		expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), tc.status, tc.reason)
	}
}

func TestProvisioningRoutesNeedTheOrganizationVerb(t *testing.T) {
	f := newProvFixture(t)
	_, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision})
	if err == nil {
		t.Fatal("ProvisioningRoutes accepted deps with no organization verb")
	}
}

// The suspend route mirrors reactivate: superuser only through the users seam alone, the rule again under the row
// lock, no revision sent, and the verb is given the org, the actor and a request id.
func TestOrgSuspendIsSuperuserOnlyThroughTheUsersSeam(t *testing.T) {
	f := newProvFixture(t)
	f.users.states[f.x] = UserState{IsActive: false, IsSuperuser: true}
	c := routeCases()[8]
	for name, tc := range map[string]struct {
		actor  uuid.UUID
		reason string
	}{
		"admin of the org":   {f.admin, reasonSuperuserRequired},
		"owner of the org":   {f.owner, reasonSuperuserRequired},
		"member of no org":   {f.target, reasonSuperuserRequired},
		"inactive superuser": {f.x, reasonDeactivated},
		"unknown actor":      {uuid.New(), reasonDeactivated},
	} {
		expectRefusal(t, f.post(c.path, encode(c.body(f, tc.actor)), nil), http.StatusForbidden, tc.reason)
		if f.reactivate.suspends != 0 {
			t.Fatalf("%s: a refused actor reached the verb", name)
		}
	}
	if response := f.post(c.path, encode(c.body(f, f.super)), nil); response.Code != http.StatusOK {
		t.Fatalf("suspend by a superuser: %d %s", response.Code, response.Body)
	}
	if f.users.roleReads != 0 {
		t.Fatalf("the route read %d roles; the actor rule of a superuser-only route reads none", f.users.roleReads)
	}
	if f.reactivate.sid != f.org || f.reactivate.sactor.PrincipalID == nil || *f.reactivate.sactor.PrincipalID != f.super ||
		f.reactivate.sactor.RequestID == nil || f.reactivate.sIfRevision != nil || f.reactivate.calls != 0 {
		t.Fatalf("the suspend verb was given %+v, want the org, the actor, a request id, no revision, and no reactivate call", f.reactivate)
	}
}

func TestOrgSuspendRunsTheActorRuleAgainUnderTheLock(t *testing.T) {
	f := newProvFixture(t)
	c := routeCases()[8]
	f.users.inTx = &fakeProvUsers{states: map[uuid.UUID]UserState{f.super: {IsActive: true}}}
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusForbidden, reasonSuperuserRequired)
	f.users.inTx = &fakeProvUsers{err: errors.New("read failed")}
	expectRefusal(t, f.post(c.path, encode(c.body(f, f.super)), nil), http.StatusServiceUnavailable, reasonStoreUnavailable)
}

func TestOrgSuspendAnswers(t *testing.T) {
	f := newProvFixture(t)
	c := routeCases()[8]
	decode := func() map[string]any {
		t.Helper()
		response := f.post(c.path, encode(c.body(f, f.super)), nil)
		if response.Code != http.StatusOK || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("answer %d (Cache-Control %q): %s", response.Code, response.Header().Get("Cache-Control"), response.Body)
		}
		var doc map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	got := decode()
	if len(got) != 3 || got["organization_id"] != f.org.String() || got["status"] != "suspended" || got["suspended"] != true {
		t.Fatalf("suspend answer = %v", got)
	}
	f.reactivate.noop = true
	got = decode()
	if len(got) != 3 || got["status"] != "suspended" || got["suspended"] != false {
		t.Fatalf("answer for an organization that is already suspended = %v, want 200 with suspended false", got)
	}
}

// Every fault is a named refusal, and a fault of the store is the same fail-closed 503 store_unavailable, never a 500:
// the acting account has no auth.principals row (the audit row's actor foreign key fails), or the store is down.
func TestOrgSuspendRefusalsAreNamedReasonCodes(t *testing.T) {
	f := newProvFixture(t)
	c := routeCases()[8]
	missingPrincipal := errors.New(`audit: security mutation failed: inserting the audit row: ERROR: insert or update on table "security_audit_events" violates foreign key constraint "security_audit_events_actor_principal_id_fkey" (SQLSTATE 23503)`)
	for name, tc := range map[string]struct {
		err    error
		status int
		reason string
	}{
		"missing organization":          {org.ErrNotFound, http.StatusNotFound, reasonOrganizationNotFound},
		"denied under the lock":         {org.ErrDenied, http.StatusForbidden, reasonSuperuserRequired},
		"store down":                    {fmt.Errorf("%w: suspend: down", org.ErrStore), http.StatusServiceUnavailable, reasonStoreUnavailable},
		"actor has no principal row":    {fmt.Errorf("%w: suspend: %w", org.ErrStore, missingPrincipal), http.StatusServiceUnavailable, reasonStoreUnavailable},
		"the raw foreign key violation": {missingPrincipal, http.StatusServiceUnavailable, reasonStoreUnavailable},
		"unclassified":                  {errors.New("unclassified"), http.StatusServiceUnavailable, reasonStoreUnavailable},
	} {
		f.reactivate.err = tc.err
		response := f.post(c.path, encode(c.body(f, f.super)), nil)
		expectRefusal(t, response, tc.status, tc.reason)
		if strings.Contains(response.Body.String(), "SQLSTATE") {
			t.Fatalf("%s: the answer carries database text: %s", name, response.Body)
		}
	}
}

// The suspend route is on the startup gate: a document without its row builds no route at all, and the refusal names it.
func TestOrgSuspendIsOnTheStartupGate(t *testing.T) {
	if !slices.Contains(provisioningPatterns, PatternOrgSuspend) {
		t.Fatalf("provisioningPatterns = %v, want the suspend route", provisioningPatterns)
	}
	f := newProvFixture(t)
	routes, err := ProvisioningRoutes(ProvisioningDeps{Credentials: f.creds, Users: f.users, Reader: noRows{}, Organizations: f.orgs,
		Memberships: f.members, Invites: f.invites, Deprovision: f.deprovision, Orgs: f.reactivate, ServiceAccounts: f.sa,
		ProfileRows: embeddedDocWithoutRow(t, PatternOrgSuspend)})
	if !errors.Is(err, errProvisioningRows) || routes != nil || !strings.Contains(err.Error(), "POST "+PatternOrgSuspend+" ") {
		t.Fatalf("a document without the suspend row: %d routes, %v; want none and a refusal naming the route", len(routes), err)
	}
}

// The embedded document serves the suspend route with the others, and no startup line calls it unmounted.
func TestOrgSuspendIsServedWithTheEmbeddedDocument(t *testing.T) {
	f := newProvFixtureWith(t, authv1.AuthServiceEndpointProfiles)
	if got := f.post(PatternOrgSuspend, encode(routeCases()[8].body(f, f.super)), nil); got.Code != http.StatusOK {
		t.Fatalf("suspend with the embedded document: %d %s, want 200", got.Code, got.Body)
	}
	if f.reactivate.suspends != 1 {
		t.Fatalf("%d suspends, want 1", f.reactivate.suspends)
	}
	for _, line := range strings.Split(f.logs.String(), "\n") {
		if strings.Contains(line, "not mounted") && strings.Contains(line, PatternOrgSuspend) {
			t.Fatalf("a not-mounted line for the suspend route: %s", line)
		}
	}
}
