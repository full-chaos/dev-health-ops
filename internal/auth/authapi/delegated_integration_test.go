//go:build integration

package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const delegatedTokenVersion = 7

// delegatedLive is the delegated routes over a real auth schema, with go-api's
// Authenticator on the same database.
type delegatedLive struct {
	*preflightStore
	actor, target, org uuid.UUID
	base               uuid.UUID
	issuer             *Issuer
	users              *fakeUsers
	identity           *identity.Store
	authenticator      *policy.Authenticator
}

func newDelegatedLive(t *testing.T, ctx context.Context) *delegatedLive {
	t.Helper()
	p := newPreflightStoreOwned(t, ctx, true)
	id, err := identity.New(identity.Config{Pool: p.runtime, Schema: p.schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	l := &delegatedLive{preflightStore: p, identity: id, actor: uuid.New(), target: uuid.New(), org: uuid.New(), issuer: hs256Issuer(t)}
	for _, user := range []uuid.UUID{l.actor, l.target} {
		if _, err := p.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, $3, now(), now())`, user, "d-"+user.String()[:8]+"@example.test", delegatedTokenVersion); err != nil {
			t.Fatalf("seed public.users: %v", err)
		}
		if err := id.Project(ctx, user); err != nil {
			t.Fatalf("Project: %v", err)
		}
	}
	if _, err := id.BackfillUser(ctx, identity.BackfillTarget{ID: l.actor, Email: "d-" + l.actor.String()[:8] + "@example.test",
		DisplayName: "d", Active: true, Verified: true, Superuser: true}, false); err != nil {
		t.Fatalf("BackfillUser: %v", err)
	}
	if _, err := p.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'delegation-test')`,
		l.org, "org-"+l.org.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := id.ProjectOrganization(ctx, l.org); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	if _, err := p.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`,
		uuid.New(), l.target, l.org); err != nil {
		t.Fatalf("seed public.memberships: %v", err)
	}
	base, err := p.store.Create(ctx, l.actor, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	l.base = base.Session.ID
	member := "member"
	l.users = &fakeUsers{
		users: map[uuid.UUID]UserState{l.target: {Email: "t@example.test", IsActive: true, TokenVersion: delegatedTokenVersion}},
		roles: map[[2]uuid.UUID]*string{{l.target, l.org}: &member},
	}
	verifier, err := edgetoken.New(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	if l.authenticator, err = policy.NewAuthenticator(verifier, policy.PGStore{Pool: p.admin, AuthSchema: "auth"}, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	return l
}

// server is the routes CompatRoutes mounts from the embedded document.
func (l *delegatedLive) server(t *testing.T) http.Handler {
	t.Helper()
	routes, err := CompatRoutes(CompatDeps{Sessions: l.store, Users: l.users, Issuer: l.issuer, Projector: l.identity,
		Logger:             slog.New(slog.DiscardHandler),
		AppendAudit:        func(context.Context, audit.AuditEvent) error { return nil },
		ServiceCredentials: &fakeServiceCredentials{id: uuid.NewString()},
		Meter:              sdkmetric.NewMeterProvider().Meter("test"),
		Limits:             mustLimits(t, httpapi.NewMemoryCounters(nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return serveRoutes(routes)
}

func (l *delegatedLive) bearerRoutes(t *testing.T) http.Handler {
	t.Helper()
	routes, err := Routes(Deps{Sessions: l.store, Users: l.users, Issuer: l.issuer, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	return serveRoutes(routes)
}

func (l *delegatedLive) body() string {
	return `{"base_session_id":"` + l.base.String() + `","target_principal_id":"` + l.target.String() +
		`","organization_id":"` + l.org.String() + `","reason":"support ticket"}`
}

type startedAnswer struct {
	Session struct {
		SessionID string `json:"session_id"`
	} `json:"session"`
	AccessToken string `json:"access_token"`
}

func (l *delegatedLive) start(t *testing.T, server http.Handler) (uuid.UUID, string) {
	t.Helper()
	r := post(server, PatternDelegatedSession, l.body(), issuerBearer)
	if r.Code != http.StatusCreated {
		t.Fatalf("start %d: %s", r.Code, r.Body)
	}
	var answer startedAnswer
	if err := json.Unmarshal(r.Body.Bytes(), &answer); err != nil {
		t.Fatal(err)
	}
	id, err := session.ParseWireSessionID(answer.Session.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	return id, answer.AccessToken
}

func (l *delegatedLive) ended(t *testing.T, ctx context.Context, id uuid.UUID) bool {
	t.Helper()
	return l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND revoked_at IS NOT NULL`, id) == 1
}

func TestDelegatedRouteWritesTheRowAndNoRefreshCredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	id, access := l.start(t, l.server(t))
	if got := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE id = $1 AND principal_id = $2 AND actor_principal_id = $3 AND organization_id = $4`,
		id, l.target, l.actor, l.org); got != 1 {
		t.Fatalf("rows = %d", got)
	}
	if got := l.count(t, ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1`, id); got != 0 {
		t.Fatalf("refresh credentials = %d", got)
	}
	claims := payloadOf(t, access)
	var end time.Time
	if err := l.admin.QueryRow(ctx, `SELECT expires_at FROM auth.sessions WHERE id = $1`, id).Scan(&end); err != nil {
		t.Fatal(err)
	}
	if int64(claims["exp"].(float64)) > end.Unix() {
		t.Fatalf("exp %v is after the row's end %v", claims["exp"], end.Unix())
	}
}

func TestDelegatedMintFailureEndsTheRealRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	l.users.roleErr = errors.New("read failed")
	r := post(l.server(t), PatternDelegatedSession, l.body(), issuerBearer)
	if r.Code != http.StatusServiceUnavailable {
		t.Fatalf("start %d: %s", r.Code, r.Body)
	}
	if got := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE actor_principal_id = $1 AND revoked_at IS NULL`, l.actor); got != 0 {
		t.Fatalf("live delegated rows = %d", got)
	}
	if got := l.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE actor_principal_id = $1`, l.actor); got != 1 {
		t.Fatalf("delegated rows = %d, want the one ended row", got)
	}
	if l.ended(t, ctx, l.base) {
		t.Fatal("the base ended with the row")
	}
}

func TestDelegatedEndRouteEndsOnlyADelegatedRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	server := l.server(t)
	id, _ := l.start(t, server)
	endBody := func(id uuid.UUID) string { return `{"session_id":"` + id.String() + `"}` }
	if r := post(server, PatternDelegatedSessionEnd, endBody(l.base), issuerBearer); r.Code != http.StatusNotFound {
		t.Fatalf("plain session %d: %s", r.Code, r.Body)
	}
	if r := post(server, PatternDelegatedSessionEnd, endBody(id), issuerBearer); r.Code != http.StatusNoContent {
		t.Fatalf("end %d: %s", r.Code, r.Body)
	}
	if !l.ended(t, ctx, id) || l.ended(t, ctx, l.base) {
		t.Fatalf("delegated ended %v, base ended %v", l.ended(t, ctx, id), l.ended(t, ctx, l.base))
	}
	if r := post(server, PatternDelegatedSessionEnd, endBody(id), issuerBearer); r.Code != http.StatusNoContent {
		t.Fatalf("second end %d: %s", r.Code, r.Body)
	}
}

func TestGoAPITakesTheDelegatedTokenUntilTheSessionEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	server := l.server(t)
	id, access := l.start(t, server)
	user, err := l.authenticator.Authenticate(ctx, access)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if user.UserID != l.target.String() || user.ImpersonatedBy == nil || *user.ImpersonatedBy != l.actor.String() {
		t.Fatalf("user %+v", user)
	}
	if r := post(server, PatternDelegatedSessionEnd, `{"session_id":"`+id.String()+`"}`, issuerBearer); r.Code != http.StatusNoContent {
		t.Fatalf("end %d: %s", r.Code, r.Body)
	}
	if _, err := l.authenticator.Authenticate(ctx, access); err == nil {
		t.Fatal("go-api took the token of an ended session")
	}
}

func TestGoAPIRefusesTheDelegatedTokenAfterItsBaseIsRevoked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	_, access := l.start(t, l.server(t))
	if _, err := l.store.Revoke(ctx, l.base, session.Actor{PrincipalID: &l.actor}); err != nil {
		t.Fatalf("Revoke: %v", err)
	}
	if _, err := l.authenticator.Authenticate(ctx, access); err == nil {
		t.Fatal("go-api took the token after its base ended")
	}
}

func TestADelegatedBearerIsDeniedOverARealRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	id, access := l.start(t, l.server(t))
	routes := l.bearerRoutes(t)
	header := map[string]string{"Authorization": "Bearer " + access}
	request := func(method, path string) int {
		f := &fixture{server: routes}
		return f.do(method, path, "", header).Code
	}
	if code := request(http.MethodGet, PatternCurrent); code != http.StatusOK {
		t.Fatalf("current %d", code)
	}
	if code := request(http.MethodPost, PatternRevokeAll); code != http.StatusForbidden {
		t.Fatalf("revoke-all %d", code)
	}
	if l.ended(t, ctx, id) {
		t.Fatal("the refused revoke-all ended the row")
	}
	if code := request(http.MethodPost, PatternRevoke); code != http.StatusNoContent {
		t.Fatalf("revoke %d", code)
	}
	if !l.ended(t, ctx, id) || l.ended(t, ctx, l.base) {
		t.Fatalf("delegated ended %v, base ended %v", l.ended(t, ctx, id), l.ended(t, ctx, l.base))
	}
}

func TestSwitchFromADelegatedSessionIsRefusedOverARealRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	server := l.server(t)
	id, _ := l.start(t, server)
	r := post(server, PatternIssueSession, issueBodyWithPredecessor(l.target, l.org.String(), `"`+id.String()+`"`), issuerBearer)
	if r.Code != http.StatusForbidden {
		t.Fatalf("switch %d: %s", r.Code, r.Body)
	}
	if l.ended(t, ctx, id) {
		t.Fatal("the refused switch ended the row")
	}
}

type noDelegatedSide struct{}

func (noDelegatedSide) ActorInDelegation(context.Context, uuid.UUID, uuid.UUID) (bool, error) {
	return false, nil
}

// go-api's two readers of the table see the row while it is open, and not after
// it ends. The acrmint reader here has its delegated side off, so its answer is
// the table's alone.
func TestDelegationPublicRowReaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	l := newDelegatedLive(t, ctx)
	row, err := l.store.StartDelegation(ctx, session.StartInput{BaseSessionID: l.base, TargetPrincipalID: l.target,
		OrganizationID: l.org, Reason: "support ticket"})
	if err != nil {
		t.Fatalf("StartDelegation: %v", err)
	}
	store := policy.PGStore{Pool: l.admin, AuthSchema: "auth"}
	mint := acrmint.PGImpersonation{Pool: l.runtime, Sessions: noDelegatedSide{}}
	active, err := store.ActiveImpersonation(ctx, l.actor)
	if err != nil || active == nil || active.ID != row.ID || active.TargetUserID != l.target || active.TargetOrgID != l.org || active.TargetRole != "member" {
		t.Fatalf("ActiveImpersonation = %+v, %v", active, err)
	}
	if got, err := mint.ImpersonatingActor(ctx, l.actor, l.base); err != nil || !got {
		t.Fatalf("ImpersonatingActor = %v, %v", got, err)
	}
	if ok, err := l.store.EndDelegation(ctx, row.ID, session.Actor{}); err != nil || !ok {
		t.Fatalf("EndDelegation = %v, %v", ok, err)
	}
	if active, err := store.ActiveImpersonation(ctx, l.actor); err != nil || active != nil {
		t.Fatalf("ActiveImpersonation after the end = %+v, %v", active, err)
	}
	if got, err := mint.ImpersonatingActor(ctx, l.actor, l.base); err != nil || got {
		t.Fatalf("ImpersonatingActor after the end = %v, %v", got, err)
	}
}
