//go:build integration

package oauthgrant

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/acrmint"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

const testChallenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"

type actorStub struct {
	refuse bool
	err    error
	calls  int
	got    [2]uuid.UUID
}

func (a *actorStub) ImpersonatingActor(_ context.Context, principal, sessionID uuid.UUID) (bool, error) {
	a.calls++
	a.got = [2]uuid.UUID{principal, sessionID}
	return a.refuse, a.err
}

type env struct {
	admin, runtime *pgxpool.Pool
	schema         authschema.ValidatedIdentifier
	role           string
	identity       *identity.Store
	sessions       *session.Store
	actors         *actorStub
	store          *Store
}

func newEnv(t *testing.T, ctx context.Context) *env {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("oauth_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	const password = "oauthgrant-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
	cfg.MaxConns = 32
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	id, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	sessions, err := session.New(session.Config{Pool: runtime, Schema: schema, UsersOwned: true,
		SuccessorKey: contracts.NewSecretString(base64.RawURLEncoding.EncodeToString(key))})
	if err != nil {
		t.Fatal(err)
	}
	actors := &actorStub{}
	store, err := New(Config{Pool: runtime, Schema: schema, Sessions: sessions, Actors: actors})
	if err != nil {
		t.Fatal(err)
	}
	return &env{admin: admin, runtime: runtime, schema: schema, role: role, identity: id, sessions: sessions, actors: actors, store: store}
}

func (e *env) user(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, false, 7, now(), now())`, id, "o-"+id.String()[:8]+"@example.test"); err != nil {
		t.Fatalf("seed public.users: %v", err)
	}
	if err := e.identity.Project(ctx, id); err != nil {
		t.Fatalf("Project: %v", err)
	}
	return id
}

func (e *env) org(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'oauth-test')`, id, "org-"+id.String()[:8]); err != nil {
		t.Fatalf("seed public.organizations: %v", err)
	}
	if err := e.identity.ProjectOrganization(ctx, id); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	return id
}

func (e *env) count(t *testing.T, ctx context.Context, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// flow is one consent in progress: a web session, a bound request and its token.
type flow struct {
	principal, organization, web, request uuid.UUID
	csrf                                  contracts.SecretString
	class                                 string
}

func (f flow) decision() Decision {
	return Decision{RequestID: f.request, WebSessionID: f.web, PrincipalID: f.principal, CSRF: f.csrf}
}

func requestInput(class string) RequestInput {
	audience := "acr-api"
	if class == ClassMCP {
		audience = "acr-mcp"
	}
	return RequestInput{ClientID: "client-" + class, ClientClass: class, RedirectURI: "https://client.example.test/cb",
		Resource: "https://" + audience + ".example.test", Audience: audience, Scope: strings.ReplaceAll(audience, "-", ":"),
		State: "client-state-value", CodeChallenge: testChallenge}
}

func (e *env) flow(t *testing.T, ctx context.Context, class string) flow {
	t.Helper()
	f := flow{principal: e.user(t, ctx), organization: e.org(t, ctx), class: class}
	web, err := e.sessions.Create(ctx, f.principal, &f.organization, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	f.web = web.Session.ID
	if f.request, err = e.store.CreateRequest(ctx, requestInput(class)); err != nil {
		t.Fatalf("CreateRequest: %v", err)
	}
	d, err := e.store.Describe(ctx, DescribeInput{RequestID: f.request, WebSessionID: f.web, PrincipalID: f.principal, OrganizationID: &f.organization})
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	f.csrf = d.CSRF
	return f
}

func (e *env) grants(t *testing.T, ctx context.Context, principal uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.oauth_grants WHERE principal_id = $1`, principal)
}

// clientSessions counts the principal's sessions beyond its web sign-ins.
func (e *env) clientSessions(t *testing.T, ctx context.Context, principal, web uuid.UUID) int {
	return e.count(t, ctx, `SELECT count(*) FROM auth.sessions WHERE principal_id = $1 AND id <> $2`, principal, web)
}

func (e *env) nothingApproved(t *testing.T, ctx context.Context, f flow) {
	t.Helper()
	if got := e.grants(t, ctx, f.principal); got != 0 {
		t.Errorf("%d grants", got)
	}
	if got := e.clientSessions(t, ctx, f.principal, f.web); got != 0 {
		t.Errorf("%d client sessions", got)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests WHERE id = $1 AND (decision IS NOT NULL OR code_hash IS NOT NULL)`, f.request); got != 0 {
		t.Errorf("the request was decided")
	}
}

func consume(f flow, code contracts.SecretString) ConsumeInput {
	in := requestInput(f.class)
	return ConsumeInput{Code: code, ClientID: in.ClientID, RedirectURI: in.RedirectURI, Resource: in.Resource, CodeChallenge: in.CodeChallenge}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// An approval commits the session, the grant, the code and two audit rows, for each
// class. The session is the client's own, and the class pairs with its binding.
func TestApproveRecordsOneSessionOneGrantAndTheirAudit(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	for class, want := range map[string]struct {
		binding string
		linked  bool
	}{ClassBrowser: {"web_session", true}, ClassMCP: {"client_session", false}} {
		t.Run(class, func(t *testing.T) {
			f := e.flow(t, ctx, class)
			got, err := e.store.Approve(ctx, f.decision())
			if err != nil {
				t.Fatalf("Approve: %v", err)
			}
			if got.SessionID == f.web {
				t.Fatal("the grant took the web sid")
			}
			if got.State != "client-state-value" || got.RedirectURI != requestInput(class).RedirectURI {
				t.Fatalf("answer = %+v", got)
			}
			var binding, clientClass string
			var link *string
			if err := e.admin.QueryRow(ctx, `SELECT g.binding, g.client_class, s.web_session_id::text
				FROM auth.oauth_grants g JOIN auth.sessions s ON s.id = g.session_id WHERE g.id = $1`, got.GrantID).
				Scan(&binding, &clientClass, &link); err != nil {
				t.Fatal(err)
			}
			if binding != want.binding || clientClass != class || (link != nil) != want.linked {
				t.Fatalf("binding %s class %s link %v", binding, clientClass, link)
			}
			if live := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests
				WHERE id = $1 AND decision = 'approved' AND grant_id = $2 AND code_hash IS NOT NULL AND code_expires_at > clock_timestamp()`, f.request, got.GrantID); live != 1 {
				t.Fatalf("request rows = %d", live)
			}
			// A code lives 60 s and a request 10 minutes, on the database clock.
			if lives := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests WHERE id = $1
				AND code_expires_at BETWEEN clock_timestamp() + interval '45 seconds' AND clock_timestamp() + interval '60 seconds'
				AND expires_at BETWEEN created_at + interval '599 seconds' AND created_at + interval '601 seconds'`, f.request); lives != 1 {
				t.Fatal("the code or the request has the wrong life")
			}
			for _, event := range []string{EventConsentApproved, EventClientSessionCreate} {
				if rows := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
					WHERE event_type = $1 AND actor_principal_id = $2 AND attributes->>'target_id' = $3
					  AND attributes->>'client_class' = $4`, event, f.principal, got.SessionID.String(), class); rows != 1 {
					t.Fatalf("%s rows = %d", event, rows)
				}
			}
			secrets := []string{got.Code.Reveal(), f.csrf.Reveal(), got.State}
			var text string
			if err := e.admin.QueryRow(ctx, `SELECT coalesce(string_agg(attributes::text, ' '), '') FROM auth.security_audit_events
				WHERE actor_principal_id = $1`, f.principal).Scan(&text); err != nil {
				t.Fatal(err)
			}
			for _, secret := range secrets {
				if strings.Contains(text, secret) {
					t.Fatal("an audit row holds a code, a token or the client's state")
				}
			}
			if rows := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests
				WHERE code_hash = $1 OR csrf_hash = $2`, []byte(got.Code.Reveal()), []byte(f.csrf.Reveal())); rows != 0 {
				t.Fatal("a raw secret is stored")
			}
			if outbox := e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE payload::text LIKE '%' || $1 || '%'`, got.SessionID.String()); outbox != 0 {
				t.Fatalf("%d outbox rows", outbox)
			}
		})
	}
}

// Approve twice, in sequence and in parallel, gives one session and one grant.
func TestApproveTwiceGivesOneSessionAndOneGrant(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	for _, class := range []string{ClassBrowser, ClassMCP} {
		t.Run(class+" in sequence", func(t *testing.T) {
			f := e.flow(t, ctx, class)
			if _, err := e.store.Approve(ctx, f.decision()); err != nil {
				t.Fatalf("Approve: %v", err)
			}
			if _, err := e.store.Approve(ctx, f.decision()); !errors.Is(err, ErrAlreadyDecided) {
				t.Fatalf("second approve: %v", err)
			}
			if grants, sessions := e.grants(t, ctx, f.principal), e.clientSessions(t, ctx, f.principal, f.web); grants != 1 || sessions != 1 {
				t.Fatalf("%d grants, %d sessions", grants, sessions)
			}
		})
		t.Run(class+" in parallel", func(t *testing.T) {
			f := e.flow(t, ctx, class)
			const callers = 16
			errs := make([]error, callers)
			var wg sync.WaitGroup
			for i := range callers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, errs[i] = e.store.Approve(ctx, f.decision())
				}()
			}
			wg.Wait()
			won := 0
			for _, err := range errs {
				switch {
				case err == nil:
					won++
				case !errors.Is(err, ErrAlreadyDecided):
					t.Fatalf("a loser: %v", err)
				}
			}
			if grants, sessions := e.grants(t, ctx, f.principal), e.clientSessions(t, ctx, f.principal, f.web); won != 1 || grants != 1 || sessions != 1 {
				t.Fatalf("%d won, %d grants, %d sessions", won, grants, sessions)
			}
		})
	}
}

// A decision needs the whole binding. Each refusal writes nothing, and the positive
// control is the same flow approved afterwards.
func TestADecisionNeedsItsWholeBinding(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassMCP)
	other := e.flow(t, ctx, ClassMCP)
	wrong, err := newSecret(csrfPrefix)
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		edit func(*Decision)
		want error
	}{
		"another session":   {func(d *Decision) { d.WebSessionID = other.web }, ErrOtherSession},
		"another principal": {func(d *Decision) { d.PrincipalID = other.principal }, ErrOtherSession},
		"a wrong token":     {func(d *Decision) { d.CSRF = wrong }, ErrBadCSRF},
		"another's token":   {func(d *Decision) { d.CSRF = other.csrf }, ErrBadCSRF},
		"no token":          {func(d *Decision) { d.CSRF = contracts.SecretString{} }, ErrBadCSRF},
		"no such request":   {func(d *Decision) { d.RequestID = uuid.New() }, ErrRequestNotFound},
	} {
		t.Run(name, func(t *testing.T) {
			d := f.decision()
			tc.edit(&d)
			if _, err := e.store.Approve(ctx, d); !errors.Is(err, tc.want) {
				t.Errorf("Approve: %v, want %v", err, tc.want)
			}
			if _, err := e.store.Deny(ctx, d); !errors.Is(err, tc.want) {
				t.Errorf("Deny: %v, want %v", err, tc.want)
			}
			e.nothingApproved(t, ctx, f)
		})
	}
	if _, err := e.store.Approve(ctx, f.decision()); err != nil {
		t.Fatalf("positive control: %v", err)
	}
}

// A request that is not bound gives no code: there is no decision to take.
func TestAnUnboundRequestCannotBeDecided(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassBrowser)
	unbound, err := e.store.CreateRequest(ctx, requestInput(ClassBrowser))
	if err != nil {
		t.Fatal(err)
	}
	d := f.decision()
	d.RequestID = unbound
	if _, err := e.store.Approve(ctx, d); !errors.Is(err, ErrRequestNotBound) {
		t.Fatalf("Approve: %v", err)
	}
	if _, err := e.store.Deny(ctx, d); !errors.Is(err, ErrRequestNotBound) {
		t.Fatalf("Deny: %v", err)
	}
}

// A code exists only after approve. A deny writes its decision and audit row and no
// code, a request with no consent has no code, and a replayed token is a decided
// request.
func TestNoConsentGivesNoCode(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassMCP)
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests WHERE id = $1 AND code_hash IS NULL`, f.request); got != 1 {
		t.Fatal("a code exists before any consent")
	}
	denied, err := e.store.Deny(ctx, f.decision())
	if err != nil {
		t.Fatalf("Deny: %v", err)
	}
	if denied.State != "client-state-value" {
		t.Fatalf("denied = %+v", denied)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests WHERE id = $1 AND decision = 'denied' AND code_hash IS NULL`, f.request); got != 1 {
		t.Fatal("the denial is not recorded, or it left a code")
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = $1 AND outcome = 'denied' AND actor_principal_id = $2`, EventConsentDenied, f.principal); got != 1 {
		t.Fatalf("denied rows = %d", got)
	}
	if got := e.grants(t, ctx, f.principal); got != 0 {
		t.Fatalf("%d grants", got)
	}
	if _, err := e.store.Approve(ctx, f.decision()); !errors.Is(err, ErrAlreadyDecided) {
		t.Fatalf("approve after deny: %v", err)
	}
	if _, err := e.store.Deny(ctx, f.decision()); !errors.Is(err, ErrAlreadyDecided) {
		t.Fatalf("deny twice: %v", err)
	}
}

func TestAnExpiredRequestCannotBeBoundOrDecided(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	expire := func(id uuid.UUID) {
		if _, err := e.admin.Exec(ctx, `UPDATE auth.oauth_authorization_requests SET expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
	}
	f := e.flow(t, ctx, ClassBrowser)
	expire(f.request)
	if _, err := e.store.Approve(ctx, f.decision()); !errors.Is(err, ErrRequestExpired) {
		t.Fatalf("Approve: %v", err)
	}
	if _, err := e.store.Deny(ctx, f.decision()); !errors.Is(err, ErrRequestExpired) {
		t.Fatalf("Deny: %v", err)
	}
	e.nothingApproved(t, ctx, f)
	fresh, err := e.store.CreateRequest(ctx, requestInput(ClassBrowser))
	if err != nil {
		t.Fatal(err)
	}
	expire(fresh)
	if _, err := e.store.Describe(ctx, DescribeInput{RequestID: fresh, WebSessionID: f.web, PrincipalID: f.principal}); !errors.Is(err, ErrRequestExpired) {
		t.Fatalf("Describe: %v", err)
	}
}

// describe is write-once: a second bind is refused, by the same session and by
// another, and the first binding and token stand.
func TestDescribeIsWriteOnce(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassBrowser)
	other := e.flow(t, ctx, ClassBrowser)
	for name, in := range map[string]DescribeInput{
		"the same session": {RequestID: f.request, WebSessionID: f.web, PrincipalID: f.principal, OrganizationID: &f.organization},
		"another session":  {RequestID: f.request, WebSessionID: other.web, PrincipalID: other.principal, OrganizationID: &other.organization},
	} {
		if _, err := e.store.Describe(ctx, in); !errors.Is(err, ErrRequestBound) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := e.store.Describe(ctx, DescribeInput{RequestID: uuid.New(), WebSessionID: f.web, PrincipalID: f.principal}); !errors.Is(err, ErrRequestNotFound) {
		t.Fatalf("no such request: %v", err)
	}
	if _, err := e.store.Approve(ctx, f.decision()); err != nil {
		t.Fatalf("the first binding stands: %v", err)
	}
}

// An org-less web session is refused at approve, for both classes.
func TestAnOrglessWebSessionIsRefusedAtApprove(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	for _, class := range []string{ClassBrowser, ClassMCP} {
		t.Run(class, func(t *testing.T) {
			principal := e.user(t, ctx)
			web, err := e.sessions.Create(ctx, principal, nil, []string{"pwd"})
			if err != nil {
				t.Fatal(err)
			}
			request, err := e.store.CreateRequest(ctx, requestInput(class))
			if err != nil {
				t.Fatal(err)
			}
			d, err := e.store.Describe(ctx, DescribeInput{RequestID: request, WebSessionID: web.Session.ID, PrincipalID: principal})
			if err != nil {
				t.Fatalf("Describe: %v", err)
			}
			f := flow{principal: principal, web: web.Session.ID, request: request, csrf: d.CSRF, class: class}
			if _, err := e.store.Approve(ctx, f.decision()); !errors.Is(err, ErrNoOrganization) {
				t.Fatalf("Approve: %v", err)
			}
			e.nothingApproved(t, ctx, f)
		})
	}
}

// The actor check refuses, and a failed read refuses, for both classes. Nothing is
// written. It is asked about the principal and the web session.
func TestApproveHonoursTheActorCheck(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	for _, class := range []string{ClassBrowser, ClassMCP} {
		for name, tc := range map[string]struct {
			set  func()
			want error
		}{
			"an actor":    {func() { e.actors.refuse = true }, ErrActorRefused},
			"a read fail": {func() { e.actors.err = errors.New("read failed") }, ErrActorUnavailable},
		} {
			t.Run(class+" "+name, func(t *testing.T) {
				f := e.flow(t, ctx, class)
				*e.actors = actorStub{}
				tc.set()
				t.Cleanup(func() { *e.actors = actorStub{} })
				if _, err := e.store.Approve(ctx, f.decision()); !errors.Is(err, tc.want) {
					t.Fatalf("Approve: %v, want %v", err, tc.want)
				}
				if e.actors.got != [2]uuid.UUID{f.principal, f.web} {
					t.Fatalf("the check read %v", e.actors.got)
				}
				e.nothingApproved(t, ctx, f)
			})
		}
	}
}

// The checker itself, as the runtime role with the production grants: the legacy row
// refuses, a delegated web session refuses through the session store, and the target
// of an impersonation is not refused.
func TestTheActorCheckRunsAsTheRuntimeRole(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	check := acrmint.PGImpersonation{Pool: e.runtime, Sessions: e.sessions}
	f := e.flow(t, ctx, ClassBrowser)
	got, err := check.ImpersonatingActor(ctx, f.principal, f.web)
	if err != nil || got {
		t.Fatalf("positive control: %v, %v", got, err)
	}
	target := e.user(t, ctx)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.impersonation_sessions
		(id, admin_user_id, target_user_id, target_org_id, target_role, expires_at)
		VALUES ($1, $2, $3, $4, 'member', now() + interval '10 minutes')`, uuid.New(), f.principal, target, f.organization); err != nil {
		t.Fatalf("seed impersonation: %v", err)
	}
	if got, err := check.ImpersonatingActor(ctx, f.principal, f.web); err != nil || !got {
		t.Fatalf("the legacy actor: %v, %v", got, err)
	}
	targetWeb, err := e.sessions.Create(ctx, target, &f.organization, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := check.ImpersonatingActor(ctx, target, targetWeb.Session.ID); err != nil || got {
		t.Fatalf("the legacy target: %v, %v", got, err)
	}
}

// A client session cannot stand behind another consent: an MCP session has no link,
// so its grant is what names it.
func TestAClientSessionCannotConsent(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassMCP)
	first, err := e.store.Approve(ctx, f.decision())
	if err != nil {
		t.Fatalf("Approve: %v", err)
	}
	request, err := e.store.CreateRequest(ctx, requestInput(ClassMCP))
	if err != nil {
		t.Fatal(err)
	}
	d, err := e.store.Describe(ctx, DescribeInput{RequestID: request, WebSessionID: first.SessionID, PrincipalID: f.principal, OrganizationID: &f.organization})
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	next := Decision{RequestID: request, WebSessionID: first.SessionID, PrincipalID: f.principal, CSRF: d.CSRF}
	if _, err := e.store.Approve(ctx, next); !errors.Is(err, session.ErrWebSessionNotLive) {
		t.Fatalf("Approve: %v", err)
	}
	if got := e.grants(t, ctx, f.principal); got != 1 {
		t.Fatalf("%d grants", got)
	}
}

// Refused at the session: a revoked web session, after the bind.
func TestApproveReadsTheWebSessionLive(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassMCP)
	if ok, err := e.sessions.Revoke(ctx, f.web, session.Actor{}); err != nil || !ok {
		t.Fatalf("Revoke: %v, %v", ok, err)
	}
	if _, err := e.store.Approve(ctx, f.decision()); !errors.Is(err, session.ErrWebSessionNotLive) {
		t.Fatalf("Approve: %v", err)
	}
	e.nothingApproved(t, ctx, f)
}

// Approve is one transaction: a failed audit write, or a failed last step, commits
// nothing. The positive control is the same flow approved once the fault is gone.
func TestAFailedStepOfApproveCommitsNothing(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	for name, fault := range map[string]struct{ on, off string }{
		"the audit write": {
			on:  fmt.Sprintf(`REVOKE INSERT ON auth.security_audit_events FROM %q`, e.role),
			off: fmt.Sprintf(`GRANT INSERT ON auth.security_audit_events TO %q`, e.role),
		},
		"the decision write": {
			on:  fmt.Sprintf(`REVOKE UPDATE ON auth.oauth_authorization_requests FROM %q`, e.role),
			off: fmt.Sprintf(`GRANT UPDATE ON auth.oauth_authorization_requests TO %q`, e.role),
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := e.flow(t, ctx, ClassMCP)
			if _, err := e.admin.Exec(ctx, fault.on); err != nil {
				t.Fatal(err)
			}
			_, err := e.store.Approve(ctx, f.decision())
			if _, offErr := e.admin.Exec(ctx, fault.off); offErr != nil {
				t.Fatal(offErr)
			}
			if !errors.Is(err, ErrStore) {
				t.Fatalf("Approve: %v, want ErrStore", err)
			}
			e.nothingApproved(t, ctx, f)
			if _, err := e.store.Approve(ctx, f.decision()); err != nil {
				t.Fatalf("positive control: %v", err)
			}
		})
	}
}

// A code is spent once: of many callers in parallel, one gets the row.
func TestACodeIsConsumedOnceInParallel(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassMCP)
	approved, err := e.store.Approve(ctx, f.decision())
	if err != nil {
		t.Fatal(err)
	}
	const callers = 24
	results := make([]Consumed, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = e.store.ConsumeCode(ctx, consume(f, approved.Code))
		}()
	}
	wg.Wait()
	won := 0
	for i, err := range errs {
		switch {
		case err == nil:
			won++
			if results[i].GrantID != approved.GrantID || results[i].PrincipalID != f.principal || results[i].OrganizationID != f.organization {
				t.Fatalf("consumed = %+v", results[i])
			}
		case !errors.Is(err, ErrCodeRefused):
			t.Fatalf("a loser: %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d callers got the row", won)
	}
}

// A code is bound to the client, the redirect, the resource and the challenge. A
// refused presentation does not spend it. An expired code is refused.
func TestACodeIsBoundAndARefusedPresentationDoesNotSpendIt(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassBrowser)
	approved, err := e.store.Approve(ctx, f.decision())
	if err != nil {
		t.Fatal(err)
	}
	unknown, err := newSecret(codePrefix)
	if err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*ConsumeInput){
		"another client":    func(in *ConsumeInput) { in.ClientID = "other" },
		"another redirect":  func(in *ConsumeInput) { in.RedirectURI += "/" },
		"another resource":  func(in *ConsumeInput) { in.Resource += "/" },
		"another challenge": func(in *ConsumeInput) { in.CodeChallenge = strings.Repeat("a", 43) },
		"an unknown code":   func(in *ConsumeInput) { in.Code = unknown },
	} {
		in := consume(f, approved.Code)
		edit(&in)
		if _, err := e.store.ConsumeCode(ctx, in); !errors.Is(err, ErrCodeRefused) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if _, err := e.store.ConsumeCode(ctx, consume(f, approved.Code)); err != nil {
		t.Fatalf("the code was spent by a refused presentation: %v", err)
	}
	if _, err := e.store.ConsumeCode(ctx, consume(f, approved.Code)); !errors.Is(err, ErrCodeRefused) {
		t.Fatalf("a spent code: %v", err)
	}
	g := e.flow(t, ctx, ClassBrowser)
	late, err := e.store.Approve(ctx, g.decision())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE auth.oauth_authorization_requests SET code_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, g.request); err == nil {
		t.Fatal("the write-once trigger let a code's expiry change")
	}
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.oauth_authorization_requests DISABLE TRIGGER oauth_requests_write_once`); err != nil {
		t.Fatal(err)
	}
	_, updateErr := e.admin.Exec(ctx, `UPDATE auth.oauth_authorization_requests SET code_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, g.request)
	if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.oauth_authorization_requests ENABLE TRIGGER oauth_requests_write_once`); err != nil {
		t.Fatal(err)
	}
	if updateErr != nil {
		t.Fatal(updateErr)
	}
	if _, err := e.store.ConsumeCode(ctx, consume(g, late.Code)); !errors.Is(err, ErrCodeRefused) {
		t.Fatalf("an expired code: %v", err)
	}
}

// The cleanup deletes a row only when the request and its code have both expired. A
// request past its expiry with a live code survives (acp, condition 5).
func TestTheCleanupKeepsALiveCode(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	set := func(t *testing.T, id uuid.UUID, assignment string) {
		t.Helper()
		if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.oauth_authorization_requests DISABLE TRIGGER oauth_requests_write_once`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := e.admin.Exec(ctx, `ALTER TABLE auth.oauth_authorization_requests ENABLE TRIGGER oauth_requests_write_once`); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err := e.admin.Exec(ctx, `UPDATE auth.oauth_authorization_requests SET `+assignment+` WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
	}
	exists := func(id uuid.UUID) bool {
		return e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests WHERE id = $1`, id) == 1
	}

	liveCode := e.flow(t, ctx, ClassMCP)
	code, err := e.store.Approve(ctx, liveCode.decision())
	if err != nil {
		t.Fatal(err)
	}
	set(t, liveCode.request, `expires_at = clock_timestamp() - interval '1 second'`)

	spent := e.flow(t, ctx, ClassMCP)
	if _, err := e.store.Approve(ctx, spent.decision()); err != nil {
		t.Fatal(err)
	}
	set(t, spent.request, `expires_at = clock_timestamp() - interval '2 seconds', code_expires_at = clock_timestamp() - interval '1 second'`)

	abandoned := e.flow(t, ctx, ClassMCP)
	set(t, abandoned.request, `expires_at = clock_timestamp() - interval '1 second'`)

	current := e.flow(t, ctx, ClassMCP)

	if _, err := e.store.CreateRequest(ctx, requestInput(ClassMCP)); err != nil {
		t.Fatal(err)
	}
	if !exists(liveCode.request) {
		t.Error("a request with a live code was deleted")
	}
	if exists(spent.request) {
		t.Error("a request whose code expired was kept")
	}
	if exists(abandoned.request) {
		t.Error("an expired request with no code was kept")
	}
	if !exists(current.request) {
		t.Error("a live request was deleted")
	}
	if _, err := e.store.ConsumeCode(ctx, consume(liveCode, code.Code)); err != nil {
		t.Fatalf("the surviving code: %v", err)
	}
}

// One new request deletes a bounded number of rows, so a backlog cannot make a
// public GET slow.
func TestTheCleanupIsBounded(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	const backlog = sweepBatch + 7
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.oauth_authorization_requests
		(id, client_id, client_class, redirect_uri, resource, audience, scope, state, code_challenge, code_challenge_method, expires_at)
		SELECT gen_random_uuid(), 'c', 'mcp', 'https://c.example.test/', 'https://r.example.test', 'acr-mcp', 'acr:mcp', 's', $1, 'S256',
			clock_timestamp() - interval '1 second' FROM generate_series(1, $2::int)`, testChallenge, backlog); err != nil {
		t.Fatal(err)
	}
	if _, err := e.store.CreateRequest(ctx, requestInput(ClassMCP)); err != nil {
		t.Fatal(err)
	}
	if got := e.count(t, ctx, `SELECT count(*) FROM auth.oauth_authorization_requests WHERE expires_at < clock_timestamp()`); got != backlog-sweepBatch {
		t.Fatalf("%d expired rows left, want %d", got, backlog-sweepBatch)
	}
}

// The schema's own rules, for any writer.
func TestTheSchemaHoldsTheRequestAndGrantShapes(t *testing.T) {
	ctx := testContext(t)
	e := newEnv(t, ctx)
	f := e.flow(t, ctx, ClassBrowser)
	approved, err := e.store.Approve(ctx, f.decision())
	if err != nil {
		t.Fatal(err)
	}
	refused := func(name, sql string, args ...any) {
		t.Helper()
		if _, err := e.admin.Exec(ctx, sql, args...); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	refused("a changed binding", `UPDATE auth.oauth_authorization_requests SET web_session_id = NULL, principal_id = NULL, organization_id = NULL, csrf_hash = NULL WHERE id = $1`, f.request)
	refused("a changed decision", `UPDATE auth.oauth_authorization_requests SET decision = 'denied' WHERE id = $1`, f.request)
	refused("a changed code", `UPDATE auth.oauth_authorization_requests SET code_hash = $2 WHERE id = $1`, f.request, make([]byte, 32))
	if _, err := e.store.ConsumeCode(ctx, consume(f, approved.Code)); err != nil {
		t.Fatal(err)
	}
	refused("a cleared consume", `UPDATE auth.oauth_authorization_requests SET consumed_at = NULL WHERE id = $1`, f.request)

	g := e.flow(t, ctx, ClassBrowser)
	h := e.flow(t, ctx, ClassBrowser)
	refused("a changed web session", `UPDATE auth.oauth_authorization_requests SET web_session_id = $2 WHERE id = $1`, g.request, h.web)
	refused("a changed principal", `UPDATE auth.oauth_authorization_requests SET principal_id = $2 WHERE id = $1`, g.request, h.principal)
	refused("a changed organization", `UPDATE auth.oauth_authorization_requests SET organization_id = $2 WHERE id = $1`, g.request, h.organization)
	refused("a changed token hash", `UPDATE auth.oauth_authorization_requests SET csrf_hash = $2 WHERE id = $1`, g.request, make([]byte, 32))
	other := e.flow(t, ctx, ClassBrowser)
	otherApproved, err := e.store.Approve(ctx, other.decision())
	if err != nil {
		t.Fatal(err)
	}
	refused("a changed grant", `UPDATE auth.oauth_authorization_requests SET grant_id = $2 WHERE id = $1`, f.request, otherApproved.GrantID)
	denied := e.flow(t, ctx, ClassBrowser)
	if _, err := e.store.Deny(ctx, denied.decision()); err != nil {
		t.Fatal(err)
	}
	refused("a cleared denial", `UPDATE auth.oauth_authorization_requests SET decision = NULL WHERE id = $1`, denied.request)
	refused("a denial turned into an approval", `UPDATE auth.oauth_authorization_requests
		SET decision = 'approved', grant_id = $2, code_hash = $3, code_expires_at = clock_timestamp() + interval '1 minute' WHERE id = $1`,
		denied.request, otherApproved.GrantID, make([]byte, 32))
	refused("a code with no decision", `UPDATE auth.oauth_authorization_requests SET code_hash = $2 WHERE id = $1`, g.request, make([]byte, 32))
	refused("an approval with no code", `UPDATE auth.oauth_authorization_requests SET decision = 'approved' WHERE id = $1`, g.request)
	refused("a decision with no binding", `INSERT INTO auth.oauth_authorization_requests
		(id, client_id, client_class, redirect_uri, resource, audience, scope, state, code_challenge, code_challenge_method, expires_at, decision)
		VALUES ($1, 'c', 'mcp', 'https://c.example.test/', 'https://r.example.test', 'acr-mcp', 'acr:mcp', 's', $2, 'S256', now(), 'denied')`, uuid.New(), testChallenge)
	refused("plain PKCE", `INSERT INTO auth.oauth_authorization_requests
		(id, client_id, client_class, redirect_uri, resource, audience, scope, state, code_challenge, code_challenge_method, expires_at)
		VALUES ($1, 'c', 'mcp', 'https://c.example.test/', 'https://r.example.test', 'acr-mcp', 'acr:mcp', 's', $2, 'plain', now())`, uuid.New(), testChallenge)
	refused("an MCP request for the browser audience", `INSERT INTO auth.oauth_authorization_requests
		(id, client_id, client_class, redirect_uri, resource, audience, scope, state, code_challenge, code_challenge_method, expires_at)
		VALUES ($1, 'c', 'mcp', 'https://c.example.test/', 'https://r.example.test', 'acr-api', 'acr:api', 's', $2, 'S256', now())`, uuid.New(), testChallenge)

	grant := func(sessionID uuid.UUID, class, audience, binding string) error {
		tx, err := e.admin.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO auth.oauth_grants
			(id, principal_id, organization_id, client_id, client_class, resource, audience, scope, session_id, binding, consented_at)
			VALUES ($1, $2, $3, 'c', $4, 'https://r.example.test', $5, 's', $6, $7, now())`,
			uuid.New(), f.principal, f.organization, class, audience, sessionID, binding)
		return err
	}
	free, err := e.sessions.Create(ctx, f.principal, &f.organization, []string{"pwd"})
	if err != nil {
		t.Fatal(err)
	}
	if err := grant(free.Session.ID, ClassBrowser, "acr-api", "web_session"); err != nil {
		t.Fatalf("positive control: %v", err)
	}
	for name, err := range map[string]error{
		"browser on a client session": grant(free.Session.ID, ClassBrowser, "acr-api", "client_session"),
		"mcp on a web session":        grant(free.Session.ID, ClassMCP, "acr-mcp", "web_session"),
		"mcp on the browser audience": grant(free.Session.ID, ClassMCP, "acr-api", "client_session"),
		"a second grant of a session": grant(approved.SessionID, ClassBrowser, "acr-api", "web_session"),
	} {
		if err == nil {
			t.Errorf("%s was accepted", name)
		}
	}

	// A grant exists only with its audit row, and it keeps its row.
	if _, err := e.runtime.Exec(ctx, `UPDATE auth.oauth_grants SET revoked_at = clock_timestamp() WHERE id = $1`, approved.GrantID); err == nil {
		t.Error("a grant was written outside audit.Commit")
	}
	if _, err := e.runtime.Exec(ctx, `DELETE FROM auth.oauth_grants WHERE id = $1`, approved.GrantID); err == nil {
		t.Error("the runtime role deleted a grant")
	}
}
