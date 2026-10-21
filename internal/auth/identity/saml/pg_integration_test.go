//go:build integration

package saml_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/saml"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// runtimeGrants are the read-only public column grants this package and the
// identity store need until the users cutover. The auth tables get their
// grants from authschema.Apply.
const runtimeGrants = `
GRANT SELECT (id, email, password_hash, is_active, is_verified, auth_provider, auth_provider_id) ON public.users TO %[1]s;
GRANT SELECT (id, org_id, protocol, status, allow_idp_initiated, auto_provision_users, config, allowed_domains) ON public.sso_providers TO %[1]s;
GRANT SELECT (user_id, org_id) ON public.memberships TO %[1]s;
GRANT SELECT (id, slug, name, is_active, tier) ON public.organizations TO %[1]s;
GRANT SELECT (org_id, tier, features_override) ON public.org_licenses TO %[1]s;
GRANT SELECT (provider_id, assertion_id) ON public.saml_assertion_replays TO %[1]s;
`

type fixture struct {
	admin   *pgxpool.Pool
	runtime *pgxpool.Pool
	legacy  *identity.Store
	owned   *identity.Store
}

var (
	db        *fixture
	dbErr     error
	dbOnce    sync.Once
	dbClosers []func()
)

func TestMain(m *testing.M) {
	code := m.Run()
	for i := len(dbClosers) - 1; i >= 0; i-- {
		dbClosers[i]()
	}
	os.Exit(code)
}

// getDB builds one PostgreSQL for the package, with a runtime role that holds
// the auth posture plus the grants above and connects with its default
// search_path.
func getDB(t *testing.T) *fixture {
	t.Helper()
	dbOnce.Do(func() { db, dbErr = newFixture(t) })
	if dbErr != nil {
		t.Fatal(dbErr)
	}
	return db
}

func newFixture(t *testing.T) (*fixture, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		return nil, err
	}
	dbClosers = append(dbClosers, func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		return nil, err
	}
	dbClosers = append(dbClosers, admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("saml_runtime", instance)
	if err != nil {
		return nil, err
	}
	ident, err := authschema.NewValidatedIdentifier(role)
	if err != nil {
		return nil, err
	}
	const password = "saml-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", authschema.Quote(ident), password)); err != nil {
		return nil, err
	}
	dbClosers = append(dbClosers, func() { containers.DropRole(admin, role, func(string, ...any) {}) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		return nil, fmt.Errorf("authschema.Apply: %w", err)
	}
	for _, stmt := range []string{fmt.Sprintf(runtimeGrants, authschema.Quote(ident))} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			return nil, fmt.Errorf("fixture: %w", err)
		}
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	dbClosers = append(dbClosers, runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		return nil, err
	}
	legacy, err := identity.New(identity.Config{Pool: runtime, Schema: schema})
	if err != nil {
		return nil, err
	}
	owned, err := identity.New(identity.Config{Pool: runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		return nil, err
	}
	return &fixture{admin: admin, runtime: runtime, legacy: legacy, owned: owned}, nil
}

type allowSSO struct{ deny bool }

func (a allowSSO) OrgHasSSO(context.Context, uuid.UUID) (bool, error) { return !a.deny, nil }

// harness is one organization, one provider row, its IdP and a Service over
// the runtime role and the real identity store (before the users cutover).
type harness struct {
	t        *testing.T
	db       *fixture
	idp      *testIdP
	provider saml.Provider
	svc      *saml.Service
}

const baseURL = "https://sp.example.test/api/v1/auth"

func newHarness(t *testing.T, allowIdPInitiated bool) *harness {
	t.Helper()
	return newHarnessWithIdP(t, newTestIdP(t), allowIdPInitiated)
}

func newHarnessWithIdP(t *testing.T, idp *testIdP, allowIdPInitiated bool) *harness {
	t.Helper()
	d := getDB(t)
	ctx := context.Background()
	p := saml.Provider{ID: uuid.New(), OrgID: uuid.New()}
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'saml-test')`, p.OrgID, "org-"+p.OrgID.String()); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`{"entity_id": %q, "sso_url": "https://idp.example.test/sso", "certificate": %q}`, idp.entityID, encodeCert(idp))
	if _, err := d.admin.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, allow_idp_initiated, config)
VALUES ($1, $2, $3, 'saml', 'active', $4, $5::json)`, p.ID, p.OrgID, "idp-"+p.ID.String(), allowIdPInitiated, config); err != nil {
		t.Fatal(err)
	}
	pg, err := saml.NewPGStore(d.runtime, mustIdent(t, "auth"))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := pg.Provider(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	svc := &saml.Service{
		Providers: pg, Entitlements: allowSSO{}, Flows: pg, Uses: pg, Store: saml.IdentityStore{Identities: d.legacy},
		BaseURL: func() string { return baseURL },
	}
	joins := &recordedJoins{stored: pg, joined: map[uuid.UUID]bool{}}
	svc.Memberships, svc.JIT = joins, joins
	return &harness{t: t, db: d, idp: idp, provider: loaded, svc: svc}
}

// recordedJoins is the membership seam of the harness: a join is recorded,
// not written (memberships stay a go-api write), and Member answers from the
// record or the stored memberships.
type recordedJoins struct {
	stored saml.Memberships
	mu     sync.Mutex
	joined map[uuid.UUID]bool
}

func (r *recordedJoins) JoinOnSignIn(_ context.Context, principalID uuid.UUID, _ saml.Provider) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.joined[principalID] = true
	return nil
}

func (r *recordedJoins) Member(ctx context.Context, principalID, orgID uuid.UUID) (bool, error) {
	r.mu.Lock()
	joined := r.joined[principalID]
	r.mu.Unlock()
	if joined {
		return true, nil
	}
	return r.stored.Member(ctx, principalID, orgID)
}

// withoutJoins removes the membership seam: memberships are only the stored rows.
func (h *harness) withoutJoins() {
	h.svc.JIT = nil
	h.svc.Memberships = h.svc.Memberships.(*recordedJoins).stored
}

// ownUsers switches the harness to the identity store after the users cutover.
func (h *harness) ownUsers() { h.svc.Store = saml.IdentityStore{Identities: h.db.owned} }

func mustIdent(t *testing.T, name string) authschema.ValidatedIdentifier {
	t.Helper()
	v, err := authschema.NewValidatedIdentifier(name)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (h *harness) sp() saml.SP {
	return saml.SP{EntityID: h.provider.SPEntityID(baseURL), ACSURL: h.provider.ACSURL(baseURL), IdPEntityID: h.idp.entityID, IdPCert: h.idp.cert}
}

// respond builds a response addressed to this provider's SP.
func (h *harness) respond(o resp) string {
	h.t.Helper()
	sp := h.sp()
	o.destination, o.recipient = strp(sp.ACSURL), strp(sp.ACSURL)
	o.audiences = [][]string{{sp.EntityID}}
	return b64(h.idp.response(h.t, o))
}

func (h *harness) initiate() (requestID string, cookies map[string]string) {
	h.t.Helper()
	init, err := h.svc.Initiate(context.Background(), h.provider.ID, "")
	if err != nil {
		h.t.Fatal(err)
	}
	requestID = requestIDFromRedirect(h.t, init.RedirectURL)
	if init.CookieName != saml.BindingCookieName(requestID) || init.CookiePath != saml.ACSPath(h.provider, baseURL) {
		h.t.Fatalf("binding cookie %q path %q does not match request %q", init.CookieName, init.CookiePath, requestID)
	}
	return requestID, map[string]string{init.CookieName: init.CookieValue}
}

func (h *harness) consume(posted string, cookies map[string]string) (saml.SignIn, error) {
	return h.svc.Consume(context.Background(), h.provider.ID, posted, func(name string) string { return cookies[name] })
}

func (h *harness) member(principal uuid.UUID) {
	h.t.Helper()
	if _, err := h.db.admin.Exec(context.Background(), `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`,
		uuid.New(), principal, h.provider.OrgID); err != nil {
		h.t.Fatal(err)
	}
}

// goAPIUser inserts a public.users row the way go-api writes one.
func (h *harness) goAPIUser(email, authProvider string, authProviderID *string, password bool) uuid.UUID {
	h.t.Helper()
	id := uuid.New()
	var hash *string
	if password {
		v := "$2b$12$abcdefghijklmnopqrstuuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0"
		hash = &v
	}
	if _, err := h.db.admin.Exec(context.Background(), `INSERT INTO public.users
	(id, email, password_hash, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, true, false, 0, now(), now())`, id, email, hash, authProvider, authProviderID); err != nil {
		h.t.Fatal(err)
	}
	return id
}

// linkedUser creates a projected account with an identity at this provider.
func (h *harness) linkedUser(email, nameID string) uuid.UUID {
	h.t.Helper()
	ctx := context.Background()
	id := h.goAPIUser(email, "local", nil, true)
	if err := h.db.legacy.Project(ctx, id); err != nil {
		h.t.Fatal(err)
	}
	subject, err := saml.Subject(nid(nameID))
	if err != nil {
		h.t.Fatal(err)
	}
	key := identity.ExternalKey{Issuer: h.idp.entityID, Provider: identity.Provider(h.provider.ID.String()), Subject: subject}
	if _, err := h.db.legacy.LinkIdentity(ctx, key, id, identity.Actor{}); err != nil {
		h.t.Fatal(err)
	}
	return id
}

const persistent = "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent"

func nid(v string) saml.NameID { return saml.NameID{Format: persistent, Value: v} }

func uniqueEmail(prefix string) string { return prefix + "-" + uuid.NewString()[:8] + "@example.com" }

func (h *harness) count(query string, args ...any) int {
	h.t.Helper()
	var n int
	if err := h.db.admin.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func (h *harness) identityCount() int {
	h.t.Helper()
	var n int
	if err := h.db.admin.QueryRow(context.Background(), `SELECT count(*) FROM auth.external_identities`).Scan(&n); err != nil {
		h.t.Fatal(err)
	}
	return n
}

func TestSAMLSolicitedSignIn(t *testing.T) {
	h := newHarness(t, false)
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	h.member(user)
	requestID, cookies := h.initiate()
	signIn, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "ignored@example.com"}), cookies)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if signIn.PrincipalID != user || signIn.OrganizationID == nil || *signIn.OrganizationID != h.provider.OrgID {
		t.Fatalf("sign-in = %+v, want principal %s in org %s", signIn, user, h.provider.OrgID)
	}
	if len(signIn.AMR) != 1 || signIn.AMR[0] != saml.AMRFederated {
		t.Fatalf("sign-in amr = %v", signIn.AMR)
	}
}

func TestSAMLRequestIDIsSingleUse(t *testing.T) {
	h := newHarness(t, false)
	h.linkedUser(uniqueEmail("u"), "user-1")
	requestID, cookies := h.initiate()
	if _, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies); err != nil {
		t.Fatalf("first Consume: %v", err)
	}
	_, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies)
	if !errors.Is(err, saml.ErrFlowConsumed) || saml.OutcomeReason(err) != "replayed" {
		t.Fatalf("second response to the same request: err = %v, want ErrFlowConsumed (replayed)", err)
	}

	t.Run("allow_idp_initiated does not reopen a used request", func(t *testing.T) {
		if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allow_idp_initiated = true WHERE id = $1`, h.provider.ID); err != nil {
			t.Fatal(err)
		}
		_, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies)
		if !errors.Is(err, saml.ErrFlowConsumed) {
			t.Fatalf("err = %v, want ErrFlowConsumed", err)
		}
	})
}

func TestSAMLRequestIsBoundToTheBrowser(t *testing.T) {
	h := newHarness(t, false)
	h.linkedUser(uniqueEmail("u"), "user-1")
	requestID, cookies := h.initiate()
	for name, jar := range map[string]map[string]string{
		"no cookie":    {},
		"wrong cookie": {saml.BindingCookieName(requestID): "not-the-nonce"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), jar)
			if !errors.Is(err, saml.ErrFlowNotFound) || saml.OutcomeReason(err) != "nonce_mismatch" {
				t.Fatalf("err = %v, want ErrFlowNotFound (nonce_mismatch)", err)
			}
		})
	}
	if _, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies); err != nil {
		t.Fatalf("the binding browser was refused after the failed attempts: %v", err)
	}
}

func TestSAMLRequestOfAnotherProviderIsRefused(t *testing.T) {
	idp := newTestIdP(t)
	a, b := newHarnessWithIdP(t, idp, false), newHarnessWithIdP(t, idp, false)
	b.linkedUser(uniqueEmail("u"), "user-1")
	requestID, cookies := a.initiate()
	_, err := b.consume(b.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies)
	if !errors.Is(err, saml.ErrFlowNotFound) {
		t.Fatalf("err = %v, want ErrFlowNotFound", err)
	}
}

func TestSAMLUnsolicitedResponseIsRefused(t *testing.T) {
	h := newHarness(t, false)
	h.linkedUser(uniqueEmail("u"), "user-1")
	_, err := h.consume(h.respond(resp{nameID: nid("user-1"), email: "a@example.com"}), nil)
	if !errors.Is(err, saml.ErrUnsolicited) {
		t.Fatalf("unsolicited: err = %v, want ErrUnsolicited", err)
	}
	_, err = h.consume(h.respond(resp{inResponseTo: "_id-never-issued", nameID: nid("user-1"), email: "a@example.com"}), nil)
	if !errors.Is(err, saml.ErrFlowNotFound) {
		t.Fatalf("unknown request id: err = %v, want ErrFlowNotFound", err)
	}
}

// TestSAMLSolicitedResponseNeedsItsBrowserWhenIdPInitiatedIsAllowed: a response
// that names a request is bound to that request and its browser, whatever the
// provider allows for responses that name none.
func TestSAMLSolicitedResponseNeedsItsBrowserWhenIdPInitiatedIsAllowed(t *testing.T) {
	h := newHarness(t, true)
	h.linkedUser(uniqueEmail("u"), "user-1")
	requestID, cookies := h.initiate()
	for name, jar := range map[string]map[string]string{
		"no cookie":    nil,
		"wrong cookie": {saml.BindingCookieName(requestID): "not-the-nonce"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), jar)
			if !errors.Is(err, saml.ErrFlowNotFound) {
				t.Fatalf("err = %v, want ErrFlowNotFound", err)
			}
		})
	}
	t.Run("a request id never issued", func(t *testing.T) {
		_, err := h.consume(h.respond(resp{inResponseTo: "_id-never-issued", nameID: nid("user-1"), email: "a@example.com"}), nil)
		if !errors.Is(err, saml.ErrFlowNotFound) {
			t.Fatalf("err = %v, want ErrFlowNotFound", err)
		}
	})
	if _, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies); err != nil {
		t.Fatalf("the binding browser was refused: %v", err)
	}
}

func TestSAMLUnsolicitedResponseNeedsTheProviderFlag(t *testing.T) {
	h := newHarness(t, true)
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	signIn, err := h.consume(h.respond(resp{nameID: nid("user-1"), email: "a@example.com"}), nil)
	if err != nil || signIn.PrincipalID != user {
		t.Fatalf("allow_idp_initiated provider: sign-in %+v err %v", signIn, err)
	}
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allow_idp_initiated = false WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.consume(h.respond(resp{nameID: nid("user-1"), email: "a@example.com"}), nil); !errors.Is(err, saml.ErrUnsolicited) {
		t.Fatalf("flag off: err = %v, want ErrUnsolicited", err)
	}
	past := time.Now().Add(-time.Hour)
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allow_idp_initiated = true WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	_, err = h.consume(h.respond(resp{nameID: nid("user-1"), email: "a@example.com", issueInstant: past, notBefore: past, notOnOrAfter: past.Add(10 * time.Minute)}), nil)
	if _, ok := saml.IsRefused(err); !ok {
		t.Fatalf("an expired unsolicited response: err = %v, want a refusal", err)
	}
}

func TestSAMLReplayedAssertionIsRefused(t *testing.T) {
	h := newHarness(t, true)
	h.linkedUser(uniqueEmail("u"), "user-1")
	posted := h.respond(resp{nameID: nid("user-1"), email: "a@example.com"})
	if _, err := h.consume(posted, nil); err != nil {
		t.Fatalf("first use: %v", err)
	}
	if _, err := h.consume(posted, nil); !errors.Is(err, saml.ErrAssertionReplayed) {
		t.Fatalf("replay: err = %v, want ErrAssertionReplayed", err)
	}

	t.Run("assertion consumed by the go-api routes", func(t *testing.T) {
		assertionID := "_a" + randomHex(t)
		if _, err := h.db.admin.Exec(context.Background(), `INSERT INTO public.saml_assertion_replays (provider_id, assertion_id, expires_at)
VALUES ($1, $2, now() + interval '5 minutes')`, h.provider.ID, assertionID); err != nil {
			t.Fatal(err)
		}
		_, err := h.consume(h.respond(resp{assertionID: assertionID, nameID: nid("user-1"), email: "a@example.com"}), nil)
		if !errors.Is(err, saml.ErrAssertionReplayed) {
			t.Fatalf("err = %v, want ErrAssertionReplayed", err)
		}
	})
}

func TestSAMLExpiredAssertionIsRefusedBeforeAnyWrite(t *testing.T) {
	h := newHarness(t, false)
	h.linkedUser(uniqueEmail("u"), "user-1")
	requestID, cookies := h.initiate()
	past := time.Now().Add(-time.Hour)
	_, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com",
		issueInstant: past, notBefore: past, notOnOrAfter: past.Add(10 * time.Minute)}), cookies)
	if reason, ok := saml.IsRefused(err); !ok || (reason != saml.ReasonExpired && reason != saml.ReasonIssueInstant) {
		t.Fatalf("err = %v, want an expiry refusal", err)
	}
	if _, err := h.consume(h.respond(resp{inResponseTo: requestID, nameID: nid("user-1"), email: "a@example.com"}), cookies); err != nil {
		t.Fatalf("a refused response consumed the request: %v", err)
	}
}

func TestSAMLEmailCollisionIsAConflict(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("users owned by auth %v", owned), func(t *testing.T) {
			h := newHarness(t, true)
			ctx := context.Background()
			existingEmail := uniqueEmail("existing")
			existing := h.goAPIUser(existingEmail, "local", nil, true)
			if owned {
				if err := h.db.legacy.Project(ctx, existing); err != nil {
					t.Fatal(err)
				}
				h.ownUsers()
			}
			before := h.identityCount()
			principals, sessions := h.count(`SELECT count(*) FROM auth.principals`), h.count(`SELECT count(*) FROM auth.sessions`)
			assertionID := "_a" + randomHex(t)
			_, err := h.consume(h.respond(resp{assertionID: assertionID, nameID: nid("other-subject"), email: strings.ToUpper(existingEmail)}), nil)
			var conflict *saml.EmailConflictError
			if !errors.As(err, &conflict) || (!owned && conflict.Method != "local") {
				t.Fatalf("err = %v, want EmailConflictError naming the local method", err)
			}
			// No account, identity or session is written; the assertion is consumed.
			if h.identityCount() != before {
				t.Fatal("a conflict wrote an identity")
			}
			if h.count(`SELECT count(*) FROM auth.principals`) != principals || h.count(`SELECT count(*) FROM auth.sessions`) != sessions {
				t.Fatal("a conflict wrote a principal or a session")
			}
			if n := h.count(`SELECT count(*) FROM auth.saml_assertion_uses WHERE issuer = $1 AND assertion_id_hash = sha256($2::bytea)`, h.idp.entityID, []byte(assertionID)); n != 1 {
				t.Fatalf("assertion-use rows for the conflicting assertion = %d, want 1", n)
			}
			var provider, providerID *string
			if err := h.db.admin.QueryRow(ctx, `SELECT auth_provider, auth_provider_id FROM public.users WHERE id = $1`, existing).Scan(&provider, &providerID); err != nil {
				t.Fatal(err)
			}
			if provider == nil || *provider != "local" || providerID != nil {
				t.Fatal("a conflict changed the existing account")
			}
			_, err = h.consume(h.respond(resp{nameID: saml.NameID{Format: saml.DefaultNameIDFormat, Value: existingEmail}}), nil)
			if !errors.As(err, &conflict) {
				t.Fatalf("email as NameID: err = %v, want EmailConflictError", err)
			}
		})
	}
}

// TestSAMLEmailCollisionNamesTheExistingMethodAfterTheCutover: with the users
// owned by auth-service, the conflict names the password method, else the
// provider of the oldest linked identity, else nothing.
func TestSAMLEmailCollisionNamesTheExistingMethodAfterTheCutover(t *testing.T) {
	ctx := context.Background()
	google := func(h *harness, owner uuid.UUID) {
		key := identity.ExternalKey{Issuer: "https://accounts.google.com", Provider: identity.ProviderGoogle, Subject: "g-" + uuid.NewString()}
		if _, err := h.db.legacy.LinkIdentity(ctx, key, owner, identity.Actor{}); err != nil {
			t.Fatal(err)
		}
	}
	sso := func(h *harness, owner uuid.UUID) {
		key := identity.ExternalKey{Issuer: h.idp.entityID, Provider: identity.Provider(h.provider.ID.String()), Subject: "saml1|x|y|" + uuid.NewString()}
		if _, err := h.db.legacy.LinkIdentity(ctx, key, owner, identity.Actor{}); err != nil {
			t.Fatal(err)
		}
	}
	for name, tc := range map[string]struct {
		setup func(h *harness, owner uuid.UUID)
		want  string
	}{
		"password": {setup: func(h *harness, owner uuid.UUID) {
			if err := h.db.owned.SetPasswordHash(ctx, owner, "$2b$12$abcdefghijklmnopqrstuuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0", identity.Actor{}); err != nil {
				t.Fatal(err)
			}
			google(h, owner)
		}, want: "local"},
		"oldest identity is social":     {setup: func(h *harness, owner uuid.UUID) { google(h, owner); sso(h, owner) }, want: "google"},
		"oldest identity is an SSO row": {setup: func(h *harness, owner uuid.UUID) { sso(h, owner); google(h, owner) }, want: "sso"},
		"no password and no identity":   {setup: func(*harness, uuid.UUID) {}, want: ""},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, true)
			email := uniqueEmail("owner")
			owner := h.goAPIUser(email, "local", nil, false)
			if err := h.db.legacy.Project(ctx, owner); err != nil {
				t.Fatal(err)
			}
			tc.setup(h, owner)
			h.ownUsers()
			_, err := h.consume(h.respond(resp{nameID: nid("subject-" + uuid.NewString()), email: email}), nil)
			var conflict *saml.EmailConflictError
			if !errors.As(err, &conflict) || conflict.Method != tc.want {
				t.Fatalf("err = %v (%+v), want EmailConflictError naming %q", err, conflict, tc.want)
			}
		})
	}
}

func TestSAMLSubjectIsScopedByProviderAndIssuer(t *testing.T) {
	homeIdP := newTestIdP(t)
	home := newHarnessWithIdP(t, homeIdP, true)
	existing := home.linkedUser(uniqueEmail("existing"), "home-subject")

	// A provider row of another organization with the same IdP entity ID and its own key.
	otherIdP := newTestIdP(t)
	otherIdP.entityID = homeIdP.entityID
	other := newHarnessWithIdP(t, otherIdP, true)
	other.ownUsers()
	before := other.writeCounts()
	signIn, err := other.consume(other.respond(resp{nameID: nid("home-subject"), email: uniqueEmail("new")}), nil)
	if !errors.Is(err, saml.ErrNotOrgMember) || signIn.PrincipalID == existing {
		t.Fatalf("another provider row: sign-in %+v err %v, want a new-account refusal, never the existing account", signIn, err)
	}
	if after := other.writeCounts(); after != before {
		t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
	}

	// The same subject from another issuer through the home provider row.
	signIn, err = home.svc.Consume(context.Background(), home.provider.ID,
		b64(otherIdP.response(t, resp{nameID: nid("home-subject"), destination: strp(home.sp().ACSURL),
			recipient: strp(home.sp().ACSURL), audiences: [][]string{{home.sp().EntityID}}})), nil)
	if err == nil {
		t.Fatalf("a response signed by another key was accepted: %+v", signIn)
	}
}

func TestSAMLLegacyAccountSignsInBySubject(t *testing.T) {
	h := newHarness(t, true)
	ctx := context.Background()
	email := uniqueEmail("legacy")
	nameID := "legacy-" + uuid.NewString()
	legacy := h.goAPIUser(email, "saml", &nameID, false)

	t.Run("not a member of the provider's organization", func(t *testing.T) {
		for asserted, address := range map[string]string{"the account's email": email, "a free email": uniqueEmail("free")} {
			t.Run(asserted, func(t *testing.T) {
				before := h.writeCounts()
				_, err := h.consume(h.respond(resp{nameID: nid(nameID), email: address}), nil)
				if !errors.Is(err, saml.ErrNotOrgMember) || saml.OutcomeReason(err) != "membership_required" {
					t.Fatalf("err = %v, want ErrNotOrgMember (membership_required)", err)
				}
				if after := h.writeCounts(); after != before {
					t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
				}
			})
		}
	})
	t.Run("member", func(t *testing.T) {
		h.member(legacy)
		signIn, err := h.consume(h.respond(resp{nameID: nid(nameID), email: email}), nil)
		if err != nil || signIn.PrincipalID != legacy || signIn.OrganizationID == nil {
			t.Fatalf("sign-in %+v err %v, want the legacy account in the provider's org", signIn, err)
		}
		subject, _ := saml.Subject(nid(nameID))
		var n int
		if err := h.db.admin.QueryRow(ctx, `SELECT count(*) FROM auth.external_identities WHERE principal_id = $1 AND issuer = $2 AND provider = $3 AND subject = $4`,
			legacy, h.idp.entityID, h.provider.ID.String(), subject).Scan(&n); err != nil || n != 1 {
			t.Fatalf("identity not linked under the composite subject (n=%d err=%v)", n, err)
		}
	})
	t.Run("two go-api accounts share the NameID", func(t *testing.T) {
		shared := "shared-" + uuid.NewString()
		for _, e := range []string{uniqueEmail("s1"), uniqueEmail("s2")} {
			h.member(h.goAPIUser(e, "saml", &shared, false))
		}
		before := h.writeCounts()
		_, err := h.consume(h.respond(resp{nameID: nid(shared), email: uniqueEmail("s3")}), nil)
		if !errors.Is(err, saml.ErrIdentityAmbiguous) || saml.OutcomeReason(err) != "identity_conflict" {
			t.Fatalf("err = %v, want ErrIdentityAmbiguous (identity_conflict)", err)
		}
		if after := h.writeCounts(); after != before {
			t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
		}
	})
}

// writeCounts counts the rows a sign-in can write: users, memberships,
// principals, auth users and external identities.
func (h *harness) writeCounts() [5]int {
	return [5]int{
		h.count(`SELECT count(*) FROM public.users`),
		h.count(`SELECT count(*) FROM public.memberships`),
		h.count(`SELECT count(*) FROM auth.principals`),
		h.count(`SELECT count(*) FROM auth.users`),
		h.identityCount(),
	}
}

// TestSAMLNameIDInTwoOrganizationsIsRefusedWithoutWrites: two go-api accounts
// in two organizations carry one NameID. With auto-provisioning on, the
// sign-in is refused and writes no account, membership or identity.
func TestSAMLNameIDInTwoOrganizationsIsRefusedWithoutWrites(t *testing.T) {
	h := newHarness(t, true)
	ctx := context.Background()
	if _, err := h.db.admin.Exec(ctx, `UPDATE public.sso_providers SET auto_provision_users = true WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	otherOrg := uuid.New()
	if _, err := h.db.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'saml-test')`, otherOrg, "org-"+otherOrg.String()); err != nil {
		t.Fatal(err)
	}
	shared := "two-orgs-" + uuid.NewString()
	inOrgEmail := uniqueEmail("in-org")
	h.member(h.goAPIUser(inOrgEmail, "saml", &shared, false))
	elsewhere := h.goAPIUser(uniqueEmail("other-org"), "saml", &shared, false)
	if _, err := h.db.admin.Exec(ctx, `INSERT INTO public.memberships (id, user_id, org_id, role) VALUES ($1, $2, $3, 'member')`, uuid.New(), elsewhere, otherOrg); err != nil {
		t.Fatal(err)
	}
	for name, email := range map[string]string{"a new email": uniqueEmail("asserted"), "the in-org account's email": inOrgEmail} {
		t.Run(name, func(t *testing.T) {
			before := h.writeCounts()
			signIn, err := h.consume(h.respond(resp{nameID: nid(shared), email: email}), nil)
			if !errors.Is(err, saml.ErrIdentityAmbiguous) {
				t.Fatalf("sign-in %+v err %v, want ErrIdentityAmbiguous", signIn, err)
			}
			if after := h.writeCounts(); after != before {
				t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
			}
		})
	}
}

// TestSAMLLinkedIdentitySignsInWithoutReadingTheEmail: on a key hit the stored
// account signs in; the assertion's email is neither required nor compared.
func TestSAMLLinkedIdentitySignsInWithoutReadingTheEmail(t *testing.T) {
	h := newHarness(t, true)
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["corp.example.com"]'::json WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	for name, email := range map[string]string{"no email": "", "a domain outside the allowed list": "a@other.example.com"} {
		t.Run(name, func(t *testing.T) {
			signIn, err := h.consume(h.respond(resp{nameID: nid("user-1"), email: email}), nil)
			if err != nil || signIn.PrincipalID != user {
				t.Fatalf("sign-in %+v err %v, want the linked account", signIn, err)
			}
		})
	}
	t.Run("a miss still needs an email", func(t *testing.T) {
		_, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString())}), nil)
		if !errors.Is(err, saml.ErrEmailMissing) {
			t.Fatalf("err = %v, want ErrEmailMissing", err)
		}
	})
}

// TestSAMLAllowedDomainsCompareWithoutCase: the email domain and the allowed
// list compare case-insensitively.
func TestSAMLAllowedDomainsCompareWithoutCase(t *testing.T) {
	h := newHarness(t, true)
	h.ownUsers()
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["Example.COM"]'::json WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	before := h.writeCounts()
	signIn, err := h.consume(h.respond(resp{nameID: nid("new-" + uuid.NewString()), email: "new-" + uuid.NewString()[:8] + "@EXAMPLE.com"}), nil)
	if !errors.Is(err, saml.ErrNotOrgMember) {
		t.Fatalf("sign-in %+v err %v, want the domain allowed and the new account refused (ErrNotOrgMember)", signIn, err)
	}
	if after := h.writeCounts(); after != before {
		t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
	}
}

// TestSAMLResponseIsBoundToTheRowOfItsRoute: a provider row whose ACS override
// names another row is refused at load, so a response addressed to that other
// row never completes a sign-in through this row's route.
func TestSAMLResponseIsBoundToTheRowOfItsRoute(t *testing.T) {
	idp := newTestIdP(t)
	a := newHarnessWithIdP(t, idp, true)
	a.linkedUser(uniqueEmail("a"), "user-1")
	b := newHarnessWithIdP(t, idp, true)
	user := b.linkedUser(uniqueEmail("b"), "user-1")
	b.member(user)
	config := fmt.Sprintf(`{"entity_id": %q, "sso_url": "https://idp.example.test/sso", "certificate": %q, "sp_acs_url": %q, "sp_entity_id": %q}`,
		idp.entityID, encodeCert(idp), a.provider.ACSURL(baseURL), a.provider.SPEntityID(baseURL))
	if _, err := b.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET config = $2::json WHERE id = $1`, b.provider.ID, config); err != nil {
		t.Fatal(err)
	}
	issuer := &recordingIssuer{}
	mux := b.mux(issuer)
	posted := a.respond(resp{nameID: nid("user-1"), email: "a@example.com"})
	rec := serve(mux, http.MethodPost, "/api/v1/auth/saml/"+b.provider.ID.String()+"/acs", map[string]string{"SAMLResponse": posted}, nil)
	if rec.Code == http.StatusOK || len(issuer.got) != 0 {
		t.Fatalf("a response addressed to row A signed in through row B's route: %d %s %+v", rec.Code, rec.Body, issuer.got)
	}
	if _, err := b.svc.Consume(context.Background(), b.provider.ID, posted, nil); !errors.Is(err, saml.ErrConfig) {
		t.Fatalf("row B with an override naming row A: err = %v, want ErrConfig", err)
	}
	if _, err := a.consume(a.respond(resp{nameID: nid("user-1"), email: "a@example.com"}), nil); err != nil {
		t.Fatalf("row A is unaffected by row B's config: %v", err)
	}
}

// TestSAMLPrefixedACSOverrideIsRefused: an override that puts a proxy prefix in
// front of /api/v1/auth/saml/{row id}/acs is not the served route; the row is
// refused at load.
func TestSAMLPrefixedACSOverrideIsRefused(t *testing.T) {
	h := newHarness(t, true)
	h.linkedUser(uniqueEmail("u"), "user-1")
	acs := "https://proxy.example.com/edge/api/v1/auth/saml/" + h.provider.ID.String() + "/acs"
	config := fmt.Sprintf(`{"entity_id": %q, "sso_url": "https://idp.example.test/sso", "certificate": %q, "sp_acs_url": %q}`, h.idp.entityID, encodeCert(h.idp), acs)
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET config = $2::json WHERE id = $1`, h.provider.ID, config); err != nil {
		t.Fatal(err)
	}
	posted := b64(h.idp.response(t, resp{nameID: nid("user-1"), email: "a@example.com", destination: strp(acs), recipient: strp(acs),
		audiences: [][]string{{h.provider.SPEntityID(baseURL)}}}))
	if _, err := h.svc.Consume(context.Background(), h.provider.ID, posted, nil); !errors.Is(err, saml.ErrConfig) {
		t.Fatalf("prefixed override: err = %v, want ErrConfig", err)
	}
}

func TestSAMLDisabledAccountIsRefused(t *testing.T) {
	h := newHarness(t, true)
	ctx := context.Background()
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	if _, err := h.db.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, user); err != nil {
		t.Fatal(err)
	}
	if err := h.db.legacy.Project(ctx, user); err != nil {
		t.Fatal(err)
	}
	_, err := h.consume(h.respond(resp{nameID: nid("user-1"), email: "a@example.com"}), nil)
	if !errors.Is(err, saml.ErrAccountDisabled) || saml.OutcomeReason(err) != "principal_deactivated" {
		t.Fatalf("err = %v, want ErrAccountDisabled", err)
	}
}

// TestSAMLNewAccountIsRefusedWithoutWrites: a sign-in that would need a new
// account is refused with ErrNotOrgMember (membership_required) before and
// after the users cutover, with the membership seam set, and writes nothing.
func TestSAMLNewAccountIsRefusedWithoutWrites(t *testing.T) {
	h := newHarness(t, true)
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("users owned by auth %v", owned), func(t *testing.T) {
			if owned {
				h.ownUsers()
			}
			before := h.writeCounts()
			signIn, err := h.consume(h.respond(resp{nameID: nid("new-" + uuid.NewString()), email: uniqueEmail("new")}), nil)
			if !errors.Is(err, saml.ErrNotOrgMember) || saml.OutcomeReason(err) != "membership_required" {
				t.Fatalf("sign-in %+v err %v, want ErrNotOrgMember (membership_required)", signIn, err)
			}
			if after := h.writeCounts(); after != before {
				t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
			}
		})
	}

	t.Run("auto provisioning disabled", func(t *testing.T) {
		if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET auto_provision_users = false WHERE id = $1`, h.provider.ID); err != nil {
			t.Fatal(err)
		}
		_, err := h.consume(h.respond(resp{nameID: nid("new-" + uuid.NewString()), email: uniqueEmail("new")}), nil)
		if !errors.Is(err, saml.ErrAutoProvisionDisabled) {
			t.Fatalf("err = %v, want ErrAutoProvisionDisabled", err)
		}
	})
}

// TestSAMLSignInWithoutMembershipIsRefusedWithoutWrites: with no membership
// seam, a linked account that is not a member of the provider's organization
// and a new account are refused with ErrNotOrgMember (membership_required)
// and write nothing; with a failing seam a new account is refused and writes
// nothing; the linked account signs in once it is a member.
func TestSAMLSignInWithoutMembershipIsRefusedWithoutWrites(t *testing.T) {
	h := newHarness(t, true)
	h.withoutJoins()
	user := h.linkedUser(uniqueEmail("u"), "user-1")
	h.ownUsers()
	for name, nameID := range map[string]string{"linked account": "user-1", "new account": "new-" + uuid.NewString()} {
		t.Run(name, func(t *testing.T) {
			before := h.writeCounts()
			signIn, err := h.consume(h.respond(resp{nameID: nid(nameID), email: uniqueEmail("free")}), nil)
			if !errors.Is(err, saml.ErrNotOrgMember) || saml.OutcomeReason(err) != "membership_required" {
				t.Fatalf("sign-in %+v err %v, want ErrNotOrgMember (membership_required)", signIn, err)
			}
			if after := h.writeCounts(); after != before {
				t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
			}
		})
	}
	t.Run("new account, the membership seam fails", func(t *testing.T) {
		h.svc.JIT = failingSeam{}
		defer func() { h.svc.JIT = nil }()
		before := h.writeCounts()
		signIn, err := h.consume(h.respond(resp{nameID: nid("new-" + uuid.NewString()), email: uniqueEmail("free")}), nil)
		if !errors.Is(err, saml.ErrNotOrgMember) {
			t.Fatalf("sign-in %+v err %v, want ErrNotOrgMember before any join", signIn, err)
		}
		if after := h.writeCounts(); after != before {
			t.Fatalf("users, memberships, principals, auth users, identities %v -> %v, want no write", before, after)
		}
	})
	h.member(user)
	signIn, err := h.consume(h.respond(resp{nameID: nid("user-1"), email: uniqueEmail("free")}), nil)
	if err != nil || signIn.PrincipalID != user || signIn.OrganizationID == nil || *signIn.OrganizationID != h.provider.OrgID {
		t.Fatalf("member: sign-in %+v err %v, want the account in the provider's org", signIn, err)
	}
}

// failingSeam is a membership seam whose writes fail and write nothing.
type failingSeam struct{}

func (failingSeam) JoinOnSignIn(context.Context, uuid.UUID, saml.Provider) error {
	return errors.New("membership store unavailable")
}

// TestSAMLAllowedDomainsAreEnforcedOnAMiss: without a linked identity the
// asserted email must be in an allowed domain; a key hit is covered by
// TestSAMLLinkedIdentitySignsInWithoutReadingTheEmail.
func TestSAMLAllowedDomainsAreEnforcedOnAMiss(t *testing.T) {
	h := newHarness(t, true)
	h.ownUsers()
	before := h.identityCount()
	if _, err := h.db.admin.Exec(context.Background(), `UPDATE public.sso_providers SET allowed_domains = '["corp.example.com"]'::json WHERE id = $1`, h.provider.ID); err != nil {
		t.Fatal(err)
	}
	for email, want := range map[string]error{
		"u-" + uuid.NewString()[:8] + "@example.com": saml.ErrDomainNotAllowed,
		"no-at-sign": saml.ErrEmailMalformed,
	} {
		_, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString()), email: email}), nil)
		if !errors.Is(err, want) {
			t.Fatalf("email %q: err = %v, want %v", email, err, want)
		}
	}
	if h.identityCount() != before {
		t.Fatal("a refused sign-up wrote an identity")
	}
	signIn, err := h.consume(h.respond(resp{nameID: nid("unlinked-" + uuid.NewString()), email: "u-" + uuid.NewString()[:8] + "@corp.example.com"}), nil)
	if !errors.Is(err, saml.ErrNotOrgMember) || h.identityCount() != before {
		t.Fatalf("an allowed domain: sign-in %+v err %v, want the new account refused (ErrNotOrgMember) with no identity written", signIn, err)
	}
}

func TestSAMLRuntimeRoleCannotRebindAStoredRequest(t *testing.T) {
	h := newHarness(t, false)
	h.initiate()
	_, err := h.db.runtime.Exec(context.Background(), `UPDATE auth.sso_flow_states SET binding_hash = decode(repeat('ab', 32), 'hex')
WHERE provider_id = $1`, h.provider.ID)
	if err == nil {
		t.Fatal("the runtime role can rebind a stored request to another browser")
	}
}
