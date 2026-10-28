//go:build integration

package local

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// statementRecorder is a pgx tracer that records the text of every statement a pool sends.
type statementRecorder struct {
	mu   sync.Mutex
	sqls []string
}

func (r *statementRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sqls = append(r.sqls, data.SQL)
	return ctx
}

func (r *statementRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (r *statementRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.sqls
	r.sqls = nil
	return out
}

func tracedLinks(t *testing.T, ctx context.Context, env *pgEnv) (*PGLinks, *statementRecorder) {
	t.Helper()
	recorder := &statementRecorder{}
	cfg := env.runtime.Config().Copy()
	cfg.ConnConfig.Tracer = recorder
	cfg.MaxConns, cfg.MinConns = 1, 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return NewPGLinks(pool, schema), recorder
}

// project copies a public organization into the auth schema, as login does before it names one.
func (e *pgEnv) project(t *testing.T, ctx context.Context, org uuid.UUID) {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := identity.New(identity.Config{Pool: e.runtime, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ProjectOrganization(ctx, org); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
}

// membership seeds a public organization, projects it into the auth schema as login does, and writes the auth
// membership edge (a guarded table, so the insert runs under the commit guard setting).
func (e *pgEnv) membership(t *testing.T, ctx context.Context, user uuid.UUID, org uuid.UUID, createdAt string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name, settings, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, '{}', 'community', 'stripe', true, now(), now()) ON CONFLICT DO NOTHING`, org, org.String()); err != nil {
		t.Fatalf("seed organization: %v", err)
	}
	e.project(t, ctx, org)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	subject := user.String()
	err = audit.Commit(ctx, e.runtime, schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.memberships (organization_id, principal_id, status, role, created_at, updated_at)
VALUES ($1, $2, 'active', 'member', `+createdAt+`, `+createdAt+`)`, org, subject)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "auth.membership.added", Outcome: audit.OutcomeAllowed, SubjectPrincipalID: &subject, Attributes: map[string]any{"reason_code": "membership_added"}},
		OffStream: audit.OffStreamLinkCredentialLifecycle,
	})
	if err != nil {
		t.Fatalf("seed membership: %v", err)
	}
}

// AuditOrganization runs as the auth runtime role, which holds no SELECT on public.memberships.
func TestAuditOrganizationRunsAsTheRuntimeRole(t *testing.T) {
	ctx := context.Background()
	env, _ := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	none, err := identity.AuditOrganization(ctx, env.runtime, `"auth"`, p)
	if err != nil || none != nil {
		t.Fatalf("no membership: %v, %v; want nil, nil", none, err)
	}
	first, later := uuid.New(), uuid.New()
	env.membership(t, ctx, p, later, "now()")
	env.membership(t, ctx, p, first, "now() - interval '2 days'")
	got, err := identity.AuditOrganization(ctx, env.runtime, `"auth"`, p)
	if err != nil || got == nil || *got != first.String() {
		t.Fatalf("AuditOrganization = %v, %v; want %s", got, err, first)
	}
}

func TestResetRequestWritesTheLinkAndOneAuditRowInTheEarliestMembershipOrganization(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	first, later := uuid.New(), uuid.New()
	env.membership(t, ctx, p, later, "now()")
	env.membership(t, ctx, p, first, "now() - interval '2 days'")
	if err := links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("rq1"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1`, p.String()); n != 1 {
		t.Errorf("link rows = %d, want 1", n)
	}
	var n int
	var org, subject, reason string
	if err := env.admin.QueryRow(ctx, `SELECT count(*) OVER (), organization_id::text, subject_principal_id::text, attributes->>'reason_code'
FROM auth.security_audit_events WHERE event_type = $1`, audit.EventPasswordResetRequested).Scan(&n, &org, &subject, &reason); err != nil {
		t.Fatal(err)
	}
	if n != 1 || org != first.String() || subject != p.String() || reason != "password_reset_requested" {
		t.Errorf("rows %d org %s (want %s) subject %s reason %s", n, org, first, subject, reason)
	}
}

func TestResetRequestOfAPrincipalWithNoMembershipNamesNoOrganization(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	if err := links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("np1"), TTL: ResetTTL}); err != nil {
		t.Fatalf("a request must not fail for a principal with no membership: %v", err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND organization_id IS NULL AND subject_principal_id = $2`, audit.EventPasswordResetRequested, p.String()); n != 1 {
		t.Errorf("rows with no organization = %d, want 1", n)
	}
}

func TestVerificationRequestWritesNoResetRequestRow(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	if err := links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposeEmailVerification, TokenHash: linkHash("v1"), TTL: VerificationTTL}); err != nil {
		t.Fatal(err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Errorf("audit rows = %d, want 0 for a verification link", n)
	}
}

func TestUnknownAddressRequestLeavesATrailWithNoPrincipalAndNoAddress(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	if err := links.IssueBound(ctx, IssueRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("u1"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials`); n != 0 {
		t.Errorf("link rows = %d, want 0 for a decoy", n)
	}
	var text string
	var subject, org *string
	if err := env.admin.QueryRow(ctx, `SELECT e::text, subject_principal_id::text, organization_id::text
FROM auth.security_audit_events e WHERE event_type = $1`, audit.EventPasswordResetRequested).Scan(&text, &subject, &org); err != nil {
		t.Fatalf("one trail row expected: %v", err)
	}
	if subject != nil || org != nil || strings.Contains(text, "@") || !strings.Contains(text, "principal_not_found") {
		t.Errorf("row %s subject %v org %v", text, subject, org)
	}
}

func TestKnownAndUnknownRequestsSendTheSameStatements(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	env.membership(t, ctx, p, uuid.New(), "now()")
	traced, recorder := tracedLinks(t, ctx, env)
	_ = links
	if err := traced.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("k1"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	known := recorder.take()
	if err := traced.IssueBound(ctx, IssueRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("k2"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	unknown := recorder.take()
	if len(known) == 0 || !reflect.DeepEqual(known, unknown) {
		t.Fatalf("statement sequences differ:\nknown   %q\nunknown %q", known, unknown)
	}
}

func TestFaultAtTheAuditInsertLeavesNoLinkRow(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	for _, stmt := range []string{
		`CREATE FUNCTION auth.fault_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit fault'; END $$`,
		`CREATE TRIGGER zz_fault_audit BEFORE INSERT ON auth.security_audit_events FOR EACH ROW EXECUTE FUNCTION auth.fault_audit()`,
	} {
		if _, err := env.admin.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("f1"), TTL: ResetTTL}); err == nil {
		t.Fatal("IssueBound succeeded with the audit insert failing")
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials`); n != 0 {
		t.Errorf("link rows = %d after an audit insert fault, want 0", n)
	}
}

func TestFaultInTheLinkInsertLeavesNoAuditRow(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.owner(t, ctx)
	for _, stmt := range []string{
		`CREATE FUNCTION auth.fault_link() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected link fault'; END $$`,
		`CREATE TRIGGER zz_fault_link BEFORE INSERT ON auth.link_credentials FOR EACH ROW EXECUTE FUNCTION auth.fault_link()`,
	} {
		if _, err := env.admin.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("f2"), TTL: ResetTTL}); err == nil {
		t.Fatal("IssueBound succeeded with the link insert failing")
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events`); n != 0 {
		t.Errorf("audit rows = %d after a link insert fault, want 0", n)
	}
}

func TestVerificationWritesExactlyOneAuditRowInTheEarliestMembershipOrganization(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	p := rig.account(t, ctx, "Verify-pass-1")
	org := uuid.New()
	rig.env.membership(t, ctx, p, org, "now()")
	if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposeEmailVerification, TokenHash: linkHash("verify-secret"), TTL: VerificationTTL}); err != nil {
		t.Fatal(err)
	}
	out, err := rig.flows.Verify(ctx, "verify-secret")
	if err != nil || out.Kind != FlowOK {
		t.Fatalf("Verify = %+v, %v", out, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type IN ('auth.email.verified', 'auth.user.email_verified')`, p.String()); n != 1 {
		t.Fatalf("verification audit rows = %d, want exactly 1", n)
	}
	var eventType, gotOrg string
	if err := rig.env.admin.QueryRow(ctx, `SELECT event_type, organization_id::text FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type LIKE '%email_verified'`, p.String()).Scan(&eventType, &gotOrg); err != nil {
		t.Fatal(err)
	}
	if eventType != "auth.user.email_verified" || gotOrg != org.String() {
		t.Errorf("event %s organization %s, want auth.user.email_verified in %s", eventType, gotOrg, org)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`, p.String()); n != 1 {
		t.Errorf("consumed link rows = %d, want 1", n)
	}
	if again, err := rig.flows.Verify(ctx, "verify-secret"); err != nil || again.Kind != FlowInvalidToken {
		t.Errorf("replay = %+v, %v; want a refusal", again, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type LIKE '%email_verified'`, p.String()); n != 1 {
		t.Errorf("rows after a replay = %d, want 1", n)
	}
}

// afterPeekAccounts runs hook once the verification link has been read and before the write that consumes it.
type afterPeekAccounts struct {
	IdentityAccounts
	hook func(ctx context.Context)
}

func (a afterPeekAccounts) PeekVerificationLink(ctx context.Context, hash [32]byte) (uuid.UUID, bool, error) {
	id, ok, err := a.IdentityAccounts.PeekVerificationLink(ctx, hash)
	if ok && a.hook != nil {
		a.hook(ctx)
	}
	return id, ok, err
}

// A link spent between the read and the write that consumes it is refused: nothing is verified and no
// verification row is written.
func TestVerificationOfALinkSpentAfterTheReadIsRefusedAndWritesNothing(t *testing.T) {
	ctx := context.Background()
	rig := newEndRig(t, ctx)
	p := rig.account(t, ctx, "Verify-pass-2")
	if err := rig.links.IssueBound(ctx, IssueRequest{PrincipalID: p, Purpose: PurposeEmailVerification, TokenHash: linkHash("raced-secret"), TTL: VerificationTTL}); err != nil {
		t.Fatal(err)
	}
	racing := afterPeekAccounts{IdentityAccounts: rig.accounts, hook: func(ctx context.Context) {
		if _, ok, err := rig.links.Redeem(ctx, RedeemRequest{Purpose: PurposeEmailVerification, TokenHash: linkHash("raced-secret")}); err != nil || !ok {
			t.Errorf("spending the link between the read and the write: ok %v, err %v", ok, err)
		}
	}}
	flows, err := NewFlows(FlowsConfig{Accounts: racing, Links: rig.links, Sessions: rig.sessions, Mail: &liveMail{},
		Locker: rig.links, UsersOwnedByAuth: true, Hash: cheapHash})
	if err != nil {
		t.Fatal(err)
	}
	out, err := flows.Verify(ctx, "raced-secret")
	if err != nil || out.Kind != FlowInvalidToken {
		t.Fatalf("Verify = %+v, %v; want FlowInvalidToken", out, err)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE subject_principal_id = $1 AND event_type = 'auth.user.email_verified'`, p.String()); n != 0 {
		t.Errorf("verification rows = %d, want 0: a refused verification writes no row", n)
	}
	if n := rig.env.count(t, ctx, `SELECT count(*) FROM public.users WHERE id = $1 AND is_verified`, p); n != 0 {
		t.Errorf("the address was marked verified by a link that was already spent")
	}
}
