//go:build integration

package keyrotation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

func (e *env) setKids(t *testing.T, ctx context.Context, id string, kids []string) {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `UPDATE auth.sessions SET signing_kids = $2::text[] WHERE id = $1`, id, kids); err != nil {
		t.Fatalf("seed signing_kids: %v", err)
	}
}

// A compromise of b revokes the sessions whose recorded kids hold b, whatever
// their age, and no other session with recorded kids; a NULL session keeps the
// window rule (in the window: revoked; outside it: kept).
func TestCompromiseRevokesExactlyTheSessionsOfTheKid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	dir := t.TempDir()
	writeKey(t, dir, "a.pem")
	writeKey(t, dir, "b.pem")
	writeKey(t, dir, "c.pem")
	manifest := writeManifest(t, dir, "a", "active", "a.pem", "b", "pending", "b.pem", "c", "pending", "c.pem")
	custody, err := token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	at := &clock{now: start}
	operator := &Operator{Pool: e.admin, Schema: e.schema, Now: at.Now}
	if _, err := operator.Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); err != nil {
		t.Fatal(err)
	}
	activatedB := start.Add(PendingFloor)
	at.set(activatedB)
	if _, err := operator.Apply(ctx, Request{Verb: VerbActivate, KeyID: "b"}); err != nil {
		t.Fatal(err)
	}
	compromisedAt := activatedB.Add(RetiringFloor + time.Minute)
	at.set(compromisedAt)
	principal := uuid.NewString()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'keyrotation')`, principal); err != nil {
		t.Fatal(err)
	}
	inWindow := compromisedAt.Add(-5 * time.Minute)
	outOfWindow := compromisedAt.Add(-RevokeWindow - time.Second)
	beforeB := activatedB.Add(-time.Minute)

	onlyB := e.session(t, ctx, principal, inWindow, nil, false)
	e.setKids(t, ctx, onlyB, []string{"b"})
	oldWithB := e.session(t, ctx, principal, outOfWindow, nil, false)
	e.setKids(t, ctx, oldWithB, []string{"a", "b"})
	onlyA := e.session(t, ctx, principal, inWindow, nil, false)
	e.setKids(t, ctx, onlyA, []string{"a"})
	hs256 := e.session(t, ctx, principal, inWindow, nil, false)
	e.setKids(t, ctx, hs256, []string{})
	nullInWindow := e.session(t, ctx, principal, inWindow, nil, false)
	nullBeforeB := e.session(t, ctx, principal, beforeB, nil, false)
	nullOutOfWindow := e.session(t, ctx, principal, outOfWindow, nil, false)
	refreshed := compromisedAt.Add(-2 * time.Minute)
	nullRefreshedInWindow := e.session(t, ctx, principal, start.Add(-time.Hour), &refreshed, false)
	keptRefreshed := e.session(t, ctx, principal, start.Add(-time.Hour), &refreshed, false)
	e.setKids(t, ctx, keptRefreshed, []string{"a"})

	plan, err := operator.Plan(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || plan.Sessions != 4 {
		t.Fatalf("compromise dry run = %+v, %v; want 4 sessions", plan, err)
	}
	result, err := operator.Apply(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || result.Sessions != 4 {
		t.Fatalf("compromise b = %+v, %v; want 4 sessions revoked", result, err)
	}
	for id, want := range map[string]string{
		onlyB: "compromised", oldWithB: "compromised", nullInWindow: "compromised", nullRefreshedInWindow: "compromised",
		onlyA: "", hs256: "", nullBeforeB: "", nullOutOfWindow: "", keptRefreshed: "",
	} {
		if got := e.revokedReason(t, ctx, id); got != want {
			t.Errorf("session %s revoked_reason = %q, want %q", id, got, want)
		}
	}
}

// delegatedCompromise is a compromisable key b with one session under it and a
// delegated child of that session.
type delegatedCompromise struct {
	operator           *Operator
	at                 *clock
	actor, target, org string
	base, child        string
	compromisedAt      time.Time
}

func (e *env) delegatedCompromise(t *testing.T, ctx context.Context) delegatedCompromise {
	t.Helper()
	dir := t.TempDir()
	writeKey(t, dir, "a.pem")
	writeKey(t, dir, "b.pem")
	writeKey(t, dir, "c.pem")
	manifest := writeManifest(t, dir, "a", "active", "a.pem", "b", "pending", "b.pem", "c", "pending", "c.pem")
	custody, err := token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	at := &clock{now: start}
	operator := &Operator{Pool: e.admin, Schema: e.schema, Now: at.Now}
	if _, err := operator.Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); err != nil {
		t.Fatal(err)
	}
	activatedB := start.Add(PendingFloor)
	at.set(activatedB)
	if _, err := operator.Apply(ctx, Request{Verb: VerbActivate, KeyID: "b"}); err != nil {
		t.Fatal(err)
	}
	compromisedAt := activatedB.Add(RetiringFloor + time.Minute)
	at.set(compromisedAt)
	actor, target, org := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, id := range []string{actor, target} {
		if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'keyrotation')`, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := audit.Commit(ctx, e.admin, e.schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			_, err := tx.Exec(ctx, `INSERT INTO auth.organizations (id, name, slug) VALUES ($1, 'o', 'keyrotation-org')`, org)
			return err
		},
		Audit:     audit.AuditEvent{EventType: "auth.session.seeded", Outcome: audit.OutcomeAllowed},
		OffStream: audit.OffStreamSessionLifecycle,
	}); err != nil {
		t.Fatal(err)
	}
	inWindow := compromisedAt.Add(-5 * time.Minute)
	base := e.session(t, ctx, actor, inWindow, nil, false)
	e.setKids(t, ctx, base, []string{"b"})
	child := uuid.NewString()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, organization_id, token_hash, issued_at, created_at, expires_at, amr,
		legacy_token_version, signing_kids, actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions)
		VALUES ($1, $2, $3, '\x0a', now(), now(), now() + interval '10 minutes', '{pwd}', 0, '{a}', $4, $5, 'support', '{analytics.read}')`,
		child, target, org, actor, base); err != nil {
		t.Fatal(err)
	}
	return delegatedCompromise{operator: operator, at: at, actor: actor, target: target, org: org, base: base, child: child, compromisedAt: compromisedAt}
}

func (e *env) wantCompromised(t *testing.T, ctx context.Context, c delegatedCompromise, operator *Operator) {
	t.Helper()
	plan, err := operator.Plan(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || plan.Sessions != 2 {
		t.Fatalf("compromise dry run = %+v, %v; want 2 sessions", plan, err)
	}
	result, err := operator.Apply(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || result.Sessions != 2 {
		t.Fatalf("compromise b = %+v, %v; want 2 sessions revoked", result, err)
	}
	for _, id := range []string{c.base, c.child} {
		if got := e.revokedReason(t, ctx, id); got != "compromised" {
			t.Errorf("session %s revoked_reason = %q, want compromised", id, got)
		}
	}
	var ended int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = 'auth.session.delegation_ended' AND attributes->>'target_id' = $1`, c.child).Scan(&ended); err != nil {
		t.Fatal(err)
	}
	if ended != 1 {
		t.Fatalf("delegation_ended rows = %d, want 1", ended)
	}
}

// A compromise that revokes a base session ends its delegated child too,
// whatever key the child carries, and Plan and Apply count the same rows.
func TestCompromiseEndsTheDelegatedChildOfARevokedSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	c := e.delegatedCompromise(t, ctx)
	e.wantCompromised(t, ctx, c, c.operator)
}

// A migration login that owns the auth schema and holds no write on go-api's
// tables runs a compromise over an open delegated row: the session ends, and
// the public row stays open to its expires_at, with no impersonation_stop row.
// The users-owned switch is off here.
func TestCompromiseAsAMigratorWithNoOpsWriteLeavesThePublicRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	pgschema.Apply(ctx, t, e.admin)
	c := e.delegatedCompromise(t, ctx)
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'compromise')`, c.org, "o-"+c.org[:8]); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{c.actor, c.target} {
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
			(id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
			VALUES ($1, $2, 'local', true, true, false, 0, now(), now())`, id, "u-"+id[:8]+"@example.test"); err != nil {
			t.Fatal(err)
		}
	}
	expires := c.compromisedAt.Add(10 * time.Minute)
	pgseed.ImpersonationSession(ctx, t, e.admin, c.child, c.actor, c.target, c.org, "member", expires, nil)
	login := e.migrator(t, ctx)
	var writes bool
	if err := e.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'public.impersonation_sessions', 'UPDATE')
		OR has_table_privilege($1, 'public.audit_logs', 'INSERT')`, login.Config().ConnConfig.User).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if writes {
		t.Fatal("the migration login holds a write on a go-api table")
	}
	e.wantCompromised(t, ctx, c, &Operator{Pool: login, Schema: e.schema, Now: c.at.Now})
	var open, stops int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM public.impersonation_sessions WHERE id = $1 AND ended_at IS NULL AND expires_at = $2`,
		c.child, expires).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 1 {
		t.Fatalf("open public rows with their expiry = %d, want 1", open)
	}
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM public.audit_logs WHERE action = 'impersonation_stop'`).Scan(&stops); err != nil {
		t.Fatal(err)
	}
	if stops != 0 {
		t.Fatalf("impersonation_stop rows = %d, want 0", stops)
	}
}

// migrator is a pool of a non-superuser login that owns every auth object, the
// migration login of a deployment.
func (e *env) migrator(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	role := "keyrotation_migrator_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	const password = "keyrotation-test-only-not-a-credential"
	if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN NOSUPERUSER PASSWORD '%s'`, role, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, statement := range []string{fmt.Sprintf(`REASSIGN OWNED BY %q TO CURRENT_USER`, role), fmt.Sprintf(`DROP OWNED BY %q`, role), fmt.Sprintf(`DROP ROLE %q`, role)} {
			if _, err := e.admin.Exec(cleanup, statement); err != nil {
				t.Logf("%s: %v", statement, err)
			}
		}
	})
	rows, err := e.admin.Query(ctx, `SELECT format('ALTER TABLE auth.%I OWNER TO %I', c.relname, $1::text)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'auth' AND c.relkind IN ('r', 'p') AND NOT c.relispartition`, role)
	if err != nil {
		t.Fatal(err)
	}
	statements, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	statements = append(statements, fmt.Sprintf(`ALTER SCHEMA auth OWNER TO %q`, role),
		fmt.Sprintf(`GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA auth TO %q`, role))
	for _, statement := range statements {
		if _, err := e.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	cfg := e.admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
