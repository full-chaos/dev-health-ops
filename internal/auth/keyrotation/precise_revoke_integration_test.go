//go:build integration

package keyrotation

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

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

// A compromise that revokes a base session ends its delegated child too,
// whatever key the child carries, and Plan and Apply count the same rows.
func TestCompromiseEndsTheDelegatedChildOfARevokedSession(t *testing.T) {
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
	pgschema.Apply(ctx, t, e.admin)
	child := uuid.NewString()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.sessions (id, principal_id, organization_id, token_hash, issued_at, created_at, expires_at, amr,
		legacy_token_version, signing_kids, actor_principal_id, delegated_from_session_id, delegation_reason, delegation_permitted_actions)
		VALUES ($1, $2, $3, '\x0a', now(), now(), now() + interval '10 minutes', '{pwd}', 0, '{a}', $4, $5, 'support', '{analytics.read}')`,
		child, target, org, actor, base); err != nil {
		t.Fatal(err)
	}
	if _, err := e.admin.Exec(ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'compromise')`, org, "o-"+org[:8]); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{actor, target} {
		if _, err := e.admin.Exec(ctx, `INSERT INTO public.users
			(id, email, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
			VALUES ($1, $2, 'local', true, true, false, 0, now(), now())`, id, "u-"+id[:8]+"@example.test"); err != nil {
			t.Fatal(err)
		}
	}
	pgseed.ImpersonationSession(ctx, t, e.admin, child, actor, target, org, "member", compromisedAt.Add(10*time.Minute), nil)
	plan, err := operator.Plan(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || plan.Sessions != 2 {
		t.Fatalf("compromise dry run = %+v, %v; want 2 sessions", plan, err)
	}
	result, err := operator.Apply(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || result.Sessions != 2 {
		t.Fatalf("compromise b = %+v, %v; want 2 sessions revoked", result, err)
	}
	for _, id := range []string{base, child} {
		if got := e.revokedReason(t, ctx, id); got != "compromised" {
			t.Errorf("session %s revoked_reason = %q, want compromised", id, got)
		}
	}
	var ended int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events
		WHERE event_type = 'auth.session.delegation_ended' AND attributes->>'target_id' = $1`, child).Scan(&ended); err != nil {
		t.Fatal(err)
	}
	if ended != 1 {
		t.Fatalf("delegation_ended rows = %d, want 1", ended)
	}
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM public.impersonation_sessions p
		JOIN public.audit_logs a ON a.action = 'impersonation_stop' AND a.user_id = p.admin_user_id AND a.org_id = p.target_org_id
			AND a.resource_type = 'session' AND a.resource_id = p.target_user_id::text
		WHERE p.id = $1 AND p.ended_at IS NOT NULL`, child).Scan(&ended); err != nil {
		t.Fatal(err)
	}
	if ended != 1 {
		t.Fatalf("ended public rows with one stop row = %d, want 1", ended)
	}
}
