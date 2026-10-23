//go:build integration

package keyrotation

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

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
