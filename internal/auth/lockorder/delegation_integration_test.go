//go:build integration

package lockorder

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/delegationend"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	authsession "github.com/full-chaos/dev-health-ops/internal/auth/session"
)

const insertSessionPrefix = `INSERT INTO "auth".sessions`

type delegationPair struct {
	actor, target, granter uuid.UUID
	input                  authsession.StartInput
	base                   authsession.Issued
}

func (e *env) makeAdmin(t *testing.T, ctx context.Context, id uuid.UUID) {
	t.Helper()
	if _, err := e.identity.BackfillUser(ctx, identity.BackfillTarget{ID: id, Email: email(id), DisplayName: "d", Active: true,
		Verified: true, Superuser: true}, false); err != nil {
		t.Fatalf("BackfillUser: %v", err)
	}
}

func (e *env) delegationPair(t *testing.T, ctx context.Context) delegationPair {
	t.Helper()
	p := delegationPair{actor: e.seedUser(t, ctx), target: e.seedUser(t, ctx), granter: e.seedUser(t, ctx)}
	e.makeAdmin(t, ctx, p.actor)
	e.makeAdmin(t, ctx, p.granter)
	org := e.seedOrg(t, ctx)
	e.add(t, ctx, org, p.target, "member", nil)
	base, err := e.sessions.Create(ctx, p.actor, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	p.base = base
	p.input = authsession.StartInput{BaseSessionID: base.Session.ID, TargetPrincipalID: p.target, OrganizationID: org, Reason: "support"}
	return p
}

func (e *env) liveDelegations(t *testing.T, ctx context.Context, actor uuid.UUID) string {
	t.Helper()
	return e.scalar(t, ctx, `SELECT count(*)::text FROM auth.sessions WHERE actor_principal_id = $1 AND revoked_at IS NULL`, actor)
}

// D1, Start and a role revoke of the actor: Start holds the shared write lock
// from before it reads the role, so the revoke waits, then ends the row.
func TestD1StartAndRoleRevokeOfTheActor(t *testing.T) {
	const cell = "D1"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	p := e.delegationPair(t, ctx)

	reached := e.authBarrier.Arm(insertSessionPrefix)
	s := goAuth("StartDelegation", func() error {
		_, err := e.sessions.StartDelegation(ctx, p.input)
		return err
	})
	awaitReached(t, cell, "Start's INSERT", reached, s)
	r := goAuth("platform role Revoke", func() error {
		return e.platform.Revoke(ctx, platformrole.RevokeInput{Actor: p.granter, Principal: p.actor, Role: platformrole.PlatformAdmin})
	})
	e.awaitWaiters(t, ctx, cell, 1, s, r)
	e.authBarrier.Release()
	e.finish(t, cell, s, r)

	wantNoErr(t, cell, s)
	wantNoErr(t, cell, r)
	if got := e.liveDelegations(t, ctx, p.actor); got != "0" {
		t.Fatalf("%s: %s live delegated rows of an actor with no role, want 0", cell, got)
	}
}

// D2, Start and a revoke of its base: Start holds the base row, so the revoke
// waits, then ends the child it can now see.
func TestD2StartAndRevokeOfItsBase(t *testing.T) {
	const cell = "D2"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	p := e.delegationPair(t, ctx)

	reached := e.authBarrier.Arm(insertSessionPrefix)
	s := goAuth("StartDelegation", func() error {
		_, err := e.sessions.StartDelegation(ctx, p.input)
		return err
	})
	awaitReached(t, cell, "Start's INSERT", reached, s)
	r := goAuth("session Revoke", func() error {
		_, err := e.sessions.Revoke(ctx, p.base.Session.ID, authsession.Actor{})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, s, r)
	e.authBarrier.Release()
	e.finish(t, cell, s, r)

	wantNoErr(t, cell, s)
	wantNoErr(t, cell, r)
	if got := e.liveDelegations(t, ctx, p.actor); got != "0" {
		t.Fatalf("%s: %s live children of a revoked base, want 0", cell, got)
	}
}

// D3, Start and an Exchange of its base: the Exchange holds the base row and
// then needs the principal, which Start holds FOR SHARE. Start does not wait
// for the row, so no cycle closes: it is ErrBusy and works after the Exchange.
func TestD3StartAndExchangeOfItsBase(t *testing.T) {
	const cell = "D3"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	p := e.delegationPair(t, ctx)

	reached := e.authBarrier.Arm(`INSERT INTO "auth".refresh_credentials`)
	x := goAuth("session Exchange", func() error {
		result, err := e.sessions.Exchange(ctx, p.base.Credential)
		if err != nil {
			return err
		}
		if !result.Reason.Success() {
			return errors.New("the exchange was refused: " + string(result.Reason))
		}
		return nil
	})
	awaitReached(t, cell, "Exchange's credential INSERT", reached, x)
	if _, err := e.sessions.StartDelegation(ctx, p.input); !errors.Is(err, authsession.ErrBusy) {
		t.Fatalf("%s: Start behind an Exchange = %v, want ErrBusy", cell, err)
	}
	e.authBarrier.Release()
	e.finish(t, cell, x)
	wantNoErr(t, cell, x)
	if _, err := e.sessions.StartDelegation(ctx, p.input); err != nil {
		t.Fatalf("%s: Start after the Exchange: %v", cell, err)
	}
}

// D4, Start and the actor's disable: the go-api write waits for Start's share
// lock on the users row, then the projection ends the row.
func TestD4StartAndDisableOfTheActor(t *testing.T) {
	const cell = "D4"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	p := e.delegationPair(t, ctx)

	reached := e.authBarrier.Arm(insertSessionPrefix)
	s := goAuth("StartDelegation", func() error {
		_, err := e.sessions.StartDelegation(ctx, p.input)
		return err
	})
	awaitReached(t, cell, "Start's INSERT", reached, s)
	d := goAuth("disable and project the actor", func() error {
		if _, err := e.admin.Exec(ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, p.actor); err != nil {
			return err
		}
		return e.identity.Project(ctx, p.actor)
	})
	e.awaitWaiters(t, ctx, cell, 1, s, d)
	e.authBarrier.Release()
	e.finish(t, cell, s, d)

	wantNoErr(t, cell, s)
	wantNoErr(t, cell, d)
	if got := e.liveDelegations(t, ctx, p.actor); got != "0" {
		t.Fatalf("%s: %s live delegated rows of a disabled actor, want 0", cell, got)
	}
	reason := e.scalar(t, ctx, `SELECT revoked_reason FROM auth.sessions WHERE actor_principal_id = $1`, p.actor)
	if reason != delegationend.ReasonActorEnded {
		t.Fatalf("%s: revoked_reason = %q, want %q", cell, reason, delegationend.ReasonActorEnded)
	}
}
