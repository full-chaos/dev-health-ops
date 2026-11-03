//go:build integration

package lockorder

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// The sign-in join holds the principal, the users row and the organization and
// has not inserted: the operator add waits and finds the member.
func TestJITJoinAndOperatorAddOneRow(t *testing.T) {
	const cell = "jit-join/operator-add"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)

	reached := e.authBarrier.Arm("INSERT INTO public.memberships")
	j := goAuth("sign-in join", func() error {
		return e.identity.JoinOnSignIn(ctx, user, identity.SSOJoin{OrgID: org, Role: "viewer"}, identity.SelfSignUp(nil), nil)
	})
	awaitReached(t, cell, "the join's public row write", reached, j)
	o := e.goOperatorAdd(ctx, "operator add", org, user)
	e.awaitWaiters(t, ctx, cell, 1, j, o.party)
	e.authBarrier.Release()
	e.finish(t, cell, j, o.party)

	if j.err != nil {
		t.Fatalf("%s: %s", cell, j)
	}
	o.wantChange(t, cell, "role -> viewer")
	e.wantOnePublicRow(t, ctx, cell, org, user, "viewer")
}

// The auth Add waits at the principal row and finds the member.
func TestJITJoinAndAuthAddOneRow(t *testing.T) {
	const cell = "jit-join/auth-add"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)

	reached := e.authBarrier.Arm("INSERT INTO public.memberships")
	j := goAuth("sign-in join", func() error {
		return e.identity.JoinOnSignIn(ctx, user, identity.SSOJoin{OrgID: org, Role: "viewer"}, identity.SelfSignUp(nil), nil)
	})
	awaitReached(t, cell, "the join's public row write", reached, j)
	a := goAuth("membership Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: "admin", Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, j, a)
	e.authBarrier.Release()
	e.finish(t, cell, j, a)

	if j.err != nil {
		t.Fatalf("%s: %s", cell, j)
	}
	if !errors.Is(a.err, membership.ErrAlreadyMember) {
		t.Fatalf("%s: %s, want ErrAlreadyMember", cell, a)
	}
	e.wantOnePublicRow(t, ctx, cell, org, user, "viewer")
}
