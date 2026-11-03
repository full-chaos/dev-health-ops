//go:build integration

package lockorder

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

// operatorParty is one `dho admin users update --id <user> --org <org>`.
type operatorParty struct {
	*party
	changes []string
}

func (e *env) goOperatorAdd(ctx context.Context, name string, org, user uuid.UUID) *operatorParty {
	o := &operatorParty{}
	o.party = goAuth(name, func() error {
		id, orgID := user.String(), org.String()
		result, err := admin.Operator{Pool: e.goPool, Now: time.Now, UsersOwned: true}.UpdateUser(ctx,
			admin.UpdateUserInput{ID: &id, MembershipOrg: &orgID})
		if err != nil {
			return err
		}
		o.changes = result.Changes
		return nil
	})
	return o
}

func (o *operatorParty) wantChange(t *testing.T, cell, text string) {
	t.Helper()
	if o.err != nil {
		t.Fatalf("%s: %s", cell, o)
	}
	if len(o.changes) != 1 || !strings.Contains(o.changes[0], text) {
		t.Fatalf("%s: %s changed %q, want one line with %q", cell, o.name, o.changes, text)
	}
}

func (e *env) wantOnePublicRow(t *testing.T, ctx context.Context, cell string, org, user uuid.UUID, role string) {
	t.Helper()
	got := e.scalar(t, ctx, `SELECT count(*)::text || ' ' || COALESCE(min(role), '') FROM public.memberships WHERE org_id = $1 AND user_id = $2`, org, user)
	if got != "1 "+role {
		t.Fatalf("%s: public.memberships has %q, want one %s row", cell, got, role)
	}
}

// awaitWaitOrEnd returns once a backend waits on a lock or p has ended.
func (e *env) awaitWaitOrEnd(t *testing.T, ctx context.Context, cell string, p *party) {
	t.Helper()
	deadline := time.Now().Add(choreographyBound)
	for !p.finished() && e.waiters(t, ctx) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: choreography: %s neither waited nor ended", cell, p.name)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The auth Add has its public row in and has not committed.
func TestOperatorAddAfterAuthAddInserted(t *testing.T) {
	const cell = "operator-add/after-auth-insert"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)

	reached := e.authBarrier.Arm(`INSERT INTO "auth".memberships`)
	a := goAuth("membership Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: "admin", Authorize: allowAll})
		return err
	})
	awaitReached(t, cell, "Add's auth row write", reached, a)
	o := e.goOperatorAdd(ctx, "operator add", org, user)
	e.awaitWaiters(t, ctx, cell, 1, a, o.party)
	e.authBarrier.Release()
	e.finish(t, cell, a, o.party)

	if a.err != nil {
		t.Fatalf("%s: %s", cell, a)
	}
	o.wantChange(t, cell, "role -> admin")
	e.wantOnePublicRow(t, ctx, cell, org, user, "admin")
}

// The auth Add holds the users row and has not inserted.
func TestOperatorAddBeforeAuthAddInserted(t *testing.T) {
	const cell = "operator-add/before-auth-insert"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)

	reached := e.authBarrier.Arm("INSERT INTO public.memberships")
	a := goAuth("membership Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: "admin", Authorize: allowAll})
		return err
	})
	awaitReached(t, cell, "Add's public row write", reached, a)
	o := e.goOperatorAdd(ctx, "operator add", org, user)
	e.awaitWaiters(t, ctx, cell, 1, a, o.party)
	e.authBarrier.Release()
	e.finish(t, cell, a, o.party)

	if a.err != nil {
		t.Fatalf("%s: %s", cell, a)
	}
	o.wantChange(t, cell, "role -> admin")
	e.wantOnePublicRow(t, ctx, cell, org, user, "admin")
}

// The operator has its row in and has not committed: the auth Add waits and
// finds the member.
func TestOperatorAddHeldAndAuthAddFinds(t *testing.T) {
	const cell = "operator-add/held"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)

	reached := e.goBarrier.Arm("commit")
	o := e.goOperatorAdd(ctx, "operator add", org, user)
	awaitReached(t, cell, "the operator's commit", reached, o.party)
	a := goAuth("membership Add", func() error {
		_, err := e.members.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: "admin", Authorize: allowAll})
		return err
	})
	e.awaitWaiters(t, ctx, cell, 1, a, o.party)
	e.goBarrier.Release()
	e.finish(t, cell, a, o.party)

	if !errors.Is(a.err, membership.ErrAlreadyMember) {
		t.Fatalf("%s: %s, want ErrAlreadyMember", cell, a)
	}
	o.wantChange(t, cell, "as member")
	e.wantOnePublicRow(t, ctx, cell, org, user, "member")
}

func TestTwoOperatorAddsOneRow(t *testing.T) {
	const cell = "operator-add/two"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org, user := e.seedOrg(t, ctx), e.seedUser(t, ctx)

	reached := e.goBarrier.Arm("commit")
	first := e.goOperatorAdd(ctx, "first operator add", org, user)
	awaitReached(t, cell, "the first operator's commit", reached, first.party)
	second := e.goOperatorAdd(ctx, "second operator add", org, user)
	e.awaitWaiters(t, ctx, cell, 1, first.party, second.party)
	e.goBarrier.Release()
	e.finish(t, cell, first.party, second.party)

	first.wantChange(t, cell, "as member")
	second.wantChange(t, cell, "role -> member")
	e.wantOnePublicRow(t, ctx, cell, org, user, "member")
}

// The invite accept has passed its member check and has not inserted.
func TestOperatorAddAndInviteAcceptOneRow(t *testing.T) {
	const cell = "operator-add/invite-accept"
	e, ctx := getEnv(t), cellContext(t)
	e.cleanup(t)
	org := e.seedOrg(t, ctx)
	owner, acceptor := e.seedPair(t, ctx)
	e.add(t, ctx, org, owner, "owner", nil)
	_, token := e.goInvite(t, ctx, org, owner, email(acceptor))

	reached := e.goBarrier.Arm("INSERT INTO memberships")
	g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
	awaitReached(t, cell, "accept-invite's membership write", reached, g)
	o := e.goOperatorAdd(ctx, "operator add", org, acceptor)
	e.awaitWaitOrEnd(t, ctx, cell, o.party)
	e.goBarrier.Release()
	e.finish(t, cell, g, o.party)

	wantStatus(t, cell, g, http.StatusOK)
	o.wantChange(t, cell, "role -> member")
	e.wantOnePublicRow(t, ctx, cell, org, acceptor, "member")
}
