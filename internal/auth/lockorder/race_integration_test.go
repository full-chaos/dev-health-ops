//go:build integration

package lockorder

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
)

const raceRounds = 20

// TestRacesOfEveryPairEndWithoutADeadlock runs both writers of each pair at
// once, with no barrier, raceRounds times on fresh rows. A deadlock (40P01) or
// a go-api 500 fails the pair. The whole test is bounded by one context
// (raceBound); a round that does not end within it fails.
func TestRacesOfEveryPairEndWithoutADeadlock(t *testing.T) {
	const raceBound = 8 * time.Minute
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), raceBound)
	defer cancel()
	order := e.fkOrder(t, ctx, "race/P2")

	pairs := []struct {
		name string
		// afterDelete: the auth verb may end refused when go-api's
		// organization delete commits first.
		afterDelete bool
		round       func(t *testing.T) (auth, goapi *party)
	}{
		{"P1", false, func(t *testing.T) (*party, *party) {
			y, x := e.seedPair(t, ctx)
			org, org2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
			e.add(t, ctx, org, y, "member", nil)
			g := goAPI("go-api add_member", func() response { return e.addMember(ctx, org2, x, &y) })
			s := goAuth("membership ChangeRole", func() error {
				_, err := e.members.ChangeRole(ctx, membership.Change{OrgID: org, PrincipalID: y, Role: "admin",
					Actor: membership.Actor{PrincipalID: &x}, Authorize: allowAll})
				return err
			})
			return s, g
		}},
		{"P2", false, func(t *testing.T) (*party, *party) {
			lo, hi := e.seedPair(t, ctx)
			acceptor, inviter := lo, hi
			if order[0] == "user_id" {
				acceptor, inviter = hi, lo
			}
			org, org2 := e.seedOrg(t, ctx), e.seedOrg(t, ctx)
			e.add(t, ctx, org2, lo, "owner", nil)
			e.add(t, ctx, org2, hi, "member", nil)
			_, token := e.goInvite(t, ctx, org, inviter, email(acceptor))
			g := goAPI("go-api accept-invite", func() response { return e.acceptInvite(ctx, acceptor, token) })
			s := goAuth("membership TransferOwnership", func() error {
				return e.members.TransferOwnership(ctx, membership.Transfer{OrgID: org2, From: lo, To: hi, Authorize: allowAll})
			})
			return s, g
		}},
		{"P3", false, func(t *testing.T) (*party, *party) {
			org := e.seedOrg(t, ctx)
			z, w, member := e.seedUser(t, ctx), e.seedUser(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, z, "owner", nil)
			e.add(t, ctx, org, w, "owner", nil)
			e.add(t, ctx, org, member, "member", &z)
			if _, err := e.members.Suspend(ctx, membership.Change{OrgID: org, PrincipalID: member, Authorize: allowAll}); err != nil {
				t.Fatalf("Suspend: %v", err)
			}
			g := goAPI("go-api update_member_role", func() response { return e.setMemberRole(ctx, org, z, "admin") })
			s := goAuth("membership Reactivate", func() error {
				_, err := e.members.Reactivate(ctx, membership.Change{OrgID: org, PrincipalID: member, Authorize: allowAll})
				return err
			})
			return s, g
		}},
		{"P4", true, func(t *testing.T) (*party, *party) {
			org := e.seedOrg(t, ctx)
			owner, acceptor := e.seedUser(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, owner, "owner", nil)
			created, err := e.invites.Create(ctx, invite.CreateInput{OrgID: org, Email: email(acceptor), Role: "member", InvitedBy: owner,
				Actor: invite.Actor{PrincipalID: &owner}, Authorize: allowInvite})
			if err != nil {
				t.Fatalf("race/P4: fixture: invite Create: %v", err)
			}
			g := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
			s := goAuth("invite Accept", func() error {
				_, err := e.invites.Accept(ctx, invite.AcceptInput{Token: created.Token, PrincipalID: acceptor, Actor: invite.Actor{PrincipalID: &acceptor}})
				return err
			})
			return s, g
		}},
		{"P5", true, func(t *testing.T) (*party, *party) {
			org := e.seedOrg(t, ctx)
			a, b := e.seedUser(t, ctx), e.seedUser(t, ctx)
			e.add(t, ctx, org, a, "member", nil)
			e.add(t, ctx, org, b, "owner", nil)
			g := goAPI("go-api delete organization", func() response { return e.deleteOrg(ctx, org) })
			s := goAuth("membership TransferOwnership", func() error {
				return e.members.TransferOwnership(ctx, membership.Transfer{OrgID: org, From: b, To: a, Authorize: allowAll})
			})
			return s, g
		}},
	}
	for _, pair := range pairs {
		t.Run(pair.name, func(t *testing.T) {
			e.resetGoLog()
			for i := range raceRounds {
				s, g := pair.round(t)
				for _, p := range []*party{s, g} {
					select {
					case <-p.done:
					case <-ctx.Done():
						t.Fatalf("race/%s round %d: %s did not end within the race bound %s", pair.name, i, p.name, raceBound)
					}
				}
				if isDeadlock(s.err) {
					t.Fatalf("race/%s round %d: deadlock: SQLSTATE 40P01 in %s", pair.name, i, s)
				}
				if lines := e.goLog.deadlockLines(); len(lines) > 0 {
					t.Fatalf("race/%s round %d: deadlock: SQLSTATE 40P01 in go-api: %v", pair.name, i, lines)
				}
				if g.resp.status == http.StatusInternalServerError || g.resp.status < 0 {
					t.Fatalf("race/%s round %d: %s", pair.name, i, g)
				}
				if s.err != nil && !pair.afterDelete {
					t.Fatalf("race/%s round %d: %s", pair.name, i, s)
				}
				if s.err != nil {
					t.Logf("race/%s round %d: %s (refused after the delete committed, not a deadlock)", pair.name, i, s)
				}
			}
		})
	}
}
