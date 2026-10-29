//go:build integration

package invite

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// The accept's organization projection runs once, with the invite's
// organization, when the token names a pending, unexpired invite. go-api not
// having the organization is the one refusal of an unusable invite, a slug
// collision is ErrOrgConflict, any other failure is ErrStore; a refused
// projection writes nothing.
func TestAcceptRunsTheOrganizationProjectionAndMapsItsRefusals(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for name, c := range map[string]struct{ err, want error }{
		"not in go-api":  {identity.ErrOrganizationNotFound, ErrInviteInvalid},
		"slug collision": {identity.ErrOrganizationSlugCollision, ErrOrgConflict},
		"store failure":  {errors.New("projection down"), ErrStore},
	} {
		t.Run(name, func(t *testing.T) {
			f := e.invite(t, ctx, "member")
			user := e.seedUser(t, ctx, f.email, true)
			var orgs []uuid.UUID
			_, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{},
				EnsureOrganization: func(_ context.Context, org uuid.UUID) error {
					orgs = append(orgs, org)
					return c.err
				}})
			if !errors.Is(err, c.want) {
				t.Fatalf("Accept = %v, want %v", err, c.want)
			}
			if len(orgs) != 1 || orgs[0] != f.org {
				t.Fatalf("the projection ran for %v, want once for the invite's organization", orgs)
			}
			if r := e.inviteRows(t, ctx, f.created.ID); r.publicStatus != "pending" || r.authStatus != "pending" {
				t.Fatalf("invite rows = %+v, want both pending", r)
			}
			if n := e.count(t, ctx, `SELECT count(*) FROM auth.memberships WHERE principal_id = $1`, user); n != 0 {
				t.Fatalf("a refused projection wrote %d memberships", n)
			}
		})
	}
	t.Run("a used token", func(t *testing.T) {
		f := e.invite(t, ctx, "member")
		user := e.seedUser(t, ctx, f.email, true)
		if _, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{}}); err != nil {
			t.Fatalf("first Accept: %v", err)
		}
		calls := 0
		_, err := e.store.Accept(ctx, AcceptInput{Token: f.created.Token, PrincipalID: user, PublicAudit: &PublicAudit{},
			EnsureOrganization: func(context.Context, uuid.UUID) error {
				calls++
				return nil
			}})
		if !errors.Is(err, ErrInviteInvalid) || calls != 0 {
			t.Fatalf("replay: %v with %d projections, want ErrInviteInvalid and none", err, calls)
		}
	})
}
