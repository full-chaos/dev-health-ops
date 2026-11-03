//go:build integration

package identity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// A JIT join and a membership-aggregate add of the same account and
// organization, started together, take the same lock order: one writes, the
// other is refused as already a member, and PostgreSQL never reports a
// deadlock. 20 accounts, one race each.
func TestJoinAndMembershipAggregateShareOneLockOrder(t *testing.T) {
	e := getEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := membership.New(membership.Config{Pool: e.runtime, Schema: schema, OwnsUsers: true})
	if err != nil {
		t.Fatal(err)
	}
	allow := func(context.Context, audit.TxOps, membership.Locked) error { return nil }
	org := e.seedAuthOwnedOrg(t, ctx)
	for i := range 20 {
		user := e.seedUser(t, ctx, uniqueEmail("lockorder"), true, true)
		e.project(t, ctx, user)
		joinErr, addErr := make(chan error, 1), make(chan error, 1)
		go func() {
			joinErr <- joinCommit(ctx, e.owned, membershipJoin{ID: uuid.New(), Principal: user, OrgID: org, Role: "member"}, nil)
		}()
		go func() {
			_, err := store.Add(ctx, membership.Change{OrgID: org, PrincipalID: user, Role: "viewer", Authorize: allow})
			addErr <- err
		}()
		j, a := <-joinErr, <-addErr
		for _, err := range []error{j, a} {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
				t.Fatalf("round %d: deadlock: %v", i, err)
			}
		}
		joined, added := j == nil, a == nil
		if joined == added {
			t.Fatalf("round %d: join err=%v add err=%v, want exactly one to write", i, j, a)
		}
		if !joined && !errors.Is(j, errJoinAlreadyMember) {
			t.Fatalf("round %d: join err = %v, want already a member", i, j)
		}
		if !added && !errors.Is(a, membership.ErrAlreadyMember) {
			t.Fatalf("round %d: add err = %v, want ErrAlreadyMember", i, a)
		}
		if p, au := e.memberRows(t, ctx, user, org); p != 1 || au != 1 {
			t.Fatalf("round %d: rows public=%d auth=%d, want 1 and 1", i, p, au)
		}
	}
}
