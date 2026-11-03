//go:build integration

package server

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// TestQueryAPIRoleServesTheUserAccessTokenUsersRead: as the query-api role
// provisioned from its manifest, query-api's users read of a user access token
// succeeds, while policy.PGStore's own AccessUser (it also selects username and
// full_name) is denied: the reason query-api wraps the store.
func TestQueryAPIRoleServesTheUserAccessTokenUsersRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	fixture := startQueryAPIRoleFixture(t, ctx)
	user := uuid.MustParse(pathsAdminUser)

	row, found, err := newQueryAPIUserStore(policy.PGStore{Pool: fixture.rolePool}).AccessUser(ctx, user)
	if err != nil || !found {
		t.Fatalf("query-api AccessUser as the role: found %v err %v", found, err)
	}
	if !row.State.IsActive {
		t.Fatalf("the seeded user reads inactive: %+v", row)
	}
	_, _, err = policy.PGStore{Pool: fixture.rolePool}.AccessUser(ctx, user)
	if grant := policy.MissingGrant(err); grant == "" {
		t.Fatalf("policy.PGStore.AccessUser as the role: err %v, want a permission denial naming the grant", err)
	}
}
