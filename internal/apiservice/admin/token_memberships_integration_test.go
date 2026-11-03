//go:build integration

package admin_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// withTokenMemberships seeds, after seed, one public.memberships row (user,
// org, role) for each non-superuser token spec whose user and org rows exist:
// a real access token is minted at login for an existing membership, and
// go-api's org-scoped routes read that row. Both planes serve the same seed.
func withTokenMemberships(seed func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any) func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any {
	return func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
		specs := seed(t, ctx, admin, v)
		for name, spec := range specs {
			if superuser, _ := spec["is_superuser"].(bool); superuser {
				continue
			}
			userID, _ := spec["user_id"].(string)
			orgID, _ := spec["org_id"].(string)
			user, userErr := uuid.Parse(userID)
			org, orgErr := uuid.Parse(orgID)
			if userErr != nil || orgErr != nil {
				continue
			}
			role, _ := spec["role"].(string)
			if role == "" {
				role = "member"
			}
			if _, err := admin.Exec(ctx, `
INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at, updated_at)
SELECT $1, u.id, o.id, $4, now(), now(), now()
FROM users u, organizations o WHERE u.id = $2 AND o.id = $3
ON CONFLICT (user_id, org_id) DO NOTHING`, uuid.New(), user, org, role); err != nil {
				t.Fatalf("seed the membership of token %q: %v", name, err)
			}
		}
		return specs
	}
}
