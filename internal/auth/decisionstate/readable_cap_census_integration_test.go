//go:build integration

package decisionstate

import (
	"context"
	"testing"
)

// C-3b census: the Go condition the snapshot loads identities on (needsIdentities) and the SQL condition the state
// version reads them on (identitiesCondition) agree on the same rows.
func TestTheGoAndSQLIdentityConditionsAgree(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		grants, rowID int
		want          bool
	}{
		{"no grant", 0, 0, false},
		{"256 path-only grants", 256, 0, false},
		{"257 path-only grants", 257, 0, true},
		{"3 path-only grants and 1 id-bearing grant", 3, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, org, _ := e.capReader(t, ctx, tc.grants, 0)
			if tc.rowID > 0 {
				e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, resource_repo_id)
VALUES ($1, $2, 'viewer', 'repository', 'acme/with-id', 'github:77')`, user, org)
			}
			grants, err := e.snapshot(t, ctx, user, org.String()).Grants(org.String())
			if err != nil {
				t.Fatal(err)
			}
			var inSQL bool
			if err := e.admin.QueryRow(ctx, `SELECT `+identitiesCondition(`"auth"`), user, org).Scan(&inSQL); err != nil {
				t.Fatal(err)
			}
			if inGo := needsIdentities(grants); inGo != tc.want || inSQL != tc.want {
				t.Fatalf("Go %v, SQL %v, want both %v", inGo, inSQL, tc.want)
			}
		})
	}
}
