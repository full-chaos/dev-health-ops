//go:build integration

package pgmigrate_test

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// The head (revision 0162, now the baseline) holds public.org_activity, the projection of the metrics activity of an organization that the auth
// plane reads: one row per organization, deleted with it, holding the two facts of the login org choice.
func TestRevision0162AddsTheOrgActivityProjection(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	d := newDownInstance(t)
	conn := connect(t, d.at(t, len(chain)))
	ctx := context.Background()

	var columns string
	if err := conn.QueryRow(ctx, `SELECT string_agg(column_name || ':' || data_type || ':' || is_nullable, ',' ORDER BY ordinal_position)
		FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'org_activity'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	const want = "org_id:uuid:NO,has_data:boolean:NO,computed_at:timestamp with time zone:YES,refreshed_at:timestamp with time zone:NO"
	if columns != want {
		t.Fatalf("org_activity columns = %q, want %q", columns, want)
	}

	var orgID string
	if err := conn.QueryRow(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at) VALUES (gen_random_uuid(), 'org-activity-t', 'T', 'community', 'stripe', true, now(), now()) RETURNING id::text`).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO org_activity (org_id, has_data, computed_at, refreshed_at) VALUES ($1, true, now(), now())`, orgID); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO org_activity (org_id, has_data, computed_at, refreshed_at) VALUES ($1, false, NULL, now())`, orgID); err == nil {
		t.Fatal("a second row for one organization was accepted")
	}
	if _, err := conn.Exec(ctx, `DELETE FROM organizations WHERE id = $1`, orgID); err != nil {
		t.Fatalf("delete organization: %v", err)
	}
	var left int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM org_activity WHERE org_id = $1`, orgID).Scan(&left); err != nil || left != 0 {
		t.Fatalf("the row of a deleted organization remains: %d (%v)", left, err)
	}
}
