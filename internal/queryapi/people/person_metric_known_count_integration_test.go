//go:build integration

package people

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// has_data is the count of stored NON-NULL values (count(col)), not the number
// of rows: a window whose rows all hold NULL for the column has no value. The
// six person metrics filter NULL out or read a non-nullable column, so this
// pins the query itself over a nullable column with no such filter (CHAOS-9044).
func TestFetchPersonMetricValueHasDataCountsStoredValuesNotRows(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	// Two rows of one day set apart by the author: one NULL, one with a value.
	crossorg.Exec(ctx, t, admin, `
        INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, loc_touched, pr_first_review_p50_hours, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, NULL, now())`, f.OrgA, f.RepoID, day, f.Identity, f.Identity, uint32(1))
	start, end := day, day.AddDate(0, 0, 1)

	value, hasData, err := fetchPersonMetricValue(ctx, client, "user_metrics_daily", "pr_first_review_p50_hours", "avg", "identity_id", []string{f.Identity}, start, end, "", f.OrgA)
	if err != nil {
		t.Fatal(err)
	}
	if hasData || value != 0 {
		t.Errorf("a window whose only row holds NULL: value %v has_data %v, want 0 and false (count() of rows would say true)", value, hasData)
	}

	crossorg.Exec(ctx, t, admin, `
        INSERT INTO user_metrics_daily (org_id, repo_id, day, author_email, identity_id, loc_touched, pr_first_review_p50_hours, computed_at)
        VALUES (?, ?, ?, ?, ?, ?, 4, now())`, f.OrgA, f.RepoID, day.AddDate(0, 0, -1), f.Identity, f.Identity, uint32(1))
	value, hasData, err = fetchPersonMetricValue(ctx, client, "user_metrics_daily", "pr_first_review_p50_hours", "avg", "identity_id", []string{f.Identity}, start.AddDate(0, 0, -1), end, "", f.OrgA)
	if err != nil {
		t.Fatal(err)
	}
	if !hasData || value != 4 {
		t.Errorf("a window with one NULL and one stored value: value %v has_data %v, want 4 and true", value, hasData)
	}
}
