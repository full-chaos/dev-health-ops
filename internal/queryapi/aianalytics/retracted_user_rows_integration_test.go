//go:build integration

package aianalytics

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// The reviewer concentration counts the people of the newest rows, and a
// person whose newest row is a retraction is no reviewer (CHAOS-9135).
func TestTheReviewerConcentrationCountsNoRetractedReviewer(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	f.OrgA = "00000000-0000-4000-8000-0000000093e1" // ai_attribution holds the organization as a UUID
	crossorg.SeedRepos(ctx, t, admin, f)
	day := time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, day)
	crossorg.Exec(ctx, t, admin, `
        INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source,
            confidence, evidence, observed_at, ingested_at)
        VALUES (generateUUIDv4(), ?, 'github', 'pull_request', '1', ?, 'ai_assisted', 'label', 0.9, '{}', ?, now64(3))`,
		f.OrgA, f.RepoID, day.Add(2*time.Hour))
	_, reviewers, err := loadReviewerConcentration(ctx, client, f.OrgA, day, day.AddDate(0, 0, 1), scope{})
	if err != nil {
		t.Fatal(err)
	}
	if reviewers != 2 {
		t.Errorf("reviewers = %d, want 2 (alice and carol): bob's newest row is a retraction", reviewers)
	}
}
