//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const gitPullRequestReviewsDDL = `CREATE TABLE git_pull_request_reviews (org_id String, repo_id UUID, number UInt32, review_id String, reviewer String, state String, submitted_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC'), source_id Nullable(UUID)) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, repo_id, number, review_id)`

func TestGitHubPullRequestReviewEffectsAreNonEmptyTenantFencedAndLeaseGuarded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer instance.Close(ctx)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, gitPullRequestReviewsDDL); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("github", "pr-reviews")
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	row := pullRequestReviewRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79",
		Number: 42, ReviewID: "9007199254740993", Reviewer: "octocat",
		State: "APPROVED", SubmittedAt: now.Add(-time.Hour), LastSynced: now,
	}
	effect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow{row},
	)
	if err != nil {
		t.Fatal(err)
	}
	safe := GitHubPullRequestReviewClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("empty inspection=%s err=%v", inspection, err)
	}

	leaseLost := errors.New("lease lost before send")
	assertions := 0
	guarded := GitHubPullRequestReviewClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
			assertions++
			if assertions == 2 {
				return leaseLost
			}
			return nil
		}),
	}
	if err := guarded.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("write after lost lease err=%v assertions=%d", err, assertions)
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectAbsent {
		t.Fatalf("lost-lease write became visible: inspection=%s err=%v", inspection, err)
	}
	if err := safe.WriteEffect(ctx, claim, effect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("persisted inspection=%s err=%v", inspection, err)
	}

	foreign := row
	foreign.OrgID = "other-org"
	foreign.LastSynced = now.Add(time.Minute)
	foreignClaim := claim
	foreignClaim.OrgID = foreign.OrgID
	foreignEffect, err := effectBatchFromValues(
		"git_pull_request_reviews", EffectReadbackRequired, []pullRequestReviewRow{foreign},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := safe.WriteEffect(ctx, foreignClaim, foreignEffect); err != nil {
		t.Fatal(err)
	}
	if inspection, err := safe.InspectEffect(ctx, claim, effect); err != nil || inspection != EffectExact {
		t.Fatalf("foreign newer row crossed tenant fence: inspection=%s err=%v", inspection, err)
	}
}
