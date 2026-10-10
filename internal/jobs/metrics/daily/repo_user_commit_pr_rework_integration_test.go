//go:build integration

package daily

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// The repo_user_commit family stores, on each repository's row of the day, the
// merged pull requests counted by review evidence, through the production
// entry point (RepoUserCommitExecutor.ComputeFamily) on the migrated schema.
// The provider of a repository comes from the repos table.
//
//	repo N (github)   3 merged, no review data
//	repo Z (github)   2 merged, both reviewed, no changes requested
//	repo R (github)   4 merged: 1 reviewed, 1 reviewed with changes requested, 2 with no review data
//	repo G (gitlab)   3 merged, each with reviews: the provider has no changes-requested event
//	repo X (no row in repos)  2 merged, each with reviews: the provider is not known
func TestRepoUserCommitStoresTheReworkCountsByReviewEvidence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	_, conn := opfixture.Start(ctx, t)

	const org = "00000000-0000-4000-8000-0000000090a0"
	repoN, repoZ, repoR, repoG, repoX := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	for repo, provider := range map[uuid.UUID]string{repoN: "github", repoZ: "github", repoR: "github", repoG: "gitlab"} {
		if err := conn.Exec(ctx, "INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES (?, ?, ?, ?, now64(3), now64(3))",
			repo, "acme/"+repo.String()[:8], provider, org); err != nil {
			t.Fatal(err)
		}
	}
	type pullRequest struct{ reviews, changesRequested uint32 }
	seed := func(repo uuid.UUID, pullRequests ...pullRequest) {
		t.Helper()
		for index, pr := range pullRequests {
			if err := conn.Exec(ctx, `INSERT INTO git_pull_requests
    (repo_id, number, title, state, author_name, author_email, created_at, merged_at, changes_requested_count, reviews_count,
     additions, deletions, changed_files, last_synced, org_id)
    VALUES (?, ?, ?, 'merged', 'Dev', 'dev@example.com', ?, ?, ?, ?, 10, 2, 1, ?, ?)`,
				repo, uint32(index+1), fmt.Sprintf("Change %d", index+1), day.Add(time.Hour), day.Add(10*time.Hour),
				pr.changesRequested, pr.reviews, day.Add(20*time.Hour), org); err != nil {
				t.Fatal(err)
			}
		}
	}
	unreviewed, reviewed, rework := pullRequest{}, pullRequest{reviews: 2}, pullRequest{reviews: 3, changesRequested: 1}
	seed(repoN, unreviewed, unreviewed, unreviewed)
	seed(repoZ, reviewed, reviewed)
	seed(repoR, reviewed, rework, unreviewed, unreviewed)
	seed(repoG, reviewed, reviewed, reviewed)
	seed(repoX, reviewed, reviewed)

	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	repos := []uuid.UUID{repoN, repoZ, repoR, repoG, repoX}
	ids := make([]RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, RepositoryID(repo.String()))
	}
	run := Run{OrganizationID: org, TargetDay: day}
	if _, err := executor.ComputeFamily(ctx, run, Partition{ID: uuid.NewString(), RunID: uuid.NewString(), RepoIDs: ids}); err != nil {
		t.Fatalf("repo_user_commit: %v", err)
	}

	value := func(v float64) *float64 { return &v }
	for _, want := range []struct {
		name                               string
		repo                               uuid.UUID
		merged, reviewed, rework, noSignal uint32
		ratio                              *float64
		// deprecated is the old ratio, still written: changes requested / ALL
		// merged pull requests.
		deprecated float64
	}{
		{"no review data", repoN, 3, 0, 0, 0, nil, 0},
		{"reviewed, no changes requested", repoZ, 2, 2, 0, 0, value(0), 0},
		{"reviewed, changes requested", repoR, 4, 2, 1, 0, value(0.5), 0.25},
		{"a provider with no changes-requested event", repoG, 3, 0, 0, 3, nil, 0},
		{"a provider that is not known", repoX, 2, 0, 0, 2, nil, 0},
	} {
		var merged uint32
		var reviewed, reworkCount, noSignal *uint32
		var ratio *float64
		var deprecated float64
		if err := conn.QueryRow(ctx, `SELECT prs_merged, prs_merged_reviewed, prs_merged_rework, prs_merged_no_rework_signal,
       pr_rework_ratio_reviewed, pr_rework_ratio
FROM repo_metrics_daily FINAL WHERE org_id = ? AND repo_id = ? AND day = ?`, org, want.repo, day).Scan(
			&merged, &reviewed, &reworkCount, &noSignal, &ratio, &deprecated); err != nil {
			t.Fatalf("%s: read the stored row: %v", want.name, err)
		}
		if reviewed == nil || reworkCount == nil || noSignal == nil {
			t.Errorf("%s: the counts are NULL (%v %v %v), want stored counts", want.name, reviewed, reworkCount, noSignal)
			continue
		}
		if merged != want.merged || *reviewed != want.reviewed || *reworkCount != want.rework || *noSignal != want.noSignal {
			t.Errorf("%s: merged %d reviewed %d rework %d no-signal %d, want %d %d %d %d",
				want.name, merged, *reviewed, *reworkCount, *noSignal, want.merged, want.reviewed, want.rework, want.noSignal)
		}
		switch {
		case want.ratio == nil && ratio != nil:
			t.Errorf("%s: the one-day ratio is %v, want NULL: no reviewed pull request is not a measured 0", want.name, *ratio)
		case want.ratio != nil && (ratio == nil || *ratio != *want.ratio):
			t.Errorf("%s: the one-day ratio is %v, want %v", want.name, ratio, *want.ratio)
		}
		if deprecated != want.deprecated {
			t.Errorf("%s: the deprecated ratio is %v, want %v as before", want.name, deprecated, want.deprecated)
		}
	}
}
