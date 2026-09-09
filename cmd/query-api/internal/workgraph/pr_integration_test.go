//go:build integration

package workgraph

import (
	"context"
	"reflect"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestResolveLinkedIssues_ParityAcrossFlagStates is CHAOS-4980's
// resolver-facing parity proof: on ONE seeded fixture, ResolveLinkedIssues
// (the function the query-api Pr resolver calls) must return identical
// []model.PullRequestIssueLink whether investmentMaterializeNativeEnabled
// is on or off. issuepr_integration_test.go already proves the two raw
// readers agree at the issuePRLinkRow level (CHAOS-4924); this test proves
// the agreement survives one layer up, through the actual GraphQL-model
// mapping the resolver serves -- a mapping bug (e.g. a field transposed
// while building model.PullRequestIssueLink) would not be caught by the
// reader-level test alone.
func TestResolveLinkedIssues_ParityAcrossFlagStates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID    = "org-4980"
		repoID   = "00000000-4980-0000-0000-000000000001"
		prNumber = 7
	)

	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_graph_issue_pr (
            org_id, repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare work_graph_issue_pr batch: %v", err)
	}
	if err := batch.Append(
		orgID, repoID, "issue:OPS-42", uint32(prNumber), float32(0.82), "native", "resolver-parity-token",
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatalf("append work_graph_issue_pr row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send work_graph_issue_pr batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	t.Setenv(investmentMaterializeNativeEnabledEnv, "")
	oracle, err := ResolveLinkedIssues(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("ResolveLinkedIssues (oracle path): %v", err)
	}

	t.Setenv(investmentMaterializeNativeEnabledEnv, "1")
	native, err := ResolveLinkedIssues(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("ResolveLinkedIssues (native path): %v", err)
	}

	if len(oracle) != 1 || len(native) != 1 {
		t.Fatalf("got %d oracle row(s), %d native row(s), want exactly 1 each: oracle=%+v native=%+v",
			len(oracle), len(native), oracle, native)
	}
	if !reflect.DeepEqual(oracle, native) {
		t.Fatalf("native path diverged from the oracle path: oracle=%+v native=%+v", oracle, native)
	}
	if got := oracle[0]; got.WorkItemID != "issue:OPS-42" || got.Provenance != "native" || got.Evidence != "resolver-parity-token" {
		t.Fatalf("got %+v, want the seeded row", got)
	}
}

// TestPRCoreRowExists_RealClickHouse is CHAOS-4980's nil-for-unknown proof
// against a REAL ClickHouse engine (the fake-client unit test in
// pr_test.go covers the query-shape/dispatch side; this proves the actual
// git_pull_requests table -- schema owned by chschema.Apply, not this
// test -- answers existence correctly for both a seeded PR and one that
// was never synced).
func TestPRCoreRowExists_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID           = "org-4980-exists"
		repoID          = "00000000-4980-0000-0000-000000000002"
		seededPRNumber  = 55
		unknownPRNumber = 999
	)

	// org_id is a REQUIRED column here, not backfilled after the fact:
	// migration 027_add_org_id_to_sorting_keys.py rebuilds git_pull_requests
	// with org_id prepended into its ORDER BY key ("(org_id, repo_id,
	// number)"), and ClickHouse refuses an ALTER ... UPDATE that targets a
	// sorting-key column -- chschema.Apply runs the full real migration
	// chain (.sql AND .py), so the table this test's INSERT targets is
	// already in that post-027 shape.
	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO git_pull_requests (
            org_id, repo_id, number, title, body, state, author_name, author_email,
            created_at, merged_at, closed_at, head_branch, base_branch,
            additions, deletions, changed_files, first_review_at,
            first_comment_at, changes_requested_count, reviews_count,
            comments_count, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare git_pull_requests batch: %v", err)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := batch.Append(
		orgID, repoID, uint32(seededPRNumber), "a title", nil, "open", nil, nil,
		now, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, uint32(0), uint32(0),
		uint32(0), now,
	); err != nil {
		t.Fatalf("append git_pull_requests row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send git_pull_requests batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	exists, err := PRCoreRowExists(ctx, client, orgID, repoID, seededPRNumber)
	if err != nil {
		t.Fatalf("PRCoreRowExists (seeded): %v", err)
	}
	if !exists {
		t.Fatal("expected the seeded PR to exist")
	}

	exists, err = PRCoreRowExists(ctx, client, orgID, repoID, unknownPRNumber)
	if err != nil {
		t.Fatalf("PRCoreRowExists (unknown): %v", err)
	}
	if exists {
		t.Fatal("expected an unseeded PR number to not exist")
	}
}

// TestFetchPRCoreRow_RealClickHouse is CHAOS-4991's proof that
// FetchPRCoreRow's Scan destinations actually match what a REAL
// ClickHouse driver hands back for every column type in play here --
// notably the Nullable(String)/Nullable(DateTime64)/Nullable(UInt32)
// double-pointer destinations and the LEFT JOIN repo_name column (see
// FetchPRCoreRow's own doc comment for the join_use_nulls=0 rationale) --
// none of which the fake-client unit tests in pr_test.go can catch (the
// SAME class of gap fetchLinkedIssueRowsFinal's doc comment already
// documents for the Float32-argMax trap).
func TestFetchPRCoreRow_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID          = "org-4991-core"
		repoID         = "00000000-4991-0000-0000-000000000001"
		seededPRNumber = 7
		bareRepoID     = "00000000-4991-0000-0000-000000000099" // no `repos` row
		barePRNumber   = 8
	)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	reposBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO repos (id, repo, ref, created_at, settings, tags, last_synced, org_id, provider, source_id)
    `)
	if err != nil {
		t.Fatalf("prepare repos batch: %v", err)
	}
	if err := reposBatch.Append(repoID, "acme/widgets", nil, now, nil, nil, now, orgID, "github", nil); err != nil {
		t.Fatalf("append repos row: %v", err)
	}
	if err := reposBatch.Send(); err != nil {
		t.Fatalf("send repos batch: %v", err)
	}

	prBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO git_pull_requests (
            org_id, repo_id, number, title, body, state, author_name, author_email,
            created_at, merged_at, closed_at, head_branch, base_branch,
            additions, deletions, changed_files, first_review_at,
            first_comment_at, changes_requested_count, reviews_count,
            comments_count, last_synced
        )
    `)
	if err != nil {
		t.Fatalf("prepare git_pull_requests batch: %v", err)
	}
	created := time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC)
	merged := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	firstReview := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	if err := prBatch.Append(
		orgID, repoID, uint32(seededPRNumber), "Fix the thing", "body text", "merged",
		"Ada Lovelace", "ada@example.com",
		created, merged, nil, "feature/x", "main",
		uint32(120), uint32(40), uint32(7), firstReview,
		nil, uint32(2), uint32(3),
		uint32(5), now,
	); err != nil {
		t.Fatalf("append seeded git_pull_requests row: %v", err)
	}
	// A second PR whose repo_id has NO matching `repos` row at all -- the
	// join_use_nulls=0 edge case FetchPRCoreRow's doc comment documents:
	// anyLast(repos.repo) must come back "" (mapped to nil RepoName), not
	// panic or error.
	if err := prBatch.Append(
		orgID, bareRepoID, uint32(barePRNumber), "No repo row", nil, "open",
		nil, nil,
		created, nil, nil, nil, nil,
		nil, nil, nil, nil,
		nil, uint32(0), uint32(0),
		uint32(0), now,
	); err != nil {
		t.Fatalf("append bare git_pull_requests row: %v", err)
	}
	if err := prBatch.Send(); err != nil {
		t.Fatalf("send git_pull_requests batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	row, ok, err := FetchPRCoreRow(ctx, client, orgID, repoID, seededPRNumber)
	if err != nil {
		t.Fatalf("FetchPRCoreRow (seeded): %v", err)
	}
	if !ok {
		t.Fatal("expected the seeded PR to be found")
	}
	if row.RepoName == nil || *row.RepoName != "acme/widgets" {
		t.Errorf("RepoName = %v, want acme/widgets", row.RepoName)
	}
	if row.Title == nil || *row.Title != "Fix the thing" {
		t.Errorf("Title = %v", row.Title)
	}
	if row.Body == nil || *row.Body != "body text" {
		t.Errorf("Body = %v", row.Body)
	}
	if row.State == nil || *row.State != "merged" {
		t.Errorf("State = %v", row.State)
	}
	if !row.CreatedAt.Equal(created) {
		t.Errorf("CreatedAt = %v, want %v", row.CreatedAt, created)
	}
	if row.MergedAt == nil || !row.MergedAt.Equal(merged) {
		t.Errorf("MergedAt = %v, want %v", row.MergedAt, merged)
	}
	if row.ClosedAt != nil {
		t.Errorf("ClosedAt = %v, want nil", row.ClosedAt)
	}
	if row.Additions == nil || *row.Additions != 120 {
		t.Errorf("Additions = %v, want 120", row.Additions)
	}
	if row.Deletions == nil || *row.Deletions != 40 {
		t.Errorf("Deletions = %v, want 40", row.Deletions)
	}
	if row.ChangedFiles == nil || *row.ChangedFiles != 7 {
		t.Errorf("ChangedFiles = %v, want 7", row.ChangedFiles)
	}
	if row.FirstReviewAt == nil || !row.FirstReviewAt.Equal(firstReview) {
		t.Errorf("FirstReviewAt = %v, want %v", row.FirstReviewAt, firstReview)
	}
	if row.FirstCommentAt != nil {
		t.Errorf("FirstCommentAt = %v, want nil", row.FirstCommentAt)
	}
	if row.ChangesRequestedCount != 2 || row.ReviewsCount != 3 || row.CommentsCount != 5 {
		t.Errorf("counts = %d/%d/%d, want 2/3/5", row.ChangesRequestedCount, row.ReviewsCount, row.CommentsCount)
	}

	bareRow, ok, err := FetchPRCoreRow(ctx, client, orgID, bareRepoID, barePRNumber)
	if err != nil {
		t.Fatalf("FetchPRCoreRow (bare repo): %v", err)
	}
	if !ok {
		t.Fatal("expected the bare-repo PR to be found")
	}
	if bareRow.RepoName != nil {
		t.Errorf("RepoName = %v, want nil for a repo_id with no matching repos row (join_use_nulls=0 case)", *bareRow.RepoName)
	}
	if bareRow.Title == nil || *bareRow.Title != "No repo row" {
		t.Errorf("Title = %v", bareRow.Title)
	}
	if bareRow.Body != nil || bareRow.State == nil || *bareRow.State != "open" {
		t.Errorf("Body/State = %v/%v", bareRow.Body, bareRow.State)
	}
	if bareRow.Additions != nil || bareRow.MergedAt != nil {
		t.Errorf("Additions/MergedAt = %v/%v, want nil/nil", bareRow.Additions, bareRow.MergedAt)
	}

	_, ok, err = FetchPRCoreRow(ctx, client, orgID, repoID, 99999)
	if err != nil {
		t.Fatalf("FetchPRCoreRow (unknown): %v", err)
	}
	if ok {
		t.Fatal("expected an unseeded PR number to not be found")
	}
}

// TestResolveReviews_RealClickHouse proves ResolveReviews's Scan
// destinations against a real ClickHouse engine and its ordering
// (submitted_at ASC, review_id ASC).
func TestResolveReviews_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID    = "org-4991-reviews"
		repoID   = "00000000-4991-0000-0000-000000000002"
		prNumber = 11
	)
	earlier := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	later := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)

	batch, err := admin.PrepareBatch(ctx, `
        INSERT INTO git_pull_request_reviews (org_id, repo_id, number, review_id, reviewer, state, submitted_at, last_synced)
    `)
	if err != nil {
		t.Fatalf("prepare git_pull_request_reviews batch: %v", err)
	}
	// Seeded out of order -- ResolveReviews must return them
	// submitted_at ASC, not insertion order.
	if err := batch.Append(orgID, repoID, uint32(prNumber), "rev-2", "hubot", "changes_requested", later, later); err != nil {
		t.Fatalf("append review row: %v", err)
	}
	if err := batch.Append(orgID, repoID, uint32(prNumber), "rev-1", "octocat", "approved", earlier, earlier); err != nil {
		t.Fatalf("append review row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send git_pull_request_reviews batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	got, err := ResolveReviews(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("ResolveReviews: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d reviews, want 2: %+v", len(got), got)
	}
	if got[0].ReviewID != "rev-1" || !got[0].SubmittedAt.Equal(earlier) {
		t.Errorf("got[0] = %+v, want rev-1 at %v (earlier first)", got[0], earlier)
	}
	if got[1].ReviewID != "rev-2" || !got[1].SubmittedAt.Equal(later) {
		t.Errorf("got[1] = %+v, want rev-2 at %v (later second)", got[1], later)
	}
	if got[0].Reviewer != "octocat" || got[0].State != "approved" {
		t.Errorf("got[0] reviewer/state = %q/%q, want octocat/approved", got[0].Reviewer, got[0].State)
	}
}

// TestResolveCommits_RealClickHouse is CHAOS-4991's proof that
// ResolveCommits's toFloat64(argMax(link.confidence, ...)) cast actually
// works against a REAL ClickHouse driver (work_graph_pr_commit.confidence
// is Float32 -- see ResolveCommits's own doc comment for the trap this
// guards, the same class fetchLinkedIssueRowsFinal's doc comment
// documents for work_graph_issue_pr), and that the LEFT JOIN against
// git_commits maps every column correctly, both for a commit WITH a
// matching git_commits row and one WITHOUT (join_use_nulls=0 edge case).
func TestResolveCommits_RealClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = ch.Close(context.Background()) }()

	chschema.Apply(ctx, t, ch)

	options, err := stdclickhouse.ParseDSN(ch.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	admin, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse admin connection: %v", err)
	}
	defer func() { _ = admin.Close() }()

	const (
		orgID    = "org-4991-commits"
		repoID   = "00000000-4991-0000-0000-000000000003"
		prNumber = 21
	)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	authorWhen := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)

	commitsBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO git_commits (
            repo_id, hash, message, author_name, author_email, author_when,
            committer_name, committer_email, committer_when, parents, last_synced, org_id
        )
    `)
	if err != nil {
		t.Fatalf("prepare git_commits batch: %v", err)
	}
	if err := commitsBatch.Append(
		repoID, "deadbeef", "fix: the bug", "Ada Lovelace", "ada@example.com", authorWhen,
		"Ada Lovelace", "ada@example.com", authorWhen, uint32(1), now, orgID,
	); err != nil {
		t.Fatalf("append git_commits row: %v", err)
	}
	if err := commitsBatch.Send(); err != nil {
		t.Fatalf("send git_commits batch: %v", err)
	}

	linkBatch, err := admin.PrepareBatch(ctx, `
        INSERT INTO work_graph_pr_commit (repo_id, pr_number, commit_hash, confidence, provenance, evidence, last_synced, org_id)
    `)
	if err != nil {
		t.Fatalf("prepare work_graph_pr_commit batch: %v", err)
	}
	// Linked commit WITH a matching git_commits row.
	if err := linkBatch.Append(repoID, uint32(prNumber), "deadbeef", float32(0.92), "native", "api_pr_commits", now, orgID); err != nil {
		t.Fatalf("append work_graph_pr_commit row: %v", err)
	}
	// Linked commit hash with NO matching git_commits row -- the
	// join_use_nulls=0 edge case ResolveCommits's doc comment documents:
	// message/author_name/author_email come back nil, author_when comes
	// back the zero value (never NULL, never an error).
	if err := linkBatch.Append(repoID, uint32(prNumber), "c0ffee00", float32(0.5), "heuristic", "commit_message_reference", now, orgID); err != nil {
		t.Fatalf("append work_graph_pr_commit row (no matching commit): %v", err)
	}
	if err := linkBatch.Send(); err != nil {
		t.Fatalf("send work_graph_pr_commit batch: %v", err)
	}

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: ch.URI})
	if err != nil {
		t.Fatalf("construct query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	got, err := ResolveCommits(ctx, client, orgID, repoID, prNumber)
	if err != nil {
		t.Fatalf("ResolveCommits: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d commits, want 2: %+v", len(got), got)
	}

	byHash := map[string]int{}
	for i, c := range got {
		byHash[c.Hash] = i
	}

	matched := got[byHash["deadbeef"]]
	if matched.Message == nil || *matched.Message != "fix: the bug" {
		t.Errorf("matched.Message = %v", matched.Message)
	}
	if matched.AuthorName == nil || *matched.AuthorName != "Ada Lovelace" {
		t.Errorf("matched.AuthorName = %v", matched.AuthorName)
	}
	if matched.AuthorWhen == nil || !matched.AuthorWhen.Equal(authorWhen) {
		t.Errorf("matched.AuthorWhen = %v, want %v", matched.AuthorWhen, authorWhen)
	}
	if matched.Confidence == nil || *matched.Confidence < 0.919 || *matched.Confidence > 0.921 {
		t.Errorf("matched.Confidence = %v, want ~0.92 (Float32->Float64 cast survives the round trip)", matched.Confidence)
	}
	if matched.Provenance == nil || *matched.Provenance != "native" {
		t.Errorf("matched.Provenance = %v", matched.Provenance)
	}

	unmatched := got[byHash["c0ffee00"]]
	if unmatched.Message != nil {
		t.Errorf("unmatched.Message = %v, want nil (no matching git_commits row)", *unmatched.Message)
	}
	if unmatched.AuthorName != nil || unmatched.AuthorEmail != nil {
		t.Errorf("unmatched.AuthorName/AuthorEmail = %v/%v, want nil/nil", unmatched.AuthorName, unmatched.AuthorEmail)
	}
	if unmatched.AuthorWhen == nil {
		t.Error("unmatched.AuthorWhen = nil, want a non-nil zero-value time (join_use_nulls=0 case, not NULL)")
	}
	if unmatched.Confidence == nil || *unmatched.Confidence < 0.499 || *unmatched.Confidence > 0.501 {
		t.Errorf("unmatched.Confidence = %v, want ~0.5", unmatched.Confidence)
	}
	if unmatched.Provenance == nil || *unmatched.Provenance != "heuristic" {
		t.Errorf("unmatched.Provenance = %v", unmatched.Provenance)
	}
}
