//go:build integration

package providersync

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// This file extends CHAOS-3162's generic, declarative, fail-on-undeclared-
// divergence comparator (diffRows / oracleDivergences, oracle_compare_test.go)
// to the readback boundary: "the row a caller wrote" versus "the row a
// caller reads back through ClickHouse's ReplacingMergeTree resolution".
//
// This is NOT a Python<->Go crossing -- Python's own sync path has no
// readback-fencing concept to compare against; the effect-ledger,
// InspectEffect-before-CommitEffect pattern is a Go-only architectural
// addition (see internal/providersync's package doc). What CHAOS-3162
// actually asked this framework to guarantee -- a DECLARATIVE pair table, a
// WHOLE-ROW comparison, and failure on ANY undeclared divergence including a
// column present on one side and absent on the other -- applies just as
// much to this crossing as to a Python<->Go one, and reuses the identical
// diffRows/oracleDivergences machinery rather than a bespoke comparison.
// Where the row-construction pair's "expected" side comes from a live
// Python subprocess, this pair's "expected" side is the Go row the test
// itself asked ClickHouse to store -- validated safe to use as ground truth
// because TestGenericOracleMatchesLivePythonForRowConstruction already
// proves, independently, that a Go-built pullRequestRow is byte-for-byte
// identical to Python's for the same input.
//
// Covers two of the six CHAOS-3162 acceptance defects that are NOT
// reachable through the row-construction boundary:
//   - "the omitted readback columns" (M6/round1): a SELECT that leaves a
//     column out of its column list.
//   - "the NULL-versus-empty-string collapse" (H2/round2): a per-column
//     argMax(col, last_synced) reconstruction, which independently maximizes
//     each column and can silently assemble a row that never existed as any
//     single physical version, because argMax skips NULL-argument rows when
//     picking the max.

// encodeOracleValue routes one hand-scanned readback column through the
// SAME typedEncode reflection-based, type-tagged encoding
// oracle_compare_test.go uses for a whole struct (codex finding #2: a
// bare, untagged encoding is exactly what let an int and an integral float,
// or two large integers, compare equal at float64 precision) -- there is
// exactly one encoding rule in this package's tests, applied consistently
// via one shared function, not two that must be kept in sync by hand.
func encodeOracleValue(t *testing.T, value any) any {
	t.Helper()
	return typedEncode(t, reflect.ValueOf(value))
}

// pullRequestReadbackComparisonExclusions are the fields the readback
// boundary deliberately does not compare: last_synced/source_id/org_id are
// Go-side bookkeeping (same reasons as oraclePullRequestGoOnlyFields), and
// repo_id/number are the lookup KEY, not a value under test -- comparing
// them would only ever prove the WHERE clause worked, not that the SELECT
// list is complete or correct.
var pullRequestReadbackComparisonExclusions = map[string]string{
	"last_synced": "Go-side effect bookkeeping, not part of the row's business data",
	"source_id":   "Go-side effect bookkeeping, not part of the row's business data",
	"org_id":      "part of the lookup key (WHERE clause), not a value the SELECT list is being tested on",
	"repo_id":     "part of the lookup key (WHERE clause), not a value the SELECT list is being tested on",
	"number":      "part of the lookup key (WHERE clause), not a value the SELECT list is being tested on",
}

// readPullRequestRowCorrectly is the CURRENT, production readback query
// (verbatim copy of inspectPullRequest's SELECT, minus the
// bookkeeping/lookup-key columns pullRequestReadbackComparisonExclusions
// declares out of scope): a FINAL point lookup, which resolves the winning
// physical ReplacingMergeTree version by reading ONE consistent row.
func readPullRequestRowCorrectly(
	ctx context.Context, t *testing.T, harness *pullRequestReadbackHarness, repoID string, number int,
) map[string]any {
	t.Helper()
	rows, err := harness.conn.Query(ctx, `
SELECT
  title, body, state, author_name, author_email, created_at, merged_at,
  closed_at, head_branch, base_branch, additions, deletions, changed_files,
  first_review_at, first_comment_at, changes_requested_count, reviews_count,
  comments_count
FROM git_pull_requests FINAL
WHERE org_id = ? AND repo_id = ? AND number = ?`,
		harness.claim.OrgID, repoID, number,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	return scanPullRequestReadbackRow(t, rows, true)
}

// readPullRequestRowOmittingStateColumn reproduces "the omitted readback
// columns" (M6/round1): otherwise the same correct FINAL point lookup, but
// the SELECT list itself leaves `state` out -- exactly the shape of bug a
// hand-picked assertion list can miss (an assertion that never mentions
// `state` cannot notice `state` went missing from the query), and exactly
// what the generic comparator's "present on one side, absent on the other"
// check exists to catch instead.
func readPullRequestRowOmittingStateColumn(
	ctx context.Context, t *testing.T, harness *pullRequestReadbackHarness, repoID string, number int,
) map[string]any {
	t.Helper()
	rows, err := harness.conn.Query(ctx, `
SELECT
  title, body, author_name, author_email, created_at, merged_at,
  closed_at, head_branch, base_branch, additions, deletions, changed_files,
  first_review_at, first_comment_at, changes_requested_count, reviews_count,
  comments_count
FROM git_pull_requests FINAL
WHERE org_id = ? AND repo_id = ? AND number = ?`,
		harness.claim.OrgID, repoID, number,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	return scanPullRequestReadbackRow(t, rows, false)
}

// readPullRequestRowViaPerColumnArgMax reproduces "the NULL-versus-empty-
// string collapse" (H2/round2, the single most dangerous finding in this
// pair's whole review history): the PRE-fix readback shape, which computes
// argMax(column, last_synced) independently per column instead of reading
// one consistent row. ClickHouse's argMax skips a row whose ARGUMENT
// (the column being maximized) is NULL when deciding the max, so if the
// winning (latest last_synced) row has NULL in some column while an older
// row has a non-NULL value there, this reconstructs a column value from a
// version that is not the winning version at all -- a row that never
// existed as any single physical write.
func readPullRequestRowViaPerColumnArgMax(
	ctx context.Context, t *testing.T, harness *pullRequestReadbackHarness, repoID string, number int,
) map[string]any {
	t.Helper()
	rows, err := harness.conn.Query(ctx, `
SELECT
  argMax(title, last_synced), argMax(body, last_synced), argMax(state, last_synced),
  argMax(author_name, last_synced), argMax(author_email, last_synced),
  argMax(created_at, last_synced), argMax(merged_at, last_synced),
  argMax(closed_at, last_synced), argMax(head_branch, last_synced),
  argMax(base_branch, last_synced), argMax(additions, last_synced),
  argMax(deletions, last_synced), argMax(changed_files, last_synced),
  argMax(first_review_at, last_synced), argMax(first_comment_at, last_synced),
  argMax(changes_requested_count, last_synced), argMax(reviews_count, last_synced),
  argMax(comments_count, last_synced)
FROM git_pull_requests
WHERE org_id = ? AND repo_id = ? AND number = ?
GROUP BY org_id, repo_id, number`,
		harness.claim.OrgID, repoID, number,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	return scanPullRequestReadbackRow(t, rows, true)
}

// scanPullRequestReadbackRow scans one row shaped like the queries above
// (title..comments_count, with `state` optionally omitted) into a
// map[string]any keyed identically to pullRequestRow's json tags / Python's
// build_git_pull_request output, so it can be diffed with diffRows exactly
// like the row-construction pair's Python output is. Fails the test if zero
// or more than one row comes back -- a readback query returning the wrong
// row COUNT is a bug this generic comparator cannot see (it only compares
// field values within one row), so it is asserted directly here instead of
// silently comparing against an arbitrary row or an empty map.
func scanPullRequestReadbackRow(t *testing.T, rows driver.Rows, includeState bool) map[string]any {
	t.Helper()
	var title, body, headBranch, baseBranch, authorEmail *string
	var state, authorName string
	var createdAt time.Time
	var mergedAt, closedAt, firstReviewAt, firstCommentAt *time.Time
	var additions, deletions, changedFiles, changesRequestedCount, reviewsCount, commentsCount uint32

	found := false
	for rows.Next() {
		if found {
			t.Fatalf("readback query returned more than one row")
		}
		found = true
		var err error
		if includeState {
			err = rows.Scan(
				&title, &body, &state, &authorName, &authorEmail, &createdAt, &mergedAt,
				&closedAt, &headBranch, &baseBranch, &additions, &deletions, &changedFiles,
				&firstReviewAt, &firstCommentAt, &changesRequestedCount, &reviewsCount, &commentsCount,
			)
		} else {
			err = rows.Scan(
				&title, &body, &authorName, &authorEmail, &createdAt, &mergedAt,
				&closedAt, &headBranch, &baseBranch, &additions, &deletions, &changedFiles,
				&firstReviewAt, &firstCommentAt, &changesRequestedCount, &reviewsCount, &commentsCount,
			)
		}
		if err != nil {
			t.Fatalf("scan readback row: %v", err)
		}
	}
	if !found {
		t.Fatalf("readback query returned no row")
	}

	row := map[string]any{
		"title": encodeOracleValue(t, title), "body": encodeOracleValue(t, body),
		"author_name": encodeOracleValue(t, authorName), "author_email": encodeOracleValue(t, authorEmail),
		"created_at": encodeOracleValue(t, createdAt), "merged_at": encodeOracleValue(t, mergedAt),
		"closed_at": encodeOracleValue(t, closedAt), "head_branch": encodeOracleValue(t, headBranch),
		"base_branch": encodeOracleValue(t, baseBranch), "additions": encodeOracleValue(t, additions),
		"deletions": encodeOracleValue(t, deletions), "changed_files": encodeOracleValue(t, changedFiles),
		"first_review_at": encodeOracleValue(t, firstReviewAt), "first_comment_at": encodeOracleValue(t, firstCommentAt),
		"changes_requested_count": encodeOracleValue(t, changesRequestedCount),
		"reviews_count":           encodeOracleValue(t, reviewsCount),
		"comments_count":          encodeOracleValue(t, commentsCount),
	}
	if includeState {
		row["state"] = encodeOracleValue(t, state)
	}
	return row
}

// expectedPullRequestRowMap converts a Go-built pullRequestRow to the same
// map[string]any shape the readback queries above produce, for diffRows.
func expectedPullRequestRowMap(t *testing.T, row pullRequestRow) map[string]any {
	t.Helper()
	full, ok := typedEncode(t, reflect.ValueOf(row)).(map[string]any)
	if !ok {
		t.Fatalf("typedEncode(pullRequestRow) did not return a map")
	}
	for excluded := range pullRequestReadbackComparisonExclusions {
		delete(full, excluded)
	}
	return full
}

// TestGenericComparatorMatchesCorrectReadback is the "current code is
// clean" half for the readback boundary: write an older then a winning
// version (the exact mixed-NULL shape
// TestGitHubPullRequestReadbackDoesNotReconstructRowFromMixedVersions
// uses), then diff the CURRENT production readback query's result against
// the winning row with zero undeclared exclusions.
func TestGenericComparatorMatchesCorrectReadback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	older := pullRequestReadbackFixture(now.Add(-time.Hour))
	older.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, older)); err != nil {
		t.Fatal(err)
	}
	winning := pullRequestReadbackFixture(now)
	winning.OrgID = claim.OrgID
	winning.State = "open"
	winning.MergedAt, winning.ClosedAt = nil, nil
	winning.FirstReviewAt = nil
	winning.ReviewsCount, winning.ChangesRequestedCount = 0, 0
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, winning)); err != nil {
		t.Fatal(err)
	}

	expected := expectedPullRequestRowMap(t, winning)
	actual := readPullRequestRowCorrectly(ctx, t, harness, winning.RepoID, winning.Number)
	messages := diffRows("winning-after-mixed-null-history", expected, actual,
		nil, pullRequestReadbackComparisonExclusions)
	for _, message := range messages {
		t.Error(message)
	}
}

// TestGenericComparatorRediscoversReadbackDefects is CHAOS-3162's
// acceptance gate for the readback boundary: the SAME generic comparator
// (diffRows), against the SAME written fixture data, must report a
// divergence when pointed at either historical buggy query shape.
func TestGenericComparatorRediscoversReadbackDefects(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness := startPullRequestReadbackHarness(t, ctx)
	claim, sink, now := harness.claim, harness.sink, harness.now

	// Same mixed-NULL shape as
	// TestGitHubPullRequestReadbackDoesNotReconstructRowFromMixedVersions:
	// the older version has non-NULL merged_at/closed_at/first_review_at:
	// the winning version has NULL in all three, which is the only shape
	// that can distinguish "read the winning row" from "assemble a row from
	// whichever version has a non-NULL value per column".
	older := pullRequestReadbackFixture(now.Add(-time.Hour))
	older.OrgID = claim.OrgID
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, older)); err != nil {
		t.Fatal(err)
	}
	winning := pullRequestReadbackFixture(now)
	winning.OrgID = claim.OrgID
	winning.State = "open"
	winning.MergedAt, winning.ClosedAt = nil, nil
	winning.FirstReviewAt = nil
	winning.ReviewsCount, winning.ChangesRequestedCount = 0, 0
	if err := sink.WriteEffect(ctx, claim, pullRequestEffect(t, winning)); err != nil {
		t.Fatal(err)
	}
	expected := expectedPullRequestRowMap(t, winning)

	t.Run("rediscovers NULL-versus-empty-string collapse (per-column argMax)", func(t *testing.T) {
		actual := readPullRequestRowViaPerColumnArgMax(ctx, t, harness, winning.RepoID, winning.Number)
		messages := diffRows("winning-after-mixed-null-history", expected, actual,
			nil, pullRequestReadbackComparisonExclusions)
		if len(messages) == 0 {
			t.Fatal("expected the generic comparator to rediscover the per-column argMax " +
				"cross-version reconstruction defect, but it reported no divergence")
		}
		for _, message := range messages {
			t.Logf("rediscovered: %s", message)
		}
	})

	t.Run("rediscovers omitted readback columns (state left out of SELECT)", func(t *testing.T) {
		actual := readPullRequestRowOmittingStateColumn(ctx, t, harness, winning.RepoID, winning.Number)
		messages := diffRows("winning-after-mixed-null-history", expected, actual,
			nil, pullRequestReadbackComparisonExclusions)
		if len(messages) == 0 {
			t.Fatal("expected the generic comparator to rediscover the omitted-state-column " +
				"defect, but it reported no divergence")
		}
		for _, message := range messages {
			t.Logf("rediscovered: %s", message)
		}
	})
}
