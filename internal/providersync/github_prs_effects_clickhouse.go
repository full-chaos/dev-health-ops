package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubPullRequestClickHouseEffects writes the single `git_pull_requests`
// effect produced by GitHubPullRequestRouteHandler. The table is
// ReplacingMergeTree(last_synced) ordered by (org_id, repo_id, number)
// (migration 027_add_org_id_to_sorting_keys.py). Deduplication is
// asynchronous, so recovery is readback-fenced rather than blind-replayed --
// the identical discipline GitHubRepositoryClickHouseEffects (`repos`) uses.
type GitHubPullRequestClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubPullRequestClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "prs" ||
		effect.Destination != "git_pull_requests" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[pullRequestRow](effect)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := row.validate(claim); err != nil {
			return err
		}
	}
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	// org_id is appended last, mirroring _insert_rows' auto-injection order in
	// storage/clickhouse.py (`columns = [*columns, "org_id"]` when org_id is
	// not already in the explicit column list).
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO git_pull_requests (
  repo_id, number, title, body, state, author_name, author_email,
  created_at, merged_at, closed_at, head_branch, base_branch,
  additions, deletions, changed_files, first_review_at, first_comment_at,
  changes_requested_count, reviews_count, comments_count, last_synced,
  source_id, org_id
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.Number, row.Title, row.Body, row.State,
			row.AuthorName, row.AuthorEmail, row.CreatedAt, row.MergedAt,
			row.ClosedAt, row.HeadBranch, row.BaseBranch, row.Additions,
			row.Deletions, row.ChangedFiles, row.FirstReviewAt,
			row.FirstCommentAt, row.ChangesRequestedCount, row.ReviewsCount,
			row.CommentsCount, row.LastSynced, row.SourceID, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

// InspectEffect fences crash recovery the same way
// GitHubRepositoryClickHouseEffects.InspectEffect does: ReplacingMergeTree
// deduplicates asynchronously, so reading the winning version back turns "we
// may have written this" into an exact answer.
func (sink GitHubPullRequestClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "prs" ||
		effect.Destination != "git_pull_requests" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pullRequestRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	exact, absent := 0, 0
	for _, row := range expected {
		inspection, err := sink.inspectPullRequest(ctx, row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectExact:
			exact++
		case EffectAbsent:
			absent++
		default:
			return EffectConflict, nil
		}
	}
	switch {
	case exact == len(expected):
		return EffectExact, nil
	case absent == len(expected):
		return EffectAbsent, nil
	default:
		return EffectConflict, nil
	}
}

// inspectPullRequest resolves the latest ReplacingMergeTree version for
// (org_id, repo_id, number) and compares the full stable persisted row
// against it -- the argMax/FINAL discipline every raw reader of this table
// owes (per repo convention).
//
// codex M6: every Nullable column is scanned into a Go pointer directly
// (never through `ifNull(col, ”)`/`ifNull(col, sentinel)`), and every
// column the row actually carries -- including first_review_at,
// first_comment_at, changes_requested_count, and reviews_count, which an
// earlier version of this query omitted entirely -- is read back. An
// "exact" comparison that cannot see a column, or that collapses NULL and
// "" to the same Go value, can report EffectExact for a row that actually
// differs, which lets crash recovery mark corrupted data as committed.
func (sink GitHubPullRequestClickHouseEffects) inspectPullRequest(
	ctx context.Context,
	expected pullRequestRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT
  argMax(title, last_synced)                    AS winning_title,
  argMax(body, last_synced)                      AS winning_body,
  argMax(state, last_synced)                     AS winning_state,
  argMax(author_name, last_synced)               AS winning_author_name,
  argMax(author_email, last_synced)              AS winning_author_email,
  argMax(created_at, last_synced)                AS winning_created_at,
  argMax(merged_at, last_synced)                 AS winning_merged_at,
  argMax(closed_at, last_synced)                 AS winning_closed_at,
  argMax(head_branch, last_synced)               AS winning_head_branch,
  argMax(base_branch, last_synced)               AS winning_base_branch,
  argMax(additions, last_synced)                 AS winning_additions,
  argMax(deletions, last_synced)                 AS winning_deletions,
  argMax(changed_files, last_synced)             AS winning_changed_files,
  argMax(first_review_at, last_synced)           AS winning_first_review_at,
  argMax(first_comment_at, last_synced)          AS winning_first_comment_at,
  argMax(changes_requested_count, last_synced)   AS winning_changes_requested_count,
  argMax(reviews_count, last_synced)             AS winning_reviews_count,
  argMax(comments_count, last_synced)            AS winning_comments_count,
  argMax(source_id, last_synced)                 AS winning_source_id,
  argMax(org_id, last_synced)                    AS winning_org_id,
  max(last_synced)                               AS winning_version
FROM git_pull_requests
WHERE org_id = ? AND repo_id = ? AND number = ?`,
		expected.OrgID, expected.RepoID, expected.Number,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var (
		actual     pullRequestRow
		sourceID   *string
		orgID      string
		lastSynced time.Time
		found      bool
	)
	for rows.Next() {
		// additions/deletions/changed_files/comments_count/
		// changes_requested_count/reviews_count are non-nullable UInt32
		// columns; clickhouse-go requires scanning into *uint32, not *int.
		var additions, deletions, changedFiles, commentsCount uint32
		var changesRequestedCount, reviewsCount uint32
		if err := rows.Scan(
			&actual.Title, &actual.Body, &actual.State, &actual.AuthorName,
			&actual.AuthorEmail, &actual.CreatedAt, &actual.MergedAt,
			&actual.ClosedAt, &actual.HeadBranch, &actual.BaseBranch,
			&additions, &deletions, &changedFiles,
			&actual.FirstReviewAt, &actual.FirstCommentAt,
			&changesRequestedCount, &reviewsCount, &commentsCount,
			&sourceID, &orgID, &lastSynced,
		); err != nil {
			return EffectConflict, err
		}
		actual.Additions, actual.Deletions = int(additions), int(deletions)
		actual.ChangedFiles, actual.CommentsCount = int(changedFiles), int(commentsCount)
		actual.ChangesRequestedCount = int(changesRequestedCount)
		actual.ReviewsCount = int(reviewsCount)
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return comparePullRequestVersion(expected, pullRequestVersion{
		Row: actual, SourceID: sourceID, OrgID: orgID,
		LastSynced: lastSynced, Found: found,
	}), nil
}

// pullRequestVersion is the winning ReplacingMergeTree version for a key, as
// scanned from ClickHouse. Nullable columns are carried straight through as
// Go pointers (via pullRequestRow's own field types) so a true SQL NULL and
// an empty string remain distinguishable all the way to the comparison.
type pullRequestVersion struct {
	Row        pullRequestRow
	SourceID   *string
	OrgID      string
	LastSynced time.Time
	Found      bool
}

// comparePullRequestVersion decides whether this effect is the version that
// currently wins for the key.
//
// codex M6 (structural fix, not just added columns): each field is its own
// `if actual != expected { return EffectConflict }` clause rather than one
// large conjunction. A monolithic `a && b && c && ...` is a single unit for
// a mutation harness to kill -- deleting or weakening ONE clause inside it
// can go unnoticed as long as the OTHER clauses in the same test's fixture
// already differ, exactly the failure mode the shared mutation harness's
// "mutate compound predicates clause by clause" rule exists to catch. Named,
// separately-returning clauses make every field its own provable unit:
// TestPullRequestReadbackClassifiesEveryVersionRelationship and the mutation
// plan in testdata/mutation-plans kill each one independently.
func comparePullRequestVersion(
	expected pullRequestRow,
	actual pullRequestVersion,
) EffectInspection {
	if !actual.Found || actual.LastSynced.IsZero() {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent
	}
	if actual.LastSynced.UTC().After(expected.LastSynced.UTC()) {
		return EffectConflict
	}
	if !stringPointersEqual(actual.Row.Title, expected.Title) {
		return EffectConflict
	}
	if !stringPointersEqual(actual.Row.Body, expected.Body) {
		return EffectConflict
	}
	if actual.Row.State != expected.State {
		return EffectConflict
	}
	if actual.Row.AuthorName != expected.AuthorName {
		return EffectConflict
	}
	if !stringPointersEqual(actual.Row.AuthorEmail, expected.AuthorEmail) {
		return EffectConflict
	}
	if !actual.Row.CreatedAt.UTC().Equal(expected.CreatedAt.UTC()) {
		return EffectConflict
	}
	if !timePointersEqual(actual.Row.MergedAt, expected.MergedAt) {
		return EffectConflict
	}
	if !timePointersEqual(actual.Row.ClosedAt, expected.ClosedAt) {
		return EffectConflict
	}
	if !stringPointersEqual(actual.Row.HeadBranch, expected.HeadBranch) {
		return EffectConflict
	}
	if !stringPointersEqual(actual.Row.BaseBranch, expected.BaseBranch) {
		return EffectConflict
	}
	if actual.Row.Additions != expected.Additions {
		return EffectConflict
	}
	if actual.Row.Deletions != expected.Deletions {
		return EffectConflict
	}
	if actual.Row.ChangedFiles != expected.ChangedFiles {
		return EffectConflict
	}
	if !timePointersEqual(actual.Row.FirstReviewAt, expected.FirstReviewAt) {
		return EffectConflict
	}
	if !timePointersEqual(actual.Row.FirstCommentAt, expected.FirstCommentAt) {
		return EffectConflict
	}
	if actual.Row.ChangesRequestedCount != expected.ChangesRequestedCount {
		return EffectConflict
	}
	if actual.Row.ReviewsCount != expected.ReviewsCount {
		return EffectConflict
	}
	if actual.Row.CommentsCount != expected.CommentsCount {
		return EffectConflict
	}
	// The native sink never writes source_id; a non-NULL value here means
	// the external-ingest path (or some other writer) owns this key's
	// current version.
	if actual.SourceID != nil {
		return EffectConflict
	}
	if actual.OrgID != expected.OrgID {
		return EffectConflict
	}
	return EffectExact
}

func stringPointersEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func timePointersEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.UTC().Equal(right.UTC())
}

var _ EffectSink = GitHubPullRequestClickHouseEffects{}
var _ EffectReadback = GitHubPullRequestClickHouseEffects{}
