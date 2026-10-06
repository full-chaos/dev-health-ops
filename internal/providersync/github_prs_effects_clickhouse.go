package providersync

import (
	"context"
	"log/slog"
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
	Conn     driver.Conn
	Lease    providerfoundation.LeaseGuard
	Provider string
}

func (sink GitHubPullRequestClickHouseEffects) provider() string {
	if sink.Provider == "" {
		return "github"
	}
	return sink.Provider
}

func (sink GitHubPullRequestClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	return sink.writePullRequestEffect(ctx, claim, effect, "prs")
}

// writePullRequestEffect owns the complete git_pull_requests row write. The
// public prs sink deliberately restricts it to github/prs; the composed
// github/pr-reviews route calls this same whole-row writer under its own unit
// claim after it has enriched the three review-derived columns. Keeping the
// SQL and validation here prevents two subtly different complete-row writers
// from racing on a ReplacingMergeTree table, which has no partial-column merge.
func (sink GitHubPullRequestClickHouseEffects) writePullRequestEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
	dataset string,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != sink.provider() || claim.Dataset != dataset ||
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
	// merged_at write-once guard: a merged pull request is
	// terminal on GitHub's side -- "reopen" is never offered for a merged
	// pull request, only for a closed-but-unmerged one, which correctly
	// holds a null merged_at throughout its own lifecycle instead of ever
	// clearing a populated one. So a null merged_at arriving here for a key
	// that already has a stored value is transport/API noise (a stale or
	// racing upstream read), never a legitimate state transition; the
	// contract table keeps it, with every other held column this writer does
	// not state.
	if err := applyPullRequestContract(ctx, sink.Conn, claim, rows, true); err != nil {
		return err
	}
	batch, err := sink.Conn.PrepareBatch(ctx, pullRequestInsert)
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

// pullRequestMergedAtRegressionGuarded is one row where the incoming batch's
// merged_at was NULL while the currently winning physical row already had a
// value, and the contract carried the stored value forward instead of
// letting the null win. It carries only the row's own key and the stored
// merged_at -- never title/body or any other row content.
type pullRequestMergedAtRegressionGuarded struct {
	RepoID         string
	Number         int
	StoredMergedAt time.Time
}

// pullRequestReviewsCarriedForward is one row whose stored review-derived
// columns were carried forward over a failed review enrichment.
type pullRequestReviewsCarriedForward struct {
	RepoID string
	Number int
}

const pullRequestReviewRegressionGuardedEvent = "providersync.pull_request.review_regression_guarded"

// logPullRequestReviewRegressionGuarded emits one WARN line per carried
// row, keyed by the row's own identity only.
func logPullRequestReviewRegressionGuarded(
	ctx context.Context, claim Claim, carried []pullRequestReviewsCarriedForward,
) {
	for _, event := range carried {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, pullRequestReviewRegressionGuardedEvent,
			slog.String("org_id", claim.OrgID),
			slog.String("provider", claim.Provider),
			slog.String("dataset", claim.Dataset),
			slog.String("unit_id", claim.ID),
			slog.String("repo_id", event.RepoID),
			slog.Int("number", event.Number),
		)
	}
}

// pullRequestMergedAtRegressionGuardedEvent is this guard's stable log event
// name. internal/providersync must not import internal/jobruntime (see
// team_repo_ownership_derivation.go's identical note), so the Prometheus-
// shaped counters jobruntime.MetricsCollector exposes elsewhere (e.g.
// ObserveReportDedupGuard) are not reachable from here. A stable-named WARN
// log line is this package's existing substitute counter for a refusal --
// the same convention incidentEntitlementRefusedEvent (incident_entitlement.go)
// and github_projects_v2.snapshot_diff (github_project_membership_snapshot_diff.go)
// already use: a counter that only increments is enough to notice, and this
// event's stable name is countable by anything that reads the log stream.
const pullRequestMergedAtRegressionGuardedEvent = "providersync.pull_request.merged_at_regression_guarded"

// logPullRequestMergedAtRegressionGuarded emits one WARN line per guarded
// row, never a batch aggregate: this is the one place in the whole system
// where the stored value and the incoming value are both in the same
// process at the same time, so it is the only place that can ever record
// what was about to be overwritten with what. Attrs are limited to the
// row's own key (org/repo/number) and the two merged_at values in conflict
// -- never title/body or any other row content.
func logPullRequestMergedAtRegressionGuarded(
	ctx context.Context, claim Claim, guarded []pullRequestMergedAtRegressionGuarded,
) {
	for _, event := range guarded {
		slog.Default().LogAttrs(ctx, slog.LevelWarn, pullRequestMergedAtRegressionGuardedEvent,
			slog.String("org_id", claim.OrgID),
			slog.String("provider", claim.Provider),
			slog.String("dataset", claim.Dataset),
			slog.String("unit_id", claim.ID),
			slog.String("repo_id", event.RepoID),
			slog.Int("number", event.Number),
			slog.Time("stored_merged_at", event.StoredMergedAt.UTC()),
			slog.String("incoming_merged_at", "null"),
		)
	}
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
	return sink.inspectPullRequestEffect(ctx, claim, effect, "prs")
}

// inspectPullRequestEffect is the companion to writePullRequestEffect. It
// uses the exact same FINAL whole-row readback for github/prs and the composed
// github/pr-reviews unit, so crash recovery cannot bless a row whose review
// columns came from a different physical version.
func (sink GitHubPullRequestClickHouseEffects) inspectPullRequestEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
	dataset string,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != sink.provider() || claim.Dataset != dataset ||
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
	// WriteEffect carried held columns into the rows it inserted. Recovery
	// rebuilds `expected` from a fresh Collect() pass that knows nothing of
	// that correction, so the same contract runs here against the same
	// stored state; otherwise a row the contract itself wrote reads back as
	// EffectConflict. A read failure fails the inspection, the same as it
	// fails WriteEffect.
	if err := applyPullRequestContract(ctx, sink.Conn, claim, expected, false); err != nil {
		return EffectConflict, err
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

// inspectPullRequest resolves the winning ReplacingMergeTree version for
// (org_id, repo_id, number) and compares the full stable persisted row
// against it.
//
// codex H2: this reads the winning version's row AS A UNIT (`FROM
// git_pull_requests FINAL`), not by assembling one `argMax(column,
// last_synced)` per column. Independent per-column argMax calls are NOT
// equivalent to "read the row with the maximum last_synced": ClickHouse's
// argMax skips a row whose ARGUMENT is NULL when picking the maximum, so a
// winning row with (say) a NULL merged_at can have its merged_at silently
// backfilled from an OLDER, non-winning row's non-NULL value -- verified
// empirically against a real (unmerged, multi-part) ReplacingMergeTree
// table: separate INSERTs of {body:"old", merged_at:T1} then {body:"new",
// merged_at:NULL} at a later last_synced produced
// argMax(body)="new" (correct) alongside argMax(merged_at)=T1 (WRONG --
// reconstructed from the OLDER row instead of the winning row's true NULL).
// `FINAL` forces the same merge-time dedup logic the table's own background
// merges eventually apply, so every column comes from the one row that
// index actually selects -- self-consistent even in the documented
// ReplacingMergeTree tie case (two versions sharing an identical
// `last_synced`), where FINAL still returns one real, whole row rather than
// letting independent per-column aggregates disagree about which physical
// row "won". The WHERE clause matches the table's full ORDER BY prefix
// (org_id, repo_id, number), which is what keeps FINAL a bounded point
// lookup rather than a full-table merge.
//
// codex M6 (unchanged by this fix): every Nullable column is still scanned
// into a Go pointer directly, and comparePullRequestVersion still compares
// every column as its own named clause -- that logic already operates on a
// materialized pullRequestVersion and doesn't care how the row was
// selected, only that it IS one consistent row.
func (sink GitHubPullRequestClickHouseEffects) inspectPullRequest(
	ctx context.Context,
	expected pullRequestRow,
) (EffectInspection, error) {
	actual, err := sink.scanWinningPullRequestVersion(
		ctx, expected.OrgID, expected.RepoID, expected.Number,
	)
	if err != nil {
		return EffectConflict, err
	}
	return comparePullRequestVersion(expected, actual), nil
}

// scanWinningPullRequestVersion runs the actual production FINAL
// point-lookup query and scan (codex H2's fix) and returns the winning
// ReplacingMergeTree version for (org_id, repo_id, number). Extracted from
// inspectPullRequest (CHAOS-3162 codex finding #3) so
// oracle_readback_integration_test.go's readback pair can call this SAME
// production code directly instead of maintaining its own copy of the
// query -- a copy is a second source of truth that can drift from this one
// silently, which is exactly the class of defect this framework exists to
// catch, not commit.
func (sink GitHubPullRequestClickHouseEffects) scanWinningPullRequestVersion(
	ctx context.Context, orgID, repoID string, number int,
) (pullRequestVersion, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT
  title, body, state, author_name, author_email, created_at, merged_at,
  closed_at, head_branch, base_branch, additions, deletions, changed_files,
  first_review_at, first_comment_at, changes_requested_count, reviews_count,
  comments_count, source_id, org_id, last_synced
FROM git_pull_requests FINAL
WHERE org_id = ? AND repo_id = ? AND number = ?`,
		orgID, repoID, number,
	)
	if err != nil {
		return pullRequestVersion{}, err
	}
	defer rows.Close()
	var (
		actual      pullRequestRow
		sourceID    *string
		actualOrgID string
		lastSynced  time.Time
		found       bool
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
			&sourceID, &actualOrgID, &lastSynced,
		); err != nil {
			return pullRequestVersion{}, err
		}
		actual.Additions, actual.Deletions = int(additions), int(deletions)
		actual.ChangedFiles, actual.CommentsCount = int(changedFiles), int(commentsCount)
		actual.ChangesRequestedCount = int(changesRequestedCount)
		actual.ReviewsCount = int(reviewsCount)
		found = true
	}
	if err := rows.Err(); err != nil {
		return pullRequestVersion{}, err
	}
	return pullRequestVersion{
		Row: actual, SourceID: sourceID, OrgID: actualOrgID,
		LastSynced: lastSynced, Found: found,
	}, nil
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
// already differ, exactly the failure mode the "mutate compound predicates
// clause by clause" rule exists to catch. Named, separately-returning clauses
// make every field its own provable unit:
// TestPullRequestReadbackClassifiesEveryVersionRelationship kills each one
// independently.
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
