package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubPullRequestReviewClickHouseEffects is the non-routed persistence
// foundation for github/pr-reviews. Both write and FINAL readback are guarded
// by the authoritative unit lease; wiring this sink does not activate a route.
type GitHubPullRequestReviewClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubPullRequestReviewClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "pr-reviews" ||
		effect.Destination != "git_pull_request_reviews" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[pullRequestReviewRow](effect)
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
	batch, err := sink.Conn.PrepareBatch(ctx, `
INSERT INTO git_pull_request_reviews (
  repo_id, number, review_id, reviewer, state, submitted_at,
  last_synced, source_id, org_id
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.RepoID, row.Number, row.ReviewID, row.Reviewer, row.State,
			row.SubmittedAt, row.LastSynced, row.SourceID, row.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubPullRequestReviewClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || sink.Conn == nil ||
		claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "pr-reviews" || effect.Destination != "git_pull_request_reviews" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[pullRequestReviewRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if len(expected) == 0 {
		return EffectAbsent, nil
	}
	exact, absent := 0, 0
	for _, row := range expected {
		if err := row.validate(claim); err != nil {
			return EffectConflict, err
		}
		inspection, err := sink.inspectReview(ctx, row)
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
	if exact == len(expected) {
		return EffectExact, nil
	}
	if absent == len(expected) {
		return EffectAbsent, nil
	}
	return EffectConflict, nil
}

func (sink GitHubPullRequestReviewClickHouseEffects) inspectReview(
	ctx context.Context, expected pullRequestReviewRow,
) (EffectInspection, error) {
	var actual pullRequestReviewRow
	rows, err := sink.Conn.Query(ctx, `
SELECT reviewer, state, submitted_at, last_synced, source_id, org_id
FROM git_pull_request_reviews FINAL
WHERE org_id = ? AND repo_id = ? AND number = ? AND review_id = ?`,
		expected.OrgID, expected.RepoID, expected.Number, expected.ReviewID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		if found {
			return EffectConflict, nil
		}
		if err := rows.Scan(
			&actual.Reviewer, &actual.State, &actual.SubmittedAt,
			&actual.LastSynced, &actual.SourceID, &actual.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	if !found {
		return EffectAbsent, nil
	}
	if actual.LastSynced.UTC().Before(expected.LastSynced.UTC()) {
		return EffectAbsent, nil
	}
	if !actual.LastSynced.UTC().Equal(expected.LastSynced.UTC()) ||
		actual.Reviewer != expected.Reviewer || actual.State != expected.State ||
		!actual.SubmittedAt.UTC().Equal(expected.SubmittedAt.UTC()) ||
		actual.SourceID != nil || actual.OrgID != expected.OrgID {
		return EffectConflict, nil
	}
	return EffectExact, nil
}

var _ EffectSink = GitHubPullRequestReviewClickHouseEffects{}
var _ EffectReadback = GitHubPullRequestReviewClickHouseEffects{}
