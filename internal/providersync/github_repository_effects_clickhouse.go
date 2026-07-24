package providersync

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubRepositoryClickHouseEffects writes the single `repos` effect produced
// by GitHubRepositoryRouteHandler. The table is
// ReplacingMergeTree(last_synced) ordered by (org_id, id), so a replayed
// identical batch is idempotent and no readback contract is required.
type GitHubRepositoryClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

func (sink GitHubRepositoryClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "repo-metadata" ||
		effect.Destination != "repos" {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	rows, err := decodeEffectRows[repositoryRow](effect)
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
INSERT INTO repos (
  id, org_id, repo, ref, created_at, settings, tags, provider, last_synced
)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(
			row.ID, row.OrgID, row.Repo, row.Ref, row.CreatedAt, row.Settings,
			row.Tags, row.Provider, row.LastSynced,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

// InspectEffect fences crash recovery. ReplacingMergeTree deduplicates
// asynchronously, so a blind reinsert would expose two physical `repos` rows
// to raw readers that join without FINAL/argMax until the next merge. Reading
// the row back turns "we may have written this" into an exact answer.
func (sink GitHubRepositoryClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	if ctx == nil || sink.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "repo-metadata" ||
		effect.Destination != "repos" {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	expected, err := decodeEffectRows[repositoryRow](effect)
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
		inspection, err := sink.inspectRepository(ctx, row)
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

func (sink GitHubRepositoryClickHouseEffects) inspectRepository(
	ctx context.Context,
	expected repositoryRow,
) (EffectInspection, error) {
	rows, err := sink.Conn.Query(ctx, `
SELECT repo, ref, created_at, settings, tags, provider, last_synced
FROM repos
WHERE org_id = ? AND id = ?`,
		expected.OrgID, expected.ID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	found, matched := 0, 0
	for rows.Next() {
		var actual repositoryRow
		if err := rows.Scan(
			&actual.Repo, &actual.Ref, &actual.CreatedAt, &actual.Settings,
			&actual.Tags, &actual.Provider, &actual.LastSynced,
		); err != nil {
			return EffectConflict, err
		}
		found++
		if actual.Repo == expected.Repo && actual.Settings == expected.Settings &&
			actual.Tags == expected.Tags && actual.Provider == expected.Provider &&
			actual.CreatedAt.UTC().Equal(expected.CreatedAt.UTC()) &&
			actual.LastSynced.UTC().Equal(expected.LastSynced.UTC()) {
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	switch {
	case found == 0:
		return EffectAbsent, nil
	// A duplicate insert of the identical row is still exactly this effect;
	// ReplacingMergeTree collapses the copies. Any differing row means another
	// writer owns the key and recovery must stop.
	case matched == found:
		return EffectExact, nil
	default:
		return EffectConflict, nil
	}
}

var _ EffectSink = GitHubRepositoryClickHouseEffects{}
var _ EffectReadback = GitHubRepositoryClickHouseEffects{}
