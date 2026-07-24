package providersync

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubRepositoryClickHouseEffects writes the single `repos` effect produced
// by GitHubRepositoryRouteHandler. The table is
// ReplacingMergeTree(last_synced) ordered by (org_id, id). Deduplication is
// asynchronous, so recovery is readback-fenced rather than blind-replayed:
// see InspectEffect.
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

// inspectRepository resolves the *latest* ReplacingMergeTree version for the
// key and compares the full stable persisted row against it.
//
// Reading every physical version would be wrong: pre-merge history for a key
// legitimately holds this occurrence's row alongside earlier occurrences'
// rows, so requiring all versions to match would report a conflict for a
// perfectly healthy table and wedge recovery. `repos` is
// ReplacingMergeTree(last_synced), so argMax over last_synced is the
// engine-defined winner — the same argMax/FINAL discipline every raw reader
// of this table owes.
func (sink GitHubRepositoryClickHouseEffects) inspectRepository(
	ctx context.Context,
	expected repositoryRow,
) (EffectInspection, error) {
	// ifNull keeps Nullable(String)/Nullable(UUID) out of the scan contract;
	// the native sink always writes ref and source_id as NULL.
	//
	// The version aggregate is aliased `winning_version`, never `last_synced`:
	// an alias that shadows the column makes the sibling argMax calls resolve
	// their weight argument to that alias, and ClickHouse rejects the whole
	// query as an aggregate nested inside an aggregate (code 184).
	rows, err := sink.Conn.Query(ctx, `
SELECT
  argMax(repo, last_synced)                            AS winning_repo,
  argMax(ifNull(ref, ''), last_synced)                 AS winning_ref,
  argMax(created_at, last_synced)                      AS winning_created_at,
  argMax(ifNull(settings, ''), last_synced)            AS winning_settings,
  argMax(ifNull(tags, ''), last_synced)                AS winning_tags,
  argMax(provider, last_synced)                        AS winning_provider,
  argMax(ifNull(toString(source_id), ''), last_synced) AS winning_source_id,
  max(last_synced)                                     AS winning_version
FROM repos
WHERE org_id = ? AND id = ?`,
		expected.OrgID, expected.ID,
	)
	if err != nil {
		return EffectConflict, err
	}
	defer rows.Close()
	var (
		actual     repositoryRow
		ref        string
		sourceID   string
		lastSynced time.Time
		found      bool
	)
	for rows.Next() {
		if err := rows.Scan(
			&actual.Repo, &ref, &actual.CreatedAt, &actual.Settings,
			&actual.Tags, &actual.Provider, &sourceID, &lastSynced,
		); err != nil {
			return EffectConflict, err
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return EffectConflict, err
	}
	return compareRepositoryVersion(expected, repositoryVersion{
		Row: actual, Ref: ref, SourceID: sourceID,
		LastSynced: lastSynced, Found: found,
	}), nil
}

// repositoryVersion is the winning ReplacingMergeTree version for a key, as
// scanned from ClickHouse.
type repositoryVersion struct {
	Row        repositoryRow
	Ref        string
	SourceID   string
	LastSynced time.Time
	Found      bool
}

// compareRepositoryVersion decides whether this effect is the version that
// currently wins for the key. It is separated from the query so every branch
// is testable without a live ClickHouse.
func compareRepositoryVersion(
	expected repositoryRow,
	actual repositoryVersion,
) EffectInspection {
	// An aggregate over no rows still yields one all-zero row.
	if !actual.Found || actual.LastSynced.IsZero() {
		return EffectAbsent
	}
	switch {
	case actual.LastSynced.UTC().Before(expected.LastSynced.UTC()):
		// The winning version predates this effect: our write never landed.
		// Pre-merge history from earlier occurrences is normal and must not
		// read as a conflict.
		return EffectAbsent
	case actual.LastSynced.UTC().After(expected.LastSynced.UTC()):
		// A newer occurrence already superseded this key. Reinserting would be
		// pointless and we cannot prove our write landed; reconcile explicitly.
		return EffectConflict
	}
	expectedRef := ""
	if expected.Ref != nil {
		expectedRef = *expected.Ref
	}
	// The full stable persisted row, including the columns the native sink
	// leaves NULL. A differing value at the same version means another writer
	// owns this key.
	if actual.Row.Repo == expected.Repo && actual.Ref == expectedRef &&
		actual.Row.Settings == expected.Settings &&
		actual.Row.Tags == expected.Tags &&
		actual.Row.Provider == expected.Provider &&
		actual.SourceID == "" &&
		actual.Row.CreatedAt.UTC().Equal(expected.CreatedAt.UTC()) {
		return EffectExact
	}
	return EffectConflict
}

var _ EffectSink = GitHubRepositoryClickHouseEffects{}
var _ EffectReadback = GitHubRepositoryClickHouseEffects{}
