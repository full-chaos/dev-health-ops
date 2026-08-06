package providersync

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ClickHouse effect adapters for the three derived destinations this lane
// owns. They deliberately do NOT share one readback shape, because the three
// tables do not share one engine:
//
//   - estimate_coverage_metrics_daily is ReplacingMergeTree(computed_at)
//     PARTITION BY toYYYYMM(day) (migration 063). FINAL collapses duplicate
//     versions, but ONLY WITHIN A PARTITION -- so the WHERE must pin `day`,
//     which fully determines toYYYYMM(day) and prunes to exactly one
//     partition. A readback keyed on the sorting key without `day` returned
//     found=2 across a month boundary on PR #1535; that class is closed here
//     by construction, not by hoping.
//   - work_item_state_durations_daily is PLAIN MergeTree. Migration 055
//     converted the two work-item rollups to ReplacingMergeTree and
//     explicitly declined to convert this one, on the grounds that its
//     readers already deduplicate with argMax(metric, computed_at). FINAL on
//     a non-Replacing engine is a NO-OP: it would silently return every
//     appended version. So this readback groups by the natural key and takes
//     argMax over computed_at, which is what production readers do.
//   - work_item_team_attributions is ReplacingMergeTree(computed_at) with NO
//     PARTITION BY clause (migration 051). One partition means FINAL
//     deduplicates globally and no partition fence exists to add. That
//     asymmetry is asserted by the integration tests rather than assumed.
//
// All three comparators test `found != 1` BEFORE looking at the scanned row.
// Checking an absent/stale branch first turns "duplicates plus a stale row"
// into an infinite rewrite loop -- the second blocking finding on #1535.

// GitHubEstimateCoverageClickHouseEffects writes and verifies
// estimate_coverage_metrics_daily.
type GitHubEstimateCoverageClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// GitHubWorkItemTeamAttributionsClickHouseEffects writes and verifies
// work_item_team_attributions.
type GitHubWorkItemTeamAttributionsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// GitHubWorkItemStateDurationsClickHouseEffects writes and verifies
// work_item_state_durations_daily.
type GitHubWorkItemStateDurationsClickHouseEffects struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

// validateGitHubWorkItemDerivedEffect re-derives the typed rows from the
// frozen effect and refuses anything whose destination, tenancy or recovery
// posture does not match the identity. Decoding from effect.Rows rather than
// from a builder result is deliberate: recovery replays a persisted manifest
// that no builder run produced.
func validateGitHubWorkItemDerivedEffect[T any](
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
	destination string,
) ([]T, error) {
	if identity.Destination != destination || effect.Destination != destination ||
		effect.Recovery != EffectReadbackRequired || identity.OrgID == "" ||
		identity.RowCount != len(effect.Rows) {
		return nil, ErrInvalidConfiguration
	}
	rows := make([]T, 0, len(effect.Rows))
	for _, raw := range effect.Rows {
		var row T
		if err := json.Unmarshal(raw, &row); err != nil {
			return nil, ErrInvalidConfiguration
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// inspectGitHubWorkItemDerivedRows returns the WEAKEST verdict across rows:
// any conflict wins, then any absence. A batch is only exact when every row
// in it reads back exactly, because the committer rewrites the whole batch.
func inspectGitHubWorkItemDerivedRows[T any](
	rows []T,
	inspect func(T) (EffectInspection, error),
) (EffectInspection, error) {
	verdict := EffectExact
	for _, row := range rows {
		inspection, err := inspect(row)
		if err != nil {
			return EffectConflict, err
		}
		switch inspection {
		case EffectConflict:
			return EffectConflict, nil
		case EffectAbsent:
			verdict = EffectAbsent
		case EffectExact:
		default:
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return verdict, nil
}

// Stored-precision truncation. A version column can only hold what its type
// can represent, so the comparator MUST compare the expected value as it would
// be STORED, not as it was stamped. Otherwise a computed_at carrying
// sub-precision digits is written, quantized by the server, read back
// different, and the version verdict is Absent forever -- replay can never
// answer Exact and the committer rewrites on every recovery. #1537 shipped 13
// green integration tests over this exact defect because every fixture used a
// zero-nanosecond instant.
//
// The builder now quantizes at its own stamping sites, so for rows this process
// built these are NO-OPS. They stay because this boundary must hold for rows it
// did NOT build: a replayed effect is decoded from a persisted snapshot, and a
// snapshot written by an older build (or by any future producer that forgets)
// still has to compare against what the column can store. Truncation is
// idempotent, so applying it twice costs nothing and removing it would make the
// adapter trust its input.
//
// The precision constants live with the builders, next to the destination whose
// column decides them, so the two layers cannot drift into disagreeing.
func githubWorkItemDerivedMillis(value time.Time) time.Time {
	return githubWorkItemDerivedStamp(value, githubEstimateCoverageStampPrecision)
}

func githubWorkItemDerivedSeconds(value time.Time) time.Time {
	return githubWorkItemDerivedStamp(value, githubStateDurationStampPrecision)
}

// githubWorkItemDerivedSortingKeyDedupe collapses rows that share a full
// ClickHouse sorting key, keeping the HIGHEST version and breaking ties by
// order (last occurrence wins).
//
// Version first, not order first, because the destination is a
// ReplacingMergeTree keyed on computed_at: the server resolves a collision by
// VERSION and ignores insertion order entirely. Ordering alone is correct only
// while every row in a batch shares one computed_at -- true today, enforced by
// nothing. The day a batch carries mixed versions, an order-only dedup names a
// row the server discards, the readback compares against a row that is not
// there, and recovery wedges at Conflict permanently.
//
// The tie-break stays LAST-wins so the equal-version behaviour, which is the
// case every real batch takes today, is unchanged.
//
// This is a PERSISTENCE-layer decision, not a compute-layer one, so it does
// not touch the D16 mirroring of the builders: Python writes every row and
// lets ReplacingMergeTree pick a winner among equal versions, which is not
// deterministic. Deduplicating before the write makes the stored outcome
// deterministic AND lets the expectation name the same winner, instead of the
// readback comparing against a row the storage silently discarded.
//
// Without this, two attribution candidates that differ only in team_name --
// which the resolver genuinely produces when two ownership facts name one team
// differently -- collapse to one stored row, `found` is 1, the team_name
// mismatch reads as Conflict, and recovery is wedged permanently.
func githubWorkItemDerivedSortingKeyDedupe[T any](
	rows []T, key func(T) string, version func(T) time.Time,
) []T {
	winner := make(map[string]int, len(rows))
	for index, row := range rows {
		current, exists := winner[key(row)]
		// `!Before` keeps the LATER index on an exact tie, which is the
		// last-wins tie-break.
		if !exists || !version(row).Before(version(rows[current])) {
			winner[key(row)] = index
		}
	}
	result := make([]T, 0, len(rows))
	for index, row := range rows {
		if winner[key(row)] == index {
			result = append(result, row)
		}
	}
	return result
}

// githubTeamAttributionVersion is the ReplacingMergeTree version column for
// work_item_team_attributions, read through the same stored-precision
// truncation the write and the comparison use. Comparing raw stamps here while
// storing truncated ones would let two rows that are one version after
// truncation be ordered by digits the column cannot hold.
func githubTeamAttributionVersion(row githubWorkItemTeamAttributionRow) time.Time {
	return githubWorkItemDerivedMillis(row.ComputedAt)
}

// githubWorkItemDerivedUniqueSortingKeys reports whether every row already
// occupies a distinct sorting key. The two map-derived destinations are unique
// BY CONSTRUCTION; asserting it here makes that impossibility measured rather
// than assumed, so it fails loudly the day a builder starts emitting a
// collision instead of wedging a readback in production.
func githubWorkItemDerivedUniqueSortingKeys[T any](rows []T, key func(T) string) bool {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		identifier := key(row)
		if _, duplicate := seen[identifier]; duplicate {
			return false
		}
		seen[identifier] = struct{}{}
	}
	return true
}

func githubEstimateCoverageSortingKey(row githubEstimateCoverageMetricsDailyRow) string {
	return strings.Join([]string{
		string(row.Day), row.Provider, row.WorkScopeID,
		githubWorkItemDerivedNullableString(row.TeamID),
	}, "\x00")
}

func githubStateDurationSortingKey(row githubWorkItemStateDurationDailyRow) string {
	return strings.Join([]string{
		string(row.Day), row.Provider, row.WorkScopeID, row.TeamID, row.Status,
	}, "\x00")
}

func githubTeamAttributionSortingKey(row githubWorkItemTeamAttributionRow) string {
	return strings.Join([]string{
		githubWorkItemDerivedRepoID(row.RepoID).String(), row.WorkItemID,
		githubWorkItemDerivedNullableString(row.TeamID), row.Source,
	}, "\x00")
}

func (sink GitHubEstimateCoverageClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubEstimateCoverageMetricsDailyRow](
		identity, effect, githubEstimateCoverageDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil || sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	// Map-derived, so one row per sorting key by construction. Measured, not
	// assumed: a collision would collapse under RMT and wedge the readback.
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubEstimateCoverageSortingKey) {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO estimate_coverage_metrics_daily
(day, provider, work_scope_id, team_id, team_name, estimated_count,
unestimated_count, backlog_size, ratio, computed_at, org_id)`)
	if err != nil {
		return err
	}
	for _, row := range rows {
		day, err := row.Day.time()
		if err != nil {
			return err
		}
		if err := batch.Append(
			day, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName,
			uint32(row.EstimatedCount), uint32(row.UnestimatedCount),
			uint32(row.BacklogSize), row.Ratio,
			githubWorkItemDerivedMillis(row.ComputedAt), identity.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubEstimateCoverageClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubEstimateCoverageMetricsDailyRow](
		identity, effect, githubEstimateCoverageDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubEstimateCoverageMetricsDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

func (sink GitHubEstimateCoverageClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubEstimateCoverageMetricsDailyRow,
) (EffectInspection, error) {
	day, err := expected.Day.time()
	if err != nil {
		return EffectConflict, err
	}
	// `day = ?` is doing double duty: it is part of the sorting key AND it
	// fully determines toYYYYMM(day), so FINAL is evaluated inside exactly one
	// partition. Dropping it would reintroduce the #1535 cross-partition
	// found=2. ifNull(team_id, '') mirrors the table's own sorting-key
	// expression so a NULL team never falls outside the fence.
	scan, err := sink.Conn.Query(ctx, `SELECT day, provider, work_scope_id, team_id,
team_name, estimated_count, unestimated_count, backlog_size, ratio, computed_at, org_id
FROM estimate_coverage_metrics_daily FINAL
WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ?
AND ifNull(team_id, '') = ?`,
		identity.OrgID, day, expected.Provider, expected.WorkScopeID,
		githubWorkItemDerivedNullableString(expected.TeamID))
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	var actual githubEstimateCoverageMetricsDailyRow
	found := 0
	for scan.Next() {
		var actualDay time.Time
		// The three counters are UInt32; the driver refuses to scan an
		// unsigned column into a Go int, which would make every readback fail
		// as a transport error rather than ever returning a verdict.
		var estimated, unestimated, backlog uint32
		if err := scan.Scan(
			&actualDay, &actual.Provider, &actual.WorkScopeID, &actual.TeamID,
			&actual.TeamName, &estimated, &unestimated, &backlog, &actual.Ratio,
			&actual.ComputedAt, &actual.OrgID,
		); err != nil {
			return EffectConflict, err
		}
		actual.Day = newGitHubWorkItemDerivedDay(actualDay)
		actual.EstimatedCount = int(estimated)
		actual.UnestimatedCount = int(unestimated)
		actual.BacklogSize = int(backlog)
		found++
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubEstimateCoverageVersion(expected, actual, found, identity.OrgID), nil
}

func (sink GitHubWorkItemTeamAttributionsClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemTeamAttributionRow](
		identity, effect, githubTeamAttributionsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil || sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	// Collisions are GENUINELY REACHABLE here, unlike the two map-derived
	// destinations: the resolver emits one candidate per ownership fact, so two
	// facts naming the same team differently produce two rows with an identical
	// sorting key that differ only in team_name.
	rows = githubWorkItemDerivedSortingKeyDedupe(
		rows, githubTeamAttributionSortingKey, githubTeamAttributionVersion,
	)
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO work_item_team_attributions
(org_id, repo_id, work_item_id, provider, team_id, team_name, source,
is_primary, confidence, evidence, computed_at)`)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := batch.Append(
			identity.OrgID, githubWorkItemDerivedRepoID(row.RepoID), row.WorkItemID,
			row.Provider, row.TeamID, row.TeamName, row.Source,
			uint8(row.IsPrimary), row.Confidence, row.Evidence,
			githubWorkItemDerivedMillis(row.ComputedAt),
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubWorkItemTeamAttributionsClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemTeamAttributionRow](
		identity, effect, githubTeamAttributionsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	// The expectation must name the row the WRITE will actually leave behind,
	// or the readback compares against a row storage discarded.
	rows = githubWorkItemDerivedSortingKeyDedupe(
		rows, githubTeamAttributionSortingKey, githubTeamAttributionVersion,
	)
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubWorkItemTeamAttributionRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

func (sink GitHubWorkItemTeamAttributionsClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubWorkItemTeamAttributionRow,
) (EffectInspection, error) {
	// This table has NO PARTITION BY, so it is a single partition and FINAL
	// deduplicates across the whole table. There is no partition key to fence
	// and adding a day predicate would be meaningless here -- the fence is the
	// full sorting key.
	scan, err := sink.Conn.Query(ctx, `SELECT org_id, repo_id, work_item_id, provider,
team_id, team_name, source, is_primary, confidence, evidence, computed_at
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND repo_id = ? AND work_item_id = ?
AND ifNull(team_id, '') = ? AND source = ?`,
		identity.OrgID, githubWorkItemDerivedRepoID(expected.RepoID), expected.WorkItemID,
		githubWorkItemDerivedNullableString(expected.TeamID), expected.Source)
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	var actual githubWorkItemTeamAttributionRow
	found := 0
	for scan.Next() {
		var repoID uuid.UUID
		var isPrimary uint8
		if err := scan.Scan(
			&actual.OrgID, &repoID, &actual.WorkItemID, &actual.Provider,
			&actual.TeamID, &actual.TeamName, &actual.Source, &isPrimary,
			&actual.Confidence, &actual.Evidence, &actual.ComputedAt,
		); err != nil {
			return EffectConflict, err
		}
		actual.RepoID = &repoID
		actual.IsPrimary = int(isPrimary)
		found++
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubWorkItemTeamAttributionVersion(expected, actual, found, identity.OrgID), nil
}

func (sink GitHubWorkItemStateDurationsClickHouseEffects) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemStateDurationDailyRow](
		identity, effect, githubStateDurationsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil || sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if !githubWorkItemDerivedUniqueSortingKeys(rows, githubStateDurationSortingKey) {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, `INSERT INTO work_item_state_durations_daily
(day, provider, work_scope_id, team_id, team_name, status, duration_hours,
items_touched, computed_at, avg_wip, org_id)`)
	if err != nil {
		return err
	}
	for _, row := range rows {
		day, err := row.Day.time()
		if err != nil {
			return err
		}
		if err := batch.Append(
			day, row.Provider, row.WorkScopeID, row.TeamID, row.TeamName,
			row.Status, row.DurationHours, uint32(row.ItemsTouched),
			githubWorkItemDerivedSeconds(row.ComputedAt), row.AvgWIP, identity.OrgID,
		); err != nil {
			return err
		}
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	return batch.Send()
}

func (sink GitHubWorkItemStateDurationsClickHouseEffects) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := validateGitHubWorkItemDerivedEffect[githubWorkItemStateDurationDailyRow](
		identity, effect, githubStateDurationsDestination,
	)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	if sink.Conn == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspectGitHubWorkItemDerivedRows(rows, func(row githubWorkItemStateDurationDailyRow) (EffectInspection, error) {
		return sink.inspect(ctx, identity, row)
	})
}

func (sink GitHubWorkItemStateDurationsClickHouseEffects) inspect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	expected githubWorkItemStateDurationDailyRow,
) (EffectInspection, error) {
	day, err := expected.Day.time()
	if err != nil {
		return EffectConflict, err
	}
	// PLAIN MergeTree: FINAL is a no-op here and would return every appended
	// version, so a re-run would read found=N and rewrite forever. argMax over
	// computed_at is what production readers do (migration 055 declined to
	// convert this table precisely because its readers already dedupe this
	// way). GROUP BY collapses to at most one row, so `found` still
	// distinguishes absent from present -- and `day` still prunes the
	// toYYYYMM(day) partition even though the engine is not Replacing.
	scan, err := sink.Conn.Query(ctx, `SELECT
argMax(team_name, computed_at), argMax(duration_hours, computed_at),
argMax(items_touched, computed_at), max(computed_at), argMax(avg_wip, computed_at)
FROM work_item_state_durations_daily
WHERE org_id = ? AND day = ? AND provider = ? AND work_scope_id = ?
AND team_id = ? AND status = ?
GROUP BY org_id, provider, work_scope_id, team_id, status, day`,
		identity.OrgID, day, expected.Provider, expected.WorkScopeID,
		expected.TeamID, expected.Status)
	if err != nil {
		return EffectConflict, err
	}
	defer scan.Close()
	actual := githubWorkItemStateDurationDailyRow{
		Day: expected.Day, Provider: expected.Provider,
		WorkScopeID: expected.WorkScopeID, TeamID: expected.TeamID,
		Status: expected.Status, OrgID: identity.OrgID,
	}
	found := 0
	for scan.Next() {
		var itemsTouched uint32
		if err := scan.Scan(
			&actual.TeamName, &actual.DurationHours, &itemsTouched,
			&actual.ComputedAt, &actual.AvgWIP,
		); err != nil {
			return EffectConflict, err
		}
		actual.ItemsTouched = int(itemsTouched)
		found++
	}
	if err := scan.Err(); err != nil {
		return EffectConflict, err
	}
	return compareGitHubWorkItemStateDurationVersion(expected, actual, found, identity.OrgID), nil
}

// githubWorkItemDerivedVersionOrder is the THREE-WAY version comparison every
// comparator in this file must apply BEFORE looking at any value column.
//
// A two-way `!Equal -> Absent` is not merely incomplete, it wedges. Consider a
// byte-identical recompute where only computed_at moved -- the steady state for
// work_item_team_attributions, whose producer is called once per backfill day
// and re-emits identical rows. Persisted carries the NEWER stamp, the effect
// being inspected carries the older one, and every value column agrees. Under
// `!Equal -> Absent` that reads Absent, the committer rewrites the OLDER
// generation, argMax (or RMT) keeps the newer one, the next readback answers
// Absent again, and the loop never terminates. On plain-MergeTree
// state_durations each iteration also APPENDS a row.
//
// Order matters as much as the three-way itself:
//   - persisted OLDER than the effect: our write never landed, so value
//     differences are EXPECTED and must not be reported as a conflict.
//   - persisted NEWER: a later generation already superseded this key.
//     Rewriting would regress it, and we cannot prove our write landed.
//   - persisted EQUAL: same generation, so any value disagreement is a real
//     contradiction rather than a version difference.
//
// This mirrors compareRepositoryVersion (github_repository_effects_clickhouse.go).
func githubWorkItemDerivedVersionOrder(actual, expected time.Time) (EffectInspection, bool) {
	switch {
	case actual.Before(expected):
		return EffectAbsent, true
	case actual.After(expected):
		return EffectConflict, true
	}
	return EffectExact, false
}

// The three comparators below all test `found != 1` FIRST. Reaching an
// absent/stale branch before the duplicate check is what turns duplicates plus
// a stale row into an infinite rewrite loop.

func compareGitHubEstimateCoverageVersion(
	expected, actual githubEstimateCoverageMetricsDailyRow, found int, orgID string,
) EffectInspection {
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		// More than one row survived FINAL inside a single partition: the
		// fence is wrong, not the data. Never report this as absent, which
		// would rewrite and duplicate again.
		return EffectConflict
	}
	// Tenancy is not a version question: a foreign tenant's row is a
	// contradiction at ANY version.
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if verdict, decided := githubWorkItemDerivedVersionOrder(
		actual.ComputedAt, githubWorkItemDerivedMillis(expected.ComputedAt),
	); decided {
		return verdict
	}
	if actual.Day != expected.Day ||
		actual.Provider != expected.Provider ||
		actual.WorkScopeID != expected.WorkScopeID ||
		!githubWorkItemDerivedStringPointerEqual(actual.TeamID, expected.TeamID) ||
		!githubWorkItemDerivedStringPointerEqual(actual.TeamName, expected.TeamName) ||
		actual.EstimatedCount != expected.EstimatedCount ||
		actual.UnestimatedCount != expected.UnestimatedCount ||
		actual.BacklogSize != expected.BacklogSize ||
		!githubWorkItemDerivedFloatPointerEqual(actual.Ratio, expected.Ratio) {
		return EffectConflict
	}
	return EffectExact
}

func compareGitHubWorkItemTeamAttributionVersion(
	expected, actual githubWorkItemTeamAttributionRow, found int, orgID string,
) EffectInspection {
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		return EffectConflict
	}
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if verdict, decided := githubWorkItemDerivedVersionOrder(
		actual.ComputedAt, githubWorkItemDerivedMillis(expected.ComputedAt),
	); decided {
		return verdict
	}
	if actual.WorkItemID != expected.WorkItemID ||
		actual.Provider != expected.Provider ||
		githubWorkItemDerivedRepoID(actual.RepoID) != githubWorkItemDerivedRepoID(expected.RepoID) ||
		!githubWorkItemDerivedStringPointerEqual(actual.TeamID, expected.TeamID) ||
		!githubWorkItemDerivedStringPointerEqual(actual.TeamName, expected.TeamName) ||
		actual.Source != expected.Source || actual.IsPrimary != expected.IsPrimary ||
		actual.Confidence != expected.Confidence || actual.Evidence != expected.Evidence {
		return EffectConflict
	}
	return EffectExact
}

func compareGitHubWorkItemStateDurationVersion(
	expected, actual githubWorkItemStateDurationDailyRow, found int, orgID string,
) EffectInspection {
	if found != 1 {
		if found == 0 {
			return EffectAbsent
		}
		// GROUP BY the full natural key can only produce one row, so more than
		// one means the grouping no longer matches the key -- a wiring defect,
		// never a reason to rewrite.
		return EffectConflict
	}
	if actual.OrgID != orgID {
		return EffectConflict
	}
	if verdict, decided := githubWorkItemDerivedVersionOrder(
		actual.ComputedAt, githubWorkItemDerivedSeconds(expected.ComputedAt),
	); decided {
		return verdict
	}
	if actual.TeamName != expected.TeamName ||
		actual.DurationHours != expected.DurationHours ||
		actual.ItemsTouched != expected.ItemsTouched ||
		actual.AvgWIP != expected.AvgWIP {
		return EffectConflict
	}
	return EffectExact
}

// time parses the calendar day back into the UTC midnight instant the driver
// binds to a ClickHouse Date column.
func (day githubWorkItemDerivedDay) time() (time.Time, error) {
	return time.ParseInLocation("2006-01-02", string(day), time.UTC)
}

// githubWorkItemDerivedRepoID mirrors what the Python sink persists for an
// item with no repo: work_item_team_attributions.repo_id is a NON-nullable
// UUID column, so a missing repo becomes the nil UUID rather than NULL.
func githubWorkItemDerivedRepoID(value *uuid.UUID) uuid.UUID {
	if value == nil {
		return uuid.Nil
	}
	return *value
}

func githubWorkItemDerivedNullableString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func githubWorkItemDerivedStringPointerEqual(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func githubWorkItemDerivedFloatPointerEqual(left, right *float64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
