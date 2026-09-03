package remaining

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/google/uuid"
)

// membershipRetentionKeep is the CHAOS-2433 round-5 "keep-latest-2" default:
// the current latest complete run PLUS one prior, so a reader mid-flight
// against the immediately-previous complete run is not pulled out from
// under it. Matches the Python sink's own `keep: int = 2` default.
const membershipRetentionKeep = 2

// MembershipOutcome reports what one org's run did -- the Go counterpart of
// backfill_memberships' returned stats dict.
type MembershipOutcome struct {
	Components          int
	Matched             int
	Skipped             int
	MembershipRows      int
	OversizedComponents int
	DroppedEdges        int
	DroppedNodes        int
}

// MembershipObserver receives one org's run stats, so a refused/degenerate
// run (e.g. every current component churned) is a counter an alert can bind
// to, not just a quiet zero-row partition -- same motivation as
// recommendations' ReadinessObserver.
type MembershipObserver interface {
	ObserveMembershipRun(orgID string, outcome MembershipOutcome)
	// ObserveMembershipPruneFailed reports a failed retention prune
	// (codex round 1, #2177, P2: a swallowed prune error was previously
	// invisible -- the projection itself is correct and complete, only
	// retention silently stopped keeping pace). Best-effort by design (see
	// ComputeOrg's own comment on why a prune failure must not fail the
	// partition); this is what makes a PERSISTENT failure visible instead
	// of merely tolerated.
	ObserveMembershipPruneFailed(orgID string)
}

// CollectorMembershipObserver adapts the metrics collector to
// MembershipObserver, so ComputeOrg stays free of the telemetry package's
// shape -- same pattern as recommendations' CollectorReadinessObserver.
//
// orgID is accepted (matching the interface every caller here needs to
// satisfy) but not forwarded: the exposition's run-stats series are
// fleet-wide sums, not per-org labels, matching capacity's and dora's own
// counters -- an unbounded org_id label on a Prometheus series is exactly
// the cardinality trap those two also avoid.
type CollectorMembershipObserver struct {
	Collector *jobruntime.MetricsCollector
}

func (observer CollectorMembershipObserver) ObserveMembershipRun(_ string, outcome MembershipOutcome) {
	if observer.Collector == nil {
		return
	}
	_ = observer.Collector.ObserveMembershipRun(
		outcome.Components, outcome.Matched, outcome.Skipped, outcome.MembershipRows,
		outcome.OversizedComponents, outcome.DroppedEdges, outcome.DroppedNodes,
	)
}

func (observer CollectorMembershipObserver) ObserveMembershipPruneFailed(_ string) {
	if observer.Collector == nil {
		return
	}
	observer.Collector.ObserveMembershipPruneFailed()
}

// membershipDistribution is one work_unit_id's latest persisted
// categorization -- the Go counterpart of one row from
// _fetch_latest_distributions (backfill.py).
type membershipDistribution struct {
	ThemeDistribution       *units.Distribution
	SubcategoryDistribution *units.Distribution
	CategorizationStatus    string
}

// ComputeOrg projects work_unit_membership for one org (or one repo scope
// within it) from the theme/subcategory distributions already persisted in
// work_unit_investments. It ports backfill_memberships (backfill.py:176)
// verbatim in shape: rebuild components from work_graph_edges, read the
// latest investment row per resulting work_unit_id, project membership rows
// via units.BuildMembershipRecords for every matched unit, write ALL rows
// first, then publish the completion marker LAST (CHAOS-2433 protocol).
//
// repoIDs empty means org-wide (the daily/post-sync fanout's only shape
// today); non-empty means repo-scoped, which writes rows but does NOT
// publish the org-wide marker (CHAOS-2433 finding #2) -- see
// membershipScope.RepoIDs' IsOrgWide-equivalent branch below.
func (executor *MembershipExecutor) ComputeOrg(
	ctx context.Context, orgID string, repoIDs []string, now time.Time,
) (MembershipOutcome, error) {
	if executor == nil || executor.conn == nil {
		return MembershipOutcome{}, ErrMembershipUnavailable
	}

	edgeRows, err := executor.edges.FetchWorkGraphEdges(ctx, chquery.EdgeQueryOptions{
		OrganizationID: orgID,
		RepoIDs:        repoIDs,
		// IncludeHeuristic stays false (the zero value): CHAOS-2775 exclusion
		// is default-ON in the reference (exclude_heuristic: bool = True),
		// and this backfill never passes the override.
	})
	if err != nil {
		return MembershipOutcome{}, fmt.Errorf("fetch work_graph_edges: %w", err)
	}

	stats := &units.BuildStats{}
	// partitionHubs: false, the safe zero value (units.BuildComponents' own
	// doc comment) -- the native materializer has not cut over yet (Python
	// is still the live producer of work_unit_investments, CHAOS-4771/
	// CHAOS-4924), so this backfill must keep minting the SAME work_unit_ids
	// Python's still-live hub-deletion grouping produces. Flipping this
	// independently of WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED --
	// which no Go caller reads yet -- would mint membership rows keyed by
	// work_unit_ids the live investments table never created, the exact
	// cross-table corruption BuildComponents' doc warns about. Flip in
	// lockstep with every other consumer at CHAOS-4924 cutover, not before.
	components := units.BuildComponents(chquery.ComponentEdges(edgeRows), nil, false, stats)
	if len(components) == 0 {
		outcome := MembershipOutcome{
			OversizedComponents: stats.OversizedComponents,
			DroppedEdges:        stats.DroppedEdges,
			DroppedNodes:        stats.DroppedNodes,
		}
		if executor.observer != nil {
			executor.observer.ObserveMembershipRun(orgID, outcome)
		}
		return outcome, nil
	}

	// Map each current work_unit_id -> its node list. units.BuildComponents
	// already deduped each component's node list, so component.Nodes IS the
	// unit_nodes Python computes as list(dict.fromkeys(nodes)) -- see
	// unitNodesFor's doc comment.
	unitNodesByID := make(map[string][]units.NodeKey, len(components))
	unitOrder := make([]string, 0, len(components))
	for _, component := range components {
		nodes := unitNodesFor(component)
		uid := units.WorkUnitID(nodes)
		if _, exists := unitNodesByID[uid]; !exists {
			unitOrder = append(unitOrder, uid)
		}
		unitNodesByID[uid] = nodes
	}

	workUnitIDs := make([]string, 0, len(unitNodesByID))
	for uid := range unitNodesByID {
		workUnitIDs = append(workUnitIDs, uid)
	}
	distributions, err := executor.distributions.FetchLatestDistributions(ctx, orgID, workUnitIDs)
	if err != nil {
		return MembershipOutcome{}, fmt.Errorf("fetch latest distributions: %w", err)
	}

	// A single run_id for the entire backfill run -- CHAOS-2433 protocol.
	runID := uuid.NewString()
	computedAt := now

	var records []units.MembershipRecord
	matched := 0
	skipped := 0
	// Iterate unitOrder (component discovery order), not the map, so the
	// membership rows this run produces are in a DETERMINISTIC sequence run
	// over run -- a map range would reorder them on every call for no
	// semantic reason, which would be a needless divergence from the
	// reference's own for-loop-over-dict order.
	for _, uid := range unitOrder {
		nodes := unitNodesByID[uid]
		persisted, ok := distributions[uid]
		if !ok {
			// No persisted investment row for this component (edges churned
			// since the last LLM run). Skip -- the run_id protocol makes
			// these nodes invisible without tombstones.
			skipped++
			continue
		}
		matched++
		records = append(records, units.BuildMembershipRecords(
			units.MembershipInput{
				UnitNodes:            nodes,
				WorkUnitID:           uid,
				CategorizationStatus: persisted.CategorizationStatus,
				ComputedAt:           computedAt,
				OrgID:                orgID,
				RunID:                runID,
			},
			persisted.ThemeDistribution,
			persisted.SubcategoryDistribution,
		)...)
	}

	// Write ALL membership rows first, THEN the completion marker. A run
	// with rows but no marker is incomplete and invisible to readers.
	rowsWritten := 0
	if len(records) > 0 {
		rowsWritten, err = executor.writer.WriteMemberships(ctx, orgID, records)
		if err != nil {
			return MembershipOutcome{}, fmt.Errorf("write work_unit_membership: %w", err)
		}
	}

	// Resolved via nowOrRefuse (executor_clock.go), never a nil-safe
	// wallClock() accessor -- CHAOS-4954, same refuse-loud shape as
	// DORA/Capacity/Recommendations. Rows are already written above at this
	// point; a refusal here still surfaces as an error (no marker gets
	// published), matching the write-then-marker protocol's own
	// incomplete-run handling.
	markerCompletedAt, err := executor.nowOrRefuse()
	if err != nil {
		return MembershipOutcome{}, err
	}
	isOrgWide := len(repoIDs) == 0
	if isOrgWide {
		if err := executor.writer.WriteMembershipRun(ctx, MembershipRunRecord{
			OrgID: orgID, RunID: runID, CompletedAt: markerCompletedAt,
		}); err != nil {
			return MembershipOutcome{}, fmt.Errorf("write completion marker: %w", err)
		}
		// Retention is best-effort (CHAOS-2433 round-5): a prune failure must
		// not FAIL the projection -- the marker is already published and
		// correct, and the next run's prune is idempotent and will catch up.
		// "Best-effort" means the partition still succeeds, NOT that the
		// failure is silent (codex round 1, #2177, P2: it previously was --
		// a swallowed error left growth invisible until someone happened to
		// notice storage). Report it, then continue.
		if _, pruneErr := executor.writer.PruneMembershipRuns(ctx, orgID, membershipRetentionKeep); pruneErr != nil {
			if executor.logger != nil {
				executor.logger.Warn(
					"membership run retention prune failed; old generations will "+
						"accumulate until a later run succeeds -- the partition "+
						"itself is unaffected and still reports success",
					"org_id", orgID, "error", pruneErr,
				)
			}
			if executor.observer != nil {
				executor.observer.ObserveMembershipPruneFailed(orgID)
			}
		}
	} else {
		scopedRecords := make([]MembershipScopedRunRecord, 0, len(repoIDs))
		uniqueRepoIDs := sortedUniqueStrings(repoIDs)
		for _, repoID := range uniqueRepoIDs {
			if repoID == "" {
				continue
			}
			scopedRecords = append(scopedRecords, MembershipScopedRunRecord{
				OrgID: orgID, ScopeKind: "repo", ScopeID: repoID,
				RunID: runID, CompletedAt: markerCompletedAt,
			})
		}
		if err := executor.writer.WriteScopedMembershipRuns(ctx, scopedRecords); err != nil {
			return MembershipOutcome{}, fmt.Errorf("write scoped completion markers: %w", err)
		}
	}

	outcome := MembershipOutcome{
		Components:          len(components),
		Matched:             matched,
		Skipped:             skipped,
		MembershipRows:      rowsWritten,
		OversizedComponents: stats.OversizedComponents,
		DroppedEdges:        stats.DroppedEdges,
		DroppedNodes:        stats.DroppedNodes,
	}
	if executor.observer != nil {
		executor.observer.ObserveMembershipRun(orgID, outcome)
	}
	return outcome, nil
}

// sortedUniqueStrings de-duplicates and sorts, matching backfill.py's
// `sorted({repo_id for repo_id in config.repo_ids or []})` for the scoped
// marker list -- a set comprehension in the reference, so the WRITE order
// here is not load-bearing for correctness (each row is independent), only
// for determinism across runs.
func sortedUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, v := range values {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		unique = append(unique, v)
	}
	sort.Strings(unique)
	return unique
}

// membershipDistributionFetcher is the narrow capability ComputeOrg needs to
// read the latest persisted investment distributions, so a test can
// substitute a fake without a live ClickHouse connection -- same reasoning as
// chqueryEdgeReader.
type membershipDistributionFetcher interface {
	FetchLatestDistributions(ctx context.Context, orgID string, workUnitIDs []string) (map[string]membershipDistribution, error)
}

// chConnDistributionFetcher is the production membershipDistributionFetcher,
// wrapping a live ClickHouse connection.
type chConnDistributionFetcher struct {
	conn membershipWriterConn
}

func (fetcher chConnDistributionFetcher) FetchLatestDistributions(
	ctx context.Context, orgID string, workUnitIDs []string,
) (map[string]membershipDistribution, error) {
	return fetchLatestDistributions(ctx, fetcher.conn, orgID, workUnitIDs)
}

// fetchLatestDistributions ports _fetch_latest_distributions (backfill.py),
// returning only the unit ids that actually have a row.
//
// # WHY mapKeys/mapValues INSTEAD OF SCANNING THE Map COLUMN DIRECTLY
//
// theme_distribution_json/subcategory_distribution_json are
// Map(String, Float64) columns. units.Distribution preserves INSERTION
// ORDER (see membership.go's doc comment: LexicalArgmax is insertion-order
// dependent when a weight is NaN, CHAOS-4840) -- but clickhouse-go's Map
// column decodes into a genuine Go map via reflect.MakeMap
// (lib/column/map.go), and Go map iteration order is randomised by design.
// Scanning the Map column directly would therefore silently discard the
// exact property this whole port exists to preserve, on every read.
//
// mapKeys()/mapValues() on the SAME underlying Map value read the physical
// key/value arrays ClickHouse stores a Map as (a Map is backed by
// Array(Tuple(K, V))) and decode into ordinary Go slices, which ARE ordered.
// Evaluated over the same expression, the two arrays are guaranteed
// positionally aligned. A CTE materializes the argMax'd distribution once so
// mapKeys/mapValues read a plain column rather than re-aggregating (avoiding
// the ILLEGAL_AGGREGATION alias-shadow trap documented on
// LATEST_WORK_UNIT_INVESTMENTS_CTE and this package's own recommendations
// loader).
func fetchLatestDistributions(
	ctx context.Context, conn membershipWriterConn, orgID string, workUnitIDs []string,
) (map[string]membershipDistribution, error) {
	if len(workUnitIDs) == 0 {
		return map[string]membershipDistribution{}, nil
	}

	rows, err := conn.Query(ctx, `
		WITH latest AS (
			SELECT
				work_unit_id,
				argMax(theme_distribution_json, computed_at) AS theme_distribution,
				argMax(subcategory_distribution_json, computed_at) AS subcategory_distribution,
				argMax(categorization_status, computed_at) AS categorization_status
			FROM work_unit_investments
			WHERE org_id = {org_id:String}
			  AND work_unit_id IN {work_unit_ids:Array(String)}
			GROUP BY org_id, work_unit_id
		)
		SELECT
			work_unit_id,
			mapKeys(theme_distribution) AS theme_categories,
			mapValues(theme_distribution) AS theme_weights,
			mapKeys(subcategory_distribution) AS subcategory_categories,
			mapValues(subcategory_distribution) AS subcategory_weights,
			categorization_status
		FROM latest
	`,
		clickhouse.Named("org_id", orgID),
		clickhouse.Named("work_unit_ids", workUnitIDs),
	)
	if err != nil {
		return nil, fmt.Errorf("query work_unit_investments: %w", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]membershipDistribution, len(workUnitIDs))
	for rows.Next() {
		var (
			workUnitID            string
			themeCategories       []string
			themeWeights          []float64
			subcategoryCategories []string
			subcategoryWeights    []float64
			categorizationStatus  string
		)
		if err := rows.Scan(
			&workUnitID, &themeCategories, &themeWeights,
			&subcategoryCategories, &subcategoryWeights, &categorizationStatus,
		); err != nil {
			return nil, fmt.Errorf("scan work_unit_investments row: %w", err)
		}
		if workUnitID == "" {
			continue
		}
		result[workUnitID] = membershipDistribution{
			ThemeDistribution:       distributionFromPairs(themeCategories, themeWeights),
			SubcategoryDistribution: distributionFromPairs(subcategoryCategories, subcategoryWeights),
			CategorizationStatus:    categorizationStatus,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_unit_investments rows: %w", err)
	}
	return result, nil
}

// distributionFromPairs builds a units.Distribution from two same-length,
// positionally-aligned slices (mapKeys/mapValues over the same Map value),
// preserving their order.
func distributionFromPairs(categories []string, weights []float64) *units.Distribution {
	pairs := make([]units.CategoryWeight, 0, len(categories))
	for i, category := range categories {
		if i >= len(weights) {
			break
		}
		pairs = append(pairs, units.CategoryWeight{Category: category, Weight: weights[i]})
	}
	return units.NewDistribution(pairs...)
}
