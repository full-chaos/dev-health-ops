package workgraph

// CHAOS-7066: ports
// dev_health_ops.api.graphql.resolvers.team_attribution.resolve_work_item_team_attributions
// (ops/src/dev_health_ops/api/graphql/resolvers/team_attribution.py) -- the
// per-WORK-ITEM team-attribution provenance CHAOS-2600 originally shipped.
// teamattribution.go's doc comment calls this sibling unported; it is
// ported here.
//
// Distinct from, and never a duplicate of, ResolveWorkUnitTeamAttributions
// above: that function COLLAPSES a work unit's member work items' primary
// attributions into ONE row per work unit (via work_unit_membership,
// argMin over a source-precedence rank). This function reads
// work_item_team_attributions directly, one row PER (work_item_id,
// team_id, source) candidate the precedence resolver persisted --
// provenance for a single item, not an aggregate over a cluster. The two
// fields answer different questions and both stay (CHAOS-2600 realigned
// work UNIT relationships, it did not retire the per-item reader; the
// per-item table is what both functions read, at two different
// granularities).
//
// Reuses this file's siblings' enum mappers (mapTeamAttributionSource,
// mapTeamAttributionConfidence) verbatim -- same source-of-truth
// enum semantics, same unrecognized-value-degrades-to-floor contract,
// ported once and shared rather than re-derived.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// ResolveWorkItemTeamAttributions mirrors team_attribution.py's
// resolve_work_item_team_attributions. orgID is the caller's
// ALREADY-AUTHORIZED org id -- same "authorized org always wins"
// convention as ResolveWorkUnitTeamAttributions and every other
// workgraph.Resolve* entrypoint; never a client-supplied GraphQL
// argument (see schema.resolvers.go's WorkItemTeamAttributions).
func ResolveWorkItemTeamAttributions(ctx context.Context, client QueryClient, orgID string, workItemIDs []string, teamID *string) ([]model.WorkItemTeamAttribution, error) {
	return resolveWorkItemTeamAttributions(ctx, client, orgID, workItemIDs, teamID, workUnitTeamAttributionsMaxRows)
}

// resolveWorkItemTeamAttributions takes an explicit limit for the same
// testability reason resolveWorkUnitTeamAttributions does (see that
// function's doc comment) -- a test can pin the truncation signal
// without seeding 5000 fixture rows.
//
// _MAX_ROWS (team_attribution.py:27) is ONE module-level constant Python
// shares between both resolve_* functions; workUnitTeamAttributionsMaxRows
// is reused here rather than declaring a second Go constant with the
// same value, for the same reason.
//
// TRUNC-1's fix (teamattribution.go's own doc comment) is reused here
// too, not merely its VALUE: Python's resolve_work_item_team_attributions
// sends a plain `LIMIT %(limit)s` with no truncation signal at all --
// the exact gap CHAOS-3969 closed for its sibling, and root AGENTS.md's
// "new decision-shape logic ships its telemetry in the same PR" applies
// identically to this one. The probe-row mechanism (LIMIT limit+1, fire
// only when the probe genuinely comes back) is copied rather than
// reintroducing the false-positive-at-exactly-limit bug TRUNC-1 already
// fixed once.
func resolveWorkItemTeamAttributions(ctx context.Context, client QueryClient, orgID string, workItemIDs []string, teamID *string, limit int) ([]model.WorkItemTeamAttribution, error) {
	// base_where (Python) = snapshot_where here: org + optional
	// work_item_ids, never team_id -- the snapshot subquery scopes to
	// every candidate team for the item, so a re-org that moved the item
	// to a different team cannot be masked by an old same-team row
	// (team_attribution.py's own comment on this exact point).
	snapshotWhere := []string{"org_id = {org_id:String}"}
	outerWhere := []string{"org_id = {org_id:String}"}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}

	if len(workItemIDs) > 0 {
		snapshotWhere = append(snapshotWhere, "work_item_id IN {work_item_ids:Array(String)}")
		outerWhere = append(outerWhere, "work_item_id IN {work_item_ids:Array(String)}")
		bindings = append(bindings, clickhouse.Binding{Name: "work_item_ids", Value: workItemIDs})
	}
	if teamID != nil && *teamID != "" {
		outerWhere = append(outerWhere, "team_id = {team_id:String}")
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: *teamID})
	}

	// ifNull(team_id/team_name, '') sidesteps Nullable(String) scanning,
	// same convention resolveWorkUnitTeamAttributions and edges.go's
	// fetchDedupedEdgeRows already use -- rowToWorkItemTeamAttribution
	// below treats an empty string as nil, matching Python's own
	// `if row.get("team_id")` falsy check.
	query := fmt.Sprintf(`
        SELECT work_item_id, provider, ifNull(team_id, '') AS team_id, ifNull(team_name, '') AS team_name,
            source, confidence, is_primary, evidence
        FROM work_item_team_attributions FINAL
        WHERE %s
          AND (work_item_id, computed_at) IN (
              SELECT work_item_id, max(computed_at)
              FROM work_item_team_attributions
              WHERE %s
              GROUP BY work_item_id
          )
        ORDER BY work_item_id, is_primary = 1 DESC, is_primary DESC, source
        LIMIT {limit:UInt64}
    `, strings.Join(outerWhere, " AND "), strings.Join(snapshotWhere, " AND "))

	probeLimit := limit + 1
	bindings = append(bindings, clickhouse.Binding{Name: "limit", Value: uint64(probeLimit)})

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("workgraph: resolve work item team attributions: %w", err)
	}
	defer rows.Close()

	var rawResults []model.WorkItemTeamAttribution
	for rows.Next() {
		var workItemID, provider, teamIDCol, teamNameCol, source, confidence, evidence string
		var isPrimary uint8
		if scanErr := rows.Scan(&workItemID, &provider, &teamIDCol, &teamNameCol, &source, &confidence, &isPrimary, &evidence); scanErr != nil {
			return nil, fmt.Errorf("workgraph: resolve work item team attributions scan: %w", scanErr)
		}
		rawResults = append(rawResults, rowToWorkItemTeamAttribution(workItemID, provider, teamIDCol, teamNameCol, source, confidence, isPrimary, evidence))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: resolve work item team attributions rows: %w", err)
	}

	genuinelyTruncated := len(rawResults) > limit
	results := rawResults
	if genuinelyTruncated {
		results = rawResults[:limit]
	}
	if genuinelyTruncated {
		recordWorkItemTeamAttributionsTruncation(ctx, orgID, limit)
	}
	if results == nil {
		results = []model.WorkItemTeamAttribution{}
	}
	return results, nil
}

// rowToWorkItemTeamAttribution mirrors team_attribution.py's
// _row_to_attribution exactly.
func rowToWorkItemTeamAttribution(workItemID, provider, teamIDCol, teamNameCol, sourceRaw, confidenceRaw string, isPrimary uint8, evidence string) model.WorkItemTeamAttribution {
	var teamID, teamName *string
	if teamIDCol != "" {
		v := teamIDCol
		teamID = &v
	}
	if teamNameCol != "" {
		v := teamNameCol
		teamName = &v
	}
	return model.WorkItemTeamAttribution{
		WorkItemID: workItemID,
		Provider:   provider,
		TeamID:     teamID,
		TeamName:   teamName,
		Source:     mapTeamAttributionSource(sourceRaw),
		Confidence: mapTeamAttributionConfidence(confidenceRaw),
		IsPrimary:  isPrimary == 1,
		Evidence:   evidence,
	}
}

// recordWorkItemTeamAttributionsTruncation reuses
// workUnitTeamAttributionsTruncationCounter (teamattribution.go), exactly
// as that counter's own doc comment anticipated: "a future
// truncation-prone read (e.g. a later WorkItemTeamAttributions port)
// adds itself as a new family/op label combination on this SAME series
// instead of minting a second ...truncation_total counter". Var, not a
// plain func, for the same test-observability reason
// recordWorkUnitTeamAttributionsTruncation is one.
var recordWorkItemTeamAttributionsTruncation = defaultRecordWorkItemTeamAttributionsTruncation

func defaultRecordWorkItemTeamAttributionsTruncation(ctx context.Context, orgID string, limit int) {
	slog.WarnContext(ctx, "query_api.workgraph.work_item_team_attributions.truncated",
		"org_id", orgID,
		"limit", limit,
		"reason", "the limit+1 probe row was returned; more matching attribution rows exist beyond the cap",
	)
	incrementWorkItemTeamAttributionsTruncationCounter(ctx)
}

// incrementWorkItemTeamAttributionsTruncationCounter is a package var,
// not a plain call inline, for the same injectable-observable reason
// incrementWorkUnitTeamAttributionsTruncationCounter (teamattribution.go)
// is one: a test needs to prove
// defaultRecordWorkItemTeamAttributionsTruncation increments the counter
// specifically, independent of resolveWorkItemTeamAttributions-level
// tests that swap the OUTER recordWorkItemTeamAttributionsTruncation var
// entirely. This is its OWN seam, not a reuse of the sibling's: that
// one's own body hardcodes op="work_unit_team_attributions", which would
// mislabel every call from this file if reused directly -- both write to
// the SAME underlying workUnitTeamAttributionsTruncationCounter series
// (that counter's own doc comment anticipates exactly this: "a future
// truncation-prone read ... adds itself as a new family/op label
// combination"), just via a second, independently-swappable seam
// carrying this file's own op label.
var incrementWorkItemTeamAttributionsTruncationCounter = defaultIncrementWorkItemTeamAttributionsTruncationCounter

func defaultIncrementWorkItemTeamAttributionsTruncationCounter(ctx context.Context) {
	workUnitTeamAttributionsTruncationCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("family", "team_attribution"),
		attribute.String("op", "work_item_team_attributions"),
	))
}
