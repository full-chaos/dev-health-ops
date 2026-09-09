package workgraph

// CHAOS-3969: ports
// dev_health_ops.api.graphql.resolvers.team_attribution.resolve_work_unit_team_attributions
// (ops/src/dev_health_ops/api/graphql/resolvers/team_attribution.py) -- the
// owning team per work UNIT (investment cluster), collapsed from its member
// work-item attributions by source precedence.
//
// Reuses this package's own _membership_run_scope.py port
// (membershipRunSubquery/legacyNodeMaxJoin/runScopePredicate, defined in
// workgraph.go) rather than re-deriving the "latest complete membership
// run" protocol -- CHAOS-2608's whole reason for pulling those constants
// out of work_graph.py into a shared module in the first place (see
// _membership_run_scope.py's own doc comment: "so the work-graph
// annotation reader and the work-unit team-attribution reader cannot
// drift"). This reader never uses the scoped-partial-run branch
// (membershipRunSubquery's second return path): team_attribution.py's own
// query only ever opens `WITH latest_run AS ({LATEST_COMPLETE_RUN_SUBQUERY})`,
// never the repo-scoped variant, so this port calls
// newFilterScope(nil, nil) -- filters=nil makes usesScopedPartial() false
// unconditionally, which is what forces membershipRunSubquery back to the
// plain latestCompleteRunSubquery every time, matching the Python source
// exactly.
//
// Sibling WorkItemTeamAttributions (CHAOS-2600's per-work-item reader,
// team_attribution.py's resolve_work_item_team_attributions) is
// DELIBERATELY left unported by this ticket -- see schema.resolvers.go's
// WorkItemTeamAttributions doc comment for why.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// workUnitTeamAttributionsMaxRows mirrors team_attribution.py's _MAX_ROWS.
// Parity of record: this port does not reshape the read itself (the LIMIT
// stays numerically identical to Python's), it only adds the truncation
// SIGNAL Python's own cap has never had -- see
// recordWorkUnitTeamAttributionsTruncation's doc comment, which is
// CHAOS-3969's actual complaint (a tenant with >5000 matching work units
// silently got an incomplete list, with no way for any caller, on either
// plane, to know).
const workUnitTeamAttributionsMaxRows = 5000

// teamAttributionSourceRankSQL mirrors team_attribution.py's
// _SOURCE_RANK_SQL verbatim -- the source-precedence order the inner
// argMin collapse orders by (lower rank wins ties, matching this ticket's
// model.TeamAttributionSource enum's 9 members exactly -- see
// mapTeamAttributionSource below).
const teamAttributionSourceRankSQL = `multiIf(` +
	`a.source='native_team',0,` +
	`a.source='issue_project',1,` +
	`a.source='project_ownership',2,` +
	`a.source='repo_ownership',3,` +
	`a.source='assignee_membership',4,` +
	`a.source='linked_issue',5,` +
	`a.source='author_membership',6,` +
	`a.source='manual_fallback',7,` +
	`a.source='unassigned',8,` +
	`9)`

// ResolveWorkUnitTeamAttributions mirrors team_attribution.py's
// resolve_work_unit_team_attributions. orgID is the caller's
// ALREADY-AUTHORIZED org id (schema.resolvers.go's WorkUnitTeamAttributions
// resolves it via authctx before calling this -- same "authorized org
// always wins" convention as every other workgraph.Resolve* entrypoint;
// never a client-supplied GraphQL argument).
func ResolveWorkUnitTeamAttributions(ctx context.Context, client QueryClient, orgID string, workUnitIDs []string, teamID *string) ([]model.WorkUnitTeamAttribution, error) {
	return resolveWorkUnitTeamAttributions(ctx, client, orgID, workUnitIDs, teamID, workUnitTeamAttributionsMaxRows)
}

// resolveWorkUnitTeamAttributions takes an explicit limit so a test can pin
// the truncation signal (recordWorkUnitTeamAttributionsTruncation) without
// seeding workUnitTeamAttributionsMaxRows (5000) fixture rows -- see
// teamattribution_integration_test.go's
// TestResolveWorkUnitTeamAttributions_TruncationSignalFiresAtLimit.
func resolveWorkUnitTeamAttributions(ctx context.Context, client QueryClient, orgID string, workUnitIDs []string, teamID *string, limit int) ([]model.WorkUnitTeamAttribution, error) {
	scope := newFilterScope(nil, nil)

	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}

	var workUnitFilter string
	if len(workUnitIDs) > 0 {
		workUnitFilter = "AND m.work_unit_id IN {work_unit_ids:Array(String)}"
		bindings = append(bindings, clickhouse.Binding{Name: "work_unit_ids", Value: workUnitIDs})
	}

	var teamFilter string
	if teamID != nil && *teamID != "" {
		teamFilter = "WHERE team_id = {team_id:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: *teamID})
	}

	// INNER JOIN (%s) AS latest_run, NOT a leading `WITH latest_run AS
	// (...)` CTE: this codebase's clickhouse.Client rejects any statement
	// whose FIRST TOKEN is not literally "SELECT"
	// (dev-health-go/clickhouse/client.go's validateReadOnlyStatement) --
	// same inlining membership.go's batchResolveMembership and
	// scope.go's themeMembershipExistsClause already use for this exact
	// latest-run subquery, reused here rather than re-derived.
	//
	// The outer SELECT wraps team_id/team_name in ifNull(..., '') --
	// sidestepping Nullable(String) scanning entirely, same convention
	// edges.go's fetchDedupedEdgeRows uses for repo_id/provider (see that
	// file's doc comment) -- rather than a **string/sql.NullString scan
	// target. rowToWorkUnitTeamAttribution below treats an empty string
	// exactly as Python's own `if row.get("team_id")` falsy check does:
	// both NULL and '' become a nil *string on the wire.
	query := fmt.Sprintf(`
        SELECT work_unit_id, ifNull(team_id, '') AS team_id, ifNull(team_name, '') AS team_name, source, confidence, member_count
        FROM (
            SELECT work_unit_id,
                argMin(team_id, sort_key) AS team_id, argMin(team_name, sort_key) AS team_name,
                argMin(source, sort_key) AS source, argMin(confidence, sort_key) AS confidence,
                argMin(member_count, sort_key) AS member_count
            FROM (
                SELECT work_unit_id, team_id,
                    argMin(team_name, src_rank) AS team_name, argMin(source, src_rank) AS source,
                    argMin(confidence, src_rank) AS confidence, count() AS member_count,
                    (min(src_rank), -toInt64(count()), team_id) AS sort_key
                FROM (
                    SELECT m.work_unit_id AS work_unit_id, a.team_id AS team_id, a.team_name AS team_name,
                        a.source AS source, a.confidence AS confidence, %s AS src_rank
                    FROM (
                        SELECT DISTINCT m.work_unit_id AS work_unit_id, m.node_id AS node_id
                        FROM work_unit_membership AS m
                        INNER JOIN (%s) AS latest_run ON 1 = 1
                        %s
                        WHERE m.org_id = {org_id:String} %s
                          AND latest_run.latest_run_id != ''
                          AND (%s)
                    ) AS m
                    INNER JOIN (
                        SELECT work_item_id, team_id, team_name, source, confidence
                        FROM work_item_team_attributions FINAL
                        WHERE org_id = {org_id:String} AND is_primary = 1
                          AND (work_item_id, computed_at) IN (
                              SELECT work_item_id, max(computed_at) FROM work_item_team_attributions
                              WHERE org_id = {org_id:String} GROUP BY work_item_id
                          )
                    ) AS a
                    ON m.node_id = a.work_item_id
                )
                GROUP BY work_unit_id, team_id
            )
            GROUP BY work_unit_id
        )
        %s
        ORDER BY member_count DESC, work_unit_id
        LIMIT {limit:UInt64}
    `, teamAttributionSourceRankSQL, membershipRunSubquery(scope), legacyNodeMaxJoin, workUnitFilter, runScopePredicate, teamFilter)

	bindings = append(bindings, clickhouse.Binding{Name: "limit", Value: uint64(limit)})

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("workgraph: resolve work unit team attributions: %w", err)
	}
	defer rows.Close()

	var results []model.WorkUnitTeamAttribution
	for rows.Next() {
		var workUnitID, teamIDCol, teamNameCol, source, confidence string
		var memberCount uint64
		if scanErr := rows.Scan(&workUnitID, &teamIDCol, &teamNameCol, &source, &confidence, &memberCount); scanErr != nil {
			return nil, fmt.Errorf("workgraph: resolve work unit team attributions scan: %w", scanErr)
		}
		results = append(results, rowToWorkUnitTeamAttribution(workUnitID, teamIDCol, teamNameCol, source, confidence, memberCount))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: resolve work unit team attributions rows: %w", err)
	}

	if len(results) == limit {
		recordWorkUnitTeamAttributionsTruncation(ctx, orgID, limit)
	}

	return results, nil
}

// rowToWorkUnitTeamAttribution mirrors team_attribution.py's
// _row_to_unit_attribution.
func rowToWorkUnitTeamAttribution(workUnitID, teamIDCol, teamNameCol, sourceRaw, confidenceRaw string, memberCount uint64) model.WorkUnitTeamAttribution {
	var teamID, teamName *string
	if teamIDCol != "" {
		v := teamIDCol
		teamID = &v
	}
	if teamNameCol != "" {
		v := teamNameCol
		teamName = &v
	}

	source := mapTeamAttributionSource(sourceRaw)
	confidence := mapTeamAttributionConfidence(confidenceRaw)

	// target = team_name or team_id or "no team" (team_attribution.py).
	target := "no team"
	switch {
	case teamName != nil:
		target = *teamName
	case teamID != nil:
		target = *teamID
	}

	// source.value in Python is the enum's lowercase wire value (e.g.
	// "native_team"); model.TeamAttributionSource's Go string value is the
	// uppercase GraphQL enum literal (e.g. "NATIVE_TEAM") -- lowering it
	// reproduces the exact same evidence text Python emits.
	evidence := fmt.Sprintf("%d member work item(s) attributed to %s via %s", memberCount, target, strings.ToLower(string(source)))

	return model.WorkUnitTeamAttribution{
		WorkUnitID:  workUnitID,
		TeamID:      teamID,
		TeamName:    teamName,
		Source:      source,
		Confidence:  confidence,
		IsPrimary:   true,
		MemberCount: int(memberCount),
		Evidence:    evidence,
	}
}

// mapTeamAttributionSource mirrors team_attribution.py's _map_source: a
// lowercase string mapped to the enum, unrecognized -> the floor value
// (UNASSIGNED) rather than an error -- deliberately NOT mapNodeType's
// validate-and-error contract (see that function's doc comment for why
// THAT port errors instead): Python's own _map_source never raises here,
// it falls back, so this port must not invent stricter behavior than the
// function it is porting.
func mapTeamAttributionSource(raw string) model.TeamAttributionSource {
	switch strings.ToLower(raw) {
	case "native_team":
		return model.TeamAttributionSourceNativeTeam
	case "issue_project":
		return model.TeamAttributionSourceIssueProject
	case "project_ownership":
		return model.TeamAttributionSourceProjectOwnership
	case "repo_ownership":
		return model.TeamAttributionSourceRepoOwnership
	case "assignee_membership":
		return model.TeamAttributionSourceAssigneeMembership
	case "linked_issue":
		return model.TeamAttributionSourceLinkedIssue
	case "author_membership":
		return model.TeamAttributionSourceAuthorMembership
	case "manual_fallback":
		return model.TeamAttributionSourceManualFallback
	default:
		return model.TeamAttributionSourceUnassigned
	}
}

// mapTeamAttributionConfidence mirrors team_attribution.py's
// _map_confidence: same unrecognized-> floor-value (NONE) fallback
// contract as mapTeamAttributionSource above.
func mapTeamAttributionConfidence(raw string) model.TeamAttributionConfidence {
	switch strings.ToLower(raw) {
	case "high":
		return model.TeamAttributionConfidenceHigh
	case "medium":
		return model.TeamAttributionConfidenceMedium
	case "low":
		return model.TeamAttributionConfidenceLow
	case "manual":
		return model.TeamAttributionConfidenceManual
	default:
		return model.TeamAttributionConfidenceNone
	}
}

// recordWorkUnitTeamAttributionsTruncation is CHAOS-3969's actual
// deliverable: team_attribution.py's `_MAX_ROWS = 5000` / `LIMIT
// %(limit)s` cap has NO signal today, on either plane -- a tenant with
// more than 5000 matching work units silently gets an incomplete list and
// nothing tells any caller so. This repo's own standing rule (root
// AGENTS.md: "new decision-shape logic ships its telemetry in the same
// PR") applies squarely here.
//
// Var, not a plain func -- same injectable-observable pattern
// membership_telemetry.go's recordMembershipRowsPerEndpoint already uses
// in this package, for the identical reason: a test needs to observe that
// this fired at all, not merely infer it from the returned slice's length
// (which cannot distinguish "exactly `limit` real rows, no more exist"
// from "truncated").
//
// ENGINEERING CALL, documented per this ticket's brief: a follow-up
// `count()` query over the identical WHERE clauses (dropping ORDER
// BY/LIMIT) would tell the caller the TRUE total instead of "likely
// truncated" -- but it would double this read's ClickHouse cost on EVERY
// request, not only the rare one actually near the cap, because there is
// no way to know in advance whether the primary read is about to hit the
// boundary without already having paid for the count. This port stays
// parity-of-record on the READ itself (LIMIT is unchanged) and pays the
// extra cost only in the one case that matters: when the primary result
// comes back exactly `limit` rows long, which is the only condition
// ClickHouse's LIMIT clause surfaces to a caller for free. A rare false
// positive (an org whose TRUE count happens to equal `limit` exactly)
// logs a warning for a request that was not actually truncated -- an
// acceptable rate for an operator-facing signal that would otherwise not
// exist at all; a false NEGATIVE is impossible (returning fewer than
// `limit` rows can only happen when the read was not truncated).
//
// Substrate: slog.WarnContext, matching this binary's own established
// degraded/near-limit signal convention (throughputforecast/resolve.go,
// capacityforecast/capacityforecast.go, analytics/sankeycoverage.go,
// analytics/investmentmembershiptelemetry.go, routeswitch/telemetry.go) --
// not this package's membership_telemetry.go OTel-histogram shape, which
// exists for a DISTRIBUTION signal (rows-returned-per-endpoint ratio
// trending over many requests); truncation is a per-request boolean-ish
// event an operator needs to grep for and act on, which is exactly what
// this repo's other near-limit/degraded call sites already use
// slog.WarnContext for.
var recordWorkUnitTeamAttributionsTruncation = defaultRecordWorkUnitTeamAttributionsTruncation

func defaultRecordWorkUnitTeamAttributionsTruncation(ctx context.Context, orgID string, limit int) {
	slog.WarnContext(ctx, "query_api.workgraph.work_unit_team_attributions.truncated",
		"org_id", orgID,
		"limit", limit,
		"reason", "returned row count equals the cap; more matching work units may exist (CHAOS-3969)",
	)
}
