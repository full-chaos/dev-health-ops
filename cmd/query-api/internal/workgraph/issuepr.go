package workgraph

import (
	"context"
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// issuePRLinkRow is one work_graph_issue_pr row, in the shape
// PullRequestIssueLink (models_gen.go) reads.
type issuePRLinkRow struct {
	workItemID string
	confidence float64
	provenance string
	evidence   string
}

// investmentMaterializeNativeEnabledEnv is the SAME flag name
// BuildComponents's own doc comment (units/components.go) names as gating
// the native materializer cutover -- read directly here, same convention as
// operationalOrderingContractEnv (displaynames.go): a fresh Go process reads
// its own env, nothing Python parsed is inherited.
const investmentMaterializeNativeEnabledEnv = "WORKGRAPH_INVESTMENT_MATERIALIZE_NATIVE_ENABLED"

// investmentMaterializeNativeEnabled is "1"-truthy, matching
// operationalOrderingIsCurrent's convention in this same package. Unset or
// anything else means the flag is off -- the safe default, since the fast
// path below is only correct once migration 084's version_rank column has
// been applied everywhere this reader might run against.
func investmentMaterializeNativeEnabled() bool {
	return os.Getenv(investmentMaterializeNativeEnabledEnv) == "1"
}

// fetchLinkedIssueRows is CHAOS-4924's fast-path reader. It reads the same
// rows and returns them in the same shape as pr.py:179-208's
// _fetch_linked_issues, and MUST match it exactly when investmentMaterializeNativeEnabled()
// is false (see issuepr_integration_test.go's differential proof) -- that
// path exists so this function has a fixed, independently-defined oracle to
// diverge from, not so both paths run in production simultaneously.
//
// UNWIRED as of this PR, deliberately: schema.resolvers.go's Pr resolver is
// still `panic("not implemented")`, so nothing calls this yet. Wiring it in
// is its own PR, tracked as blocked by this one -- see this PR's own ticket
// for the follow-up. Reader and its proof only here, no writer changes.
//
// When the flag is on, work_graph_issue_pr FINAL (an expensive whole-part
// merge on every query) is replaced by a plain argMax(col, version_rank)
// collapse -- correct without FINAL because migration 084 made version_rank
// a MATERIALIZED ReplacingMergeTree version column: ClickHouse's own
// background merges already keep the highest-version_rank row per identity,
// so a live GROUP BY collapse over any not-yet-merged duplicates picks the
// same winner FINAL's forced merge would, without paying for the merge on
// every read. See migration 084's header for why provenance (not
// last_synced alone) has to decide the winner, and why reader-side
// precedence was withdrawn in favor of the version column.
func fetchLinkedIssueRows(
	ctx context.Context, client QueryClient, orgID, repoID string, prNumber int,
) ([]issuePRLinkRow, error) {
	if investmentMaterializeNativeEnabled() {
		return fetchLinkedIssueRowsFastPath(ctx, client, orgID, repoID, prNumber)
	}
	return fetchLinkedIssueRowsFinal(ctx, client, orgID, repoID, prNumber)
}

// fetchLinkedIssueRowsFinal is the bit-exact port of pr.py:179-208's
// _fetch_linked_issues -- the oracle fetchLinkedIssueRowsFastPath must
// match. Never change this query to track the fast path; if the two drift
// intentionally, that is exactly what the differential test exists to
// force a conscious decision about.
//
// toFloat64(argMax(confidence, ...)): work_graph_issue_pr.confidence is
// Float32 (014_work_graph.sql) and argMax() preserves its input's
// ClickHouse type -- the native Go driver refuses to scan a Float32 result
// column into *float64 outright, the SAME trap edges.go's
// fetchDedupedEdgeRows doc comment already documents for work_graph_edges'
// identically-typed confidence column. Caught here by this PR's own golden
// test the first time this query actually ran against real ClickHouse, not
// by inspection -- a fake-based unit test cannot catch a driver-level type
// mismatch. Python's driver has no such restriction, so this cast is a
// Go-side necessity, not a value-changing divergence from Python.
func fetchLinkedIssueRowsFinal(
	ctx context.Context, client QueryClient, orgID, repoID string, prNumber int,
) ([]issuePRLinkRow, error) {
	const query = `
        SELECT
            work_item_id,
            toFloat64(argMax(confidence, last_synced)) AS confidence,
            argMax(provenance, last_synced) AS provenance,
            argMax(evidence, last_synced) AS evidence
        FROM work_graph_issue_pr FINAL
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND pr_number = {pr_number:UInt32}
        GROUP BY work_item_id
        ORDER BY confidence DESC, work_item_id ASC
        LIMIT 500
    `
	return scanIssuePRLinkRows(ctx, client, query, orgID, repoID, prNumber)
}

// fetchLinkedIssueRowsFastPath is CHAOS-4924's native fast path: same
// filter, grouping and ordering as fetchLinkedIssueRowsFinal, but collapsed
// by argMax(col, version_rank) instead of a forced FINAL merge on
// last_synced. See fetchLinkedIssueRows's doc comment for why this is
// correct once migration 084 has run, and fetchLinkedIssueRowsFinal's doc
// comment for why confidence is wrapped in toFloat64.
func fetchLinkedIssueRowsFastPath(
	ctx context.Context, client QueryClient, orgID, repoID string, prNumber int,
) ([]issuePRLinkRow, error) {
	const query = `
        SELECT
            work_item_id,
            toFloat64(argMax(confidence, version_rank)) AS confidence,
            argMax(provenance, version_rank) AS provenance,
            argMax(evidence, version_rank) AS evidence
        FROM work_graph_issue_pr
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND pr_number = {pr_number:UInt32}
        GROUP BY work_item_id
        ORDER BY confidence DESC, work_item_id ASC
        LIMIT 500
    `
	return scanIssuePRLinkRows(ctx, client, query, orgID, repoID, prNumber)
}

func scanIssuePRLinkRows(
	ctx context.Context, client QueryClient, query, orgID, repoID string, prNumber int,
) ([]issuePRLinkRow, error) {
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: repoID},
		{Name: "pr_number", Value: prNumber},
	}

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []issuePRLinkRow
	for rows.Next() {
		var r issuePRLinkRow
		if scanErr := rows.Scan(&r.workItemID, &r.confidence, &r.provenance, &r.evidence); scanErr != nil {
			return nil, fmt.Errorf("workgraph: issue-pr links scan: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: issue-pr links rows: %w", err)
	}
	return out, nil
}
