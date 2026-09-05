package daily

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiworkflow"
)

// AIWorkflowExecutor is the NATIVE implementation of the ai_workflow
// metrics.daily family (CHAOS-4280 part B / CHAOS-4286 part B). It ports
// job_daily.py:301-367 (PR + issue-linkage load) and
// extract_ai_workflow_from_pull_requests (work_graph/extractors/ai_workflow.py),
// the OTHER half of _extract_ai_workflow_for_day from work_graph_edges
// (#2263/#2273), which already ports extract_review_deployment_incident_edges.
// The two families share NO input tables.
type AIWorkflowExecutor struct {
	conn        driver.Conn
	nowUTC      func() time.Time
	jobProvider string
}

var errAIWorkflowUnavailable = fmt.Errorf("ai_workflow native executor unavailable")

// NewAIWorkflowExecutor fails closed on a nil connection, matching every
// other native family.
func NewAIWorkflowExecutor(conn driver.Conn, jobProvider string) (*AIWorkflowExecutor, error) {
	if conn == nil {
		return nil, errAIWorkflowUnavailable
	}
	if jobProvider == "" {
		jobProvider = "auto"
	}
	return &AIWorkflowExecutor{
		conn:        conn,
		nowUTC:      func() time.Time { return time.Now().UTC() },
		jobProvider: jobProvider,
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *AIWorkflowExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errAIWorkflowUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	orgUUID, err := uuid.Parse(run.OrganizationID)
	if err != nil {
		// Python returns six empty lists when org_id is not a UUID
		// (job_daily.py:280-288, CHAOS-2187) rather than fabricating
		// attribution. The caller has already validated OrganizationID is
		// non-empty above; reaching an unparseable value here is a
		// precondition failure, not a routine skip.
		return 0, fmt.Errorf("%w: ai_workflow org_id %q: %v", ErrInvalidState, run.OrganizationID, err)
	}

	targetDay := run.TargetDay.UTC()
	dayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.Add(24 * time.Hour)
	computedAt := executor.nowUTC()

	prs, err := LoadAIWorkflowPullRequests(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}

	prNumbers := make([]int64, 0, len(prs))
	for _, pr := range prs {
		prNumbers = append(prNumbers, pr.Number)
	}
	links, linkErr := LoadAIWorkflowIssuePRLinks(ctx, executor.conn, run.OrganizationID, repoIDs, prNumbers)
	if linkErr != nil {
		// Python logs a warning and continues with issue_ids_by_pr={} on any
		// row-hygiene issue, never on a query failure -- job_daily.py has no
		// try/except around this specific query, so a real query failure
		// there is NOT swallowed, it propagates. Matching that: a linkage
		// query failure here is NOT swallowed either. It is, however, worth
		// a structured log distinct from the propagated error, since an
		// operator scanning logs for "how many PRs lost issue linkage" needs
		// a signal that survives even when the caller only surfaces a bare
		// error string up the stack.
		slog.Error("ai_workflow issue-pr linkage query failed",
			"org_id", run.OrganizationID, "partition_id", partition.ID,
			"target_day", targetDay.Format("2006-01-02"), "error", linkErr)
		return 0, linkErr
	}
	issueIDsByPR := make(map[string][]string)
	for _, link := range links {
		prID := link.RepoID.String() + ":" + strconv.FormatInt(link.PRNumber, 10)
		issueIDsByPR[prID] = append(issueIDsByPR[prID], link.WorkItemID)
	}

	providers, err := LoadWorkGraphEdgeRepoProviders(ctx, executor.conn, run.OrganizationID, executor.jobProvider, repoIDs)
	if err != nil {
		return 0, err
	}

	prsByProvider := make(map[string][]aiworkflow.PullRequestRow)
	for _, pr := range prs {
		provider := workGraphEdgesProviderFor(providers, pr.RepoID)
		prsByProvider[provider] = append(prsByProvider[provider], pr)
	}
	providersPresent := make([]string, 0, len(prsByProvider))
	for provider := range prsByProvider {
		providersPresent = append(providersPresent, provider)
	}
	sort.Strings(providersPresent)

	var runs []aiworkflow.Run
	var artifactEdges []aiworkflow.ArtifactEdge
	var issueEdges []aiworkflow.IssueEdge
	for _, provider := range providersPresent {
		result := aiworkflow.Compute(prsByProvider[provider], orgUUID, provider, issueIDsByPR, computedAt)
		runs = append(runs, result.Runs...)
		artifactEdges = append(artifactEdges, result.ArtifactEdges...)
		issueEdges = append(issueEdges, result.IssueEdges...)
	}

	// codex round chaos-4280 astra review, finding 6: log (not just count)
	// an AI-positive PR whose artifact edge exists but produced no issue
	// edge -- silent today in both Python and every existing Go family. This
	// does not distinguish "genuinely no linked work item" from "linkage
	// query missed it": both are legitimately silent per PR, but the
	// AGGREGATE rate is the operator signal worth having, same reasoning as
	// aiimpact.RecordLinkageUnavailable.
	if aiPositiveWithoutLinkage := countAIPositiveWithoutIssueLinkage(artifactEdges, issueEdges); aiPositiveWithoutLinkage > 0 {
		slog.Debug("ai_workflow: AI-positive PR(s) with no issue linkage found",
			"org_id", run.OrganizationID, "partition_id", partition.ID,
			"target_day", targetDay.Format("2006-01-02"), "count", aiPositiveWithoutLinkage)
	}

	// THREE TABLES, NO TRANSACTION: report the TRUE rows-written count on a
	// mid-sequence failure, same pattern as work_graph_edges
	// (work_graph_edges_native_executor.go:131-182) -- adopted per codex
	// round chaos-4280 astra review, finding 6.
	written := 0
	step := 0
	partial := func(err error) (int, error) { return wrapAIWorkflowPartialWrite(written, step, err) }

	step++
	writtenRuns, err := WriteAIWorkflowRuns(ctx, executor.conn, runs, computedAt)
	written += writtenRuns
	if err != nil {
		return partial(err)
	}
	step++
	writtenArtifacts, err := WriteAIWorkflowArtifactEdges(ctx, executor.conn, artifactEdges, computedAt)
	written += writtenArtifacts
	if err != nil {
		return partial(err)
	}
	step++
	writtenIssues, err := WriteAIWorkflowIssueEdges(ctx, executor.conn, issueEdges, computedAt)
	written += writtenIssues
	if err != nil {
		return partial(err)
	}
	return written, nil
}

// countAIPositiveWithoutIssueLinkage counts artifact edges (one per
// AI-positive PR, keyed by run_id since each PR yields exactly one run and
// one artifact edge) whose run_id never appears in the issue-edge slice.
func countAIPositiveWithoutIssueLinkage(artifactEdges []aiworkflow.ArtifactEdge, issueEdges []aiworkflow.IssueEdge) int {
	linkedRunIDs := make(map[string]struct{}, len(issueEdges))
	for _, edge := range issueEdges {
		linkedRunIDs[edge.RunID] = struct{}{}
	}
	count := 0
	for _, edge := range artifactEdges {
		if _, linked := linkedRunIDs[edge.RunID]; !linked {
			count++
		}
	}
	return count
}

// wrapAIWorkflowPartialWrite mirrors wrapWorkGraphEdgesPartialWrite exactly
// (same reasoning: written>0 guards against suppressing the compatibility
// bridge's legitimate fallback for a partition where nothing landed).
func wrapAIWorkflowPartialWrite(written, step int, err error) (int, error) {
	if written == 0 {
		return 0, err
	}
	table := "UNREGISTERED TABLE -- aiWorkflowWriteOrder is missing an entry"
	if step >= 1 && step <= len(aiWorkflowWriteOrder) {
		table = aiWorkflowWriteOrder[step-1]
	}
	return written, fmt.Errorf("%w: ai_workflow failed on write %d of %d (%s) after %d row(s) landed: %w",
		ErrPartialWrite, step, len(aiWorkflowWriteOrder), table, written, err)
}

var aiWorkflowWriteOrder = [...]string{
	"ai_workflow_runs",
	"ai_workflow_artifact_edges",
	"ai_workflow_issue_edges",
}
