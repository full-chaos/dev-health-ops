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
		//
		// codex round chaos-5220 r1, P3: this is a single BULK query scoped
		// to the whole partition (every PR number in one call), so there is
		// no single repo/PR to name -- unlike the per-PR sample logged
		// below. repo_ids (the partition's own repo scope, already known
		// before this query ran) and pr_count are the most specific
		// identifiers this failure mode actually has; naming them is still
		// strictly more actionable than the org/day/partition tuple alone.
		slog.Error("ai_workflow issue-pr linkage query failed",
			"org_id", run.OrganizationID, "partition_id", partition.ID,
			"target_day", targetDay.Format("2006-01-02"), "repo_ids", repoIDs,
			"pr_count", len(prNumbers), "error", linkErr)
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

	// codex round chaos-4280 astra review, finding 6 (sharpened by chaos-5220
	// r1, P3): log a bounded SAMPLE of the actual PR identities (not just an
	// aggregate count) for an AI-positive PR whose artifact edge exists but
	// produced no issue edge -- silent today in both Python and every
	// existing Go family. This does not distinguish "genuinely no linked
	// work item" from "linkage query missed it": both are legitimately
	// silent per PR, but naming the affected PRs (up to
	// unlinkedLogSampleCap) is what actually lets an operator investigate or
	// re-drive specific rows, versus a bare count that names nothing
	// repairable.
	if unlinked := aiPositiveWithoutIssueLinkage(artifactEdges, issueEdges); len(unlinked) > 0 {
		sample := unlinked
		truncated := false
		if len(sample) > unlinkedLogSampleCap {
			sample = sample[:unlinkedLogSampleCap]
			truncated = true
		}
		slog.Debug("ai_workflow: AI-positive PR(s) with no issue linkage found",
			"org_id", run.OrganizationID, "partition_id", partition.ID,
			"target_day", targetDay.Format("2006-01-02"), "count", len(unlinked),
			"pr_ids_sample", sample, "sample_truncated", truncated)
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

// aiPositiveWithoutIssueLinkage returns the artifact_id (the PR's own
// "repoID:number" identity string, see prIDFor) of every AI-positive PR
// (keyed by run_id since each PR yields exactly one run and one artifact
// edge) whose run_id never appears in the issue-edge slice.
//
// codex round chaos-5220 r1, P3: an aggregate COUNT alone (the original form
// of this helper) tells an operator "N PRs lost linkage" but not WHICH ones,
// so nothing is actually repairable from the log line. Returning the PR
// identities lets the caller log a bounded sample an operator can act on.
func aiPositiveWithoutIssueLinkage(artifactEdges []aiworkflow.ArtifactEdge, issueEdges []aiworkflow.IssueEdge) []string {
	linkedRunIDs := make(map[string]struct{}, len(issueEdges))
	for _, edge := range issueEdges {
		linkedRunIDs[edge.RunID] = struct{}{}
	}
	var unlinked []string
	for _, edge := range artifactEdges {
		if _, linked := linkedRunIDs[edge.RunID]; !linked {
			unlinked = append(unlinked, edge.ArtifactID)
		}
	}
	return unlinked
}

// unlinkedLogSampleCap bounds how many PR identities a single log line
// names -- a partition with thousands of unlinked PRs must not turn one log
// line into an unbounded write; the total count is always logged alongside
// the (possibly truncated) sample.
const unlinkedLogSampleCap = 10

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
