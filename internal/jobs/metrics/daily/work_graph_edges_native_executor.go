package daily

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
)

// WorkGraphEdgesExecutor is the NATIVE implementation of the work_graph_edges
// metrics.daily family (CHAOS-4286). It is a thin ClickHouse adapter over the
// pure internal/jobs/metrics/workgraphedges kernel; the fidelity notes for the
// compute live on that package and the query notes on the loaders.
//
// # THE FAMILY NAME DOES NOT MATCH THE TABLE NAME
//
// This family is called work_graph_edges and writes
// work_graph_pr_review_outcome_edges, work_graph_pr_deployment_edges and
// work_graph_deployment_incident_edges. The table actually called
// work_graph_edges belongs to a DIFFERENT family, written by
// internal/jobs/workgraph/edges. Nothing here touches it.
//
// # WRITE ORDER IS NOT LOAD-BEARING HERE, UNLIKE ai_governance
//
// #2229 had to order its two writes because ai_policy_events carries a random
// uuid4 event_id in its sorting key on the Python side, so a fallback rewrite
// could never merge (CHAOS-5102). None of this family's three tables has that
// shape: their keys are (org_id, pr_id, review_outcome_id, source),
// (org_id, pr_id, deployment_id, source) and
// (org_id, deployment_id, incident_id, source) -- all deterministic, and
// notably none contains edge_id. A Python fallback rewrite after an ack-loss
// lands on the SAME key and the ReplacingMergeTree collapses it.
//
// So the order below is merely stable, not protective, and this executor
// carries no CHAOS-5102 exposure. Copying #2229's write-order caveat into this
// family's PR would have asserted a risk it does not have.
type WorkGraphEdgesExecutor struct {
	conn   driver.Conn
	nowUTC func() time.Time
	// jobProvider is the daily job's `provider` argument, which discover_repos
	// falls back to when a repo's own provider column is empty or the literal
	// "unknown" (see LoadWorkGraphEdgeRepoProviders). Python's default is
	// "auto", and "auto" therefore appears in the provider column of real
	// rows.
	jobProvider string
}

var errWorkGraphEdgesUnavailable = fmt.Errorf("work_graph_edges native executor unavailable")

// NewWorkGraphEdgesExecutor fails closed on a nil connection, matching every
// other native family: a refused executor never enters PartitionHandler's
// native map and the family stays on the Python compatibility bridge until the
// worker restarts with a healthy connection.
func NewWorkGraphEdgesExecutor(conn driver.Conn, jobProvider string) (*WorkGraphEdgesExecutor, error) {
	if conn == nil {
		return nil, errWorkGraphEdgesUnavailable
	}
	if jobProvider == "" {
		// job_daily.py's discover_repos signature defaults provider="auto";
		// an empty string here would silently turn every fallback into
		// "unknown" instead.
		jobProvider = "auto"
	}
	return &WorkGraphEdgesExecutor{
		conn:        conn,
		nowUTC:      func() time.Time { return time.Now().UTC() },
		jobProvider: jobProvider,
	}, nil
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *WorkGraphEdgesExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errWorkGraphEdgesUnavailable
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

	targetDay := run.TargetDay.UTC()
	dayStart := time.Date(targetDay.Year(), targetDay.Month(), targetDay.Day(), 0, 0, 0, 0, time.UTC)
	// HALF-OPEN, matching job_daily.py's `>= start AND < end`. This family does
	// NOT use ai_governance's inclusive time.max bound.
	dayEnd := dayStart.Add(24 * time.Hour)
	computedAt := executor.nowUTC()

	providers, err := LoadWorkGraphEdgeRepoProviders(ctx, executor.conn, run.OrganizationID, executor.jobProvider)
	if err != nil {
		return 0, err
	}
	reviews, err := LoadWorkGraphEdgeReviews(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	deployments, err := LoadWorkGraphEdgeDeployments(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd)
	if err != nil {
		return 0, err
	}
	// asOf is Python's wf_params["as_of"] = datetime.now(timezone.utc), used by
	// the mapping validity window. Reusing computedAt keeps one clock reading
	// per partition instead of two that could straddle a mapping change.
	startedIncidents, err := LoadIncidentsStarted(
		ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd, computedAt, nil,
	)
	if err != nil {
		return 0, err
	}
	incidents := workGraphEdgeIncidents(startedIncidents)

	result, err := extractPerProvider(
		providers, reviews, deployments, incidents, run.OrganizationID, computedAt,
	)
	if err != nil {
		return 0, err
	}

	// THREE TABLES, NO TRANSACTION: report the TRUE rows-written count on a
	// mid-sequence failure (#2240 round 1, P2).
	//
	// This used to `return 0, err` from each step, which is a lie once an
	// earlier table has already committed: the caller records "refused, 0 rows"
	// for a partition that really does have review edges in ClickHouse. The
	// count below is what actually landed, whatever happens after it.
	//
	// A failure AFTER anything has landed wraps ErrPartialWrite, so
	// computeNativeFamilies adds this family to skipFamilies and the bridge
	// does NOT rewrite the tables. These three tables are ReplacingMergeTrees
	// keyed without edge_id, so a bridge rewrite would in fact merge cleanly --
	// but the skip is still correct: the partition is re-driven with a truthful
	// row count rather than papered over, and the family must not depend on its
	// output tables' engine for the fallback decision to be safe.
	//
	// THE GUARD IS `written > 0` AND THAT IS THE WHOLE POINT (partialwrite.go:29):
	// wrapping a failure BEFORE the first write would suppress the bridge's
	// LEGITIMATE fallback and lose the family for the partition -- the opposite
	// mistake, equally silent. The natural refactor here is one error path for
	// the whole function; that refactor is wrong. Keep the condition explicit.
	written := 0

	partial := func(err error) (int, error) { return wrapWorkGraphEdgesPartialWrite(written, err) }

	writtenReviews, err := WriteWorkGraphPRReviewOutcomeEdges(ctx, executor.conn, result.ReviewOutcomeEdges, computedAt)
	written += writtenReviews
	if err != nil {
		return partial(err)
	}
	writtenDeployments, err := WriteWorkGraphPRDeploymentEdges(ctx, executor.conn, result.PRDeploymentEdges, computedAt)
	written += writtenDeployments
	if err != nil {
		return partial(err)
	}
	writtenIncidents, err := WriteWorkGraphDeploymentIncidentEdges(ctx, executor.conn, result.DeploymentIncidentEdges, computedAt)
	written += writtenIncidents
	if err != nil {
		return partial(err)
	}
	return written, nil
}

// extractPerProvider ports job_daily.py:452-462: the extractor runs ONCE PER
// PROVIDER, over `sorted(edge_providers)` where edge_providers is the UNION of
// the providers present in the three row sets.
//
// The per-provider split is not cosmetic. deployments_by_repo -- the index the
// heuristic incident fallback walks -- is rebuilt inside each call, so an
// incident never links heuristically to a deployment belonging to a different
// provider. Flattening this into one call would silently widen that fan-out.
//
// The sort is Python's, and it makes the emitted order deterministic. The
// sibling PR loop in the same Python function iterates prs_by_provider.items()
// UNSORTED; that asymmetry belongs to PR3b, not here.
//
// `now` is passed in rather than read from the executor's clock field for two
// reasons, both found by TestExtractPerProviderSplitsAndSortsLikePython:
// a struct built directly in a test has a nil clock func and panicked here,
// and reading the clock inside the provider loop gave each provider a
// DIFFERENT fallback timestamp. One reading per partition, supplied by the
// caller, fixes both and makes the only clock dependency visible in the
// signature.
func extractPerProvider(
	providers map[string]string,
	reviews []workgraphedges.ReviewRow,
	deployments []workgraphedges.DeploymentRow,
	incidents []workgraphedges.IncidentRow,
	organizationID string,
	now time.Time,
) (workgraphedges.Result, error) {
	orgUUID, err := uuid.Parse(organizationID)
	if err != nil {
		// Python returns six empty lists when org_id is not a UUID
		// (job_daily.py:284, CHAOS-2187) rather than fabricating attribution.
		// Here the caller has already validated it, so reaching this is a
		// precondition failure, not a routine skip.
		return workgraphedges.Result{}, fmt.Errorf("%w: work_graph_edges org_id %q: %v", ErrInvalidState, organizationID, err)
	}

	reviewsByProvider := make(map[string][]workgraphedges.ReviewRow)
	for _, row := range reviews {
		provider := workGraphEdgesProviderFor(providers, row.RepoID)
		reviewsByProvider[provider] = append(reviewsByProvider[provider], row)
	}
	deploymentsByProvider := make(map[string][]workgraphedges.DeploymentRow)
	for _, row := range deployments {
		provider := workGraphEdgesProviderFor(providers, row.RepoID)
		deploymentsByProvider[provider] = append(deploymentsByProvider[provider], row)
	}
	incidentsByProvider := make(map[string][]workgraphedges.IncidentRow)
	for _, row := range incidents {
		provider := workGraphEdgesProviderFor(providers, row.RepoID)
		incidentsByProvider[provider] = append(incidentsByProvider[provider], row)
	}

	providerSet := make(map[string]struct{})
	for provider := range reviewsByProvider {
		providerSet[provider] = struct{}{}
	}
	for provider := range deploymentsByProvider {
		providerSet[provider] = struct{}{}
	}
	for provider := range incidentsByProvider {
		providerSet[provider] = struct{}{}
	}
	edgeProviders := make([]string, 0, len(providerSet))
	for provider := range providerSet {
		edgeProviders = append(edgeProviders, provider)
	}
	sort.Strings(edgeProviders)

	var combined workgraphedges.Result
	for _, provider := range edgeProviders {
		result, err := workgraphedges.ExtractReviewDeploymentIncidentEdges(workgraphedges.Params{
			OrgID:       orgUUID,
			Provider:    provider,
			Reviews:     reviewsByProvider[provider],
			Deployments: deploymentsByProvider[provider],
			Incidents:   incidentsByProvider[provider],
			Now:         now,
		})
		if err != nil {
			return workgraphedges.Result{}, err
		}
		combined.ReviewOutcomeEdges = append(combined.ReviewOutcomeEdges, result.ReviewOutcomeEdges...)
		combined.PRDeploymentEdges = append(combined.PRDeploymentEdges, result.PRDeploymentEdges...)
		combined.DeploymentIncidentEdges = append(combined.DeploymentIncidentEdges, result.DeploymentIncidentEdges...)
	}
	return combined, nil
}

// wrapWorkGraphEdgesPartialWrite decides whether a write failure is a PARTIAL
// write or an ordinary one, and is a named function rather than an inline
// closure so both directions can be tested without faking a ClickHouse
// connection and three loader queries.
//
// Both directions matter equally. Wrapping when nothing was written suppresses
// the compatibility bridge's legitimate fallback and loses the family for the
// partition (partialwrite.go:29). NOT wrapping when something was written
// leaves the bridge to add a second copy of the rows that already landed. The
// two mistakes are symmetric and both are silent, which is why
// TestWorkGraphEdgesPartialWriteGuardPinsBothDirections asserts each one -- a
// test covering only the ErrPartialWrite path would still pass if ordinary
// failures had quietly stopped failing open.
func wrapWorkGraphEdgesPartialWrite(written int, err error) (int, error) {
	if written > 0 {
		return written, fmt.Errorf("%w: work_graph_edges wrote %d row(s) before failing: %w",
			ErrPartialWrite, written, err)
	}
	return 0, err
}
