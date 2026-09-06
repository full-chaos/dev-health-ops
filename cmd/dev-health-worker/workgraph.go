package main

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issuepredges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/prcommit"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/riverqueue/river"
)

func buildWorkgraphWorker(cfg config.Config, database workerDatabase, registry *jobruntime.Registry, observer jobruntime.Observer, logger *slog.Logger, workers *river.Workers) (workerFamily, error) {
	if !anyQueueSelected(cfg.Queues, "workgraph", "investment") || registry == nil {
		return workerFamily{}, nil
	}
	if workers == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	kinds := []string{jobcontract.KindWorkGraphBuild, jobcontract.KindInvestmentMaterialize}
	specs := make([]jobruntime.HandlerSpec, 0, len(kinds))
	for _, kind := range kinds {
		descriptor, ok := registry.Descriptor(kind)
		if !ok {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		if queueSelected(cfg.Queues, descriptor.Queue) && descriptor.Executable() {
			specs = append(specs, descriptor)
		}
	}
	if len(specs) == 0 {
		return workerFamily{}, nil
	}
	postgresDatabase, ok := database.(*postgresWorkerDatabase)
	if !ok || postgresDatabase.pools == nil || observer == nil || logger == nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// The work-graph store reports a release-lost lease directly: generic
	// middleware cannot tell that outcome apart from an ordinary release, and
	// only the store that ran the fenced UPDATE knows it matched zero rows
	// because the lease had already expired (CHAOS-4002).
	var leaseObservers []jobruntime.WorkGraphLeaseObserver
	if leaseObserver, ok := observer.(jobruntime.WorkGraphLeaseObserver); ok {
		leaseObservers = append(leaseObservers, leaseObserver)
	}
	store, err := workgraph.NewPostgresStore(postgresDatabase.pools.Domain, leaseObservers...)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	compatibility, err := workgraph.NewHTTPCompatibilityExecutor(
		workgraphCompatibilityHTTPClient(cfg.OperationalBridgeTimeout),
		workgraph.HTTPCompatibilityConfig{
			Endpoint:              strings.TrimRight(cfg.OperationalBridgeURL, "/") + "/internal/worker/workgraph/v1/execute",
			BearerToken:           cfg.OperationalBridgeToken.Reveal(),
			AllowInsecureInternal: cfg.OperationalBridgeAllowInsecure,
		},
	)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	// THE CUTOVER (CHAOS-4441). investment.materialize's compute is native Go
	// from here; workgraph.build's is not yet (CHAOS-4924 carries its six
	// remaining sub-builders), so the two kinds now take DIFFERENT executors
	// and `compatibility` is no longer the single executor for every kind.
	//
	// A ClickHouse failure REFUSES the family rather than falling back to the
	// bridge. The fallback would be the more "available" choice and the wrong
	// one: it would silently return the family to Python compute, so the
	// cutover would appear complete while production kept running the old
	// plane -- exactly the state this ticket exists to end, and one nothing
	// downstream could detect. Same reasoning as workgraphBuildPreSteps'.
	nativeInvestment, nativeErr := buildNativeInvestmentExecutor(cfg, specs, logger)
	if nativeErr != nil {
		return workerFamily{}, nativeErr
	}

	idempotency, err := newOperationalIdempotency(postgresDatabase.pools.Domain, observer)
	if err != nil {
		return workerFamily{}, errWorkerDependencyUnavailable
	}
	dependencies := jobruntime.Dependencies{Logger: logger, Observer: observer, TenantScope: operationalTenantScope{}, Budget: newOperationalBudget(postgresDatabase.pools.Domain, observer), Idempotency: idempotency}

	buildPreSteps, buildPostSteps, preStepErr := workgraphBuildPreSteps(cfg, specs, observer, logger)
	if preStepErr != nil {
		return workerFamily{}, preStepErr
	}

	registered := make([]jobruntime.HandlerSpec, 0, len(specs))
	for _, spec := range specs {
		if err := addWorkgraphWorker(workers, registry, spec, store, compatibility, nativeInvestment, dependencies, buildPreSteps, buildPostSteps); err != nil {
			return workerFamily{}, errWorkerDependencyUnavailable
		}
		registered = append(registered, spec)
	}
	budgets := selectedQueueBudgets(
		cfg.Queues, []string{"workgraph", "investment"}, cfg.WorkerQueueConcurrency,
	)
	return workerFamily{
		handlers: registered,
		queues:   budgets,
	}, nil
}

func workgraphCompatibilityHTTPClient(connectTimeout time.Duration) *http.Client {
	// Work-graph and investment handler contracts have substantially different
	// execution budgets. The River execution context is the authoritative
	// deadline; the shared 30-second operational bridge timeout would abort a
	// healthy synchronous investment materialization.
	return contractDeadlineHTTPClient(connectTimeout)
}

// workgraphBuildPreSteps constructs the native Go producers that run inside a
// build execution before the Python bridge. Returns nil when no build kind is
// selected for this worker, so a workgraph process running only the investment
// kinds takes no ClickHouse dependency it does not need.
//
// A ClickHouse failure REFUSES the family rather than registering the build
// handler without its pre-step. Once the mapping is produced natively, a build
// that skipped it would still succeed and still write edges -- just an edge set
// missing every provider-attached issue-PR link. A wrong answer that looks
// healthy is worse than a family that will not start, and `newHandler` refuses
// a nil step for the same reason.
func workgraphBuildPreSteps(
	cfg config.Config, specs []jobruntime.HandlerSpec, observer jobruntime.Observer, logger *slog.Logger,
) ([]workgraph.NativePreStep, []workgraph.NativePostStep, error) {
	declared := buildPreStepOrder()
	buildSelected := false
	for _, spec := range specs {
		if spec.Kind == jobcontract.KindWorkGraphBuild {
			buildSelected = true
			break
		}
	}
	if !buildSelected {
		return nil, nil, nil
	}

	connection, connectionErr := clickhousestore.Open(
		context.Background(), clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if connectionErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	loader, loaderErr := issueprlinks.NewLoader(connection)
	if loaderErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	writer, writerErr := issueprlinks.NewWriter(connection)
	if writerErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	service, serviceErr := issueprlinks.NewService(loader, writer, logger)
	if serviceErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	if candidate, ok := observer.(issueprlinks.Observer); ok {
		service.SetObserver(candidate)
	}
	step, stepErr := newIssuePRLinksPreStep(service)
	if stepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}

	issuePREdgesLoader, issuePREdgesLoaderErr := issuepredges.NewLoader(connection)
	if issuePREdgesLoaderErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	issuePREdgesLinksWriter, issuePREdgesLinksWriterErr := issueprlinks.NewWriter(connection)
	if issuePREdgesLinksWriterErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	issuePREdgesService, issuePREdgesServiceErr := issuepredges.NewService(issuePREdgesLoader, connection, issuePREdgesLinksWriter, logger)
	if issuePREdgesServiceErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	issuePREdgesSharedWindow := newSharedIssuePREdgesWindow()
	issuePREdgesFastPathStep, issuePREdgesFastPathStepErr := newIssuePREdgesFastPathPreStep(issuePREdgesService, issuePREdgesSharedWindow)
	if issuePREdgesFastPathStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	issuePREdgesTextParseStep, issuePREdgesTextParseStepErr := newIssuePREdgesTextParsePreStep(issuePREdgesService, issuePREdgesSharedWindow)
	if issuePREdgesTextParseStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	issuePREdgesHeuristicStep, issuePREdgesHeuristicStepErr := newIssuePREdgesHeuristicPreStep(issuePREdgesService, issuePREdgesSharedWindow)
	if issuePREdgesHeuristicStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}

	prCommitLoader, prCommitLoaderErr := prcommit.NewLoader(connection)
	if prCommitLoaderErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	prCommitWriter, prCommitWriterErr := prcommit.NewWriter(connection)
	if prCommitWriterErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	prCommitService, prCommitServiceErr := prcommit.NewService(prCommitLoader, prCommitWriter, connection, logger)
	if prCommitServiceErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	prCommitSharedWindow := newSharedPRCommitWindow()
	prCommitLinksStep, prCommitLinksStepErr := newPRCommitLinksPreStep(prCommitService, prCommitSharedWindow)
	if prCommitLinksStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	prCommitEdgesStep, prCommitEdgesStepErr := newPRCommitEdgesPreStep(prCommitService, prCommitSharedWindow)
	if prCommitEdgesStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}

	flagGuardsStep, flagGuardsStepErr := newFlagGuardsEdgesPreStep(connection)
	if flagGuardsStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	operationalIncidentStep, operationalIncidentStepErr := newOperationalIncidentEdgesPreStep(connection)
	if operationalIncidentStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}

	steps := []workgraph.NativePreStep{
		step, issuePREdgesFastPathStep, issuePREdgesTextParseStep, issuePREdgesHeuristicStep,
		prCommitLinksStep, prCommitEdgesStep, flagGuardsStep, operationalIncidentStep,
	}

	// The constructed steps must match the DECLARED order exactly. Without
	// this, the declaration would be a comment: a step could be added to the
	// construction, run in whatever position it happened to be appended, and
	// the ordering invariant in NativePreStep would be violated silently.
	if len(steps) != len(declared) {
		return nil, nil, errWorkerDependencyUnavailable
	}
	for index, name := range declared {
		if steps[index].Name() != name {
			return nil, nil, errWorkerDependencyUnavailable
		}
	}

	// The post-step seam, on the SAME connection. It carries the identical
	// constructed-vs-declared refusal, because a post-step that is constructed
	// but not declared -- or declared and silently missing -- fails in the way
	// this whole arrangement exists to prevent: the build succeeds and the rows
	// carry Python's values.
	edgeObserver, hasObserver := observer.(jobruntime.WorkGraphIssueEdgesObserver)
	if !hasObserver {
		return nil, nil, errWorkerDependencyUnavailable
	}
	edgeStep, edgeStepErr := newIssueIssueEdgesPostStep(connection, edgeObserver)
	if edgeStepErr != nil {
		return nil, nil, errWorkerDependencyUnavailable
	}
	postSteps := []workgraph.NativePostStep{edgeStep}
	declaredPost := buildPostStepOrder()
	if len(postSteps) != len(declaredPost) {
		return nil, nil, errWorkerDependencyUnavailable
	}
	for index, name := range declaredPost {
		if postSteps[index].Name() != name {
			return nil, nil, errWorkerDependencyUnavailable
		}
	}
	return steps, postSteps, nil
}

// buildPostStepOrder is the DECLARED order of the native POST-steps, and the
// single place that order is decided. Same split as buildPreStepOrder: a pure
// function a test can assert without a ClickHouse connection.
//
// `issue_issue_edges` is here rather than in buildPreStepOrder because Python's
// stage OVERWRITES it -- see NativePostStep and the step's own doc. Its sibling
// half, `issue_pr_edges_from_fast_path`, READS what issue_pr_links writes and
// so belongs in the PRE-step order after it. That is the straddle
// buildPreStepOrder's comment refers to: this lane's producer sits on both
// sides of the mapping, and the two halves land in different seams for
// different reasons.
func buildPostStepOrder() []string {
	return []string{"issue_issue_edges"}
}

// buildPreStepOrder is the DECLARED order of the native pre-steps that run
// inside a work-graph build, and the single place that order is decided.
//
// It is a pure function so a test can assert the order without a ClickHouse
// connection -- the same "declared source of truth, asserted separately from
// the actual dispatch" split daily_native_family_registration_test.go uses.
//
// Appending here is a real decision, not a formality: see the ordering
// invariant on workgraph.NativePreStep. "issue_pr_edges_fast_path"
// (CHAOS-4924) READS work_graph_issue_pr, which "issue_pr_links" WRITES, so
// it must register strictly after it; "issue_pr_edges_text_parse" has no such
// table dependency on the fast-path step, but registers immediately after it
// anyway to match Python's own call order (builder.py:434-443) for a PR that
// satisfies both derivations in one build, and because all three issue_pr_edges
// steps share ONE computed window (sharedIssuePREdgesWindow) the same way
// pr_commit_links/pr_commit_edges do. "issue_pr_edges_heuristic" READS
// work_graph_issue_pr fresh (see issuepredges.ExplicitLink's doc comment for
// why: it needs to see every row the native issue_pr_links step AND the two
// steps ahead of it here have already committed), so it must register after
// all three of those. "pr_commit_edges" (CHAOS-5264) is the realized case the
// ordering invariant was written for: it READS work_graph_pr_commit, which
// "pr_commit_links" WRITES, so it must register strictly after it -- unlike
// issue_pr_links' still-Python fast-path half, both halves of the PR<->commit
// straddle are native here, registered back to back. "flag_guards_edges" and
// "operational_incident_edges" (CHAOS-4924) read neither work_graph_issue_pr
// nor any table another pre-step writes, so their position relative to the
// other six is free -- placed last, preserving Python's own relative order
// between the two of them (builder.py:468/470).
func buildPreStepOrder() []string {
	return []string{
		"issue_pr_links",
		"issue_pr_edges_fast_path", "issue_pr_edges_text_parse", "issue_pr_edges_heuristic",
		"pr_commit_links", "pr_commit_edges",
		"flag_guards_edges", "operational_incident_edges",
	}
}

// addWorkgraphWorker routes each kind to its executor. `executor` is the
// Python bridge and still serves workgraph.build; `nativeInvestment` serves
// investment.materialize only. investment.dispatch/chunk/finalize (the
// pre-cutover partitioned pipeline) were deleted under CHAOS-4438: zero
// producers ever created a request row for them (RequestWriter.WriteTx's
// sole call site only ever writes workgraph.build), confirmed exhaustively
// after #2227 landed investment.materialize's native cutover.
func addWorkgraphWorker(workers *river.Workers, registry *jobruntime.Registry, spec jobruntime.HandlerSpec, store workgraph.Store, executor workgraph.CompatibilityExecutor, nativeInvestment workgraph.CompatibilityExecutor, dependencies jobruntime.Dependencies, buildPreSteps []workgraph.NativePreStep, buildPostSteps []workgraph.NativePostStep) error {
	switch spec.Kind {
	case jobcontract.KindWorkGraphBuild:
		h, err := workgraph.NewBuildHandler(store, executor, buildPreSteps, buildPostSteps, dependencies.Logger)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.WorkGraphBuildArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	case jobcontract.KindInvestmentMaterialize:
		// NATIVE. Refuse rather than fall back -- see buildWorkgraphWorker.
		if nativeInvestment == nil {
			return errWorkerDependencyUnavailable
		}
		h, err := workgraph.NewMaterializeHandler(store, nativeInvestment, dependencies.Logger)
		if err != nil {
			return err
		}
		a, err := jobruntime.NewAdapter[jobruntime.InvestmentMaterializeArgs](registry, spec, h, dependencies)
		if err != nil {
			return err
		}
		return river.AddWorkerSafely(workers, a)
	default:
		return errWorkerDependencyUnavailable
	}
}

// buildNativeInvestmentExecutor constructs the native investment.materialize
// executor, or nil when this worker process is not running that kind.
//
// Returning nil for an unselected kind is deliberate and mirrors
// workgraphBuildPreSteps: a process configured for the workgraph queue alone
// takes no ClickHouse dependency it will never use, so a ClickHouse outage does
// not refuse a family that would not have touched it. addWorkgraphWorker turns
// a nil into a refusal at the one place it matters -- the materialize case.
func buildNativeInvestmentExecutor(
	cfg config.Config, specs []jobruntime.HandlerSpec, logger *slog.Logger,
) (workgraph.CompatibilityExecutor, error) {
	materializeSelected := false
	for _, spec := range specs {
		if spec.Kind == jobcontract.KindInvestmentMaterialize {
			materializeSelected = true
			break
		}
	}
	if !materializeSelected {
		return nil, nil
	}

	connection, connectionErr := clickhousestore.Open(
		context.Background(), clickhousestore.DefaultConfig(cfg.ClickHouseURI.Reveal()),
	)
	if connectionErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	reader, readerErr := chquery.NewReader(connection)
	if readerErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	writer, writerErr := chwrite.NewWriter(connection)
	if writerErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	executor, executorErr := investment.NewNativeExecutor(reader, writer, logger)
	if executorErr != nil {
		return nil, errWorkerDependencyUnavailable
	}
	return executor, nil
}
