package investment

// materialize.go is the native orchestrator for investment.materialize: the
// port of materialize_investments' body (materialize.py:1169-1854) MINUS the
// per-component assembly, which materializecomponent.go already owns.
//
// It is the piece that was missing. #2171/#2175/#2178 landed the fetch, the
// assembly, the write and the LLM plane as separate, individually-golden
// packages with no caller between them; this file is that caller.
//
// WHAT IS DELIBERATELY NOT PORTED HERE (each tracked, none silently dropped):
//   - Provider BATCH mode (_categorize_with_provider_batch, materialize.py:1553-1559).
//     The sync path is what production runs; batch is an opt-in that no scope
//     this executor receives sets. Porting it needs batch_store.py too.
//   - llm_telemetry*.py metric emission. units/telemetrylabels.go has the label
//     helpers; the emit sites are not ported. Counters, not correctness.
//   - Org-scoped BYO provider/credential resolution (CHAOS-5006). The caller
//     supplies an already-resolved provider.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// minEvidenceChars is constants.py:7 MIN_EVIDENCE_CHARS. A bundle under this
// many characters never reaches the LLM.
const minEvidenceChars = 300

// maxLLMConcurrency is materialize.py:94 _MAX_LLM_CONCURRENCY -- the hard
// ceiling a requested concurrency is clamped to, independent of configuration.
const maxLLMConcurrency = 32

// partitionOversizedHubs is the CHAOS-4771 hub-fate gate passed to
// units.BuildComponents.
//
// FALSE, deliberately, and this is the single most consequential constant in
// this file. BuildComponents' own doc requires every live consumer of that
// package to pass the SAME value, because two consumers disagreeing mint
// DIFFERENT work_unit_ids for the same graph -- the cross-table corruption
// CHAOS-4771's acceptance condition #4 names. The other live consumer,
// membership_native_clickhouse.go:130, passes false. Matching it keeps the two
// planes agreeing AND makes this executor bit-exact with the deployed Python
// materializer, which is what the differential oracle asserts.
//
// Flipping this to true is the CHAOS-4771 fix and is a SEPARATE change that
// must move both consumers at once. It is not a knob for this executor.
const partitionOversizedHubs = false

// Config is the run scope, already decoded and defaulted. It mirrors
// MaterializeConfig's fields that the worker path actually populates -- the
// bridge's own scope allowlist (worker_workgraph.py:82-94) is the authority on
// which those are.
type Config struct {
	OrgID string
	// FromTS/ToTS bound which components get NEW investment rows. They do NOT
	// bound coverage of the membership projection, which is full-coverage by
	// construction (see runner.py:262-277's CHAOS-2776 note).
	FromTS time.Time
	ToTS   time.Time
	// RepoIDs and TeamIDs are alternatives: repo ids win, team ids are
	// resolved to repo ids, neither means org-wide.
	RepoIDs []string
	TeamIDs []string
	// Force skips the skip-existing lookup, re-categorising every unit at full
	// LLM cost.
	Force bool
	// LLMConcurrency is the requested parallel categorization count, before
	// clamping to [1, maxLLMConcurrency].
	LLMConcurrency int
	// ProviderName and Model are the RESOLVED provider identity, used for the
	// model-version string and the token-usage row. The Provider on the
	// Materializer is already bound to them.
	ProviderName string
	Model        string
	// PersistEvidenceSnippets gates writing work_unit_investment_quotes.
	PersistEvidenceSnippets bool
	// MaxComponentNodes nil resolves through units.ResolveMaxComponentNodes.
	MaxComponentNodes *int
	// RunID and ComputedAt are the run-level stamps every written row carries.
	// Both are supplied rather than generated here so the executor can put the
	// request id in the evidence and so a test can pin the clock.
	RunID      string
	ComputedAt time.Time
}

// Stats mirrors materialize_investments' return dict (materialize.py:1836-1849)
// -- the shape that becomes the execution ledger's output_evidence.
type Stats struct {
	Components          int            `json:"components"`
	TotalComponents     int            `json:"total_components"`
	Records             int            `json:"records"`
	RepoEffortRecords   int            `json:"repo_effort_records"`
	Quotes              int            `json:"quotes"`
	SkippedExisting     int            `json:"skipped_existing"`
	LLMCalls            int            `json:"llm_calls"`
	LLMInputTokens      int            `json:"llm_input_tokens"`
	LLMOutputTokens     int            `json:"llm_output_tokens"`
	LLMFailures         int            `json:"llm_failures"`
	LLMFailureCounts    map[string]int `json:"llm_failure_counts"`
	OversizedComponents int            `json:"oversized_components"`
	DroppedEdges        int            `json:"dropped_edges"`
	DroppedNodes        int            `json:"dropped_nodes"`
	PartitionedHubs     int            `json:"partitioned_hubs"`
	// RepoCascadeOwn/Ancestor/Children/Unassigned (CHAOS-5359) partition every
	// component this run built by how (or whether) it resolved a repo --
	// own-edges, inherited from an ancestor, inherited from unanimous
	// children, or neither. They always sum to Components.
	RepoCascadeOwn        int `json:"repo_cascade_own"`
	RepoCascadeAncestor   int `json:"repo_cascade_ancestor"`
	RepoCascadeChildren   int `json:"repo_cascade_children"`
	RepoCascadeUnassigned int `json:"repo_cascade_unassigned"`
}

// Materializer holds the collaborators one org-scoped run needs. All three are
// required; a nil one is a wiring bug, not a degraded mode.
type Materializer struct {
	reader   *chquery.Reader
	writer   *chwrite.Writer
	provider categorize.Provider
	logger   *slog.Logger
}

// ErrUnavailable reports a Materializer built without a collaborator it needs.
var ErrUnavailable = errors.New("investment: materializer dependency unavailable")

// NewMaterializer refuses rather than degrading. A nil provider in particular
// is CHAOS-2476's exact shape -- that bug was "silently persists MOCK
// categorization when no API key set", and the fix is that there is no such
// thing as a materializer without a provider.
func NewMaterializer(reader *chquery.Reader, writer *chwrite.Writer, provider categorize.Provider, logger *slog.Logger) (*Materializer, error) {
	if reader == nil || writer == nil || provider == nil || logger == nil {
		return nil, ErrUnavailable
	}
	return &Materializer{reader: reader, writer: writer, provider: provider, logger: logger}, nil
}

// preprocessed is materialize.py's PreprocessedComponent plus the assembled
// record, since materializecomponent.go produces both in one pass.
type preprocessed struct {
	index  int
	result MaterializeComponentResult
}

// Run executes one materialization. The step order is materialize.py's and the
// ordering is load-bearing at three points, each noted at its site.
func (m *Materializer) Run(ctx context.Context, cfg Config) (Stats, error) {
	if m == nil || m.reader == nil || m.writer == nil || m.provider == nil {
		return Stats{}, ErrUnavailable
	}

	repoIDs, err := m.resolveRepoIDs(ctx, cfg)
	if err != nil {
		return Stats{}, err
	}

	edgeRows, err := m.reader.FetchWorkGraphEdges(ctx, chquery.EdgeQueryOptions{
		OrganizationID: cfg.OrgID,
		RepoIDs:        repoIDs,
		// IncludeHeuristic stays false: heuristic edges fuse unrelated work
		// into one component (CHAOS-2775) and the zero value is the safe one.
	})
	if err != nil {
		return Stats{}, fmt.Errorf("fetch work graph edges: %w", err)
	}

	stats := Stats{LLMFailureCounts: map[string]int{}}
	buildStats := &units.BuildStats{}
	components := units.BuildComponents(
		chquery.ComponentEdges(edgeRows), cfg.MaxComponentNodes, partitionOversizedHubs, buildStats,
	)
	stats.OversizedComponents = buildStats.OversizedComponents
	stats.DroppedEdges = buildStats.DroppedEdges
	stats.DroppedNodes = buildStats.DroppedNodes
	stats.PartitionedHubs = buildStats.PartitionedHubs
	stats.TotalComponents = len(components)
	stats.Components = len(components)

	if len(components) == 0 {
		// materialize.py:1217-1226 returns the component stats and nothing
		// else. Note it reports components=0 and OMITS total_components, so the
		// two disagree on this path in the reference; Stats carries both and
		// they are both zero here, which is the same information.
		m.logger.InfoContext(ctx, "no work graph components found for investment materialization")
		return stats, nil
	}

	entities, err := m.fetchEntities(ctx, cfg, components)
	if err != nil {
		return Stats{}, err
	}
	edgeRepoIDs := make(map[string]string, len(edgeRows))
	for _, row := range edgeRows {
		if row.RepoID != "" {
			edgeRepoIDs[row.Edge.EdgeID] = row.RepoID
		}
	}

	// CHAOS-5359 (R22, 4452 design-of-record vol.2): a pre-pass computing each
	// component's OWN repo resolution (the same collectSingleRepoID logic
	// MaterializeComponent runs per-component, mirrored here org-wide so the
	// cascade below can see EVERY component's own resolution, not just this
	// one's), then walking the issue hierarchy for every component that came
	// back unresolved. This has to run before the per-component loop, because
	// an ancestor/child's resolution can only be known once every component in
	// the batch has been examined.
	ownRepoByComponent := make(map[int]*uuid.UUID, len(components))
	for index, component := range components {
		ownRepoByComponent[index] = collectSingleRepoID(component.Edges, edgeRepoIDs)
	}
	issueComponent := buildIssueComponentIndex(components)
	repoCascades := computeRepoHierarchyCascade(components, ownRepoByComponent, entities.WorkItems, issueComponent)
	for _, cascade := range repoCascades {
		switch {
		case cascade.Source == RepoSourceChildren:
			stats.RepoCascadeChildren++
		case cascade.Source != "":
			stats.RepoCascadeAncestor++
		}
	}
	stats.RepoCascadeOwn = 0
	for _, repoID := range ownRepoByComponent {
		if repoID != nil {
			stats.RepoCascadeOwn++
		}
	}
	stats.RepoCascadeUnassigned = len(components) - stats.RepoCascadeOwn - stats.RepoCascadeAncestor - stats.RepoCascadeChildren

	// PREPROCESS. Every component is assembled deterministically first, then
	// split into "needs an LLM call" and "already has its answer". The split
	// has to happen before any call is made, because the skip-existing lookup
	// below is a single batched query over the whole pending set.
	var pending []preprocessed
	outcomes := make(map[int]categorize.CategorizationOutcome, len(components))
	all := make([]preprocessed, 0, len(components))

	for index, component := range components {
		cascade := repoCascades[index]
		result, err := MaterializeComponent(MaterializeComponentInput{
			Component: component, WorkItems: entities.WorkItems, PRs: entities.PRs, Commits: entities.Commits,
			EdgeRepoIDs: edgeRepoIDs, PRChurn: entities.PRChurn, CommitChurn: entities.CommitChurn,
			ActiveHours: entities.ActiveHours, ParentTitles: entities.ParentTitles, EpicTitles: entities.EpicTitles,
			FromTS: cfg.FromTS, ToTS: cfg.ToTS,
			CascadeRepoID: cascade.RepoID, CascadeRepoSource: cascade.Source,
		})
		if err != nil {
			return Stats{}, fmt.Errorf("assemble component %d: %w", index, err)
		}
		if result.Skipped != "" {
			continue // no bounds, or entirely outside the window
		}
		entry := preprocessed{index: index, result: result}
		all = append(all, entry)

		// materialize.py:1363-1381. Both gates produce a fallback outcome
		// rather than skipping the unit: a low-evidence unit still gets an
		// investment row, carrying the fallback distribution and a status
		// naming why. Dropping it instead would make the unit vanish from the
		// allocation view rather than show up as uncategorised.
		switch {
		case result.Bundle.TextCharCount < minEvidenceChars:
			outcomes[index] = categorize.FallbackOutcome(categorize.StatusInsufficientChars)
		case result.Bundle.TextSourceCount == 0:
			outcomes[index] = categorize.FallbackOutcome(categorize.StatusNoTextSources)
		default:
			pending = append(pending, entry)
		}
	}
	fallbackCount := len(outcomes)

	modelVersion := categorize.EffectiveModelVersion(cfg.ProviderName, resolvedModelName(cfg))

	// SKIP-EXISTING. Runs only when not forced, and only over the pending set.
	skippedExisting := map[int]struct{}{}
	if len(pending) > 0 && !cfg.Force {
		keys := make([]chquery.InvestmentKey, 0, len(pending))
		for _, entry := range pending {
			keys = append(keys, chquery.InvestmentKey{
				WorkUnitID: entry.result.Investment.WorkUnitID,
				InputHash:  entry.result.Bundle.InputHash,
			})
		}
		existing, err := m.reader.FetchExistingInvestmentKeys(ctx, cfg.OrgID, keys, modelVersion)
		if err != nil {
			return Stats{}, fmt.Errorf("fetch existing investment keys: %w", err)
		}
		remaining := pending[:0:0]
		for _, entry := range pending {
			key := chquery.InvestmentKey{
				WorkUnitID: entry.result.Investment.WorkUnitID,
				InputHash:  entry.result.Bundle.InputHash,
			}
			if _, ok := existing[key]; ok {
				skippedExisting[entry.index] = struct{}{}
				continue
			}
			remaining = append(remaining, entry)
		}
		pending = remaining
	}
	stats.SkippedExisting = len(skippedExisting)

	// CATEGORIZE.
	if len(pending) > 0 {
		if err := m.categorizePending(ctx, cfg, pending, outcomes, &stats); err != nil {
			// FLUSH TOKEN USAGE BEFORE ABORTING (codex r1 P2-a).
			//
			// A deterministic failure aborts the run, but the calls made
			// before it were really billed. Python flushes usage and only
			// THEN re-raises (materialize.py:1583-1600); returning straight
			// out here would skip the single WriteTokenUsage below and lose
			// every token this run already spent -- silently, since the run
			// fails and nobody reconciles a failed run's cost.
			//
			// Errors from the flush are deliberately swallowed: the run is
			// already failing on a more important error, and replacing that
			// cause with a bookkeeping error would hide why it aborted.
			// LOGGED, not discarded (codex r3 P3). The flush error must not
			// REPLACE the provider failure that is aborting the run -- that
			// would hide why it aborted -- but discarding it silently loses the
			// only signal that a billed run's accounting row went missing.
			// Python emits a debug traceback here (llm_token_usage.py:53-54).
			if flushErr := m.flushTokenUsage(ctx, cfg, stats); flushErr != nil {
				m.logger.WarnContext(ctx, "llm token usage write failed on the deterministic-abort path",
					"run_id", cfg.RunID, "error", flushErr.Error())
			}
			return Stats{}, err
		}
	}

	// POST-PROCESS: turn outcomes into rows.
	investments := make([]chwrite.InvestmentRecord, 0, len(all))
	repoEfforts := make([]chwrite.RepoEffortRecord, 0, len(all))
	quotes := make([]chwrite.QuoteRecord, 0)

	for _, entry := range all {
		// A unit skipped as unchanged still gets its repo-effort rows.
		//
		// This is materialize.py:1628-1668 and it is the single easiest thing
		// in this whole port to get wrong by omission: repo-effort allocation
		// is derived from structural churn with no LLM involvement, so a
		// steady-state run where every unit is unchanged would write NO
		// allocation rows at all and the investment repo Sankey would read
		// empty. The investment row is correctly not rewritten; the effort rows
		// are.
		if _, skipped := skippedExisting[entry.index]; skipped {
			repoEfforts = append(repoEfforts, m.stampRepoEffort(entry.result.RepoEffort, cfg)...)
			continue
		}

		outcome, ok := outcomes[entry.index]
		if !ok {
			// materialize.py:1671-1676: a pending unit with no recorded
			// outcome means its LLM task failed non-fatally. It still gets a
			// row, with the fallback distribution.
			outcome = categorize.FallbackOutcome(categorize.StatusLLMTaskFailed)
		}

		record := entry.result.Investment
		record.SubcategoryDistribution = outcome.Subcategories
		record.ThemeDistribution = units.RollupSubcategoriesToThemes(outcome.Subcategories)
		record.CategorizationStatus = outcome.Status
		record.CategorizationModelVersion = modelVersion
		record.CategorizationRunID = cfg.RunID
		record.ComputedAt = cfg.ComputedAt

		// materialize.py:1686-1687: an unparseable LLM answer CAPS evidence
		// quality at 0.3 rather than leaving the structural score, and the BAND
		// is recomputed from the capped value. Recomputing the band is the
		// half a port forgets -- the assembly already derived it from the
		// uncapped number.
		if outcome.Status == categorize.StatusInvalidLLMOutput {
			if record.EvidenceQuality > 0.3 {
				record.EvidenceQuality = 0.3
			}
			record.EvidenceQualityBand = units.EvidenceQualityBand(record.EvidenceQuality)
		}

		auditJSON, err := marshalCategorizationAudit(outcome)
		if err != nil {
			return Stats{}, fmt.Errorf("encode categorization audit for %s: %w", record.WorkUnitID, err)
		}
		record.CategorizationErrorsJSON = auditJSON

		investments = append(investments, record)
		repoEfforts = append(repoEfforts, m.stampRepoEffort(entry.result.RepoEffort, cfg)...)

		if cfg.PersistEvidenceSnippets {
			for _, quote := range outcome.EvidenceQuotes {
				quotes = append(quotes, chwrite.QuoteRecord{
					WorkUnitID:          record.WorkUnitID,
					Quote:               quote.Quote,
					SourceType:          quote.SourceType,
					SourceID:            quote.SourceID,
					ComputedAt:          cfg.ComputedAt,
					CategorizationRunID: cfg.RunID,
				})
			}
		}
	}

	// TOKEN USAGE FIRST, BEFORE the output tables (codex r2 P1).
	//
	// Order and error-handling both matter, and both are Python's
	// (materialize.py:1616 flushes before the post-process/write block;
	// llm_token_usage.py:53 swallows the failure and returns False).
	//
	// Writing it LAST and propagating its error stranded the chain: the three
	// output batches had already landed, then a token-usage failure returned an
	// error, handler.work marked the request ambiguous, and the completion
	// fence was never written -- so durable rows existed while every
	// prerequisite-gated downstream job stayed blocked, on a BOOKKEEPING write.
	// Flushing first and swallowing means the worst case is a missing usage row,
	// which is recoverable, instead of a stranded fence, which is not.
	if err := m.flushTokenUsage(ctx, cfg, stats); err != nil {
		m.logger.WarnContext(ctx, "llm token usage write failed; continuing",
			"run_id", cfg.RunID, "error", err.Error())
	}

	// WRITE. Same three tables, same order, same "skip the call when empty"
	// shape as materialize.py:1826-1831.
	if len(investments) > 0 {
		if _, err := m.writer.WriteInvestments(ctx, cfg.OrgID, investments); err != nil {
			return Stats{}, fmt.Errorf("write work_unit_investments: %w", err)
		}
	}
	if len(repoEfforts) > 0 {
		if _, err := m.writer.WriteRepoEffort(ctx, cfg.OrgID, repoEfforts); err != nil {
			return Stats{}, fmt.Errorf("write work_unit_repo_effort: %w", err)
		}
	}
	if len(quotes) > 0 {
		if _, err := m.writer.WriteQuotes(ctx, cfg.OrgID, quotes); err != nil {
			return Stats{}, fmt.Errorf("write work_unit_investment_quotes: %w", err)
		}
	}

	stats.Records = len(investments)
	stats.RepoEffortRecords = len(repoEfforts)
	stats.Quotes = len(quotes)

	_ = fallbackCount

	m.logger.InfoContext(ctx, "investment materialization complete",
		"components", stats.Components, "records", stats.Records, "quotes", stats.Quotes,
		"skipped_existing", stats.SkippedExisting,
		"llm", categorize.FormatFailureSummary(len(outcomes)-fallbackCount, stats.LLMFailureCounts),
	)
	return stats, nil
}

// categorizePending runs the bounded-concurrency LLM fan-out.
//
// # THE DETERMINISTIC-FAILURE ABORT IS THE POINT OF THIS FUNCTION
//
// materialize.py:1571-1601 cancels every in-flight task and re-raises the
// moment one failure is deterministic (bad key, unknown model, exhausted
// quota). Without that, a run against a misconfigured provider spends one
// failed call PER COMPONENT -- hundreds of round-trips all discovering the
// same fact -- and then writes a full set of fallback rows over good data.
// Non-deterministic failures (rate limit, server error, timeout) do NOT abort;
// they are counted, the unit falls back, and the run continues.
func (m *Materializer) categorizePending(
	ctx context.Context, cfg Config, pending []preprocessed,
	outcomes map[int]categorize.CategorizationOutcome, stats *Stats,
) error {
	limit := cfg.LLMConcurrency
	if limit < 1 {
		limit = 1
	}
	if limit > maxLLMConcurrency {
		m.logger.WarnContext(ctx, "llm concurrency exceeds maximum; clamping",
			"requested", cfg.LLMConcurrency, "maximum", maxLLMConcurrency)
		limit = maxLLMConcurrency
	}

	// The adaptive halve-on-sustained-rate-limit limiter (materialize.py:
	// 1431-1487) is NOT ported: it is a throughput optimisation whose absence
	// costs retries, not correctness, and reproducing its exact streak
	// thresholds has no observable effect on any written row. A fixed bound
	// plus the abort below is the behaviour that matters. Tracked in the PR's
	// RISK-NOTES rather than silently dropped.
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		mu       sync.Mutex
		fatalErr error
		wg       sync.WaitGroup
	)

	// A FIXED WORKER POOL, not a goroutine per component (codex r3 P2).
	//
	// The previous shape launched one goroutine per pending component and used a
	// buffered channel as a token bucket. That bounds concurrent PROVIDER CALLS
	// but not goroutines: with a blocked provider, 250k evidence-bearing
	// components meant 250k goroutines, 249,999 of them parked on the channel,
	// able to exhaust worker memory before post-processing ever ran. The comment
	// called it "bounded" because the thing it bounded was the visible one.
	//
	// Here `limit` goroutines drain a channel instead, so both the call
	// concurrency AND the goroutine count are bounded by the same number, and
	// the memory cost is independent of corpus size.
	work := make(chan preprocessed)
	go func() {
		defer close(work)
		for _, entry := range pending {
			select {
			case work <- entry:
			case <-callCtx.Done():
				return
			}
		}
	}()

	for worker := 0; worker < limit; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for entry := range work {
				outcome, err := categorize.CategorizeTextBundle(callCtx, entry.result.Bundle, categorize.CategorizeOptions{
					Provider: m.provider, ProviderName: cfg.ProviderName, Model: cfg.Model,
				})

				mu.Lock()
				if err != nil {
					// A cancellation caused by our OWN abort is not a new
					// failure; counting it would report every in-flight unit as
					// having failed independently.
					if fatalErr != nil && errors.Is(err, context.Canceled) {
						mu.Unlock()
						continue
					}
					class := categorize.FailureClass(err)
					stats.LLMFailureCounts[class]++
					stats.LLMFailures++
					if categorize.IsDeterministicFailure(err) && fatalErr == nil {
						fatalErr = err
						cancel()
					}
					mu.Unlock()
					continue
				}
				outcomes[entry.index] = outcome
				stats.LLMCalls += outcome.LLMCalls
				stats.LLMInputTokens += outcome.InputTokens
				stats.LLMOutputTokens += outcome.OutputTokens
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if fatalErr != nil {
		return fmt.Errorf("investment categorization stopped on deterministic LLM failure (%s): %w",
			categorize.FormatFailureSummary(len(outcomes), stats.LLMFailureCounts), fatalErr)
	}
	return nil
}

// flushTokenUsage writes the run's llm_token_usage row.
//
// Reached from BOTH the success path and the deterministic-failure abort, which
// is the whole point: an aborted run still spent whatever it spent, and the
// all-zero skip inside WriteTokenUsage means a run that made no calls still
// writes nothing.
func (m *Materializer) flushTokenUsage(ctx context.Context, cfg Config, stats Stats) error {
	// ctx is honoured, NOT detached (codex r3 P1).
	//
	// This previously used context.WithoutCancel, added for the deterministic-
	// abort path. That was wrong twice over. First, the abort cancels only the
	// CHILD callCtx (see categorizePending), never Run's ctx, so detaching was
	// never needed for the case it was written for. Second, it created a real
	// hazard: handler.work cancels the executor context when the LEASE is lost
	// (workgraph/handler.go:146-149), and a detached write would then persist an
	// llm_token_usage row for a run that was abandoned mid-flight -- no
	// completion fence, and the retry mints a fresh run id, so the row belongs
	// to nothing.
	//
	// A lost lease means "stop writing", and that has to include bookkeeping.
	writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := m.writer.WriteTokenUsage(writeCtx, cfg.OrgID, chwrite.TokenUsageRecord{
		RunID:        cfg.RunID,
		Provider:     cfg.ProviderName,
		Model:        resolvedModelName(cfg),
		Source:       chwrite.TokenUsageSourceInvestmentMaterialize,
		InputTokens:  stats.LLMInputTokens,
		OutputTokens: stats.LLMOutputTokens,
		Calls:        stats.LLMCalls,
		ComputedAt:   cfg.ComputedAt,
	}); err != nil {
		return fmt.Errorf("write llm_token_usage: %w", err)
	}
	return nil
}

// stampRepoEffort fills the run-level columns materializecomponent.go
// deliberately leaves to the caller.
func (m *Materializer) stampRepoEffort(records []chwrite.RepoEffortRecord, cfg Config) []chwrite.RepoEffortRecord {
	stamped := make([]chwrite.RepoEffortRecord, len(records))
	for i, record := range records {
		record.CategorizationRunID = cfg.RunID
		record.ComputedAt = cfg.ComputedAt
		stamped[i] = record
	}
	return stamped
}

// resolveRepoIDs ports materialize.py:1156-1166 _resolve_repo_ids: explicit
// repo ids win outright, team ids are resolved through the team->repo table,
// and neither means org-wide (nil, not an empty slice -- an empty slice would
// read as "restrict to no repositories").
func (m *Materializer) resolveRepoIDs(ctx context.Context, cfg Config) ([]string, error) {
	if len(cfg.RepoIDs) > 0 {
		return cfg.RepoIDs, nil
	}
	if len(cfg.TeamIDs) > 0 {
		resolved, err := m.reader.ResolveRepoIDsForTeams(ctx, cfg.TeamIDs, cfg.OrgID)
		if err != nil {
			return nil, fmt.Errorf("resolve repo ids for teams: %w", err)
		}
		return resolved, nil
	}
	return nil, nil
}

// entitySet is the org-wide fetch result every component shares.
type entitySet struct {
	WorkItems    map[string]chquery.WorkItem
	PRs          map[string]chquery.PullRequest
	Commits      map[string]chquery.Commit
	PRChurn      map[string]float64
	CommitChurn  map[string]float64
	ActiveHours  map[string]float64
	ParentTitles map[string]string
	EpicTitles   map[string]string
}

// fetchEntities ports materialize.py:1235-1287 -- collect every node id across
// every component, then fetch each entity type ONCE for the whole run rather
// than per component.
func (m *Materializer) fetchEntities(ctx context.Context, cfg Config, components []units.Component) (entitySet, error) {
	issueIDs, prIDs, commitIDs := collectNodeIDs(components)

	workItems, err := m.reader.FetchWorkItems(ctx, issueIDs, cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch work items: %w", err)
	}
	activeHours, err := m.reader.FetchWorkItemActiveHours(ctx, issueIDs, cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch work item active hours: %w", err)
	}
	prs, err := m.reader.FetchPullRequests(ctx, groupPRsByRepo(prIDs), cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch pull requests: %w", err)
	}
	repoCommits := groupCommitsByRepo(commitIDs)
	commits, err := m.reader.FetchCommits(ctx, repoCommits, cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch commits: %w", err)
	}
	commitChurn, err := m.reader.FetchCommitChurn(ctx, repoCommits, cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch commit churn: %w", err)
	}

	workItemMap := make(map[string]chquery.WorkItem, len(workItems))
	parentIDSet := map[string]struct{}{}
	epicIDSet := map[string]struct{}{}
	for _, item := range workItems {
		workItemMap[item.WorkItemID] = item
		if item.ParentID != "" {
			parentIDSet[item.ParentID] = struct{}{}
		}
		if item.EpicID != "" {
			epicIDSet[item.EpicID] = struct{}{}
		}
	}
	parentTitles, err := m.reader.FetchParentTitles(ctx, sortedKeys(parentIDSet), cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch parent titles: %w", err)
	}
	epicTitles, err := m.reader.FetchParentTitles(ctx, sortedKeys(epicIDSet), cfg.OrgID)
	if err != nil {
		return entitySet{}, fmt.Errorf("fetch epic titles: %w", err)
	}

	prMap := make(map[string]chquery.PullRequest, len(prs))
	prChurn := make(map[string]float64, len(prs))
	for _, pr := range prs {
		if pr.RepoID == "" {
			continue
		}
		// materialize.py's _map_prs/_pr_churn_map both key on
		// "{repo_id}#pr{number}" -- the canonical PR node id.
		id := fmt.Sprintf("%s#pr%d", pr.RepoID, pr.Number)
		prMap[id] = pr
		prChurn[id] = pr.Additions + pr.Deletions
	}
	commitMap := make(map[string]chquery.Commit, len(commits))
	for _, commit := range commits {
		if commit.RepoID == "" || commit.Hash == "" {
			continue
		}
		commitMap[fmt.Sprintf("%s@%s", commit.RepoID, commit.Hash)] = commit
	}

	return entitySet{
		WorkItems: workItemMap, PRs: prMap, Commits: commitMap,
		PRChurn: prChurn, CommitChurn: commitChurn, ActiveHours: activeHours,
		ParentTitles: parentTitles, EpicTitles: epicTitles,
	}, nil
}

// collectNodeIDs flattens every component's nodes into three deduped, sorted id
// lists. Sorted rather than first-seen because these feed IN-list query
// parameters, and a stable order makes the emitted SQL reproducible.
func collectNodeIDs(components []units.Component) (issueIDs, prIDs, commitIDs []string) {
	issues := map[string]struct{}{}
	prs := map[string]struct{}{}
	commits := map[string]struct{}{}
	for _, component := range components {
		for _, node := range component.Nodes {
			switch node.Type {
			case "issue":
				issues[node.ID] = struct{}{}
			case "pr":
				prs[node.ID] = struct{}{}
			case "commit":
				commits[node.ID] = struct{}{}
			}
		}
	}
	return sortedKeys(issues), sortedKeys(prs), sortedKeys(commits)
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// groupPRsByRepo ports materialize.py:934-940 _group_prs_by_repo. A node id
// that does not parse is DROPPED, matching Python's `if repo_id and number is
// not None` guard.
func groupPRsByRepo(prIDs []string) map[string][]uint32 {
	grouped := map[string][]uint32{}
	for _, prID := range prIDs {
		repoID, number, ok := units.ParsePRFromID(prID)
		if !ok || repoID == nil || number < 0 {
			continue
		}
		grouped[repoID.String()] = append(grouped[repoID.String()], uint32(number))
	}
	return grouped
}

// groupCommitsByRepo ports materialize.py:943-949 _group_commits_by_repo.
func groupCommitsByRepo(commitIDs []string) map[string][]string {
	grouped := map[string][]string{}
	for _, commitID := range commitIDs {
		repoID, hash, ok := units.ParseCommitFromID(commitID)
		if !ok || repoID == nil || hash == "" {
			continue
		}
		grouped[repoID.String()] = append(grouped[repoID.String()], hash)
	}
	return grouped
}

// marshalCategorizationAudit ports materialize.py:1689,1741 --
// `json.dumps([*outcome.errors, *outcome.warnings])`, errors first.
//
// # WHY THIS IS NOT encoding/json
//
// The differential oracle caught this: Python's `json.dumps` defaults diverge
// from Go's `json.Marshal` in TWO ways on this exact value, and both are
// invisible until something compares bytes.
//
//  1. SEPARATORS. Python's default item separator is ", " (comma SPACE); Go
//     emits ",". Every multi-entry audit array differs.
//  2. ensure_ascii. Python's default escapes every non-ASCII rune as \uXXXX;
//     Go emits raw UTF-8. This is reachable, not theoretical: llm_schema's
//     errors embed values echoed from LLM output (`unknown_subcategory:<key>`),
//     so a non-ASCII key produces divergent bytes in a durable column.
//
// pythonparity.MarshalPythonJSON deliberately refuses lists (its contract
// covers only build_text_bundle's string/map shapes), so the array framing is
// assembled here and each element goes through AppendPythonJSONString, which
// is the escaping half already proven against the Python reference.
//
// The slice is initialised EMPTY rather than nil so an outcome with neither
// errors nor warnings renders as `[]`, which is what Python writes. A nil
// slice through encoding/json would render `null`, and categorization_errors_json
// is a plain String column, so `null` would be stored literally and every
// JSONExtract reader would see a different type than it does today.
func marshalCategorizationAudit(outcome categorize.CategorizationOutcome) (string, error) {
	audit := make([]string, 0, len(outcome.Errors)+len(outcome.Warnings))
	audit = append(audit, outcome.Errors...)
	audit = append(audit, outcome.Warnings...)

	encoded := make([]byte, 0, 64)
	encoded = append(encoded, '[')
	for index, entry := range audit {
		if index > 0 {
			encoded = append(encoded, ',', ' ')
		}
		encoded = pythonparity.AppendPythonJSONString(encoded, entry)
	}
	encoded = append(encoded, ']')
	return string(encoded), nil
}

// resolvedModelName is the model STAMPED into
// work_unit_investments.categorization_model_version and the llm_token_usage
// row. It delegates to the same categorize.ResolveModelName the PROVIDER is
// built from, which is the fix for codex r2's P1.
//
// The previous version fell back to cfg.ProviderName, so an empty model_ref --
// the state the live post-sync producer always creates, since
// cmd/dev-health-worker/sync_dispatch.go:298-308 sets no ModelRef -- stamped
// `model=openai` while the provider ran the env-configured model. Every native
// row then carried a categorization_model_version Python never wrote, so the
// skip-existing lookup missed every historical row (full re-categorization at
// LLM cost) and later runs skipped on a key that did not track the real model.
//
// The org-settings branch stays omitted (CHAOS-5006).
func resolvedModelName(cfg Config) string {
	return categorize.ResolveModelName(categorize.ProviderKind(cfg.ProviderName), cfg.Model)
}
