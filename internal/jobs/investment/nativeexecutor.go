package investment

// nativeexecutor.go is the seam that replaces the Python bridge for
// investment.materialize.
//
// It satisfies workgraph.CompatibilityExecutor -- the SAME interface the HTTP
// bridge satisfies -- so nothing else in the execution path changes: the
// handler still claims the request, renews its lease, calls Execute exactly
// once, and completes the request, which writes the
// `work_graph_execution_request:<id>` fence that the outbox's
// prerequisite_completion_key gate reads. Scheduler, reconciler, River and the
// fence semantics are untouched by construction, not by care.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// defaultWindowDays is run_investment_materialize's `window_days: int = 30`
// default (work_graph_tasks.py:172). A scope that omits both from_date and
// to_date gets a 30-day window ending now.
const defaultWindowDays = 30

// NativeExecutor runs investment.materialize in Go.
type NativeExecutor struct {
	reader *chquery.Reader
	writer *chwrite.Writer
	logger *slog.Logger
	// now is injectable so a test can pin the run clock; production passes
	// time.Now. computed_at is a ReplacingMergeTree VERSION column on all three
	// output tables, so the clock is not cosmetic -- two runs with the same
	// stamp collapse into one row.
	now func() time.Time
	// newProvider resolves and constructs the LLM provider for one run. A
	// function rather than a stored Provider because the scope names the
	// provider and because the run must Close() it, mirroring Python's
	// `await provider_instance.aclose()` in materialize_investments' finally.
	// The model argument is load-bearing, not decorative: the request row's
	// model_ref must reach the PROVIDER, not merely the model-version string
	// (codex r1 P1-b).
	newProvider func(requested, model string) (categorize.Provider, categorize.ProviderKind, error)
}

// NewNativeExecutor builds the executor. Every collaborator is required.
func NewNativeExecutor(reader *chquery.Reader, writer *chwrite.Writer, logger *slog.Logger) (*NativeExecutor, error) {
	if reader == nil || writer == nil || logger == nil {
		return nil, ErrUnavailable
	}
	return &NativeExecutor{
		reader: reader, writer: writer, logger: logger,
		now:         func() time.Time { return time.Now().UTC() },
		newProvider: resolveProviderFromEnv,
	}, nil
}

func resolveProviderFromEnv(requested, model string) (categorize.Provider, categorize.ProviderKind, error) {
	kind, err := categorize.ResolveProviderKind(requested)
	if err != nil {
		return nil, "", err
	}
	// REFUSE a kind this port has no real client for (codex r1 P1-a).
	//
	// NewProviderFromEnv returns an `unimplementedProvider` with a NIL ERROR
	// for anthropic/gemini/qwen. Left unchecked, every Complete() fails, the
	// failure classifies as non-deterministic, the run CONTINUES, and every
	// unit is written with a fallback distribution and status
	// llm_task_failed -- while Python has a real client for all three. That
	// is a wrong answer that looks healthy, and it is the same silent-
	// degradation shape CHAOS-2476 filed ("silently persists MOCK
	// categorization when no API key set"). IsProviderKindImplemented exists
	// for exactly this check.
	if !categorize.IsProviderKindImplemented(kind) {
		return nil, "", fmt.Errorf(
			"llm provider kind %q has no native Go client yet; refusing rather than "+
				"writing fallback categorizations over real data", kind)
	}
	// model is the request row's model_ref. Python passes it into get_provider
	// (materialize.py:1189-1195); dropping it here would call the env-default
	// model while STAMPING the requested one into
	// categorization_model_version -- and the skip-existing lookup would then
	// treat that wrong-model row as a valid cached result for the requested
	// model, so the error compounds instead of self-correcting.
	provider, err := categorize.NewProviderFromEnvWithModel(kind, model)
	if err != nil {
		return nil, "", err
	}
	return provider, kind, nil
}

// materializeScope is the decoded request scope.
//
// The field set is EXACTLY worker_workgraph.py:82-94's allowlist for
// investment.materialize. Pointer types distinguish "absent" from "present and
// zero", which matters for every field whose absent-default is not the zero
// value (window_days defaults to 30, not 0).
type materializeScope struct {
	FromDate                    *string  `json:"from_date"`
	ToDate                      *string  `json:"to_date"`
	WindowDays                  *int     `json:"window_days"`
	RepoIDs                     []string `json:"repo_ids"`
	TeamIDs                     []string `json:"team_ids"`
	LLMProvider                 *string  `json:"llm_provider"`
	Force                       *bool    `json:"force"`
	AllowUnscoped               *bool    `json:"allow_unscoped"`
	LLMBatchMode                *string  `json:"llm_batch_mode"`
	LLMBatchMinItems            *int     `json:"llm_batch_min_items"`
	LLMBatchPollIntervalSeconds *float64 `json:"llm_batch_poll_interval_seconds"`
	LLMBatchTimeoutSeconds      *float64 `json:"llm_batch_timeout_seconds"`
}

// Execute runs one investment.materialize request.
//
// The returned bytes become work_graph_execution_ledger.output_evidence, and
// its SHAPE is a compatibility contract, not this function's choice -- see
// buildEvidence.
func (executor *NativeExecutor) Execute(ctx context.Context, claim workgraph.Claim) ([]byte, error) {
	if executor == nil || executor.reader == nil || executor.writer == nil {
		return nil, workgraph.ErrUnavailable
	}
	if claim.Request.Kind != workgraph.KindMaterialize {
		// A handler wired to the wrong executor is a construction bug. Fail
		// rather than materialize under a request that meant something else.
		return nil, fmt.Errorf("native investment executor received kind %q", claim.Request.Kind)
	}

	scope, err := decodeMaterializeScope(claim.Request.Scope)
	if err != nil {
		return nil, err
	}

	// Batch mode is NOT ported (see materialize.go's header). Python's default
	// is "sync" (materialize.py:820), so this refuses nothing that runs today
	// -- but it refuses LOUDLY rather than silently downgrading a
	// provider_batch request to serial calls, which would change cost and
	// latency invisibly. Note this also means the Go plane ignores
	// INVESTMENT_LLM_BATCH_MODE entirely rather than reading a new env var:
	// an operator who sets it gets a refusal, not a silent divergence.
	if scope.LLMBatchMode != nil && *scope.LLMBatchMode != "" && *scope.LLMBatchMode != "sync" {
		return nil, fmt.Errorf(
			"native investment executor supports llm_batch_mode=sync only, got %q", *scope.LLMBatchMode)
	}

	requestedProvider := "auto"
	if scope.LLMProvider != nil && *scope.LLMProvider != "" {
		requestedProvider = *scope.LLMProvider
	}
	provider, kind, err := executor.newProvider(requestedProvider, claim.Request.ModelRef)
	if err != nil {
		return nil, fmt.Errorf("resolve llm provider: %w", err)
	}
	defer func() { _ = provider.Close() }()

	orgID := claim.Request.OrganizationID
	allowUnscoped := scope.AllowUnscoped != nil && *scope.AllowUnscoped

	// materialize.py:1196-1202. The `none` provider produces empty completions,
	// so every unit would fall back to the prior distribution and OVERWRITE
	// real categorizations with it. Refusing is the reference's behaviour and
	// the only safe one.
	if kind == categorize.ProviderKindNone {
		return nil, errors.New(
			"llm provider 'none' cannot materialize investment categorizations; " +
				"configure a real provider or request 'mock' for tests")
	}
	// materialize.py:1179-1188: an unscoped run against a REAL provider writes
	// empty-org rows, which is almost always a mistake and is expensive. mock
	// is exempt because it costs nothing and is how tests run unscoped.
	if orgID == "" && kind != categorize.ProviderKindMock && !allowUnscoped {
		return nil, errors.New(
			"investment materialize requires a non-empty org for real LLM providers; " +
				"set allow_unscoped to write empty-org rows intentionally")
	}

	now := executor.now()
	fromTS, toTS, err := materializeWindow(scope, now)
	if err != nil {
		return nil, err
	}
	// NO ORDERING REJECTION -- deliberately (codex r3 P1).
	//
	// An earlier version refused `from >= to`, citing runner.py:216-218. That
	// citation was from the WRONG ENTRY POINT: runner.py is the `dev-hops` CLI,
	// which the bridge never calls. The path this executor replaces is
	// worker_workgraph.py -> work_graph_tasks.py:57-81 `_parse_materialize_window`,
	// which has NO ordering check at all, and `window_days: 0` is an ACCEPTED
	// scope key (worker_workgraph.py:82-95).
	//
	// So Python answers a zero-width window with a successful zero-record run,
	// while refusing here returned before any read -- handler.work then marked
	// the request ambiguous and never published the completion fence, blocking
	// every prerequisite-gated job behind it. A refusal that strands the chain
	// is worse than the empty result Python produces.
	//
	// A zero-width or inverted window DOES need handling downstream, and this
	// comment previously claimed the opposite: it said MaterializeComponent
	// already skips such components. It did not. Its two bounds checks skip a
	// component lying WHOLLY before or after the interval, and a component that
	// straddles an empty interval satisfies neither -- so accepting the window
	// silently WROTE investment rows where Python writes none. Fixed at the
	// predicate itself (materializecomponent.go): an empty interval now skips
	// every component, so acceptance yields the zero-record run Python yields.

	materializer, err := NewMaterializer(executor.reader, executor.writer, provider, executor.logger)
	if err != nil {
		return nil, err
	}

	cfg := Config{
		OrgID:   orgID,
		FromTS:  fromTS,
		ToTS:    toTS,
		RepoIDs: scope.RepoIDs,
		TeamIDs: scope.TeamIDs,
		Force:   scope.Force != nil && *scope.Force,
		// llm_concurrency is injected onto the arguments from the REQUEST ROW,
		// not the scope (worker_workgraph.py:141), which is why it is read off
		// claim.Request rather than materializeScope.
		LLMConcurrency: claim.Request.LLMConcurrency,
		ProviderName:   string(kind),
		// model_ref is the same story: the row's column, not a scope key.
		Model: claim.Request.ModelRef,
		// work_graph_tasks.py:243 hardcodes persist_evidence_snippets=True on
		// the worker path -- it is not a scope key and not configurable here.
		PersistEvidenceSnippets: true,
		// A FRESH run id per run, matching Python (codex r2 P1).
		//
		// Python generates uuid.uuid4().hex at materialize.py:1292 because the
		// bridge cannot pass run_id for this kind (worker_workgraph.py:82's
		// allowlist has no run_id key). Using the durable REQUEST id made every
		// categorization_run_id differ from Python's in both form (dashed
		// 36-char vs 32-char hex) and semantics (stable across retries vs fresh
		// per run) across investments, repo-effort, quotes and token usage.
		//
		// The retry-stable id is arguably the better design and is deliberately
		// NOT smuggled in here: plan.md section 1.4 is explicit that this port
		// reproduces the current algorithm bit-exactly, and a differential
		// oracle would flag this column on every row. Proposed as a follow-up
		// on CHAOS-4441 instead.
		RunID:      newRunID(),
		ComputedAt: now,
	}

	stats, err := materializer.Run(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return executor.buildEvidence(claim, stats)
}

// buildEvidence reproduces the bridge's output_evidence shape
// (worker_workgraph.py:404-414) exactly.
//
// # WHY THE SHAPE IS COPIED RATHER THAN SIMPLIFIED
//
// This value is what already sits in every historical
// work_graph_execution_ledger.output_evidence row, and readers -- the repair
// endpoint, the operator CLI readbacks, anything reconstructing what a run did
// -- parse it. Emitting a tidier native-only shape would leave the column
// holding two incompatible schemas discriminated by nothing, so a reader would
// have to guess which era a row came from. The nested "outcome" wrapper carries
// the same {"status","stats"} the Celery task returned.
//
// The result must also stay under workgraph.validEvidence's 4096-byte bound
// (postgres.go:232); Stats is fixed-width scalars plus a small failure-count
// map, so it does unless the failure vocabulary grows unboundedly -- which it
// cannot, FailureClass returns from a closed set.
func (executor *NativeExecutor) buildEvidence(claim workgraph.Claim, stats Stats) ([]byte, error) {
	evidence := map[string]any{
		"kind":                   string(claim.Request.Kind),
		"model_ref":              claim.Request.ModelRef,
		"prompt_ref":             claim.Request.PromptRef,
		"llm_concurrency":        claim.Request.LLMConcurrency,
		"spend_limit_microunits": claim.Request.SpendLimitMicrounits,
		"outcome": map[string]any{
			"status": "success",
			"stats":  stats,
		},
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return nil, fmt.Errorf("encode execution evidence: %w", err)
	}
	return encoded, nil
}

// newRunID is Python's `uuid.uuid4().hex` -- 32 lowercase hex digits, NO
// dashes. uuid.New().String() gives the dashed 36-character form, which is a
// different value in a String column that readers group by.
func newRunID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// decodeMaterializeScope decodes the request scope STRICTLY.
//
// DisallowUnknownFields mirrors the bridge's own `set(scope) - allowed[kind]`
// refusal (worker_workgraph.py:136-137). Without it, a scope key this port does
// not know -- one a future Python change adds, or one already used by a caller
// this port has not seen -- would be silently dropped and the run would proceed
// with the wrong scope, writing plausible rows for the wrong window.
func decodeMaterializeScope(raw []byte) (materializeScope, error) {
	scope := materializeScope{}
	if len(raw) == 0 {
		// An absent scope is the org-wide default run, matching a Celery call
		// with no keyword arguments.
		return scope, nil
	}
	// REJECT a non-object scope (codex r1 P2-b). `null`, a bare string, a
	// number and an array are all valid JSON, and workgraph's publisher only
	// checks json.Valid -- so a `scope = 'null'::jsonb` row reaches here. Go's
	// decoder would happily unmarshal `null` into a ZERO-VALUE struct, which
	// this executor reads as "no scope supplied" and runs as an ORG-WIDE
	// 30-day materialization. The bridge refuses it instead
	// (worker_workgraph.py:71-73, `if not isinstance(scope, dict)`), so the
	// same durable row is a no-write on Python and an org-wide write on Go.
	// Decoding into a map first is what makes `null` distinguishable from `{}`.
	var probe map[string]any
	if err := json.Unmarshal(raw, &probe); err != nil {
		return materializeScope{}, fmt.Errorf("decode investment.materialize scope: %w", err)
	}
	if probe == nil {
		return materializeScope{}, errors.New(
			"investment.materialize scope must be a JSON object; got null or a non-object")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&scope); err != nil {
		return materializeScope{}, fmt.Errorf("decode investment.materialize scope: %w", err)
	}
	return scope, nil
}

// materializeWindow ports work_graph_tasks.py:57-81 _parse_materialize_window.
//
// # THE to_date +1 DAY IS NOT AN OFF-BY-ONE
//
// A supplied to_date is parsed as a DATE and advanced by one day at midnight
// UTC, making the window END-EXCLUSIVE over whole days -- `to_date=2026-09-04`
// covers all of the 4th. Dropping the +1 would silently exclude the last day of
// every explicitly-bounded run. An ABSENT to_date is `now` instead, not
// midnight, so the two branches are genuinely different instants and not a
// shared helper.
func materializeWindow(scope materializeScope, now time.Time) (time.Time, time.Time, error) {
	toTS := now
	if scope.ToDate != nil && *scope.ToDate != "" {
		parsed, err := time.Parse(time.DateOnly, *scope.ToDate)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid to_date %q: %w", *scope.ToDate, err)
		}
		toTS = parsed.AddDate(0, 0, 1).UTC()
	}

	if scope.FromDate != nil && *scope.FromDate != "" {
		parsed, err := time.Parse(time.DateOnly, *scope.FromDate)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid from_date %q: %w", *scope.FromDate, err)
		}
		return parsed.UTC(), toTS, nil
	}

	windowDays := defaultWindowDays
	if scope.WindowDays != nil {
		windowDays = *scope.WindowDays
	}
	return toTS.AddDate(0, 0, -windowDays), toTS, nil
}

// compile-time proof the executor is substitutable for the HTTP bridge. If this
// stops compiling, the seam has changed and the cutover needs re-reading.
var _ workgraph.CompatibilityExecutor = (*NativeExecutor)(nil)
