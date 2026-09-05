package investment

// Regression tests for codex r1's four findings on #2227. Each one fails on the
// pre-fix code -- see the PR's proof ledger for the mutation that reddens it.

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chquery"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

// --- P2-b: a non-object scope must be refused, not read as "no scope" ---

// TestNonObjectScopeIsRefused pins the distinction Go's decoder erases.
//
// `null` unmarshals into a zero-value struct without error, which this executor
// would otherwise read as "no scope supplied" and run as an ORG-WIDE 30-day
// materialization. The bridge refuses the same row
// (worker_workgraph.py:71-73), so before this fix one durable row meant
// "no-write" on Python and "org-wide write" on Go.
func TestNonObjectScopeIsRefused(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `"scope"`, `7`, `true`} {
		t.Run(raw, func(t *testing.T) {
			if _, err := decodeMaterializeScope([]byte(raw)); err == nil {
				t.Fatalf("scope %s was accepted; it must be refused as a non-object", raw)
			}
		})
	}
}

// TestEmptyAndAbsentScopeStillMeanOrgWide is the counter-test that stops the
// fix above from over-refusing. An ABSENT scope (zero bytes) and an EMPTY
// OBJECT are both legitimately "org-wide defaults" and must keep working --
// a refusal that swallowed them would break every unscoped run.
func TestEmptyAndAbsentScopeStillMeanOrgWide(t *testing.T) {
	for name, raw := range map[string][]byte{"absent": nil, "empty_object": []byte(`{}`)} {
		t.Run(name, func(t *testing.T) {
			scope, err := decodeMaterializeScope(raw)
			if err != nil {
				t.Fatalf("scope %q must be accepted: %v", name, err)
			}
			if scope.WindowDays != nil || len(scope.RepoIDs) != 0 {
				t.Fatalf("expected an empty scope, got %+v", scope)
			}
		})
	}
}

// TestUnknownScopeKeyIsRefused pins the strict decode, which the map probe
// added for P2-b must not bypass -- the probe accepts any object, so the
// DisallowUnknownFields pass after it is what still rejects a key this port
// does not know.
func TestUnknownScopeKeyIsRefused(t *testing.T) {
	if _, err := decodeMaterializeScope([]byte(`{"from_date":"2026-09-01","not_a_real_key":1}`)); err == nil {
		t.Fatal("an unknown scope key was accepted; the strict decode is not running")
	}
}

// --- P1-a: a provider kind with no native client must refuse the run ---

func materializeClaim(t *testing.T, scope string) workgraph.Claim {
	t.Helper()
	return workgraph.Claim{
		Request: workgraph.Request{
			ID:             "6f1a3c2e-4b5d-4e6f-8a9b-0c1d2e3f4a5b",
			OrganizationID: "70d529e0-3c06-4597-8480-794fd02328b6",
			Kind:           workgraph.KindMaterialize,
			Scope:          []byte(scope),
			ModelRef:       "gpt-requested",
		},
		Token: "1f2e3d4c-5b6a-4798-8a9b-0c1d2e3f4a5b",
	}
}

// TestExecuteRefusesAProviderKindWithNoNativeClient is P1-a.
//
// `NewProviderFromEnv` returns an `unimplementedProvider` with a NIL ERROR for
// anthropic/gemini/qwen. Unchecked, every Complete() fails, the failure
// classifies as non-deterministic, the run CONTINUES, and every unit is written
// with a fallback distribution -- a plausible-looking wrong answer over real
// data, while Python has a real client for all three.
//
// The collaborators below are REAL Reader/Writer values over fake connections
// that panic if queried. That is the assertion: the refusal must land before
// any ClickHouse work, so a passing test also proves no query was issued.
// (An earlier version of this test passed nil collaborators and "passed" for
// the wrong reason -- Execute's own nil guard short-circuits first, returning
// ErrUnavailable before provider resolution is ever reached.)
func TestExecuteRefusesAProviderKindWithNoNativeClient(t *testing.T) {
	for _, kind := range []categorize.ProviderKind{
		categorize.ProviderKindAnthropic,
		categorize.ProviderKindGemini,
		categorize.ProviderKindQwen,
	} {
		t.Run(string(kind), func(t *testing.T) {
			if categorize.IsProviderKindImplemented(kind) {
				t.Skipf("%s now has a native client; drop it from this table", kind)
			}
			provider, err := categorize.NewProviderFromEnv(kind)
			if err != nil || provider == nil {
				t.Fatalf("precondition: NewProviderFromEnv(%s) must return a stub with a nil error, got %v/%v", kind, provider, err)
			}

			executor := &NativeExecutor{
				reader: unusedReader(t), writer: unusedWriter(t),
				logger: testLogger(),
				now:    stubNow,
				newProvider: func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
					return resolveProviderFromEnv(string(kind), "")
				},
			}
			_, execErr := executor.Execute(context.Background(), materializeClaim(t, `{}`))
			if execErr == nil {
				t.Fatal("Execute accepted a provider kind with no native client; it must refuse rather than write fallback rows")
			}
			if !strings.Contains(execErr.Error(), "no native Go client") {
				t.Fatalf("refusal must name the cause, got: %v", execErr)
			}
		})
	}
}

// --- model resolution: the STAMP must equal the model that RUNS ---
//
// r1 fixed "model_ref never reached the provider". r2 then found the mirror
// defect left behind: the durable STAMP still fell back to the provider NAME.
// The r1 proof (M8) exercised ProviderModel -- the model handed to the provider
// -- and never touched resolvedModelName, where the stamp is computed. It
// proved the adjacent function and was recorded as covering the finding. These
// tests target resolvedModelName ITSELF.

// TestStampedModelIsTheModelThatRunsNotTheProviderName is r2's P1.
//
// The live post-sync producer sets NO ModelRef (sync_dispatch.go:298-308), so
// an empty model_ref is the PRODUCTION path, not an edge case.
func TestStampedModelIsTheModelThatRunsNotTheProviderName(t *testing.T) {
	t.Setenv("LLM_MODEL", "gpt-env-default")
	got := resolvedModelName(Config{ProviderName: "openai", Model: ""})
	if got == "openai" {
		t.Fatal("stamp fell back to the PROVIDER NAME; it must be the model that actually runs")
	}
	if got != "gpt-env-default" {
		t.Fatalf("stamped model = %q, want the env-configured %q", got, "gpt-env-default")
	}
}

// TestStampedModelCannotDriftFromTheProviderModel is the structural guarantee:
// two call sites independently computing "the model" produced BOTH r1's and
// r2's findings, so this asserts they share one resolver.
func TestStampedModelCannotDriftFromTheProviderModel(t *testing.T) {
	for name, env := range map[string]map[string]string{
		"generic_only":             {"LLM_MODEL": "gpt-generic"},
		"provider_specific_only":   {"LLM_MODEL_OPENAI": "gpt-specific"},
		"both_generic_wins":        {"LLM_MODEL": "gpt-generic", "LLM_MODEL_OPENAI": "gpt-specific"},
		"neither_platform_default": {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("LLM_MODEL", "")
			t.Setenv("LLM_MODEL_OPENAI", "")
			for k, v := range env {
				t.Setenv(k, v)
			}
			stamp := resolvedModelName(Config{ProviderName: "openai", Model: ""})
			provider := categorize.ResolveModelName(categorize.ProviderKindOpenAI, "")
			if stamp != provider {
				t.Fatalf("stamp %q != provider model %q -- they must not drift", stamp, provider)
			}
			if stamp == "" {
				t.Fatal("resolved to an empty model; the platform default must apply")
			}
		})
	}
}

// TestRequestedModelWinsOverTheEnvironment keeps r1's fix pinned.
func TestRequestedModelWinsOverTheEnvironment(t *testing.T) {
	t.Setenv("LLM_MODEL", "gpt-env-default")
	t.Setenv("LLM_MODEL_OPENAI", "gpt-openai-specific")
	if got := categorize.ResolveModelName(categorize.ProviderKindOpenAI, "gpt-requested"); got != "gpt-requested" {
		t.Fatalf("model = %q, want the request's model_ref %q", got, "gpt-requested")
	}
	if got := resolvedModelName(Config{ProviderName: "openai", Model: "gpt-requested"}); got != "gpt-requested" {
		t.Fatalf("stamp = %q, want %q", got, "gpt-requested")
	}
}

// TestGenericEnvBeatsProviderSpecific pins chris's CHAOS-4978 precedence, which
// INVERTS Python's order. Following Python here would re-open r2's P1: the
// provider path is ruled generic-first, so a stamp on Python's order would
// again record a model the run did not use.
func TestGenericEnvBeatsProviderSpecific(t *testing.T) {
	t.Setenv("LLM_MODEL", "gpt-generic")
	t.Setenv("LLM_MODEL_OPENAI", "gpt-specific")
	if got := categorize.ResolveModelName(categorize.ProviderKindOpenAI, ""); got != "gpt-generic" {
		t.Fatalf("model = %q, want the GENERIC override %q (CHAOS-4978)", got, "gpt-generic")
	}
}

// TestPlatformDefaultAppliesWhenNothingIsConfigured ports
// DEFAULT_MODEL_BY_PROVIDER: without it an unconfigured deployment stamps an
// EMPTY model for a run that used the provider's built-in default.
func TestPlatformDefaultAppliesWhenNothingIsConfigured(t *testing.T) {
	t.Setenv("LLM_MODEL", "")
	t.Setenv("LLM_MODEL_OPENAI", "")
	if got := categorize.ResolveModelName(categorize.ProviderKindOpenAI, ""); got != "gpt-5-mini" {
		t.Fatalf("model = %q, want the platform default %q", got, "gpt-5-mini")
	}
}

// TestRunIDIsAFreshPythonStyleHex is r2's run-id P1: Python emits
// uuid.uuid4().hex -- 32 lowercase hex digits, no dashes, fresh per run.
func TestRunIDIsAFreshPythonStyleHex(t *testing.T) {
	first, second := newRunID(), newRunID()
	if len(first) != 32 {
		t.Fatalf("run id %q has length %d, want 32 (uuid4().hex)", first, len(first))
	}
	if strings.Contains(first, "-") {
		t.Fatalf("run id %q contains dashes; Python's uuid4().hex has none", first)
	}
	if first == second {
		t.Fatal("run id is not fresh per call; Python generates a new one per run")
	}
	for _, r := range first {
		if !strings.ContainsRune("0123456789abcdef", r) {
			t.Fatalf("run id %q contains a non-lowercase-hex rune %q", first, r)
		}
	}
}

// TestExecutorPassesModelRefToTheFactory proves the executor actually THREADS
// model_ref through, rather than the factory merely being capable of accepting
// one -- the defect was a dropped argument at the call site, not a missing
// feature.
func TestExecutorPassesModelRefToTheFactory(t *testing.T) {
	var sawModel string
	executor := &NativeExecutor{
		reader: unusedReader(t), writer: unusedWriter(t),
		logger: testLogger(),
		now:    stubNow,
		newProvider: func(_, model string) (categorize.Provider, categorize.ProviderKind, error) {
			sawModel = model
			return nil, "", errRefuseForTest
		},
	}
	_, _ = executor.Execute(context.Background(), materializeClaim(t, `{}`))
	if sawModel != "gpt-requested" {
		t.Fatalf("factory received model %q, want the request's model_ref %q", sawModel, "gpt-requested")
	}
}

var errRefuseForTest = errors.New("refused for test")

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func stubNow() time.Time {
	return time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
}

// unusedReader/unusedWriter are real chquery.Reader / chwrite.Writer values
// over connections that FAIL the test if touched. Every test in this file
// refuses before any ClickHouse work, so a query here is a regression in
// ordering, not a test-harness detail.
type panicConn struct{ t *testing.T }

func (c panicConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.t.Fatal("ClickHouse was queried; the refusal must land before any query")
	return nil, nil
}

func (c panicConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.t.Fatal("a ClickHouse batch was prepared; the refusal must land before any write")
	return nil, nil
}

func unusedReader(t *testing.T) *chquery.Reader {
	t.Helper()
	reader, err := chquery.NewReader(panicConn{t: t})
	if err != nil {
		t.Fatalf("build reader: %v", err)
	}
	return reader
}

func unusedWriter(t *testing.T) *chwrite.Writer {
	t.Helper()
	writer, err := chwrite.NewWriter(panicConn{t: t})
	if err != nil {
		t.Fatalf("build writer: %v", err)
	}
	return writer
}

// --- P2-a: an aborted run must still record the tokens it spent ---

// recordingBatch is the minimum driver.Batch surface WriteTokenUsage touches.
type recordingBatch struct{ appended *int }

func (b recordingBatch) Append(...any) error           { *b.appended++; return nil }
func (b recordingBatch) AppendStruct(any) error        { return nil }
func (b recordingBatch) Column(int) driver.BatchColumn { return nil }
func (b recordingBatch) Flush() error                  { return nil }
func (b recordingBatch) Send() error                   { return nil }
func (b recordingBatch) IsSent() bool                  { return true }
func (b recordingBatch) Abort() error                  { return nil }
func (b recordingBatch) Rows() int                     { return 1 }
func (b recordingBatch) Columns() []column.Interface   { return nil }
func (b recordingBatch) Close() error                  { return nil }

type recordingConn struct{ appended *int }

func (c recordingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errRefuseForTest
}

func (c recordingConn) PrepareBatch(ctx context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	// The context handed down must still be usable. Before the fix this write
	// was skipped entirely on the abort path; the WithoutCancel wrapper is what
	// keeps it usable once the run's own cancel has fired.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return recordingBatch{appended: c.appended}, nil
}

// TestTokenUsageIsNotWrittenWhenTheContextIsCancelled is codex r3's P1, and it
// is the INVERSION of the test that used to live here.
//
// The previous version asserted that a cancelled caller context STILL wrote a
// token-usage row, and it passed — because the code used context.WithoutCancel.
// Both were wrong together, which is why the mutation proof looked healthy: M10
// reddened when WithoutCancel was removed, so the test appeared load-bearing
// while pinning the wrong boundary.
//
// The boundary that actually matters is the LEASE. handler.work cancels the
// executor context when lease renewal fails (workgraph/handler.go:146-149), and
// a detached write then persists llm_token_usage for a run that was abandoned
// mid-flight: no completion fence, and the retry mints a fresh run id, so the
// row belongs to nothing. A lost lease means stop writing, bookkeeping
// included.
//
// The deterministic-abort path is unaffected: it cancels only the child
// callCtx, never Run's ctx, so the flush there still runs.
func TestTokenUsageIsNotWrittenWhenTheContextIsCancelled(t *testing.T) {
	appended := 0
	materializer := tokenUsageMaterializer(t, &appended)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel() // stands in for handler.work's lease-lost cancellation

	err := materializer.flushTokenUsage(cancelled, tokenUsageConfig(), Stats{
		LLMCalls: 3, LLMInputTokens: 120, LLMOutputTokens: 45,
	})
	if err == nil {
		t.Fatal("flushTokenUsage succeeded under a cancelled context; a lost lease must stop the write")
	}
	if appended != 0 {
		t.Fatalf("wrote %d token-usage rows under a cancelled context, want 0 -- the row would belong to an abandoned run", appended)
	}
}

// TestTokenUsageIsWrittenWhenTheContextIsLive is the POSITIVE CONTROL for the
// test above. Without it, the inverted assertion would also pass if
// flushTokenUsage were broken outright and never wrote anything at all.
func TestTokenUsageIsWrittenWhenTheContextIsLive(t *testing.T) {
	appended := 0
	materializer := tokenUsageMaterializer(t, &appended)

	if err := materializer.flushTokenUsage(context.Background(), tokenUsageConfig(), Stats{
		LLMCalls: 3, LLMInputTokens: 120, LLMOutputTokens: 45,
	}); err != nil {
		t.Fatalf("flushTokenUsage on a live context: %v", err)
	}
	if appended != 1 {
		t.Fatalf("wrote %d token-usage rows on a live context, want 1", appended)
	}
}

func tokenUsageConfig() Config {
	return Config{
		OrgID: "70d529e0-3c06-4597-8480-794fd02328b6", RunID: "run-1",
		ProviderName: "openai", Model: "gpt-5-nano", ComputedAt: stubNow(),
	}
}

func tokenUsageMaterializer(t *testing.T, appended *int) *Materializer {
	t.Helper()
	writer, err := chwrite.NewWriter(recordingConn{appended: appended})
	if err != nil {
		t.Fatalf("build writer: %v", err)
	}
	reader, err := chquery.NewReader(recordingConn{appended: appended})
	if err != nil {
		t.Fatalf("build reader: %v", err)
	}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, testLogger())
	if err != nil {
		t.Fatalf("build materializer: %v", err)
	}
	return materializer
}

// TestFlushSkipsAnAllZeroRunEvenOnTheAbortPath keeps the fix from turning the
// abort path into a source of zero rows: consumers SUM this table, and a run
// that made no calls before aborting must write nothing.
func TestFlushSkipsAnAllZeroRunEvenOnTheAbortPath(t *testing.T) {
	appended := 0
	writer, err := chwrite.NewWriter(recordingConn{appended: &appended})
	if err != nil {
		t.Fatalf("build writer: %v", err)
	}
	reader, err := chquery.NewReader(recordingConn{appended: &appended})
	if err != nil {
		t.Fatalf("build reader: %v", err)
	}
	materializer, err := NewMaterializer(reader, writer, categorize.MockProvider{}, testLogger())
	if err != nil {
		t.Fatalf("build materializer: %v", err)
	}
	if err := materializer.flushTokenUsage(context.Background(), Config{
		OrgID: "70d529e0-3c06-4597-8480-794fd02328b6", RunID: "run-2",
		ProviderName: "openai", ComputedAt: stubNow(),
	}, Stats{}); err != nil {
		t.Fatalf("flushTokenUsage: %v", err)
	}
	if appended != 0 {
		t.Fatalf("wrote %d rows for a run that made no calls, want 0", appended)
	}
}

// TestZeroWidthWindowIsAcceptedNotRefused is codex r3's second P1.
//
// `window_days: 0` is an ACCEPTED scope key on the bridge
// (worker_workgraph.py:82-95) and `_parse_materialize_window`
// (work_graph_tasks.py:57-81) applies NO ordering check, so Python answers a
// zero-width window with a successful zero-record run.
//
// The executor used to refuse it, citing runner.py:216-218 -- the `dev-hops`
// CLI, an entry point the bridge never calls. That refusal returned before any
// read, so handler.work marked the request ambiguous and never published the
// completion fence, blocking every prerequisite-gated job behind it. A refusal
// that strands the chain is worse than the empty result Python produces.
//
// Asserted at the window layer rather than end-to-end: reaching Run needs a
// ClickHouse reader, and the defect was the refusal itself.
func TestZeroWidthWindowIsAcceptedNotRefused(t *testing.T) {
	zero := 0
	from, to, err := materializeWindow(materializeScope{WindowDays: &zero}, stubNow())
	if err != nil {
		t.Fatalf("materializeWindow refused a zero-width window: %v", err)
	}
	if !from.Equal(to) {
		t.Fatalf("expected a zero-width window, got from=%s to=%s", from, to)
	}

	// And the executor must not refuse it either.
	//
	// The provider MUST SUCCEED here. An earlier version of this test used a
	// refusing provider, and provider resolution happens BEFORE the window is
	// computed -- so Execute short-circuited on the provider error and the
	// window assertion was unreachable. It passed, and mutation M17 (restoring
	// the rejection) did NOT redden it: the test constrained nothing. Same
	// shape as the nil-collaborator mistake earlier in this file.
	//
	// With a mock provider, Execute reaches the window, then the materializer,
	// then the first ClickHouse read -- which fails on the recording connection.
	// That fetch error is the PROOF the window was accepted: a restored
	// rejection returns before any read and yields the window error instead.
	appended := 0
	reader, err := chquery.NewReader(recordingConn{appended: &appended})
	if err != nil {
		t.Fatalf("build reader: %v", err)
	}
	writer, err := chwrite.NewWriter(recordingConn{appended: &appended})
	if err != nil {
		t.Fatalf("build writer: %v", err)
	}
	executor := &NativeExecutor{
		reader: reader, writer: writer,
		logger: testLogger(), now: stubNow,
		newProvider: func(string, string) (categorize.Provider, categorize.ProviderKind, error) {
			return categorize.MockProvider{}, categorize.ProviderKindMock, nil
		},
	}
	_, execErr := executor.Execute(context.Background(), materializeClaim(t, `{"window_days":0}`))
	if execErr == nil {
		t.Fatal("expected the run to fail at the ClickHouse read, which is what proves the window was accepted")
	}
	if strings.Contains(execErr.Error(), "is not before end") {
		t.Fatalf("executor still refuses a zero-width window: %v", execErr)
	}
	if !strings.Contains(execErr.Error(), "fetch work graph edges") {
		t.Fatalf("expected to reach the edge fetch (proving the window passed), got: %v", execErr)
	}
}

// TestCategorizeFanOutIsBoundedByLimit is codex r3's P2.
//
// The previous shape launched one goroutine PER COMPONENT and used a buffered
// channel as a token bucket. That bounds concurrent provider CALLS but not
// goroutines: with a blocked provider, a large corpus parks one goroutine per
// pending unit and can exhaust worker memory before post-processing runs. The
// comment called the fan-out "bounded" because the bounded thing was the
// visible one.
//
// This measures the goroutine DELTA across a fan-out over many units with a
// provider that blocks until released, so a regression to goroutine-per-unit
// shows up as a delta scaling with the corpus rather than with `limit`.
func TestCategorizeFanOutIsBoundedByLimit(t *testing.T) {
	const units = 500
	const limit = 4

	release := make(chan struct{})
	provider := &blockingProvider{release: release}

	materializer, err := NewMaterializer(unusedReader(t), unusedWriter(t), provider, testLogger())
	if err != nil {
		t.Fatalf("build materializer: %v", err)
	}

	pending := make([]preprocessed, units)
	for i := range pending {
		pending[i] = preprocessed{index: i}
	}
	outcomes := map[int]categorize.CategorizationOutcome{}
	stats := Stats{LLMFailureCounts: map[string]int{}}

	before := runtime.NumGoroutine()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = materializer.categorizePending(context.Background(),
			Config{LLMConcurrency: limit, ProviderName: "mock"}, pending, outcomes, &stats)
	}()

	// Let the pool fill and block.
	for waited := 0; provider.inFlight() < limit && waited < 200; waited++ {
		time.Sleep(5 * time.Millisecond)
	}
	// POSITIVE CONTROL -- the loop above is a WAIT, not a proof of arrival, and
	// without this assertion the goroutine bound below passes VACUOUSLY: if
	// categorization never starts, the wait simply expires, `peak` is ~0, and
	// `peak > limit+20` is false. Codex confirmed it by short-circuiting
	// categorizePending before pool creation -- the test still passed, in 1.04s.
	// This is the fourth time a proof in this PR asserted on a path its own setup
	// never reached (M8, M10, M17), so the control is stated rather than implied.
	if reached := provider.inFlight(); reached < limit {
		t.Fatalf("categorization never reached the worker pool: inFlight=%d, want >= %d -- "+
			"the goroutine bound below would pass vacuously", reached, limit)
	}
	peak := runtime.NumGoroutine() - before
	close(release)
	<-done

	// Generous ceiling: the pool plus the feeder plus test/runtime noise. The
	// point is that it does NOT scale with `units` -- the old shape would show
	// a delta near 500 here.
	if peak > limit+20 {
		t.Fatalf("goroutine delta %d exceeds the bound for limit=%d over %d units -- the fan-out is not pool-bounded", peak, limit, units)
	}
}

// blockingProvider parks every call until released, so the fan-out's goroutine
// footprint can be measured at its peak.
type blockingProvider struct {
	release chan struct{}
	mu      sync.Mutex
	active  int
}

func (p *blockingProvider) Complete(ctx context.Context, _ categorize.CompletionRequest) (categorize.CompletionResult, error) {
	p.mu.Lock()
	p.active++
	p.mu.Unlock()
	select {
	case <-p.release:
	case <-ctx.Done():
	}
	return categorize.CompletionResult{}, errRefuseForTest
}

func (p *blockingProvider) Close() error { return nil }

func (p *blockingProvider) inFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active
}
