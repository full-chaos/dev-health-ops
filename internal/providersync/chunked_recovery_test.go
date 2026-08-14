package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// A crash between the final chunk commit and MarkInventoryComplete leaves the
// route holding a TERMINAL cursor. Resuming there must publish completion
// metadata, never re-enter pagination: the terminal encodings (GitHub
// phase=artifacts with an empty next URL, GitLab phase=reports with page 0)
// are what a paginator reads as "start at page 1" (CHAOS-3820).
func TestGitHubTestsChunkRouteTerminalCursorDoesNotRefetch(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	doer := &githubTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{
		"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture,
	})}
	claim := nativeTestClaim("github", "tests")

	var terminal string
	var firstFinal CompleteRouteBatch
	run := func(resume string) (emissions, finals int) {
		if err := (GitHubTestsRouteHandler{}).CollectChunks(
			context.Background(), claim, providerfoundation.Credential{},
			githubTestsClient(t, doer), now, resume,
			func(emission ChunkRouteEmission) error {
				emissions++
				if emission.Final {
					finals++
					terminal = emission.CursorAfter
					if firstFinal.Result == nil {
						firstFinal = emission.Batch
					}
				}
				return nil
			},
		); err != nil {
			t.Fatalf("CollectChunks(%q): %v", resume, err)
		}
		return emissions, finals
	}

	if _, finals := run(""); finals != 1 {
		t.Fatalf("first pass finals=%d", finals)
	}
	doer.requests = nil
	emissions, finals := run(terminal)
	if len(doer.requests) != 0 {
		t.Fatalf("terminal resume issued %d provider request(s): %v", len(doer.requests), doer.requests)
	}
	if emissions != 1 || finals != 1 {
		t.Fatalf("terminal resume emissions=%d finals=%d, want exactly one final metadata emission",
			emissions, finals)
	}
}

func TestGitLabTestsChunkRouteTerminalCursorDoesNotRefetch(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	doer := &gitLabTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{
		"coverage.info": githubTestsLCOVFixture,
	})}
	claim := nativeTestClaim("gitlab", "tests")
	client := gitLabRepositoryClient(t, doer, "https://gitlab.example")

	var terminal string
	run := func(resume string) (emissions, finals int) {
		if err := (GitLabTestsRouteHandler{}).CollectChunks(
			context.Background(), claim, providerfoundation.Credential{}, client, now, resume,
			func(emission ChunkRouteEmission) error {
				emissions++
				if emission.Final {
					finals++
					terminal = emission.CursorAfter
				}
				return nil
			},
		); err != nil {
			t.Fatalf("CollectChunks(%q): %v", resume, err)
		}
		return emissions, finals
	}
	if _, finals := run(""); finals != 1 {
		t.Fatalf("first pass finals=%d", finals)
	}
	before := len(doer.requests)
	emissions, finals := run(terminal)
	if len(doer.requests) != before {
		t.Fatalf("terminal resume issued %d provider request(s)", len(doer.requests)-before)
	}
	if emissions != 1 || finals != 1 {
		t.Fatalf("terminal resume emissions=%d finals=%d", emissions, finals)
	}
}

// The provider page budget must be cumulative across attempt-neutral
// continuations. Enforcing it against a per-invocation counter let a route
// renew its full allowance on every resume and never report the cap
// (CHAOS-3822).
func TestGitHubTestsChunkRoutePageBudgetIsCumulative(t *testing.T) {
	if _, err := remainingPageBudget(3, 0); err != nil {
		t.Fatalf("fresh budget: %v", err)
	}
	if allowance, err := remainingPageBudget(3, 2); err != nil || allowance != 1 {
		t.Fatalf("partially spent budget allowance=%d err=%v", allowance, err)
	}
	if _, err := remainingPageBudget(3, 3); !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("exhausted budget err=%v, want ErrPaginationCapExceeded", err)
	}
	if _, err := remainingPageBudget(3, 9); !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("overspent budget err=%v, want ErrPaginationCapExceeded", err)
	}

	// The route must actually carry the spend forward in its cursor.
	doer := &githubTestsHighVolumeDoer{t: t}
	claim := nativeTestClaim("github", "tests")
	spent, err := encodeGitHubTestsChunkCursor(githubTestsChunkCursor{
		Phase: "runs", RunPages: 3, Repo: "acme/api",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = (GitHubTestsRouteHandler{MaxRuns: 300}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{},
		githubTestsClient(t, doer), time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), spent,
		func(ChunkRouteEmission) error { return nil },
	)
	if !errors.Is(err, ErrPaginationCapExceeded) {
		t.Fatalf("resume with an exhausted cumulative budget err=%v, want ErrPaginationCapExceeded", err)
	}
	if doer.runListRequests != 0 {
		t.Fatalf("exhausted budget still fetched %d listing page(s)", doer.runListRequests)
	}
}

// recoveryRowSink records every physical row, the way a non-FINAL
// ReplacingMergeTree reader sees them.
type recoveryRowSink struct {
	mu       sync.Mutex
	rows     map[string]int
	writes   int
	failFrom int
}

func (sink *recoveryRowSink) WriteEffect(_ context.Context, _ Claim, batch EffectBatch) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.writes++
	if sink.failFrom > 0 && sink.writes >= sink.failFrom {
		return errors.New("simulated crash mid-emission")
	}
	if sink.rows == nil {
		sink.rows = map[string]int{}
	}
	for _, row := range batch.Rows {
		sink.rows[batch.Destination+"|"+string(row)]++
	}
	return nil
}

func (sink *recoveryRowSink) duplicates() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	count := 0
	for _, n := range sink.rows {
		if n > 1 {
			count++
		}
	}
	return count
}

// One provider emission is the durable unit of recovery. A crash between its
// sub-chunks must not leave committed rows for an emission the cursor has not
// advanced past, because recovery would refetch that item and write its rows
// again under fresh ordinals with pending ledgers (CHAOS-3821).
func TestChunkedExecutorSubchunkCrashDoesNotDuplicateRows(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := (CompleteRouteSwitches{GithubCICD: true}).Descriptor("github", "cicd")
	if !ok {
		t.Fatal("descriptor")
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 1, MaxEffectRows: 1, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: 8, MaxWallTime: time.Minute,
	}
	store := newChunkMemoryStore()
	sink := &recoveryRowSink{failFrom: 7}

	build := func() *singleItemChunkHandler {
		return &singleItemChunkHandler{batch: threeRowChunkBatch(t, claim)}
	}
	executor := completeRouteExecutor(now, build(), store, sink)
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	if _, err := executor.Execute(context.Background(), session, descriptor); err == nil {
		t.Fatal("attempt 1 should have failed at the sink")
	}
	crash, err := store.LoadChunkCheckpoint(context.Background(), claim, now)
	if err != nil {
		t.Fatal(err)
	}
	// The emission is the atomic unit: the crash landed while committing, so
	// every sub-chunk of the item must already be durable and the cursor must
	// already sit past it. A partially prepared emission here is the defect —
	// it is what forced recovery to refetch the item and rewrite its rows.
	if crash.PreparedChunks != 3 {
		t.Fatalf("emission was not prepared atomically: PreparedChunks=%d want 3", crash.PreparedChunks)
	}
	if crash.NextOrdinal >= crash.PreparedChunks {
		t.Fatalf("premise broken: nothing was left uncommitted (NextOrdinal=%d PreparedChunks=%d)",
			crash.NextOrdinal, crash.PreparedChunks)
	}
	if crash.NextCursor == "" {
		t.Fatal("a fully prepared emission must have published its continuation")
	}

	sink.failFrom = 0
	recovery := completeRouteExecutor(now, build(), store, sink)
	recovery.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	recovery.Credentials.Decryptor = chunkedCredentialDecryptor{}
	if _, err := recovery.Execute(context.Background(), session, descriptor); err != nil {
		t.Fatalf("recovery: %v", err)
	}
	if duplicated := sink.duplicates(); duplicated != 0 {
		t.Fatalf("%d row(s) physically written more than once across the crash boundary", duplicated)
	}
}

// The final chunk carries metadata and no rows, so the shadow comparison must
// take its record count from the checkpoint's cumulative committed rows. Taken
// from the final batch it reported zero for a sync of any size (CHAOS-3823).
func TestChunkedExecutorReportsCumulativeRecordCount(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := (CompleteRouteSwitches{GithubCICD: true}).Descriptor("github", "cicd")
	if !ok {
		t.Fatal("descriptor")
	}
	descriptor.ChunkPolicy = ChunkPolicy{
		MaxSourceItems: 2, MaxEffectRows: 2, MaxPreparedBytes: 64 << 10,
		MaxChunksPerAttempt: 64, MaxWallTime: time.Minute,
	}
	store := newChunkMemoryStore()
	executor := completeRouteExecutor(now,
		&singleItemChunkHandler{batch: threeRowChunkBatch(t, claim)}, store, &recoveryRowSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}

	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	// Six destinations x three rows, emitted once.
	if result.Comparison.NativeRecords != 18 || result.Comparison.PythonRecords != 18 {
		t.Fatalf("records native=%d python=%d, want 18 — a zero here means the comparison "+
			"is reading the empty final chunk", result.Comparison.NativeRecords, result.Comparison.PythonRecords)
	}
}

// singleItemChunkHandler emits ONE provider item, then the final metadata. It
// re-serves that item whenever the resume cursor has not advanced past it,
// which is what a real paginating route does.
type singleItemChunkHandler struct {
	batch      CompleteRouteBatch
	lastCursor string
}

func (handler *singleItemChunkHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *singleItemChunkHandler) CollectChunks(
	_ context.Context, claim Claim, _ providerfoundation.Credential,
	_ *providerfoundation.HTTPClient, _ time.Time, resumeCursor string,
	emit func(ChunkRouteEmission) error,
) error {
	handler.lastCursor = resumeCursor
	if resumeCursor == "" {
		batch := handler.batch
		batch.Result, batch.Watermark, batch.Evidence = nil, nil, FetchEvidence{}
		if err := emit(ChunkRouteEmission{
			Batch: batch, CursorAfter: `{"item":1}`,
		}); err != nil {
			return err
		}
	}
	empty, err := testOpsEffects(nil, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	return emit(ChunkRouteEmission{
		Batch: CompleteRouteBatch{
			Effects: empty, Result: map[string]any{"complete": true}, Watermark: claim.BeforeAt,
		},
		CursorBefore: `{"item":1}`, CursorAfter: `{"item":1}`, Final: true,
	})
}

func threeRowChunkBatch(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	destinations := []string{"ci_pipeline_runs", "ci_job_runs", "ci_acceptance_checks",
		"test_suite_results", "test_case_results", "coverage_snapshots"}
	effects := make([]EffectBatch, 0, len(destinations))
	for _, destination := range destinations {
		rows := make([]json.RawMessage, 0, 3)
		for id := 1; id <= 3; id++ {
			rows = append(rows, json.RawMessage(
				`{"org_id":"`+claim.OrgID+`","id":`+string(rune('0'+id))+`,"destination":"`+destination+`"}`))
		}
		effect, err := BuildEffectBatch(destination, EffectReplaySafe, rows)
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	watermark := time.Date(2026, 8, 14, 11, 59, 59, 0, time.UTC)
	return CompleteRouteBatch{
		Effects: effects, Result: map[string]any{"complete": true}, Watermark: &watermark,
		Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Records: 18},
	}
}
