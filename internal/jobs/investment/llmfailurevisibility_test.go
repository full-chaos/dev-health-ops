package investment

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph"
)

func newTestAccount(buf *bytes.Buffer) *categorizeAccount {
	_, cancel := context.WithCancel(context.Background())
	return &categorizeAccount{
		outcomes: map[int]categorize.CategorizationOutcome{},
		stats:    &Stats{LLMFailureCounts: map[string]int{}},
		cancel:   cancel,
		logger:   slog.New(slog.NewTextHandler(buf, nil)),
		orgID:    "org-1",
		runID:    "run-1",
	}
}

func TestCategorizeAccountLogsFirstFailureOnceWithClassAndRedactedMessage(t *testing.T) {
	var buf bytes.Buffer
	account := newTestAccount(&buf)
	secret := "sk-ant-api03-abcdefghijklmnopqrstuvwxyz0123456789"
	long := errors.New("dial tcp: connection refused api_key=" + secret + " " + strings.Repeat("x", 400))
	account.record(0, categorize.CategorizationOutcome{}, long)
	account.record(1, categorize.CategorizationOutcome{}, errors.New("second failure"))
	out := buf.String()
	if n := strings.Count(out, "first of run"); n != 1 {
		t.Fatalf("first-failure WARN lines = %d, want 1\n%s", n, out)
	}
	if !strings.Contains(out, "class=llm_error") {
		t.Fatalf("log lacks class: %s", out)
	}
	if strings.Contains(out, secret) {
		t.Fatalf("log holds the secret: %s", out)
	}
	if strings.Contains(out, strings.Repeat("x", 250)) {
		t.Fatalf("message not cut to 200 chars: %s", out)
	}
	if strings.Contains(out, "second failure") {
		t.Fatalf("second failure was logged: %s", out)
	}
}

func TestFailedUnitOutcomeCarriesFailureClassInAudit(t *testing.T) {
	var buf bytes.Buffer
	account := newTestAccount(&buf)
	account.record(3, categorize.CategorizationOutcome{}, errors.New("boom"))
	audit, err := marshalCategorizationAudit(failedUnitOutcome(*account.stats, 3))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(audit, "llm_failure_class:llm_error") || !strings.Contains(audit, "llm_task_failed") {
		t.Fatalf("audit = %s", audit)
	}
	other, _ := marshalCategorizationAudit(failedUnitOutcome(*account.stats, 9))
	if strings.Contains(other, "llm_failure_class") {
		t.Fatalf("unit without a recorded failure got a class: %s", other)
	}
}

func TestAllLLMCallsFailedEvidenceIsDegradedNotSuccess(t *testing.T) {
	executor := &NativeExecutor{}
	claim := workgraph.Claim{}
	degraded, err := executor.buildEvidence(claim, Stats{LLMFailures: 2, LLMAllFailed: true})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(degraded, []byte(`"status":"degraded"`)) || !bytes.Contains(degraded, []byte(`"reason":"all_llm_calls_failed"`)) {
		t.Fatalf("evidence = %s", degraded)
	}
	healthy, err := executor.buildEvidence(claim, Stats{LLMFailures: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(healthy, []byte(`"status":"success"`)) || bytes.Contains(healthy, []byte("reason")) {
		t.Fatalf("evidence = %s", healthy)
	}
}

func TestCategorizeAccountCountsSuccessSoAllFailedNeedsZero(t *testing.T) {
	var buf bytes.Buffer
	account := newTestAccount(&buf)
	account.record(0, categorize.CategorizationOutcome{LLMCalls: 1}, nil)
	account.record(1, categorize.CategorizationOutcome{}, errors.New("boom"))
	if account.stats.llmSucceeded != 1 || account.stats.LLMFailures != 1 {
		t.Fatalf("succeeded=%d failures=%d", account.stats.llmSucceeded, account.stats.LLMFailures)
	}
}

func TestLLMFailureSummaryLogsPerClassCountsAtLevelByOutcome(t *testing.T) {
	cfg := Config{OrgID: "org-1", RunID: "run-1"}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	ctx := context.Background()

	logLLMFailureSummary(ctx, logger, cfg, Stats{})
	if buf.Len() != 0 {
		t.Fatalf("no failures must log nothing: %s", buf.String())
	}

	partial := Stats{LLMFailures: 3, llmSucceeded: 2, LLMFailureCounts: map[string]int{"rate_limit": 2, "llm_error": 1}}
	logLLMFailureSummary(ctx, logger, cfg, partial)
	out := buf.String()
	if !strings.Contains(out, "level=WARN") || strings.Contains(out, "level=ERROR") {
		t.Fatalf("partial failure must be WARN: %s", out)
	}
	if !strings.Contains(out, "rate_limit") || !strings.Contains(out, "all_failed=false") {
		t.Fatalf("per-class counts missing: %s", out)
	}

	buf.Reset()
	all := Stats{LLMFailures: 3, LLMAllFailed: true, LLMFailureCounts: map[string]int{"rate_limit": 3}}
	logLLMFailureSummary(ctx, logger, cfg, all)
	out = buf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "all_failed=true") {
		t.Fatalf("all-failed must be ERROR: %s", out)
	}
}

type scriptedProvider struct {
	replies []func() (categorize.CompletionResult, error)
	calls   int
}

func (p *scriptedProvider) Complete(context.Context, categorize.CompletionRequest) (categorize.CompletionResult, error) {
	reply := p.replies[p.calls]
	p.calls++
	return reply()
}
func (p *scriptedProvider) Close() error  { return nil }
func (p *scriptedProvider) Model() string { return "scripted" }

func TestRepairFailureAfterModelAnswerIsNotAllCallsFailed(t *testing.T) {
	pending := batchPending(t, 2)
	provider := &scriptedProvider{replies: []func() (categorize.CompletionResult, error){
		func() (categorize.CompletionResult, error) { return categorize.CompletionResult{Text: "not json"}, nil },
		func() (categorize.CompletionResult, error) {
			return categorize.CompletionResult{}, errors.New("repair down")
		},
		func() (categorize.CompletionResult, error) { return categorize.CompletionResult{Text: "not json"}, nil },
		func() (categorize.CompletionResult, error) {
			return categorize.CompletionResult{}, errors.New("repair down")
		},
	}}
	cfg := batchTestConfig(LLMBatchModeSync)
	cfg.LLMConcurrency = 1
	got := runCategorize(t, context.Background(), provider, cfg, pending)
	if got.stats.LLMFailures != 2 || got.stats.llmSucceeded != 2 {
		t.Fatalf("failures=%d answered=%d, want 2 and 2", got.stats.LLMFailures, got.stats.llmSucceeded)
	}
	if allLLMCallsFailed(got.stats) {
		t.Fatal("a run whose units all got a first model answer was called all-failed")
	}
	if !allLLMCallsFailed(Stats{LLMFailures: 1}) {
		t.Fatal("zero answers with a failure must be all-failed")
	}
}

func TestRedactedMessageKeepsBodyAndPromptTextOut(t *testing.T) {
	leak := errors.New("upstream said BODY_SENTINEL for prompt PROMPT_SENTINEL")
	if got := categorize.RedactedMessage(leak); strings.Contains(got, "SENTINEL") || !strings.Contains(got, "*errors.errorString") {
		t.Fatalf("untyped error text leaked or type missing: %q", got)
	}
	wrapped := fmt.Errorf("wrapped: %w", leak)
	if got := categorize.RedactedMessage(wrapped); strings.Contains(got, "SENTINEL") {
		t.Fatalf("wrapped untyped error text leaked: %q", got)
	}
	if got := categorize.RedactedMessage(context.DeadlineExceeded); !strings.Contains(got, "deadline") {
		t.Fatalf("context error text lost: %q", got)
	}
	var buf bytes.Buffer
	account := newTestAccount(&buf)
	account.record(0, categorize.CategorizationOutcome{}, leak)
	if strings.Contains(buf.String(), "SENTINEL") {
		t.Fatalf("first-failure log leaked: %s", buf.String())
	}
}
