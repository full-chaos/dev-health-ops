package investment

import (
	"bytes"
	"context"
	"errors"
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
