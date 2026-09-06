package daily

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// CHAOS-4290. #2243's metrics-executed-proof failed with EVERY finalize job on
// EVERY run exhausting its four attempts -- 96 starts, 96 discards -- and the
// only evidence of why was 96 identical copies of the adapter's wrapper string
// "dev-health job failed [retryable]". The cause was serialised nowhere.
//
// A family that fails every attempt on every run must not be that quiet, so
// this pins the log line rather than the fix that will follow from reading it.
func TestAFailedFinalizeLogsItsCause(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	const cause = "clickhouse: Unknown expression identifier `prs_authored`"
	var captured bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError}))

	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{err: errors.New(cause)},
	}); err != nil {
		t.Fatal(err)
	}

	execution := finalizeExecutionFor(testRunID)
	execution.Logger = logger
	execution.Attempt = 2
	execution.Definition.MaxAttempts = 4

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work succeeded after a native family failed")
	}

	line := captured.String()
	if line == "" {
		t.Fatal("nothing was logged -- the failure is exactly as invisible as before")
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.Split(line, "\n")[0])), &record); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, line)
	}

	// THE ASSERTION THAT MATTERS: the CAUSE, not merely that something failed.
	// The adapter already logs "Job errored"; a second line saying the same
	// thing in different words would add nothing.
	if got, _ := record["error"].(string); !strings.Contains(got, "prs_authored") {
		t.Errorf("logged error = %q, want it to contain the underlying cause %q -- "+
			"a line that omits the cause leaves the operator exactly where the "+
			"96 wrapper strings did", got, cause)
	}
	// The scope fields are what make one line actionable: WHICH run, WHICH org,
	// and whether this attempt was the last one.
	for _, field := range []string{"run_id", "organization_id", "target_day", "attempt", "max_attempts"} {
		if _, present := record[field]; !present {
			t.Errorf("log line is missing %q; got keys %v", field, keysOf(record))
		}
	}
	if terminal, _ := record["terminal"].(bool); terminal {
		t.Error("attempt 2 of 4 was logged as terminal -- an operator would think " +
			"retries were exhausted when three remained")
	}
}

// The other arm: the FINAL attempt must say so, or the log cannot distinguish
// "will retry" from "gave up", which is the distinction that decides whether
// anyone needs to act now.
func TestTheFinalFinalizeAttemptIsLoggedAsTerminal(t *testing.T) {
	defer restoreRecognisedFinalizeFamilies(pythonRecognisedFinalizeFamilies)
	pythonRecognisedFinalizeFamilies = []string{"ic_finalize"}

	var captured bytes.Buffer
	handler, err := NewFinalizeHandler(finalizeStoreWithClaim())
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.SetNativeFinalizeFamilies(map[string]NativeFinalizeFamilyExecutor{
		"ic_finalize": &stubFinalizeFamily{err: errors.New("boom")},
	}); err != nil {
		t.Fatal(err)
	}
	execution := finalizeExecutionFor(testRunID)
	execution.Logger = slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{Level: slog.LevelError}))
	execution.Attempt = 4
	execution.Definition.MaxAttempts = 4

	if err := handler.Work(context.Background(), execution); err == nil {
		t.Fatal("Work succeeded after a native family failed")
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(strings.Split(captured.String(), "\n")[0])), &record); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	if terminal, _ := record["terminal"].(bool); !terminal {
		t.Errorf("attempt 4 of 4 was not logged as terminal; got %v", record["terminal"])
	}
}

func keysOf(record map[string]any) []string {
	keys := make([]string, 0, len(record))
	for key := range record {
		keys = append(keys, key)
	}
	return keys
}
