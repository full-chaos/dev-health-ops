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

type failingDispatchStore struct {
	fakeStore
	calls  int
	failed bool
	err    error
}

func (store *failingDispatchStore) ClaimDispatch(context.Context, string) (*Run, error) {
	run := runningRun()
	run.RepositoryDiscoveryRequired = true
	return &run, nil
}

func (store *failingDispatchStore) FailDispatchPermanently(context.Context, Run) (bool, error) {
	store.calls++
	return store.failed, store.err
}

// The terminal write is attempted only on the last attempt of a discovery
// error, logs one WARN when it ends the run, and never changes the error the
// attempt returns.
func TestDispatchFailsAnUnmaterializedRunOnlyOnItsLastAttempt(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		attempt   int
		failed    bool
		wantCalls int
		wantWarns int
	}{
		{name: "an earlier attempt", attempt: 1, failed: true, wantCalls: 0, wantWarns: 0},
		{name: "the last attempt ends the run", attempt: 3, failed: true, wantCalls: 1, wantWarns: 1},
		{name: "the last attempt finds nothing to end", attempt: 3, failed: false, wantCalls: 1, wantWarns: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			store := &failingDispatchStore{fakeStore: fakeStore{run: runningRun()}, failed: testCase.failed}
			discoveryErr := errors.New("clickhouse unavailable")
			handler, err := NewDispatcher(store, fakePublisher{}, &fakeRepositoryDiscoverer{err: discoveryErr})
			if err != nil {
				t.Fatal(err)
			}
			execution := blockedDispatchExecution()
			execution.Attempt = testCase.attempt
			execution.Definition.MaxAttempts = 3
			var captured bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, nil)))
			workErr := handler.Work(context.Background(), execution)
			slog.SetDefault(previous)
			if !errors.Is(workErr, discoveryErr) {
				t.Fatalf("Work = %v, want the discovery error unchanged", workErr)
			}
			warns := 0
			for _, line := range strings.Split(strings.TrimSpace(captured.String()), "\n") {
				var record map[string]any
				if line == "" || json.Unmarshal([]byte(line), &record) != nil {
					continue
				}
				if record["msg"] == DispatchEndedRunFailedLogMessage && record["level"] == "WARN" {
					warns++
				}
			}
			if store.calls != testCase.wantCalls || warns != testCase.wantWarns {
				t.Fatalf("terminal calls=%d warns=%d, want %d and %d", store.calls, warns, testCase.wantCalls, testCase.wantWarns)
			}
		})
	}
}
