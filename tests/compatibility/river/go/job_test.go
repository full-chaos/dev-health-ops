package rivercompat

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

func TestPythonFixtureDecodes(t *testing.T) {
	t.Parallel()

	data := readFixture(t, "python_job_v1.json")
	args, err := DecodeJobArgs(data)
	if err != nil {
		t.Fatalf("DecodeJobArgs() error = %v", err)
	}
	want := JobArgs{ContractVersion: 1, Marker: "python-fixture", Source: "python"}
	if args != want {
		t.Fatalf("DecodeJobArgs() = %#v, want %#v", args, want)
	}
	if args.Kind() != JobKind {
		t.Fatalf("Kind() = %q, want %q", args.Kind(), JobKind)
	}
}

func TestGoFixtureIsDeterministic(t *testing.T) {
	t.Parallel()

	want, err := json.MarshalIndent(
		JobArgs{ContractVersion: 1, Marker: "go-fixture", Source: "go"},
		"",
		"  ",
	)
	if err != nil {
		t.Fatalf("json.MarshalIndent() error = %v", err)
	}
	want = append(want, '\n')
	got := readFixture(t, "go_job_v1.json")
	if string(got) != string(want) {
		t.Fatalf("go fixture is not the deterministic JobArgs encoding\ngot:  %s\nwant: %s", got, want)
	}
}

func TestDecodeJobArgsRejectsContractDrift(t *testing.T) {
	t.Parallel()

	for name, payload := range map[string]string{
		"unknown field":   `{"contract_version":1,"marker":"m","source":"python","extra":true}`,
		"unknown version": `{"contract_version":2,"marker":"m","source":"python"}`,
		"unknown source":  `{"contract_version":1,"marker":"m","source":"other"}`,
		"multiple values": `{"contract_version":1,"marker":"m","source":"python"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeJobArgs([]byte(payload)); err == nil {
				t.Fatal("DecodeJobArgs() error = nil, want contract rejection")
			}
		})
	}
}

func TestWorkerScenarios(t *testing.T) {
	t.Parallel()

	worker := NewWorker()
	worker.Register("cancel", ScenarioCancel)
	worker.Register("block", ScenarioBlockFirst)
	worker.Register("recover", ScenarioRecover)

	makeJob := func(marker string, attempt int) *river.Job[JobArgs] {
		return &river.Job[JobArgs]{
			JobRow: &rivertype.JobRow{ID: int64(attempt), Attempt: attempt},
			Args: JobArgs{
				ContractVersion: ContractVersion,
				Marker:          marker,
				Source:          "go",
			},
		}
	}

	if err := worker.Work(context.Background(), makeJob("execute", 1)); err != nil {
		t.Fatalf("execute Work() error = %v", err)
	}
	if err := worker.Work(context.Background(), makeJob("cancel", 1)); !errors.Is(err, ErrIntentionalCancel) {
		t.Fatalf("cancel Work() error = %v, want intentional cancel", err)
	}
	if err := worker.Work(context.Background(), makeJob("recover", 1)); !errors.Is(err, ErrIntentionalRecovery) {
		t.Fatalf("recover attempt 1 Work() error = %v, want intentional recovery", err)
	}
	if err := worker.Work(context.Background(), makeJob("recover", 2)); err != nil {
		t.Fatalf("recover attempt 2 Work() error = %v", err)
	}

	blockingWorker := NewWorker()
	blockingWorker.Register("block", ScenarioBlockFirst)
	ctx, cancel := context.WithCancelCause(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- blockingWorker.Work(ctx, makeJob("block", 1)) }()
	<-blockingWorker.Starts()
	cancel(river.ErrJobCancelledRemotely)
	if err := <-errCh; !errors.Is(err, river.ErrJobCancelledRemotely) {
		t.Fatalf("block Work() error = %v, want remote cancellation", err)
	}
	finish := <-blockingWorker.Finishes()
	if !errors.Is(finish.Cause, river.ErrJobCancelledRemotely) {
		t.Fatalf("block finish cause = %v, want remote cancellation", finish.Cause)
	}

	blockingWorker.Register("released", ScenarioBlockFirst)
	releaseErrCh := make(chan error, 1)
	go func() { releaseErrCh <- blockingWorker.Work(context.Background(), makeJob("released", 1)) }()
	<-blockingWorker.Starts()
	if err := blockingWorker.Release("released"); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := <-releaseErrCh; !errors.Is(err, ErrProbeRelease) {
		t.Fatalf("released Work() error = %v, want probe release", err)
	}
	releasedFinish := <-blockingWorker.Finishes()
	if !errors.Is(releasedFinish.Cause, ErrProbeRelease) {
		t.Fatalf("released finish cause = %v, want probe release", releasedFinish.Cause)
	}
}

func TestSummarizeLatenciesUsesNearestRank(t *testing.T) {
	t.Parallel()

	samples := []float64{20, 1, 19, 2, 18, 3, 17, 4, 16, 5, 15, 6, 14, 7, 13, 8, 12, 9, 11, 10}
	got := summarizeLatencies(samples, 19)
	if got.Count != 20 || got.Min != 1 || got.P50 != 10 || got.P95 != 19 || got.Max != 20 {
		t.Fatalf("summarizeLatencies() = %#v", got)
	}
	if !got.WithinLimit {
		t.Fatal("summarizeLatencies() should pass at the p95 limit")
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "fixtures", name))
	if err != nil {
		t.Fatalf("read fixture %q: %v", name, err)
	}
	return data
}
