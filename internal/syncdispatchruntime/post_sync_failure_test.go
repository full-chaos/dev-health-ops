package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type failureCountObserver struct {
	outcomes []jobruntime.PostSyncFanoutOutcome
}

func (observer *failureCountObserver) ObservePostSyncFanout(outcome jobruntime.PostSyncFanoutOutcome) error {
	observer.outcomes = append(observer.outcomes, outcome)
	return nil
}

// A return of the bare sentinel hides the cause (CHAOS-8784): every return goes through unavailable().
func TestNoBareErrPostSyncUnavailableReturnInThePackage(t *testing.T) {
	bare := regexp.MustCompile(`\bErrPostSyncUnavailable\b`)
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob = %v, %v", files, err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for number, line := range strings.Split(string(body), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") || !bare.MatchString(line) {
				continue
			}
			isDecl := strings.HasPrefix(trimmed, "var ErrPostSyncUnavailable =")
			isMatch := file == "post_sync_failure.go" && (strings.Contains(line, "ErrPostSyncUnavailable.Error()") || strings.Contains(line, "target == ErrPostSyncUnavailable"))
			if !isDecl && !isMatch {
				t.Errorf("%s:%d returns or assigns the bare sentinel: %s", file, number+1, trimmed)
			}
		}
	}
}

func TestUnavailableKeepsTheSentinelTheStageAndTheCause(t *testing.T) {
	cause := &pgconn.PgError{Code: "42703", Message: "column secret_value does not exist"}
	err := unavailable(stageReference, cause)
	if !errors.Is(err, ErrPostSyncUnavailable) {
		t.Fatal("errors.Is(err, ErrPostSyncUnavailable) = false")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42703" {
		t.Fatalf("errors.As PgError = %v", pgErr)
	}
	if postSyncStageOf(err) != stageReference {
		t.Fatalf("stage = %q", postSyncStageOf(err))
	}
	if strings.Contains(err.Error(), "secret_value") {
		t.Fatalf("Error() carries the cause text: %q", err.Error())
	}
}

func TestCoordinatorWorkErrorNamesStageClassAndSQLStateNotTheText(t *testing.T) {
	cause := &pgconn.PgError{Code: "42703", Message: "column secret_value does not exist"}
	got := (&coordinatorWorkError{cause: unavailable(stageReference, cause)}).Error()
	want := "coordinator job failed: *pgconn.PgError stage=reference_read class=postgres code=42703"
	if got != want {
		t.Fatalf("River job error = %q, want %q", got, want)
	}
}

func TestFanoutFailureLogsOneErrorLineWithRunStageAndClassAndCountsIt(t *testing.T) {
	var logs bytes.Buffer
	observer := &failureCountObserver{}
	service := &NativePostSyncService{
		logger:         synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))),
		fanoutObserver: observer,
	}
	args := PostSyncArgs{TransportArgs{OrgID: "00000000-0000-4000-8000-000000000001", RunID: "00000000-0000-4000-8000-000000000002"}}
	if err := service.Fanout(context.Background(), args); !errors.Is(err, ErrPostSyncUnavailable) {
		t.Fatalf("Fanout error = %v", err)
	}
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "post_sync_fanout.failed" {
			lines = append(lines, record)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("post_sync_fanout.failed lines = %d:\n%s", len(lines), logs.String())
	}
	record := lines[0]
	failure, _ := record["error"].(map[string]any)
	if record["level"] != "ERROR" || record["sync_run_id"] != "00000000-0000-4000-8000-000000000002" || record["reason"] != "args" || failure["class"] == nil {
		t.Fatalf("failure line = %v", record)
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
		t.Fatalf("counter outcomes = %v, want one error", observer.outcomes)
	}
}

func TestStagedWriterErrorKeepsTextChainAndNamesTheStage(t *testing.T) {
	writerErr := errors.New("writer says no")
	staged := stagedFailure(stageDaily, writerErr)
	if staged.Error() != "writer says no" || !errors.Is(staged, writerErr) || errors.Is(staged, ErrPostSyncUnavailable) {
		t.Fatalf("staged = %q, Is(writer)=%v, Is(sentinel)=%v", staged, errors.Is(staged, writerErr), errors.Is(staged, ErrPostSyncUnavailable))
	}
	if postSyncStageOf(staged) != stageDaily || stagedFailure(stageDora, staged) != staged {
		t.Fatalf("stage = %q, restaged = %v", postSyncStageOf(staged), stagedFailure(stageDora, staged))
	}
	got := (&coordinatorWorkError{cause: staged}).Error()
	if want := "coordinator job failed: *errors.errorString stage=daily class=other"; got != want {
		t.Fatalf("River job error = %q, want %q", got, want)
	}
}

func TestFanoutPanicLogsTheFailedLineCountsItAndPanicsOn(t *testing.T) {
	var logs bytes.Buffer
	observer := &failureCountObserver{}
	service := &NativePostSyncService{
		pool:           &pgxpool.Pool{},
		logger:         synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))),
		fanoutObserver: observer,
		now:            func() time.Time { return time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC) },
	}
	args := PostSyncArgs{TransportArgs{
		Version: ContractVersionV1, OrgID: "00000000-0000-4000-8000-000000000001", RunID: "00000000-0000-4000-8000-000000000002",
		DispatchOutbox: "00000000-0000-4000-8000-000000000003", RouteGeneration: 1,
	}}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the panic was swallowed")
			}
		}()
		_ = service.Fanout(context.Background(), args)
	}()
	if got := strings.Count(logs.String(), `"msg":"post_sync_fanout.failed"`); got != 1 || !strings.Contains(logs.String(), `"reason":"panic"`) {
		t.Fatalf("failed lines = %d:\n%s", got, logs.String())
	}
	if !strings.Contains(logs.String(), `"step":"args"`) {
		t.Fatalf("the panic line must carry the stage the fan-out had reached (args):\n%s", logs.String())
	}
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
		t.Fatalf("counter outcomes = %v, want one error", observer.outcomes)
	}
}

type nilMessageStageError struct{ message *string }

func (err *nilMessageStageError) Error() string { return *err.message }

type selfUnwrapStageError struct{}

func (err *selfUnwrapStageError) Error() string { return "cycle" }
func (err *selfUnwrapStageError) Unwrap() error { return err }

func TestStageAndJobErrorTextSurviveATypedNilAndACyclicWriterError(t *testing.T) {
	var nilCause *nilMessageStageError
	staged := stagedFailure(stageDaily, error(nilCause))
	if staged.Error() == "" || postSyncStageOf(staged) != stageDaily {
		t.Fatalf("typed nil: text %q stage %q", staged.Error(), postSyncStageOf(staged))
	}
	if text := (&coordinatorWorkError{cause: staged}).Error(); !strings.Contains(text, "stage=daily") {
		t.Fatalf("job error text = %q", text)
	}
	done := make(chan postSyncStage, 1)
	go func() { done <- postSyncStageOf(stagedFailure(stageDora, &selfUnwrapStageError{})) }()
	select {
	case stage := <-done:
		if stage != stageDora {
			t.Fatalf("cyclic stage = %q", stage)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stage walk hung on an Unwrap that returns itself")
	}
}

// A panic(nil) under GODEBUG=panicnil=1 is invisible to recover: Fanout must still let it reach River. GODEBUG is read at process
// start, so the scenario runs in a child process of this test binary.
func TestFanoutPanicNilIsNotSwallowed(t *testing.T) {
	if os.Getenv("FANOUT_PANICNIL_CHILD") == "1" {
		service := &NativePostSyncService{
			pool: &pgxpool.Pool{}, logger: synclog.New(slog.New(slog.NewJSONHandler(io.Discard, nil))),
			now: func() time.Time { panic(nil) },
		}
		args := PostSyncArgs{TransportArgs{
			Version: ContractVersionV1, OrgID: "00000000-0000-4000-8000-000000000001", RunID: "00000000-0000-4000-8000-000000000002",
			DispatchOutbox: "00000000-0000-4000-8000-000000000003", RouteGeneration: 1,
		}}
		returned := false
		func() {
			defer func() { _ = recover() }()
			_ = service.Fanout(context.Background(), args)
			returned = true
		}()
		if returned {
			os.Exit(3)
		}
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestFanoutPanicNilIsNotSwallowed$")
	command.Env = append(os.Environ(), "FANOUT_PANICNIL_CHILD=1", "GODEBUG=panicnil=1")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Fanout swallowed a panic(nil): %v\n%s", err, output)
	}
}

func TestChainDefectIsNamedForADeepChainATypedNilAndAPanickingUnwrap(t *testing.T) {
	var nilCause *nilMessageStageError
	deep := error(&cyclicStageError{})
	cases := map[string]error{
		chainDefectBound:     deep,
		chainDefectErrorText: stagedFailure(stageDaily, error(nilCause)),
	}
	for want, err := range cases {
		if got := chainDefect(err); got != want {
			t.Errorf("chainDefect = %q, want %q", got, want)
		}
	}
	if got := chainDefect(unavailable(stageCommit, errors.New("x"))); got != "" {
		t.Errorf("a plain chain has defect %q", got)
	}
	var logs bytes.Buffer
	service := &NativePostSyncService{logger: synclog.New(slog.New(slog.NewJSONHandler(&logs, nil)))}
	service.observeFanoutFailure(context.Background(), PostSyncArgs{TransportArgs{OrgID: "00000000-0000-4000-8000-000000000001", RunID: "00000000-0000-4000-8000-000000000002"}}, deep, true)
	if !strings.Contains(logs.String(), `"msg":"post_sync_fanout.chain_defect"`) || !strings.Contains(logs.String(), `"level":"WARN"`) || !strings.Contains(logs.String(), `"reason":"chain_bound"`) {
		t.Fatalf("no chain-defect Warn line:\n%s", logs.String())
	}
}

type cyclicStageError struct{}

func (err *cyclicStageError) Error() string { return "cycle" }
func (err *cyclicStageError) Unwrap() error { return err }

func TestJoinedWriterErrorKeepsItsSQLStateInTheJobErrorAndWarnsOnAPanickingBranch(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "42703", Message: "column secret does not exist"}
	joined := stagedFailure(stageWorkGraph, errors.Join(pgErr, errors.New("second")))
	want := "coordinator job failed: *errors.joinError stage=workgraph class=postgres code=42703"
	if got := (&coordinatorWorkError{cause: joined}).Error(); got != want {
		t.Fatalf("River job error = %q, want %q", got, want)
	}
	var nilBranch *nilUnwrapStageError
	broken := stagedFailure(stageDaily, errors.Join(errors.New("a"), error(nilBranch)))
	if got := chainDefect(broken); got != chainDefectUnwrap {
		t.Fatalf("chainDefect of a joined error with a panicking Unwrap branch = %q, want %q", got, chainDefectUnwrap)
	}
}

type nilUnwrapStageError struct{ inner error }

func (err *nilUnwrapStageError) Error() string { return "branch" }
func (err *nilUnwrapStageError) Unwrap() error { return err.inner }

func TestChainDefectCounterIsIncremented(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test").Int64Counter("post_sync_error_chain_defect_total")
	if err != nil {
		t.Fatal(err)
	}
	previous := chainDefects
	chainDefects = counter
	t.Cleanup(func() { chainDefects = previous })
	recordChainDefect(context.Background(), chainDefectBound)
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, found := range scope.Metrics {
			if found.Name != "post_sync_error_chain_defect_total" {
				continue
			}
			sum, ok := found.Data.(metricdata.Sum[int64])
			if !ok || len(sum.DataPoints) != 1 || sum.DataPoints[0].Value != 1 {
				t.Fatalf("counter data = %#v", found.Data)
			}
			return
		}
	}
	t.Fatal("post_sync_error_chain_defect_total was not recorded")
}
