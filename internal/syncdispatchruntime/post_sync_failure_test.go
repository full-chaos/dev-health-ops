package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
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
	if len(observer.outcomes) != 1 || observer.outcomes[0] != jobruntime.PostSyncFanoutOutcomeError {
		t.Fatalf("counter outcomes = %v, want one error", observer.outcomes)
	}
}
