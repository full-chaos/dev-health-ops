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

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

type recordingFanoutObserver struct {
	outcomes []jobruntime.PostSyncFanoutOutcome
}

func (observer *recordingFanoutObserver) ObservePostSyncFanout(outcome jobruntime.PostSyncFanoutOutcome) error {
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

func TestFanoutFailureLogsOneErrorLineWithRunStageAndClassAndCountsIt(t *testing.T) {
	var logs bytes.Buffer
	observer := &recordingFanoutObserver{}
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
