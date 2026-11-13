package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
)

const manualOutcomeSecretText = "s3ntinel-credential-ghp_0123456789"

// answerOutcome answers one outcome on a handler whose logger writes JSON lines to the returned buffer.
func answerOutcome(t *testing.T, outcome synchandoff.Outcome) (*httptest.ResponseRecorder, []map[string]any, string, bool) {
	t.Helper()
	logs := &bytes.Buffer{}
	h := &handlers{logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	org := uuid.NewString()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/sync-configs/cfg-1/trigger", nil)
	request = request.WithContext(policy.WithUser(context.Background(), &policy.User{OrgID: org}))
	recorder := httptest.NewRecorder()
	answered := h.answerManualOutcome(recorder, request, "cfg-1", synchandoff.Trigger{OccurrenceID: "occ-1"}, outcome)
	var lines []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(line), &parsed); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		lines = append(lines, parsed)
	}
	return recorder, lines, org, answered
}

func TestManualOutcomeLogsOneWarnLineForAFailedSync(t *testing.T) {
	recorder, lines, org, answered := answerOutcome(t, synchandoff.Outcome{State: synchandoff.StateQuarantined, ErrorCode: "credential_missing", Reason: manualOutcomeSecretText})
	if !answered || recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"status":"failed"`) {
		t.Fatalf("answered=%v %d %s, want the 202 failed answer", answered, recorder.Code, recorder.Body)
	}
	if len(lines) != 1 {
		t.Fatalf("%d log lines, want exactly one: %v", len(lines), lines)
	}
	want := map[string]any{"level": "WARN", "msg": "sync_now_failed", "status": "failed", "reason_code": "credential_missing",
		"config_id": "cfg-1", "org_id": org, "occurrence_id": "occ-1"}
	for key, value := range want {
		if lines[0][key] != value {
			t.Errorf("field %s = %v, want %v (line %v)", key, lines[0][key], value, lines[0])
		}
	}
	if strings.Contains(recorder.Body.String()+toJSON(lines), manualOutcomeSecretText) {
		t.Fatal("the outcome's reason text reached the log or the answer")
	}
}

func TestManualOutcomeLogsOneInfoLineForAPendingSync(t *testing.T) {
	recorder, lines, org, answered := answerOutcome(t, synchandoff.Outcome{State: synchandoff.StatePending})
	if !answered || recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"status":"pending"`) {
		t.Fatalf("answered=%v %d %s, want the 202 pending answer", answered, recorder.Code, recorder.Body)
	}
	if len(lines) != 1 {
		t.Fatalf("%d log lines, want exactly one: %v", len(lines), lines)
	}
	want := map[string]any{"level": "INFO", "msg": "sync_now_pending", "status": "pending", "config_id": "cfg-1", "org_id": org, "occurrence_id": "occ-1"}
	for key, value := range want {
		if lines[0][key] != value {
			t.Errorf("field %s = %v, want %v (line %v)", key, lines[0][key], value, lines[0])
		}
	}
	if _, present := lines[0]["reason_code"]; present {
		t.Errorf("a pending line carries a reason code: %v", lines[0])
	}
}

func TestManualOutcomeLogsOneInfoLineForADisabledSync(t *testing.T) {
	recorder, lines, org, answered := answerOutcome(t, synchandoff.Outcome{State: synchandoff.StateTerminal, JobRunID: "run-1", TotalUnits: 0})
	if !answered || recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"status":"disabled"`) {
		t.Fatalf("answered=%v %d %s, want the 202 disabled answer", answered, recorder.Code, recorder.Body)
	}
	if len(lines) != 1 {
		t.Fatalf("%d log lines, want exactly one: %v", len(lines), lines)
	}
	want := map[string]any{"level": "INFO", "msg": "sync_now_disabled", "status": "disabled", "config_id": "cfg-1", "org_id": org, "occurrence_id": "occ-1"}
	for key, value := range want {
		if lines[0][key] != value {
			t.Errorf("field %s = %v, want %v (line %v)", key, lines[0][key], value, lines[0])
		}
	}
}

// A plan made is answered by the route itself and writes no line here.
func TestManualOutcomeWritesNoLineForAPlannedRun(t *testing.T) {
	recorder, lines, _, answered := answerOutcome(t, synchandoff.Outcome{State: synchandoff.StateMaterialized, JobRunID: "run-1", TotalUnits: 3})
	if answered || recorder.Body.Len() != 0 || len(lines) != 0 {
		t.Fatalf("answered=%v body %q lines %v, want not answered, no body and no line", answered, recorder.Body, lines)
	}
}

func toJSON(lines []map[string]any) string {
	raw, _ := json.Marshal(lines)
	return string(raw)
}
