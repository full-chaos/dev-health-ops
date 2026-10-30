package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// A Commit failure the mutation declares as a refusal logs at WARN; every
// other failure, and the same error undeclared, logs at ERROR. Neither line
// carries the error text.
func TestCommitLogsADeclaredRefusalAtWarnAndAnyOtherFailureAtError(t *testing.T) {
	refusal := errors.New("probe refusal text")
	declared := Mutation{Audit: AuditEvent{EventType: "auth.probe.failed", Outcome: OutcomeError}, OffStream: "probe", Refusals: []error{refusal}}
	undeclared := declared
	undeclared.Refusals = nil
	refused := fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, refusal)
	for _, tc := range []struct {
		name  string
		m     Mutation
		err   error
		level string
		msg   string
	}{
		{"declared refusal", declared, refused, "WARN", "audit: Commit refused; its own transaction rolled back"},
		{"other failure", declared, fmt.Errorf("%w: committing", ErrMutationFailed), "ERROR", "audit: Commit failed; its own transaction rolled back"},
		{"undeclared refusal", undeclared, refused, "ERROR", "audit: Commit failed; its own transaction rolled back"},
	} {
		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
		logCommitFailure(context.Background(), tc.m, tc.err)
		slog.SetDefault(previous)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		if len(lines) != 1 {
			t.Fatalf("%s: %d log lines, want 1: %s", tc.name, len(lines), buf.String())
		}
		var rec struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
			Type  string `json:"audit_event_type"`
		}
		if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
			t.Fatalf("%s: %v: %s", tc.name, err, lines[0])
		}
		if rec.Level != tc.level || rec.Msg != tc.msg || rec.Type != "auth.probe.failed" {
			t.Errorf("%s: logged %+v, want level %s msg %q", tc.name, rec, tc.level, tc.msg)
		}
		if strings.Contains(lines[0], "probe refusal text") {
			t.Errorf("%s: the error text reached the log: %s", tc.name, lines[0])
		}
	}
}
