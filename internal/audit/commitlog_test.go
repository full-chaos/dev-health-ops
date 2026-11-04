package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
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

// A declared refusal is matched by errors.Is only: through any wrapping or a
// join, never by its text. Its WARN line carries the fields the same error logs
// at ERROR when the mutation declares nothing.
func TestCommitRefusalIsMatchedByIdentityAndKeepsTheFailureFields(t *testing.T) {
	refusal := errors.New("probe refusal text")
	declared := Mutation{
		Audit:    AuditEvent{EventType: "auth.probe.failed", Outcome: OutcomeError},
		Events:   []OutboxEvent{{AggregateType: "user", AggregateID: "u-1", EventType: "user.email_verified"}},
		Refusals: []error{refusal},
	}
	undeclared := declared
	undeclared.Refusals = nil
	logged := func(m Mutation, err error) map[string]any {
		t.Helper()
		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
		logCommitFailure(context.Background(), m, err)
		slog.SetDefault(previous)
		rec := map[string]any{}
		if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
			t.Fatalf("%v: %s", err, buf.String())
		}
		if strings.Contains(buf.String(), "probe refusal text") {
			t.Errorf("the error text reached the log: %s", buf.String())
		}
		delete(rec, "time")
		return rec
	}
	for name, tc := range map[string]struct {
		err   error
		level string
	}{
		"wrapped twice":   {fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, fmt.Errorf("outer: %w", refusal)), "WARN"},
		"joined":          {errors.Join(errors.New("other"), refusal), "WARN"},
		"same text":       {fmt.Errorf("%w: %w", ErrMutationFailed, errors.New("probe refusal text")), "ERROR"},
		"not in the list": {fmt.Errorf("%w: %w", ErrMutationFailed, ErrNoStateChange), "ERROR"},
	} {
		rec, failure := logged(declared, tc.err), logged(undeclared, tc.err)
		if rec["level"] != tc.level || failure["level"] != "ERROR" {
			t.Errorf("%s: level %v (undeclared %v), want %s (ERROR)", name, rec["level"], failure["level"], tc.level)
		}
		if _, ok := failure["refusal"]; ok {
			t.Errorf("%s: the undeclared line names a refusal: %v", name, failure)
		}
		if want := map[string]any{"WARN": "*errors.errorString", "ERROR": nil}[tc.level]; rec["refusal"] != want {
			t.Errorf("%s: refusal %v, want %v", name, rec["refusal"], want)
		}
		for _, r := range []map[string]any{rec, failure} {
			delete(r, "level")
			delete(r, "msg")
			delete(r, "refusal")
		}
		if !reflect.DeepEqual(rec, failure) {
			t.Errorf("%s: fields %v, want the failure line's %v", name, rec, failure)
		}
	}
}

type probeRefusal string

func (e probeRefusal) Error() string { return string(e) }

// A declared refusal's WARN line names its Go type (the refusal class), never
// its text; a failure the mutation does not declare names none.
func TestCommitRefusalLineNamesTheRefusalTypeNotItsText(t *testing.T) {
	refusal := probeRefusal("probe refusal text")
	other := errors.New("other refusal text")
	m := Mutation{Audit: AuditEvent{EventType: "auth.probe.failed", Outcome: OutcomeError}, OffStream: "probe", Refusals: []error{other, refusal}}
	for name, tc := range map[string]struct {
		err     error
		level   string
		refusal any
	}{
		"declared":   {fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, refusal), "WARN", "audit.probeRefusal"},
		"undeclared": {fmt.Errorf("%w: applying the state mutation: %w", ErrMutationFailed, probeRefusal("probe refusal text 2")), "ERROR", nil},
	} {
		var buf bytes.Buffer
		previous := slog.Default()
		slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
		logCommitFailure(context.Background(), m, tc.err)
		slog.SetDefault(previous)
		rec := map[string]any{}
		if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &rec); err != nil {
			t.Fatalf("%s: %v: %s", name, err, buf.String())
		}
		if rec["level"] != tc.level || rec["refusal"] != tc.refusal {
			t.Errorf("%s: level %v refusal %v, want %s %v", name, rec["level"], rec["refusal"], tc.level, tc.refusal)
		}
		if strings.Contains(buf.String(), "refusal text") {
			t.Errorf("%s: the error text reached the log: %s", name, buf.String())
		}
	}
}
