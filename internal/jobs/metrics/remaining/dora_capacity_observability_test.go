package remaining

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// capturingLogHandler records emitted slog.Record values so tests can assert
// on structured attributes directly (org_id/day/team_id/etc.) rather than on
// a rendered text line. Asserting on rendered text would pin the handler's
// formatting, not the record, and would pass just as happily if every
// attribute collapsed into one string -- which is the failure mode that makes
// a "structured" log unqueryable in production. Mirrors the pattern already
// used for this in internal/providersync/github_project_membership_log_test.go
// and internal/platform/lifecycle/runtime_test.go.
type capturingLogHandler struct {
	records []slog.Record
}

func (h *capturingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingLogHandler) Handle(_ context.Context, record slog.Record) error {
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *capturingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingLogHandler) WithGroup(string) slog.Handler      { return h }

func logRecordAttrs(record slog.Record) map[string]any {
	attrs := make(map[string]any, record.NumAttrs())
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}

func findLogRecords(records []slog.Record, message string) []slog.Record {
	var found []slog.Record
	for _, record := range records {
		if record.Message == message {
			found = append(found, record.Clone())
		}
	}
	return found
}

// attrInt64 reads an integer attribute regardless of whether slog stored it
// as int or int64 -- IntValue(v int) is stored as Int64Value(int64(v))
// internally, so a naive `attrs[key] != someInt` comparison against a Go int
// literal fails on the type mismatch even when the values agree.
func attrInt64(t *testing.T, attrs map[string]any, key string) int64 {
	t.Helper()
	switch v := attrs[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	default:
		t.Fatalf("attribute %q is %T, not an int", key, attrs[key])
		return 0
	}
}

// TestDORALogsPerOrgDayRowsWritten pins CHAOS-5382 F1 for the dora family:
// the deleted Python job (job_dora.py:247-251) logged one Info line per
// (org, day) naming how many metric rows it wrote; the native executor's
// DORAObserver counter reports only a partition-level aggregate, with no way
// to tell which org/day pair moved or that one moved at all. This drives a
// 2-org fixture (2 days each, one of them a quiet day) through
// logPartitionDay directly and asserts the restored line's fields.
func TestDORALogsPerOrgDayRowsWritten(t *testing.T) {
	handler := &capturingLogHandler{}
	executor := &DORAExecutor{logger: slog.New(handler)}

	fixture := []struct {
		orgID string
		day   string
		rows  int
	}{
		{orgID: "org-a", day: "2026-08-20", rows: 3},
		// A quiet day: job_dora.py only logged inside `if rows:`, so a day
		// that wrote nothing must stay silent here too, not gain a new line
		// the deleted job never had.
		{orgID: "org-a", day: "2026-08-21", rows: 0},
		{orgID: "org-b", day: "2026-08-20", rows: 5},
		{orgID: "org-b", day: "2026-08-21", rows: 2},
	}
	for _, row := range fixture {
		day, err := time.Parse("2006-01-02", row.day)
		if err != nil {
			t.Fatal(err)
		}
		executor.logPartitionDay(row.orgID, day, row.rows)
	}

	records := findLogRecords(handler.records, "dora: wrote metric rows")
	if len(records) != 3 {
		t.Fatalf("got %d log records, want 3 (the zero-row day must stay silent): %v",
			len(records), records)
	}

	want := []struct {
		orgID string
		day   string
		rows  int64
	}{
		{orgID: "org-a", day: "2026-08-20", rows: 3},
		{orgID: "org-b", day: "2026-08-20", rows: 5},
		{orgID: "org-b", day: "2026-08-21", rows: 2},
	}
	for index, record := range records {
		if record.Level != slog.LevelInfo {
			t.Errorf("record %d level = %v, want Info", index, record.Level)
		}
		attrs := logRecordAttrs(record)
		if attrs["org_id"] != want[index].orgID {
			t.Errorf("record %d org_id = %v, want %v", index, attrs["org_id"], want[index].orgID)
		}
		if attrs["day"] != want[index].day {
			t.Errorf("record %d day = %v, want %v", index, attrs["day"], want[index].day)
		}
		if attrs["table"] != "dora_metrics_daily" {
			t.Errorf("record %d table = %v, want dora_metrics_daily", index, attrs["table"])
		}
		if got := attrInt64(t, attrs, "rows_written"); got != want[index].rows {
			t.Errorf("record %d rows_written = %d, want %d", index, got, want[index].rows)
		}
	}
}

// TestCapacityLogsPerTeamScope pins CHAOS-5382 F1 for the capacity family:
// the deleted Python job (job_capacity.py:78) logged one Info line per
// (team, work_scope) before computing its forecast, plus Warning lines for
// scopes it skipped (no history, no target items) or flagged (insufficient
// history, high variance) -- all lost when the native executor's
// CapacityObserver counters replaced them with an aggregate skip count. This
// drives a 2-org, 2-team fixture through the restored logging helpers and
// asserts every line's fields.
func TestCapacityLogsPerTeamScope(t *testing.T) {
	handler := &capturingLogHandler{}
	executor := &CapacityExecutor{logger: slog.New(handler)}

	teamA, scopeA := "team-a", "scope-a"
	teamB, scopeB := "team-b", "scope-b"

	fixture := []struct {
		orgID  string
		target capacityTarget
	}{
		{orgID: "org-a", target: capacityTarget{TeamID: &teamA, WorkScopeID: &scopeA}},
		{orgID: "org-a", target: capacityTarget{TeamID: &teamB, WorkScopeID: &scopeB}},
		{orgID: "org-b", target: capacityTarget{TeamID: &teamA, WorkScopeID: &scopeA}},
		{orgID: "org-b", target: capacityTarget{TeamID: &teamB, WorkScopeID: &scopeB}},
	}
	for _, entry := range fixture {
		executor.logScopeStart(entry.orgID, entry.target)
	}

	records := findLogRecords(handler.records, "capacity: computing forecast")
	if len(records) != len(fixture) {
		t.Fatalf("got %d log records, want %d", len(records), len(fixture))
	}
	for index, record := range records {
		if record.Level != slog.LevelInfo {
			t.Errorf("record %d level = %v, want Info", index, record.Level)
		}
		attrs := logRecordAttrs(record)
		want := fixture[index]
		if attrs["org_id"] != want.orgID {
			t.Errorf("record %d org_id = %v, want %v", index, attrs["org_id"], want.orgID)
		}
		if attrs["team_id"] != *want.target.TeamID {
			t.Errorf("record %d team_id = %v, want %v", index, attrs["team_id"], *want.target.TeamID)
		}
		if attrs["work_scope_id"] != *want.target.WorkScopeID {
			t.Errorf("record %d work_scope_id = %v, want %v",
				index, attrs["work_scope_id"], *want.target.WorkScopeID)
		}
	}

	// The skip/flag warnings restore WHY a scope produced no row or a flagged
	// one -- the aggregate skip counter alone cannot say.
	target := capacityTarget{TeamID: &teamA, WorkScopeID: &scopeA}
	executor.logNoHistory("org-a", target)
	executor.logNoTargetItems("org-a", target)
	executor.logInsufficientHistory("org-a", target, 12)
	executor.logHighVariance("org-a", target)
	executor.logWroteForecasts("org-a", 4)

	assertWarn := func(message string, wantHistoryDays *int64) {
		t.Helper()
		found := findLogRecords(handler.records, message)
		if len(found) != 1 {
			t.Fatalf("message %q: got %d records, want 1", message, len(found))
		}
		if found[0].Level != slog.LevelWarn {
			t.Errorf("message %q level = %v, want Warn", message, found[0].Level)
		}
		attrs := logRecordAttrs(found[0])
		if attrs["org_id"] != "org-a" {
			t.Errorf("message %q org_id = %v, want org-a", message, attrs["org_id"])
		}
		if attrs["team_id"] != teamA {
			t.Errorf("message %q team_id = %v, want %v", message, attrs["team_id"], teamA)
		}
		if attrs["work_scope_id"] != scopeA {
			t.Errorf("message %q work_scope_id = %v, want %v", message, attrs["work_scope_id"], scopeA)
		}
		if wantHistoryDays != nil {
			if got := attrInt64(t, attrs, "history_days"); got != *wantHistoryDays {
				t.Errorf("message %q history_days = %d, want %d", message, got, *wantHistoryDays)
			}
		}
	}
	assertWarn("capacity: no throughput history for team/scope", nil)
	assertWarn("capacity: no target items for team/scope", nil)
	twelve := int64(12)
	assertWarn("capacity: insufficient history for team/scope", &twelve)
	assertWarn("capacity: high throughput variance detected for team/scope", nil)

	info := findLogRecords(handler.records, "capacity: wrote forecast rows")
	if len(info) != 1 {
		t.Fatalf("got %d 'wrote forecast rows' records, want 1", len(info))
	}
	if info[0].Level != slog.LevelInfo {
		t.Errorf("level = %v, want Info", info[0].Level)
	}
	attrs := logRecordAttrs(info[0])
	if attrs["org_id"] != "org-a" {
		t.Errorf("org_id = %v, want org-a", attrs["org_id"])
	}
	if attrs["table"] != "capacity_forecasts" {
		t.Errorf("table = %v, want capacity_forecasts", attrs["table"])
	}
	if got := attrInt64(t, attrs, "rows_written"); got != 4 {
		t.Errorf("rows_written = %d, want 4", got)
	}

	// A partition that forecasts nothing must stay silent, matching Python's
	// `if persist and results:` gate (job_capacity.py:114).
	before := len(handler.records)
	executor.logWroteForecasts("org-b", 0)
	if len(handler.records) != before {
		t.Error("logWroteForecasts(0) must not log, matching the deleted job's gate")
	}
}
