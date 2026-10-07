package syncadmin

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestSelectionMetricsCountPerProviderAndDirectionAndIgnoreZero(t *testing.T) {
	m := newSelectionMetrics()
	m.observeRows("github", "enabled", 2)
	m.observeRows("github", "enabled", 1)
	m.observeRows("github", "disabled", 4)
	m.observeRows("jira", "disabled", 0)
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"# TYPE sync_config_dataset_rows_changed_total counter\n",
		`sync_config_dataset_rows_changed_total{provider="github",direction="disabled"} 4` + "\n",
		`sync_config_dataset_rows_changed_total{provider="github",direction="enabled"} 3` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	// A zero observation creates no series, and the family of the retired
	// base list is not written.
	if strings.Contains(got, "jira") || strings.Contains(got, "stale_base") {
		t.Fatalf("a zero observation must not create a series and no stale-base family is written:\n%s", got)
	}
}

// TestRecordSelectionChangeCountsAndLogsOnlyWhatHappened: a save that changed
// no row counts nothing and logs nothing; a save that changed rows counts
// each direction and logs the keys.
func TestRecordSelectionChangeCountsAndLogsOnlyWhatHappened(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	read := func() string {
		var out bytes.Buffer
		if err := SelectionMetricsSource().WritePrometheus(&out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	before := read()
	recordSelectionChange(context.Background(), logger, "org", uuid.New(), "selectiontest", nil, nil)
	if read() != before || logs.Len() != 0 {
		t.Fatalf("a save that changed nothing must not count or log; logs=%q", logs.String())
	}
	recordSelectionChange(context.Background(), logger, "org", uuid.New(), "selectiontest", []string{"prs", "pr-reviews"}, []string{"cicd"})
	got := read()
	for _, want := range []string{
		`sync_config_dataset_rows_changed_total{provider="selectiontest",direction="enabled"} 2` + "\n",
		`sync_config_dataset_rows_changed_total{provider="selectiontest",direction="disabled"} 1` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	for _, want := range []string{"sync_config_dataset_rows_changed", "enabled_dataset_keys=prs,pr-reviews", "disabled_dataset_keys=cicd"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log misses %q: %q", want, logs.String())
		}
	}
}
