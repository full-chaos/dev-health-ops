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
	m.observeStaleBase("gitlab")
	m.observeStaleBase("gitlab")
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"# TYPE sync_config_dataset_rows_changed_total counter\n",
		`sync_config_dataset_rows_changed_total{provider="github",direction="disabled"} 4` + "\n",
		`sync_config_dataset_rows_changed_total{provider="github",direction="enabled"} 3` + "\n",
		"# TYPE sync_config_save_stale_base_total counter\n",
		`sync_config_save_stale_base_total{provider="gitlab"} 2` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	// A zero observation creates no series.
	if strings.Contains(got, "jira") {
		t.Fatalf("a zero observation must not create a series:\n%s", got)
	}
}

// TestRecordSelectionChangeCountsAndLogsOnlyWhatHappened: a save that changed
// no row and had no stale base counts nothing and logs nothing; a save that
// changed rows counts each direction and logs the keys; a stale base counts
// once and logs without any target list.
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
	recordSelectionChange(context.Background(), logger, "org", uuid.New(), "selectiontest", nil, nil, false)
	if read() != before || logs.Len() != 0 {
		t.Fatalf("a save that changed nothing must not count or log; logs=%q", logs.String())
	}
	recordSelectionChange(context.Background(), logger, "org", uuid.New(), "selectiontest", []string{"prs", "pr-reviews"}, []string{"cicd"}, true)
	got := read()
	for _, want := range []string{
		`sync_config_dataset_rows_changed_total{provider="selectiontest",direction="enabled"} 2` + "\n",
		`sync_config_dataset_rows_changed_total{provider="selectiontest",direction="disabled"} 1` + "\n",
		`sync_config_save_stale_base_total{provider="selectiontest"} 1` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
	for _, want := range []string{"sync_config_dataset_rows_changed", "enabled_dataset_keys=prs,pr-reviews", "disabled_dataset_keys=cicd", "sync_config_save_stale_base"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log misses %q: %q", want, logs.String())
		}
	}
}
