package syncadmin

import (
	"bytes"
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestDriftedDatasetKeysExcludesWhatTheDeltaNames(t *testing.T) {
	// "a" was desired before and is not now: the edit itself disabled it (not drift).
	// "b" was never desired before and is disabled now: the planner was syncing it unaccounted for.
	got := driftedDatasetKeys([]string{"a", "b"}, map[string]bool{"a": true}, map[string]bool{})
	if !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("drifted = %v, want [b]", got)
	}
	if got := driftedDatasetKeys(nil, nil, nil); len(got) != 0 {
		t.Fatalf("nothing disabled, drifted = %v", got)
	}
}

func TestDriftRepairedMetricsCountPerProviderAndIgnoreZero(t *testing.T) {
	m := &DriftRepairedMetrics{repaired: map[string]uint64{}}
	m.observe("github", 2)
	m.observe("github", 1)
	m.observe("jira", 1)
	m.observe("jira", 0)
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"# TYPE sync_target_dataset_drift_repaired_total counter\n",
		`sync_target_dataset_drift_repaired_total{provider="github"} 3` + "\n",
		`sync_target_dataset_drift_repaired_total{provider="jira"} 1` + "\n",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %q in:\n%s", want, got)
		}
	}
}

func TestRecordDatasetDriftCountsByProviderAndLogsOnlyWhenDrifted(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	read := func() string {
		var out bytes.Buffer
		if err := DriftRepairedMetricsSource().WritePrometheus(&out); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	before := read()
	// "a" is the edit's own disable (named by the delta): no drift, nothing counted, nothing logged.
	recordDatasetDrift(context.Background(), logger, "org", uuid.New(), "drifttest", []string{"a"}, map[string]bool{"a": true}, map[string]bool{})
	if read() != before || logs.Len() != 0 {
		t.Fatalf("no drift must not count or log; logs=%q", logs.String())
	}
	recordDatasetDrift(context.Background(), logger, "org", uuid.New(), "drifttest", []string{"a", "b", "c"}, map[string]bool{"a": true}, map[string]bool{})
	if !strings.Contains(read(), `sync_target_dataset_drift_repaired_total{provider="drifttest"} 2`+"\n") {
		t.Fatalf("two drifted keys (b, c) must count 2:\n%s", read())
	}
	if !strings.Contains(logs.String(), "sync_target_dataset_drift_repaired") || !strings.Contains(logs.String(), "drifted_count=2") {
		t.Fatalf("the WARN log must still fire: %q", logs.String())
	}
}
