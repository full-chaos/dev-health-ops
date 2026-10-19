package synchandoff

import (
	"bytes"
	"testing"
)

func TestRefusalMetricsWriteOneSeriesPerReason(t *testing.T) {
	m := &refusalMetrics{count: map[string]uint64{}}
	m.observe(refusalUnselectedSource)
	m.observe(refusalUnnamedCaller)
	m.observe(refusalUnselectedSource)
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	want := "# HELP dev_health_sync_manual_trigger_refused_total Manual sync triggers refused before any write, by reason: unnamed_caller (the trigger path names no caller) or unselected_source (a source the configuration does not select).\n" +
		"# TYPE dev_health_sync_manual_trigger_refused_total counter\n" +
		`dev_health_sync_manual_trigger_refused_total{reason="unnamed_caller"} 1` + "\n" +
		`dev_health_sync_manual_trigger_refused_total{reason="unselected_source"} 2` + "\n"
	if out.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", out.String(), want)
	}
}
