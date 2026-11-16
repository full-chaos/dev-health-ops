package providersync

import (
	"strings"
	"testing"
)

const nonFiniteCounterName = "dev_health_cicd_nonfinite_test_time_total"

func assertNonFiniteCounter(t *testing.T, walk nonFiniteWalk, route, value string) {
	t.Helper()
	var rendered strings.Builder
	if err := walk.metrics.WritePrometheus(&rendered); err != nil {
		t.Fatal(err)
	}
	line := nonFiniteCounterName + `{provider="gitlab",dataset="tests"} 1`
	wantCounted := value != "-3" && value != "1.25"
	if got := strings.Contains(rendered.String(), line); got != wantCounted {
		t.Fatalf("%s/%s: counter line %q present=%v, want %v", route, value, line, got, wantCounted)
	}
	if !wantCounted && strings.Contains(rendered.String(), nonFiniteCounterName+"{") {
		t.Fatalf("%s/%s: counter series exists for a finite value", route, value)
	}
}
