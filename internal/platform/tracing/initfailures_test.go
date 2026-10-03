package tracing

import (
	"bytes"
	"strings"
	"testing"
)

func scrapeInitFailures(t *testing.T) string {
	t.Helper()
	var out bytes.Buffer
	if err := InitFailuresSource().WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

func TestInitFailureCounterIsPresentAtZeroAndMovesOnBadSampleRate(t *testing.T) {
	before := scrapeInitFailures(t)
	if !strings.Contains(before, `dev_health_otel_init_failures_total{attempt="initial"} 0`) || !strings.Contains(before, `{attempt="final"} `) {
		t.Fatalf("both series must be present:\n%s", before)
	}
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "not-a-number")
	if c := InitWithServiceName(nil, "x"); c.provider != nil {
		t.Fatal("init must fail on a bad sample rate")
	}
	after := scrapeInitFailures(t)
	if after == before {
		t.Fatalf("a failed init did not move the counter:\n%s", after)
	}
	if !strings.Contains(after, `{attempt="initial"} 0`) {
		t.Fatalf("initial must stay zero (Go does not retry):\n%s", after)
	}
}

func TestInitFailureCounterStaysFlatWhenTracingIsDisabled(t *testing.T) {
	before := scrapeInitFailures(t)
	t.Setenv("OTEL_ENABLED", "false")
	InitWithServiceName(nil, "x")
	if scrapeInitFailures(t) != before {
		t.Fatal("a disabled tracer is not a failure")
	}
}

// NOT pinned: the newProvider failure path's record() (tracing.go). The exporter
// accepts every endpoint shape the environment can supply, so no input drives
// it without a seam; the sample-rate path above shares the same one-line call.
