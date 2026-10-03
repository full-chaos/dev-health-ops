package tracing

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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

func TestInitFailureCounterMovesWhenTheProviderCannotBeBuilt(t *testing.T) {
	before := scrapeInitFailures(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "0.1")
	original := buildProvider
	buildProvider = func(string, string, string, float64) (*sdktrace.TracerProvider, error) {
		return nil, errors.New("provider construction failed")
	}
	t.Cleanup(func() { buildProvider = original })
	if c := InitWithServiceName(nil, "x"); c.provider != nil {
		t.Fatal("init must fail when the provider cannot be built")
	}
	after := scrapeInitFailures(t)
	if after == before {
		t.Fatalf("the provider construction failure path did not count:\n%s", after)
	}
}

func TestInitFailureCounterCountsExactlyOnePerFailureAndIsATypedCounter(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SAMPLE_RATE", "not-a-number")
	read := func() string {
		for _, line := range strings.Split(scrapeInitFailures(t), "\n") {
			if strings.HasPrefix(line, `dev_health_otel_init_failures_total{attempt="final"} `) {
				return line
			}
		}
		t.Fatal("final series missing")
		return ""
	}
	first := read()
	InitWithServiceName(nil, "x")
	second := read()
	n1, _ := strconv.Atoi(strings.TrimPrefix(first, `dev_health_otel_init_failures_total{attempt="final"} `))
	n2, _ := strconv.Atoi(strings.TrimPrefix(second, `dev_health_otel_init_failures_total{attempt="final"} `))
	if n2-n1 != 1 {
		t.Fatalf("one failed init must count exactly 1, moved by %d", n2-n1)
	}
	if !strings.Contains(scrapeInitFailures(t), "# TYPE dev_health_otel_init_failures_total counter\n") {
		t.Fatal("the family must be a counter, as Python's is")
	}
}
