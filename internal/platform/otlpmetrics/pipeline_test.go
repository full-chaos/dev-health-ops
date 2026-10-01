package otlpmetrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func envOf(values map[string]string) LookupFunc {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func TestOptionsFromEnv(t *testing.T) {
	cases := []struct {
		name        string
		env         map[string]string
		defaultName string
		enabled     bool
		service     string
		endpoint    string
		interval    time.Duration
		wantErr     bool
	}{
		{name: "defaults", defaultName: "dev-health-go-heavy", enabled: true, service: "dev-health-go-heavy", endpoint: "localhost:4317", interval: time.Minute},
		{name: "no binary default", enabled: true, service: "dev-health-ops", endpoint: "localhost:4317", interval: time.Minute},
		{name: "env service name wins", env: map[string]string{"OTEL_SERVICE_NAME": "x"}, defaultName: "bin", enabled: true, service: "x", endpoint: "localhost:4317", interval: time.Minute},
		{name: "tracing switched off", env: map[string]string{"OTEL_ENABLED": "false"}, defaultName: "bin", enabled: false, service: "bin", endpoint: "localhost:4317", interval: time.Minute},
		{name: "metrics switched off alone", env: map[string]string{"OTEL_METRICS_ENABLED": "0"}, defaultName: "bin", enabled: false, service: "bin", endpoint: "localhost:4317", interval: time.Minute},
		{name: "metrics 'no'", env: map[string]string{"OTEL_METRICS_ENABLED": "No"}, defaultName: "bin", enabled: false, service: "bin", endpoint: "localhost:4317", interval: time.Minute},
		{name: "url endpoint and interval", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector.example:4317", "OTEL_METRIC_EXPORT_INTERVAL": "15000"}, defaultName: "bin", enabled: true, service: "bin", endpoint: "http://collector.example:4317", interval: 15 * time.Second},
		{name: "malformed interval disables", env: map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "soon"}, defaultName: "bin", enabled: false, service: "bin", endpoint: "localhost:4317", interval: time.Minute, wantErr: true},
		{name: "zero interval disables", env: map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "0"}, defaultName: "bin", enabled: false, service: "bin", endpoint: "localhost:4317", interval: time.Minute, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			options, err := OptionsFromEnv(c.defaultName, envOf(c.env))
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if options.Enabled != c.enabled || options.ServiceName != c.service || options.Endpoint != c.endpoint || options.Interval != c.interval {
				t.Errorf("options = %+v", options)
			}
			if options.Environment != "production" {
				t.Errorf("environment = %q, want the tracing default production", options.Environment)
			}
		})
	}
}

func TestDialOptionsPicksByEndpointShape(t *testing.T) {
	if got := len(dialOptions("collector:4317")); got != 2 {
		t.Errorf("bare host:port options = %d, want endpoint + insecure", got)
	}
	if got := len(dialOptions("http://collector.example:4317")); got != 2 {
		t.Errorf("http URL options = %d, want url + insecure", got)
	}
	if got := len(dialOptions("https://collector:4317")); got != 1 {
		t.Errorf("https URL options = %d, want url only (TLS)", got)
	}
}

func TestAFailedExportIsLoggedAndCountedNotSwallowed(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	// Nothing listens on this port: the export must fail, and say so.
	pipeline, err := NewPipeline(context.Background(), Options{
		Enabled: true, Endpoint: "127.0.0.1:1", Interval: time.Hour, ServiceName: "svc",
	}, textSource{"s": "# TYPE a_total counter\na_total 1\n"}, nil, logger)
	if err != nil {
		t.Fatalf("NewPipeline must not dial: %v", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(pipeline.Reader), sdkmetric.WithResource(pipeline.Resource))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := provider.ForceFlush(ctx); err == nil {
		t.Fatal("a flush to a closed port reported success")
	}
	shutdownCtx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	_ = provider.Shutdown(shutdownCtx)
	if pipeline.Stats.ExportFailures() < 1 || pipeline.Stats.Exports() != 0 {
		t.Errorf("failures = %d exports = %d, want >=1 and 0", pipeline.Stats.ExportFailures(), pipeline.Stats.Exports())
	}
	if !strings.Contains(logs.String(), "otlp metrics export failed") || !strings.Contains(logs.String(), "127.0.0.1:1") {
		t.Errorf("no loud log line naming the endpoint: %s", logs.String())
	}
	var text bytes.Buffer
	if err := pipeline.Stats.WritePrometheus(&text); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "dev_health_otlp_metrics_export_failures_total ") || strings.Contains(text.String(), "dev_health_otlp_metrics_export_failures_total 0") {
		t.Errorf("the failure is not on /metrics: %s", text.String())
	}
}

type failingSource struct{}

func (failingSource) EachMetricsFragment(skip map[string]bool, fn func(string, []byte, error)) {
	fn("good", []byte("# TYPE good_total counter\ngood_total 2\n"), nil)
	fn("broken_write", nil, errors.New("disk on fire"))
	fn("broken_parse", []byte("# TYPE bad counter\nbad{unterminated 1\n"), nil)
	fn("later_good", []byte("# TYPE later gauge\nlater 7\n"), nil)
}

func TestABrokenSourceIsSkippedLoudlyAndTheOthersStillFlow(t *testing.T) {
	var logs bytes.Buffer
	stats := &Stats{}
	gatherer := &Gatherer{Source: failingSource{}, Logger: slog.New(slog.NewTextHandler(&logs, nil)), Stats: stats}
	families, err := gatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, f := range families {
		names = append(names, f.GetName())
	}
	if fmt.Sprint(names) != "[good_total later]" {
		t.Errorf("families = %v, want the two healthy sources", names)
	}
	failures := stats.SourceFailures()
	if failures["broken_write"] != 1 || failures["broken_parse"] != 1 || len(failures) != 2 {
		t.Errorf("source failures = %v", failures)
	}
	for _, want := range []string{"broken_write", "broken_parse", "disk on fire"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log does not name %q: %s", want, logs.String())
		}
	}
}

func TestTwoSourcesSharingAFamilyMergeAndATypeConflictIsCounted(t *testing.T) {
	stats := &Stats{}
	gatherer := &Gatherer{Source: textSource{
		"a": "# TYPE shared gauge\nshared 1\n",
		"b": "# TYPE shared gauge\nshared{stream=\"x\"} 2\n# TYPE clash counter\nclash 1\n",
		"c": "# TYPE clash gauge\nclash 9\n",
	}, Stats: stats}
	families, _ := gatherer.Gather()
	byName := map[string]int{}
	for _, f := range families {
		byName[f.GetName()] = len(f.GetMetric())
	}
	if byName["shared"] != 2 {
		t.Errorf("shared family series = %d, want both sources' series merged", byName["shared"])
	}
	if byName["clash"] != 1 || stats.SourceFailures()["c"] != 1 {
		t.Errorf("clash series = %d, failures = %v: the conflicting source must be dropped and counted", byName["clash"], stats.SourceFailures())
	}
}

func TestNoFamiliesIsNotAFailureButAnEmptyComparisonIs(t *testing.T) {
	stats := &Stats{}
	families, err := (&Gatherer{Source: textSource{}, Stats: stats}).Gather()
	if err != nil || len(families) != 0 || len(stats.SourceFailures()) != 0 {
		t.Errorf("families = %d err = %v failures = %v", len(families), err, stats.SourceFailures())
	}
}
