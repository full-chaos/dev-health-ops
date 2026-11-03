package removedenv

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const gaugeName = "dev_health_auth_orgs_owned_ignored"

func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func useReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	return reader
}

func gaugeValue(t *testing.T, reader *sdkmetric.ManualReader) (int64, bool) {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, found := range scope.Metrics {
			if found.Name != gaugeName {
				continue
			}
			gauge, ok := found.Data.(metricdata.Gauge[int64])
			if !ok || len(gauge.DataPoints) != 1 {
				t.Fatalf("%s: want one int64 gauge point, got %T", gaugeName, found.Data)
			}
			if len(gauge.DataPoints[0].Attributes.ToSlice()) != 0 {
				t.Fatalf("%s carries labels", gaugeName)
			}
			return gauge.DataPoints[0].Value, true
		}
	}
	return 0, false
}

func TestASetKeyIsIgnoredWithOneErrorLineAndTheGauge(t *testing.T) {
	for _, value := range []string{"true", "false", "TRUE", "1", "zz-not-a-switch-4471", " true "} {
		reader := useReader(t)
		var logs bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logs, nil))
		if !ObserveOrgsOwned(context.Background(), lookupOf(map[string]string{OrgsOwnedKey: value}), logger) {
			t.Errorf("%q: not reported as set", value)
		}
		lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
		if len(lines) != 1 || lines[0] == "" {
			t.Fatalf("%q: %d log lines, want 1: %q", value, len(lines), logs.String())
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
			t.Fatalf("%q: %v", value, err)
		}
		if record["level"] != "ERROR" || !strings.Contains(lines[0], OrgsOwnedKey) {
			t.Errorf("%q: want an ERROR line naming %s, got %s", value, OrgsOwnedKey, lines[0])
		}
		if strings.Contains(value, "zz-not-a-switch") && strings.Contains(lines[0], "zz-not-a-switch") {
			t.Errorf("%q: the value reached the log: %s", value, lines[0])
		}
		if got, ok := gaugeValue(t, reader); !ok || got != 1 {
			t.Errorf("%q: gauge = %d (found %v), want 1", value, got, ok)
		}
	}
}

func TestAnUnsetKeyLogsNothingAndTheGaugeIsZero(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"absent": nil, "empty": {OrgsOwnedKey: ""}, "blank": {OrgsOwnedKey: "  "},
	} {
		reader := useReader(t)
		var logs bytes.Buffer
		if ObserveOrgsOwned(context.Background(), lookupOf(values), slog.New(slog.NewJSONHandler(&logs, nil))) {
			t.Errorf("%s: reported as set", name)
		}
		if logs.Len() != 0 {
			t.Errorf("%s: logged %q", name, logs.String())
		}
		if got, ok := gaugeValue(t, reader); !ok || got != 0 {
			t.Errorf("%s: gauge = %d (found %v), want 0", name, got, ok)
		}
	}
}

func TestANilLoggerDoesNotPanic(t *testing.T) {
	ObserveOrgsOwned(context.Background(), lookupOf(map[string]string{OrgsOwnedKey: "true"}), nil)
}
