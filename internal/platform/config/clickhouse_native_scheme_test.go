package config

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

func nonNativeGaugeByKey(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, found := range scope.Metrics {
			if found.Name != nonNativeGauge {
				continue
			}
			gauge, ok := found.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s: want int64 gauge, got %T", nonNativeGauge, found.Data)
			}
			for _, point := range gauge.DataPoints {
				key, _ := point.Attributes.Value("key")
				got[key.AsString()] = point.Value
			}
		}
	}
	return got
}

func TestServiceClickHouseDSNWarnsOnNonNativeScheme(t *testing.T) {
	for _, key := range []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"} {
		other := "API_CLICKHOUSE_URI"
		if key == other {
			other = "CLICKHOUSE_URI"
		}
		for _, scheme := range []string{"http", "https", "clickhouse"} {
			t.Run(key+"/"+scheme, func(t *testing.T) {
				reader := sdkmetric.NewManualReader()
				previous := otel.GetMeterProvider()
				otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
				t.Cleanup(func() { otel.SetMeterProvider(previous) })

				dsn := scheme + "://analytics:hunter2-secret@ch.internal:8123/default"
				cfg, err := Load(workerSpec(map[string]string{key: dsn}))
				if err != nil {
					t.Fatalf("Load refused a %s:// %s: %v", scheme, key, err)
				}
				var out bytes.Buffer
				ObserveNonNativeClickHouse(context.Background(), slog.New(slog.NewJSONHandler(&out, nil)), cfg)

				want := int64(0)
				if scheme != "clickhouse" {
					want = 1
				}
				gauge := nonNativeGaugeByKey(t, reader)
				if gauge[key] != want || gauge[other] != 0 {
					t.Fatalf("gauge = %v, want %s=%d and %s=0", gauge, key, want, other)
				}
				lines := strings.Split(strings.TrimSpace(out.String()), "\n")
				if want == 0 {
					if out.Len() != 0 {
						t.Fatalf("native DSN logged: %s", out.String())
					}
					return
				}
				if len(lines) != 1 {
					t.Fatalf("want one WARN line, got %d: %s", len(lines), out.String())
				}
				var record map[string]any
				if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
					t.Fatalf("log line: %v", err)
				}
				if record["level"] != "WARN" || record["key"] != key || record["scheme"] != scheme {
					t.Fatalf("record = %v, want WARN with key %s scheme %s", record, key, scheme)
				}
				for _, leak := range []string{"hunter2-secret", "analytics", "ch.internal", dsn} {
					if strings.Contains(out.String(), leak) {
						t.Fatalf("log leaks %q: %s", leak, out.String())
					}
				}
			})
		}
	}
}

func TestServiceClickHouseDSNAcceptsBracketedIPv6(t *testing.T) {
	t.Parallel()

	for _, dsn := range []string{"http://[::1]:8123/db", "https://[2001:db8::1]:8443/db", "http://h:8123/db"} {
		cfg, err := Load(workerSpec(map[string]string{"CLICKHOUSE_URI": dsn}))
		if err != nil || len(cfg.NonNativeClickHouseKeys) != 1 {
			t.Fatalf("Load(%q) = %v, flagged %v", dsn, err, cfg.NonNativeClickHouseKeys)
		}
	}
}

func TestServiceClickHouseDSNStillRefusesHostlessNonNative(t *testing.T) {
	t.Parallel()

	for _, key := range []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"} {
		for _, dsn := range []string{"http:///default", "https:///default", "https://:8123/x", "http://:8123", "http://,/default", "https://,/default", "https://,:8123/x", "http://[::1"} {
			t.Run(key+"/"+dsn, func(t *testing.T) {
				t.Parallel()
				if _, err := Load(workerSpec(map[string]string{key: dsn})); err == nil {
					t.Fatalf("%s accepted hostless %q", key, dsn)
				}
			})
		}
	}
}

func TestObserveNonNativeClickHouseNilLoggerStillSetsGauge(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })

	cfg, err := Load(workerSpec(map[string]string{"CLICKHOUSE_URI": "http://h:8123/db"}))
	if err != nil {
		t.Fatal(err)
	}
	ObserveNonNativeClickHouse(context.Background(), nil, cfg)
	if got := nonNativeGaugeByKey(t, reader); got["CLICKHOUSE_URI"] != 1 {
		t.Fatalf("gauge = %v, want CLICKHOUSE_URI=1", got)
	}
}

func TestResolveDSNStillAcceptsHTTPClickHouseForCLIs(t *testing.T) {
	t.Parallel()

	for _, dsn := range []string{"http://ch:ch@127.0.0.1:8123/default", "https://ch:ch@127.0.0.1:8443/default"} {
		got, configured, err := ResolveDSN(lookup(map[string]string{"CLICKHOUSE_URI": dsn}), "CLICKHOUSE_URI", ClickHouseSpec)
		if err != nil || !configured || got.Reveal() != dsn {
			t.Fatalf("ResolveDSN(%q) = %v, %v, %v", dsn, got.Configured(), configured, err)
		}
	}
}
