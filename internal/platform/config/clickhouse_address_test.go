package config

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func hostProblemGaugeByKeyCause(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatalf("collect: %v", err)
	}
	got := map[string]int64{}
	for _, scope := range data.ScopeMetrics {
		for _, found := range scope.Metrics {
			if found.Name != hostProblemGauge {
				continue
			}
			gauge, ok := found.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s: want int64 gauge, got %T", hostProblemGauge, found.Data)
			}
			for _, point := range gauge.DataPoints {
				key, _ := point.Attributes.Value("key")
				cause, _ := point.Attributes.Value("cause")
				got[key.AsString()+"/"+cause.AsString()] = point.Value
			}
		}
	}
	return got
}

func observeDSN(t *testing.T, key, dsn string) (Config, string, map[string]int64, map[string]int64, error) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previous) })
	cfg, err := Load(workerSpec(map[string]string{key: dsn}))
	if err != nil {
		return cfg, "", nil, nil, err
	}
	var out bytes.Buffer
	ObserveNonNativeClickHouse(context.Background(), slog.New(slog.NewJSONHandler(&out, nil)), cfg)
	return cfg, out.String(), nonNativeGaugeByKey(t, reader), hostProblemGaugeByKeyCause(t, reader), nil
}

func TestServiceClickHouseDSNAcceptsAddressLists(t *testing.T) {
	for _, key := range []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"} {
		for dsn, nonNative := range map[string]bool{
			"clickhouse://u:p@a:9000,b:9000/db":         false,
			"clickhouse://a:9000,b:9000/db?secure=true": false,
			"clickhouse://[::1]:9000,[fe80::2]:9000/db": false,
			"clickhouse://a,b:9000/db":                  false,
			"clickhouse://u:p@a:9000,b:9000":            false,
			"http://u:p@a:8443,b:8443/db":               true,
			"http://[::1]:8443,[fe80::2]:8443/db":       true,
			"https://a:8443,b:8443/db":                  true,
		} {
			t.Run(key+"/"+dsn, func(t *testing.T) {
				_, out, native, problems, err := observeDSN(t, key, dsn)
				if err != nil {
					t.Fatalf("Load refused a driver-valid address list: %v", err)
				}
				want := int64(0)
				if nonNative {
					want = 1
				}
				if native[key] != want {
					t.Fatalf("non-native gauge = %v, want %s=%d", native, key, want)
				}
				for cause, v := range problems {
					if v != 0 {
						t.Fatalf("host-problem gauge %s = %d on a valid list", cause, v)
					}
				}
				if (strings.Contains(out, "non-native")) != nonNative || strings.Contains(out, "no usable host") {
					t.Fatalf("WARN output = %q, want non-native=%v and no host warning", out, nonNative)
				}
			})
		}
	}
}

func TestServiceClickHouseDSNWarnsOnNoUsableHost(t *testing.T) {
	cases := map[string]string{
		"clickhouse://:9000/db":                         hostProblemNoUsableHost,
		"clickhouse://u:p@:9000/db":                     hostProblemNoUsableHost,
		"clickhouse://,/db":                             hostProblemNoUsableHost,
		"clickhouse://:9000,:9001/db":                   hostProblemNoUsableHost,
		"clickhouse://a:9000,:9001/db":                  hostProblemEmptyHostElement,
		"clickhouse://[::1]:9000,:9001/db":              hostProblemEmptyHostElement,
		"clickhouse://a:9000,,b:9000/db":                hostProblemEmptyHostElement,
		"clickhouse://a:9000/db?hosts=:9001":            hostProblemEmptyHostElement,
		"clickhouse://a:9000/db?alt_hosts=b:9000,:9001": hostProblemEmptyHostElement,
		"clickhouse://:9000/db?alt_hosts=b:9000":        hostProblemEmptyHostElement,
	}
	for _, key := range []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"} {
		for dsn, cause := range cases {
			t.Run(key+"/"+dsn, func(t *testing.T) {
				cfg, out, _, problems, err := observeDSN(t, key, dsn)
				if err != nil {
					t.Fatalf("Load refused a form that loads today (D6465): %v", err)
				}
				if !slices.Equal(cfg.ClickHouseHostProblems, []ClickHouseHostProblem{{Key: key, Cause: cause}}) {
					t.Fatalf("problems = %v, want %s/%s", cfg.ClickHouseHostProblems, key, cause)
				}
				for _, other := range []string{"CLICKHOUSE_URI", "API_CLICKHOUSE_URI"} {
					for _, c := range []string{hostProblemNoUsableHost, hostProblemEmptyHostElement} {
						want := int64(0)
						if other == key && c == cause {
							want = 1
						}
						if got := problems[other+"/"+c]; got != want {
							t.Fatalf("gauge %s/%s = %d, want %d (%v)", other, c, got, want, problems)
						}
					}
				}
				lines := strings.Split(strings.TrimSpace(out), "\n")
				if len(lines) != 1 {
					t.Fatalf("want one WARN line, got %d: %s", len(lines), out)
				}
				var record map[string]any
				if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
					t.Fatal(err)
				}
				if record["level"] != "WARN" || record["key"] != key || record["cause"] != cause {
					t.Fatalf("record = %v", record)
				}
				if strings.Contains(out, "9000") || strings.Contains(out, "u:p") {
					t.Fatalf("log leaks the DSN: %s", out)
				}
			})
		}
	}
}

func TestServiceClickHouseDSNStillRefusesWhatItRefusedBefore(t *testing.T) {
	for _, dsn := range []string{"clickhouse://a:9000,/db", "clickhouse://[::1]:9000,[not-ip]:9000/db", "http://a:8443,/db", "http://:8443,:8444/db", "https://a:8443,:8444/db", "http://[::1]:8443,[x]:1/db", "http://a:8443/db?alt_hosts=:1"} {
		if _, err := Load(workerSpec(map[string]string{"CLICKHOUSE_URI": dsn})); err == nil {
			t.Fatalf("Load accepted %q", dsn)
		}
	}
}

func TestClickHouseAddrsMatchDriverParseDSN(t *testing.T) {
	elements := []string{"a", "a:9000", "b:9001", ":9000", "", "[::1]:9000", "[fe80::2]", "[bad]:1", "[1.2.3.4]:1", "c:x"}
	queries := []string{"", "?hosts=p:1,,q", "?alt_hosts=:5,z", "?hosts=p&alt_hosts=q", "?hosts=%20p%20,q", "?alt_hosts=%20,r&hosts=s"}
	var dsns []string
	var build func(prefix []string, depth int)
	build = func(prefix []string, depth int) {
		if len(prefix) > 0 {
			for _, scheme := range []string{"clickhouse", "http"} {
				for _, q := range queries {
					dsns = append(dsns, scheme+"://u:p@"+strings.Join(prefix, ",")+"/db"+q)
				}
			}
		}
		if depth == 0 {
			return
		}
		for _, e := range elements {
			build(append(slices.Clone(prefix), e), depth-1)
		}
	}
	build(nil, 3)
	compared := 0
	for _, dsn := range dsns {
		options, err := clickhouse.ParseDSN(dsn)
		if err != nil {
			continue
		}
		parsed, ok := parseClickHouseDSN(dsn)
		if !ok {
			t.Fatalf("driver parsed %q, we did not", dsn)
		}
		if got := clickHouseAddrs(parsed); !slices.Equal(got, options.Addr) {
			t.Fatalf("%q: addrs = %q, driver = %q", dsn, got, options.Addr)
		}
		compared++
	}
	if compared < 1000 {
		t.Fatalf("only %d of %d seeded DSNs parsed by the driver, want >= 1000", compared, len(dsns))
	}
}
