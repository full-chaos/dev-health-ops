package investmentexplain

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type failingWriteConn struct{ err error }

func (c failingWriteConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, c.err
}

// TestExplainWriteFailuresAreLoggedAndCountedAndTheRequestStillSucceeds: both best-effort writes fail (the batch cannot
// be prepared). The request answers as before, and each failed write leaves one WARN with the error class and Go type
// (never the error text) and one count, so a cache that stopped persisting is visible.
func TestExplainWriteFailuresAreLoggedAndCountedAndTheRequestStillSucceeds(t *testing.T) {
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	reader := sdkmetric.NewManualReader()
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	t.Cleanup(func() { otel.SetMeterProvider(previousProvider) })

	const secret = "dsn=clickhouse://user:hunter2@host/db"
	writer, err := NewCacheWriter(failingWriteConn{err: errors.New(secret)})
	if err != nil {
		t.Fatal(err)
	}
	explainReader, err := NewReader(mixAgreementClient{breakdown: mixRows()})
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	got, err := explainReader.ExplainInvestmentMix(context.Background(), writer, alwaysAvailable, countingComplete(completionWithKeys(10), &calls), explainOpts())
	if err != nil {
		t.Fatalf("the request failed on a failed best-effort write: %v", err)
	}
	if calls != 1 || got.Summary == "" {
		t.Fatalf("calls = %d, summary %q: the request outcome moved", calls, got.Summary)
	}

	out := logs.String()
	for _, write := range []string{"write=cache", "write=token_usage"} {
		line := ""
		for _, l := range strings.Split(out, "\n") {
			if strings.Contains(l, "query_api.investment_mix_explain.write_failed") && strings.Contains(l, write) {
				line = l
			}
		}
		if line == "" {
			t.Fatalf("no write_failed WARN for %s, logs:\n%s", write, out)
		}
		for _, want := range []string{"level=WARN", "org_id=org-9555", "error_class=", "error_type="} {
			if !strings.Contains(line, want) {
				t.Errorf("%s line lacks %q: %s", write, want, line)
			}
		}
	}
	if strings.Contains(out, "hunter2") || strings.Contains(out, secret) {
		t.Fatalf("the error text reached the logs:\n%s", out)
	}

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_query_api_investment_explain_write_failed_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric data is %T, want Sum[int64]", m.Data)
			}
			for _, point := range sum.DataPoints {
				value, _ := point.Attributes.Value("write")
				counts[value.AsString()] += point.Value
			}
		}
	}
	if counts["cache"] != 1 || counts["token_usage"] != 1 {
		t.Fatalf("write failure counts = %v, want cache=1 token_usage=1", counts)
	}
}
