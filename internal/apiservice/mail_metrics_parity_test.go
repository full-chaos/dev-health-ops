package apiservice

import (
	"bytes"
	"context"
	"regexp"
	"sort"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/linkmail"
)

var mailSeriesLabels = regexp.MustCompile(`\{purpose="([^"]+)",outcome="([^"]+)"\} 0$`)

// TestMailSendFamiliesShareOneLabelSet pins the go-api and auth-service
// sign-up mail counters to the same label names and the same purpose and
// outcome values, every series at 0 before any send.
func TestMailSendFamiliesShareOneLabelSet(t *testing.T) {
	var scrape bytes.Buffer
	if err := session.MailMetricsSource().WritePrometheus(&scrape); err != nil {
		t.Fatal(err)
	}
	api := map[string]bool{}
	for _, line := range bytes.Split(scrape.Bytes(), []byte("\n")) {
		if match := mailSeriesLabels.FindSubmatch(line); match != nil {
			api[string(match[1])+"/"+string(match[2])] = true
		}
	}

	reader := metric.NewManualReader()
	sender, err := linkmail.New(linkmail.Config{Meter: metric.NewMeterProvider(metric.WithReader(reader)).Meter("parity")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sender.Close(context.Background()) })
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	auth := map[string]bool{}
	labelNames := map[string]bool{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != linkmail.MetricName {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				purpose, _ := point.Attributes.Value("purpose")
				outcome, _ := point.Attributes.Value("outcome")
				if point.Value != 0 {
					t.Errorf("series %s/%s starts at %d, want 0", purpose.AsString(), outcome.AsString(), point.Value)
				}
				auth[purpose.AsString()+"/"+outcome.AsString()] = true
				for _, kv := range point.Attributes.ToSlice() {
					labelNames[string(kv.Key)] = true
				}
			}
		}
	}
	if len(labelNames) != 2 || !labelNames["purpose"] || !labelNames["outcome"] {
		t.Errorf("auth-service label names %v, want purpose and outcome", labelNames)
	}
	if len(api) == 0 || len(api) != len(auth) {
		t.Fatalf("series differ: go-api %v, auth-service %v", keysOf(api), keysOf(auth))
	}
	for key := range api {
		if !auth[key] {
			t.Errorf("series %s is only on the go-api family", key)
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
