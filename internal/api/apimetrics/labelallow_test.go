package apimetrics

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The provider every process installs filters labels by the pinned table: a planted unbounded label on a listed instrument
// and on an unlisted one never reaches the reader the /metrics text is written from.
func TestTheRealProviderDropsUnboundedLabels(t *testing.T) {
	provider, source, err := buildProvider(PushOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	meter := provider.Meter("test")
	listed, _ := meter.Int64Counter("dev_health_api_http_requests_total")
	unlisted, _ := meter.Int64Counter("dev_health_test_unlisted_total")
	ctx := context.Background()
	listed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("route", "/api/v1/scrape/{id}"), attribute.String("method", "GET"), attribute.String("status_class", "2xx"), attribute.String("listener", "internal"),
		attribute.String("request_id", "9f1c2d3e-0000-4000-8000-000000000001"), attribute.String("email", "synthetic.person@example.invalid")))
	unlisted.Add(ctx, 4, metric.WithAttributes(attribute.String("path", "/api/v1/orgs/synthetic-org/secret-path")))
	var text bytes.Buffer
	if err := source.WritePrometheus(&text); err != nil {
		t.Fatal(err)
	}
	out := text.String()
	for _, want := range []string{
		`dev_health_api_http_requests_total{listener="internal",method="GET",route="/api/v1/scrape/{id}",status_class="2xx"} 1`,
		`dev_health_test_unlisted_total 4`,
		`dev_health_metric_labels_dropped_total{instrument="dev_health_api_http_requests_total",label="request_id"}`,
		`dev_health_metric_labels_dropped_total{instrument="dev_health_test_unlisted_total",label="*"}`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics text lacks %q:\n%s", want, out)
		}
	}
	for _, leaked := range []string{"9f1c2d3e-0000-4000-8000-000000000001", "synthetic.person@example.invalid", "secret-path", `email=`, `path=`} {
		if strings.Contains(out, leaked) {
			t.Errorf("metrics text carries %q", leaked)
		}
	}
}
