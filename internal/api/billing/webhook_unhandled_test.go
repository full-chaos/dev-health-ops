package billing

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// TestUnhandledStripeEventIsLoggedAndCounted sends signed events of a gap
// type and of an unknown type through the route: each is a 200 that logs
// its type and event id and increments the counter. The gap list is empty
// (every type Python applies is routed), so the test names one of its own.
func TestUnhandledStripeEventIsLoggedAndCounted(t *testing.T) {
	const gapType = "test.gap_unit"
	stripeEventGaps[gapType] = "a gap named by this test"
	t.Cleanup(func() { delete(stripeEventGaps, gapType) })
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var logs bytes.Buffer
	now := time.Unix(1790000000, 0)
	h := handlers{stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}),
		webhookSecret: secrets.NewValue("whsec_unit"), logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		now: func() time.Time { return now }}
	send := func(eventType, id string) int {
		body := fmt.Sprintf(`{"id": %q, "object": "event", "type": %q, "data": {"object": {}}}`, id, eventType)
		mac := hmac.New(sha256.New, []byte("whsec_unit"))
		fmt.Fprintf(mac, "%d.%s", now.Unix(), body)
		request := httptest.NewRequest(http.MethodPost, "/api/v1/billing/webhooks/stripe", strings.NewReader(body))
		request.Header.Set("Stripe-Signature", fmt.Sprintf("t=%d,v1=%s", now.Unix(), hex.EncodeToString(mac.Sum(nil))))
		recorder := httptest.NewRecorder()
		h.stripeWebhook(recorder, request)
		return recorder.Code
	}
	if code := send(gapType, "evt_gap_unit"); code != http.StatusOK {
		t.Fatalf("gap type answered %d", code)
	}
	if code := send("customer.created", "evt_other_unit"); code != http.StatusOK {
		t.Fatalf("customer.created answered %d", code)
	} // A charge.refunded whose charge carries no refund list (every current
	// Stripe API version) writes nothing, so it is logged and counted too.
	if code := send("charge.refunded", "evt_no_list_unit"); code != http.StatusOK {
		t.Fatalf("charge.refunded answered %d", code)
	}
	for _, want := range []string{`"type":"` + gapType + `"`, `"event_id":"evt_gap_unit"`, `"level":"WARN"`,
		`"type":"customer.created"`, `"event_id":"evt_other_unit"`, `"event_id":"evt_no_list_unit"`, "no refund list"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log lacks %s:\n%s", want, logs.String())
		}
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, series := range scope.Metrics {
			if series.Name != "dev_health_api_stripe_webhook_unhandled_events_total" {
				continue
			}
			for _, point := range series.Data.(metricdata.Sum[int64]).DataPoints {
				label, _ := point.Attributes.Value(attribute.Key("event_type"))
				counts[label.AsString()] += point.Value
			}
		}
	}
	if counts[gapType] != 1 || counts["other"] != 1 || counts["charge.refunded"] != 1 {
		t.Errorf("counter by event_type = %v, want the gap type=1 other=1 charge.refunded=1", counts)
	}
}
