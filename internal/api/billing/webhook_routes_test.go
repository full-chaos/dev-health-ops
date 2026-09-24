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
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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

// pythonWebhookDispatch reads router.py's stripe_webhook and returns the
// event types it dispatches by name and the prefixes it dispatches by
// startswith.
func pythonWebhookDispatch(t *testing.T) (types map[string]bool, prefixes []string) {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	source, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "src", "dev_health_ops", "api", "billing", "router.py"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	start := strings.Index(text, "\nasync def stripe_webhook(")
	if start < 0 {
		t.Fatal("router.py has no stripe_webhook")
	}
	body := text[start+1:]
	if end := regexp.MustCompile(`\n(async def|def|@|class) `).FindStringIndex(body[1:]); end != nil {
		body = body[:end[0]+1]
	}
	types = map[string]bool{}
	for _, match := range regexp.MustCompile(`event_type\s*==\s*"([^"]+)"`).FindAllStringSubmatch(body, -1) {
		types[match[1]] = true
	}
	for _, match := range regexp.MustCompile(`event_type\s+in\s*\(([^)]*)\)`).FindAllStringSubmatch(body, -1) {
		for _, quoted := range regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(match[1], -1) {
			types[quoted[1]] = true
		}
	}
	for _, match := range regexp.MustCompile(`event_type\.startswith\("([^"]+)"\)`).FindAllStringSubmatch(body, -1) {
		prefixes = append(prefixes, match[1])
	}
	return types, prefixes
}

// TestStripeEventRoutesCoverPythonDispatch holds the Go dispatch to the
// Python one: every type Python applies is routed here or named in
// stripeEventGaps, every gap is a type Python applies and Go does not
// route, and Go routes no type Python does not dispatch.
func TestStripeEventRoutesCoverPythonDispatch(t *testing.T) {
	types, prefixes := pythonWebhookDispatch(t)
	if len(types) < 7 || len(prefixes) == 0 {
		t.Fatalf("read %d types and %d prefixes from stripe_webhook: the parse measured too little", len(types), len(prefixes))
	}
	for eventType := range types {
		routed, gap := stripeEventRoute(eventType) != "", stripeEventGaps[eventType] != ""
		switch {
		case routed && gap:
			t.Errorf("%s is routed and still listed as a gap", eventType)
		case !routed && !gap:
			t.Errorf("%s: Python applies it; Go answers 200 and drops it (route it or name it in stripeEventGaps)", eventType)
		}
	}
	for _, prefix := range prefixes {
		for _, probe := range []string{prefix + "paid", prefix + "a_type_stripe_adds_later"} {
			if stripeEventRoute(probe) == "" {
				t.Errorf("%s: Python dispatches every %q type; Go drops it", probe, prefix)
			}
		}
	}
	for gap := range stripeEventGaps {
		if !types[gap] {
			t.Errorf("gap %s is not a type Python dispatches: a stale entry", gap)
		}
	}
	for eventType := range stripeEventRoutes {
		if !types[eventType] {
			t.Errorf("Go routes %s, which Python does not dispatch", eventType)
		}
	}
	t.Logf("python dispatches %d types and %v; named gaps: %d", len(types), prefixes, len(stripeEventGaps))
}

// TestUnhandledStripeEventIsLoggedAndCounted sends signed events of a gap
// type and of an unknown type through the route: each is a 200 that logs
// its type and event id and increments the counter.
func TestUnhandledStripeEventIsLoggedAndCounted(t *testing.T) {
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
	if code := send("charge.refunded", "evt_gap_unit"); code != http.StatusOK {
		t.Fatalf("charge.refunded answered %d", code)
	}
	if code := send("customer.created", "evt_other_unit"); code != http.StatusOK {
		t.Fatalf("customer.created answered %d", code)
	}
	for _, want := range []string{`"type":"charge.refunded"`, `"event_id":"evt_gap_unit"`, `"level":"WARN"`,
		`"type":"customer.created"`, `"event_id":"evt_other_unit"`} {
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
	if counts["charge.refunded"] != 1 || counts["other"] != 1 {
		t.Errorf("counter by event_type = %v, want charge.refunded=1 other=1", counts)
	}
}
