package billing

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// TestARefusedStripeSignatureIsLoggedAndCountedByClass sends, through the
// real route, one request of each way a signature check fails. Each must get
// the same answer as before (400, the same body bytes) AND leave one warning
// line with its class and one count under that class. The line must hold
// nothing of the request: not the header value, not the body, not the secret.
//
// A request with a good signature must leave no such line and no count.
func TestARefusedStripeSignatureIsLoggedAndCountedByClass(t *testing.T) {
	const (
		signingSecret = "whsec_refusal_unit"
		otherSecret   = "whsec_not_the_configured_one"
		bodyMarker    = "body-marker-9f3c"
		body          = `{"id": "evt_refusal_unit", "object": "event", "type": "customer.created", "data": {"object": {"note": "` + bodyMarker + `"}}}`
		refusal       = `{"detail":"Invalid Stripe signature"}`
		line          = "Stripe webhook refused: the signature check failed"
		counter       = "dev_health_api_stripe_webhook_signature_failures_total"
	)
	now := time.Unix(1790000000, 0)
	sign := func(secret string, at time.Time) string {
		mac := hmac.New(sha256.New, []byte(secret))
		fmt.Fprintf(mac, "%d.%s", at.Unix(), body)
		return hex.EncodeToString(mac.Sum(nil))
	}
	good := sign(signingSecret, now)
	tooOld := now.Add(-(stripeWebhookTolerance + 1) * time.Second)
	// The edge of the tolerance: a timestamp of exactly now - 300 s is refused
	// as soon as the clock is a fraction of a second past the whole second.
	edge := now.Add(-stripeWebhookTolerance * time.Second)
	pastTheSecond := now.Add(time.Nanosecond)

	cases := []struct {
		name   string
		header string // "" sends no header
		class  string
		at     time.Time // the clock of the route; zero is now
	}{
		{name: "no header", class: "header_missing"},
		{name: "a timestamp item with no value", header: fmt.Sprintf("t,v1=%s", good), class: "header_malformed"},
		{name: "a timestamp that is not a number", header: fmt.Sprintf("t=soon,v1=%s", good), class: "header_malformed"},
		{name: "no timestamp item", header: fmt.Sprintf("v1=%s", good), class: "header_malformed"},
		{name: "a v1 item with no value", header: fmt.Sprintf("t=%d,v1", now.Unix()), class: "header_malformed"},
		{name: "no v1 item", header: fmt.Sprintf("t=%d,v0=%s", now.Unix(), good), class: "header_malformed"},
		{name: "signed with another secret", header: fmt.Sprintf("t=%d,v1=%s", now.Unix(), sign(otherSecret, now)), class: "signature_mismatch"},
		{name: "a good signature of a time before the tolerance",
			header: fmt.Sprintf("t=%d,v1=%s", tooOld.Unix(), sign(signingSecret, tooOld)), class: "timestamp_outside_tolerance"},
		{name: "a good signature at the edge of the tolerance, a nanosecond late",
			header: fmt.Sprintf("t=%d,v1=%s", edge.Unix(), sign(signingSecret, edge)), class: "timestamp_outside_tolerance", at: pastTheSecond},
	}
	classes := map[string]bool{}
	for _, c := range cases {
		classes[c.class] = true
	}
	// The four classes, written out: the names are what an operator filters
	// the log and the counter by.
	for _, class := range []string{"header_missing", "header_malformed", "signature_mismatch", "timestamp_outside_tolerance"} {
		if !classes[class] {
			t.Fatalf("no case for the class %q", class)
		}
	}
	if len(classes) != 4 {
		t.Fatalf("the cases name %d classes, want the 4 above: %v", len(classes), classes)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			at := c.at
			if at.IsZero() {
				at = now
			}
			logs, counts, answer := deliver(t, signingSecret, at, body, c.header)
			if answer.Code != http.StatusBadRequest || answer.Body.String() != refusal {
				t.Fatalf("answer = %d %q, want 400 %q", answer.Code, answer.Body.String(), refusal)
			}
			var records, warnings []map[string]any
			for _, raw := range strings.Split(strings.TrimSpace(logs), "\n") {
				if raw == "" {
					continue
				}
				var entry map[string]any
				if err := json.Unmarshal([]byte(raw), &entry); err != nil {
					t.Fatalf("log line is not JSON: %q", raw)
				}
				records = append(records, entry)
				if entry["msg"] == line {
					warnings = append(warnings, entry)
				}
			}
			if len(warnings) != 1 {
				t.Fatalf("the refusal left %d lines %q, want 1; the log:\n%s", len(warnings), line, logs)
			}
			// The refusal is the ONLY record of the request, and it holds the
			// class and nothing more. A substring check alone would miss a
			// part of the header or of the body in a second record or in one
			// more field; with one record and a fixed set of fields there is
			// no place for any of it.
			if len(records) != 1 {
				t.Errorf("the refused request left %d log records, want 1 (the refusal):\n%s", len(records), logs)
			}
			var fields []string
			for field := range warnings[0] {
				fields = append(fields, field)
			}
			sort.Strings(fields)
			if want := []string{"class", "level", "msg", "time"}; !reflect.DeepEqual(fields, want) {
				t.Errorf("the fields of the refusal line = %v, want exactly %v", fields, want)
			}
			if warnings[0]["level"] != "WARN" || warnings[0]["class"] != c.class {
				t.Errorf("the line = level %v class %v, want WARN %s", warnings[0]["level"], warnings[0]["class"], c.class)
			}
			for what, secret := range map[string]string{
				"the configured signing secret": signingSecret, "the body": bodyMarker,
				"the signature of the header": good, "another secret's signature": sign(otherSecret, now),
				"the signature of the old time": sign(signingSecret, tooOld), "the signature of the edge time": sign(signingSecret, edge),
			} {
				if strings.Contains(logs, secret) {
					t.Errorf("the log holds %s:\n%s", what, logs)
				}
			}
			if c.header != "" && strings.Contains(logs, c.header) {
				t.Errorf("the log holds the header value:\n%s", logs)
			}
			if len(counts) != 1 || counts[c.class] != 1 {
				t.Errorf("%s by class = %v, want %s=1 and no other class", counter, counts, c.class)
			}
		})
	}

	t.Run("the edge of the tolerance is accepted on the whole second", func(t *testing.T) {
		_, counts, answer := deliver(t, signingSecret, now, body, fmt.Sprintf("t=%d,v1=%s", edge.Unix(), sign(signingSecret, edge)))
		if answer.Code != http.StatusOK || len(counts) != 0 {
			t.Fatalf("answer = %d, counts %v; want 200 and no count", answer.Code, counts)
		}
	})

	t.Run("a good signature leaves no refusal line and no count", func(t *testing.T) {
		logs, counts, answer := deliver(t, signingSecret, now, body, fmt.Sprintf("t=%d,v1=%s", now.Unix(), good))
		if answer.Code != http.StatusOK {
			t.Fatalf("answer = %d %q, want 200", answer.Code, answer.Body.String())
		}
		if strings.Contains(logs, line) || len(counts) != 0 {
			t.Errorf("a verified request was logged or counted as refused: counts %v, log:\n%s", counts, logs)
		}
	})
}

// meterReader returns the one metric reader of this test binary. The counters
// of the package are created once, at package load, and stay with the first
// meter provider the process installs: a second provider set by a later test
// would receive none of their counts. So the tests of the package share this
// reader, and each reads the counts it added.
var meterReader = sync.OnceValue(func() *sdkmetric.ManualReader {
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	return reader
})

// counterByLabel reads one counter of the package, summed by one label.
func counterByLabel(t *testing.T, name, label string) map[string]int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := meterReader().Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, series := range scope.Metrics {
			if series.Name != name {
				continue
			}
			for _, point := range series.Data.(metricdata.Sum[int64]).DataPoints {
				value, _ := point.Attributes.Value(attribute.Key(label))
				counts[value.AsString()] += point.Value
			}
		}
	}
	return counts
}

// deliver sends one webhook request through the route and returns its log,
// the counts it added to the refusal counter by class, and the answer.
func deliver(t *testing.T, signingSecret string, now time.Time, body, header string) (string, map[string]int64, *httptest.ResponseRecorder) {
	t.Helper()
	const counter = "dev_health_api_stripe_webhook_signature_failures_total"
	before := counterByLabel(t, counter, "class")

	var logs bytes.Buffer
	h := handlers{stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}),
		webhookSecret: secrets.NewValue(signingSecret), logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		now: func() time.Time { return now }}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/billing/webhooks/stripe", strings.NewReader(body))
	if header != "" {
		request.Header.Set("Stripe-Signature", header)
	}
	recorder := httptest.NewRecorder()
	h.stripeWebhook(recorder, request)

	counts := map[string]int64{}
	for class, total := range counterByLabel(t, counter, "class") {
		if added := total - before[class]; added != 0 {
			counts[class] = added
		}
	}
	return logs.String(), counts, recorder
}
