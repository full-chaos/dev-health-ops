package billing

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/api/standinkey"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	unverifiableBody    = `{"id": "evt_unverifiable_unit", "object": "event", "type": "customer.created", "data": {"object": {}}}`
	unverifiableCounter = "dev_health_api_stripe_webhook_signature_failures_total"
)

var timeField = regexp.MustCompile(`"time":"[^"]*",?`)

func withoutTime(logs string) string { return timeField.ReplaceAllString(logs, "") }

type webhookOutcome struct {
	status  int
	body    string
	headers http.Header
	logs    string
	counts  map[string]int64
}

func stripeMAC(key string, at time.Time, body string) string {
	mac := hmac.New(sha256.New, []byte(key))
	fmt.Fprintf(mac, "%d.%s", at.Unix(), body)
	return hex.EncodeToString(mac.Sum(nil))
}

func driveWebhook(t *testing.T, signingSecret, apiKey string, now time.Time, body, header string) webhookOutcome {
	t.Helper()
	before := counterByLabel(t, unverifiableCounter, "class")
	var logs bytes.Buffer
	h := handlers{stripe: stripeclient.New(stripeclient.Options{Key: apiKey}),
		webhookSecret: newWebhookCredential(secrets.NewValue(signingSecret)), logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		now: func() time.Time { return now }}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/billing/webhooks/stripe", strings.NewReader(body))
	if header != "" {
		request.Header.Set("Stripe-Signature", header)
	}
	recorder := httptest.NewRecorder()
	h.stripeWebhook(recorder, request)
	counts := map[string]int64{}
	for class, total := range counterByLabel(t, unverifiableCounter, "class") {
		if added := total - before[class]; added != 0 {
			counts[class] = added
		}
	}
	return webhookOutcome{recorder.Code, recorder.Body.String(), recorder.Header(), withoutTime(logs.String()), counts}
}

// The same delivery gets the same answer, log and count whether or not the signing secret (or the API
// client) is set: status, body, headers, log records and the refusal counter.
func TestUnverifiableWebhookAnswersLikeAWrongSecret(t *testing.T) {
	now := time.Unix(1790000000, 0)
	const configured = "whsec_configured_unit"
	good := stripeMAC("whsec_some_guess", now, unverifiableBody)
	cases := []struct{ name, body, header string }{
		{"no header", unverifiableBody, ""},
		{"timestamp item without a value", unverifiableBody, "t,v1=" + good},
		{"timestamp that is not a number", unverifiableBody, "t=soon,v1=" + good},
		{"no timestamp", unverifiableBody, "v1=" + good},
		{"no v1 item", unverifiableBody, fmt.Sprintf("t=%d", now.Unix())},
		{"guessed signature", unverifiableBody, fmt.Sprintf("t=%d,v1=%s", now.Unix(), good)},
		{"several signatures", unverifiableBody, fmt.Sprintf("t=%d,v1=%s,v1=%s", now.Unix(), good, stripeMAC("another", now, unverifiableBody))},
		{"stale timestamp", unverifiableBody, fmt.Sprintf("t=%d,v1=%s", now.Add(-time.Hour).Unix(), stripeMAC("x", now.Add(-time.Hour), unverifiableBody))},
		{"empty body", "", fmt.Sprintf("t=%d,v1=%s", now.Unix(), good)},
		{"not json", "not json at all", fmt.Sprintf("t=%d,v1=%s", now.Unix(), stripeMAC("x", now, "not json at all"))},
		{"non UTF-8 body", "\xff\xfe", fmt.Sprintf("t=%d,v1=%s", now.Unix(), good)},
		{"non-ASCII signature", unverifiableBody, fmt.Sprintf("t=%d,v1=é", now.Unix())},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wrong := driveWebhook(t, configured, "sk_test_unit", now, c.body, c.header)
			absent := driveWebhook(t, "", "sk_test_unit", now, c.body, c.header)
			noClient := driveWebhook(t, "", "", now, c.body, c.header)
			for name, got := range map[string]webhookOutcome{"absent secret": absent, "absent secret and API client": noClient} {
				if got.status != wrong.status || got.body != wrong.body {
					t.Fatalf("%s: %d %q, a wrong secret gives %d %q", name, got.status, got.body, wrong.status, wrong.body)
				}
				if !reflect.DeepEqual(got.headers, wrong.headers) {
					t.Fatalf("%s: headers %v, a wrong secret gives %v", name, got.headers, wrong.headers)
				}
				if got.logs != wrong.logs {
					t.Fatalf("%s: log\n%s\na wrong secret logs\n%s", name, got.logs, wrong.logs)
				}
				if !reflect.DeepEqual(got.counts, wrong.counts) {
					t.Fatalf("%s: counts %v, a wrong secret gives %v", name, got.counts, wrong.counts)
				}
			}
			if wrong.status == http.StatusOK || (absent.status == http.StatusInternalServerError && wrong.status != absent.status) {
				t.Fatalf("status %d for a delivery nobody can have signed", absent.status)
			}
		})
	}
}

// A delivery signed with the EMPTY key must be refused when the secret is absent: the stand-in key is
// random, not empty.
func TestUnverifiableWebhookRefusesADeliverySignedWithTheEmptyKey(t *testing.T) {
	now := time.Unix(1790000000, 0)
	header := fmt.Sprintf("t=%d,v1=%s", now.Unix(), stripeMAC("", now, unverifiableBody))
	got := driveWebhook(t, "", "sk_test_unit", now, unverifiableBody, header)
	if got.status != http.StatusBadRequest || got.body != `{"detail":"Invalid Stripe signature"}` {
		t.Fatalf("%d %q, want the refusal", got.status, got.body)
	}
}

// A validly signed delivery to a server with no API client is a 500: a limit, only Stripe can send it.
func TestAValidlySignedDeliveryWithoutAnAPIClientIsA500(t *testing.T) {
	now := time.Unix(1790000000, 0)
	header := fmt.Sprintf("t=%d,v1=%s", now.Unix(), stripeMAC("whsec_configured_unit", now, unverifiableBody))
	got := driveWebhook(t, "whsec_configured_unit", "", now, unverifiableBody, header)
	if got.status != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", got.status)
	}
}

// The derived key accepts exactly what a direct HMAC with the secret accepted, for every key length
// around the block size; the stale-timestamp class is kept.
func TestDerivedWebhookKeyMatchesADirectCheckForEveryLength(t *testing.T) {
	now := time.Unix(1790000000, 0)
	for _, length := range []int{1, 63, 64, 65, 128, 4096} {
		secret := strings.Repeat("s", length-1) + "z"
		header := fmt.Sprintf("t=%d,v1=%s", now.Unix(), stripeMAC(secret, now, unverifiableBody))
		if failure, err := classifyStripeSignature([]byte(unverifiableBody), header, secret, now); err != nil {
			t.Fatalf("length %d: a signature from a direct HMAC was refused (%s, %v)", length, failure, err)
		}
		stale := fmt.Sprintf("t=%d,v1=%s", now.Add(-time.Hour).Unix(), stripeMAC(secret, now.Add(-time.Hour), unverifiableBody))
		if failure, err := classifyStripeSignature([]byte(unverifiableBody), stale, secret, now); err == nil || failure != signatureTimestampOutsideTolerance {
			t.Fatalf("length %d: a stale delivery gave %s, %v", length, failure, err)
		}
		if _, err := classifyStripeSignature([]byte(unverifiableBody), header, secret+"x", now); err == nil {
			t.Fatalf("length %d: another secret verified", length)
		}
	}
}

// Deriving the key is a startup step: no request does it.
func TestNoWebhookRequestDerivesTheKey(t *testing.T) {
	now := time.Unix(1790000000, 0)
	h := handlers{stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}), webhookSecret: newWebhookCredential(secrets.NewValue("")),
		logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), now: func() time.Time { return now }}
	before := standinkey.Derivations()
	for i := 0; i < 5; i++ {
		h.stripeWebhook(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", strings.NewReader(unverifiableBody)))
	}
	if after := standinkey.Derivations(); after != before {
		t.Fatalf("%d derivations during requests, want 0", after-before)
	}
}

// The work of a refused delivery (allocations) does not depend on the secret being set or on its length.
func TestRefusedWebhookWorkDoesNotDependOnTheSecret(t *testing.T) {
	now := time.Unix(1790000000, 0)
	header := fmt.Sprintf("t=%d,v1=%s", now.Unix(), stripeMAC("guess", now, unverifiableBody))
	measure := func(secret string) float64 {
		h := handlers{stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}), webhookSecret: newWebhookCredential(secrets.NewValue(secret)),
			logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)), now: func() time.Time { return now }}
		return testing.AllocsPerRun(100, func() {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(unverifiableBody))
			req.Header.Set("Stripe-Signature", header)
			rec := httptest.NewRecorder()
			h.stripeWebhook(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", rec.Code)
			}
		})
	}
	absent, short, long := measure(""), measure(strings.Repeat("k", 38)), measure(strings.Repeat("k", 4096))
	near := func(a, b float64) bool { return a-b <= 3 && b-a <= 3 }
	if !near(absent, short) || !near(absent, long) {
		t.Fatalf("allocations per refused delivery: absent %v, 38-byte %v, 4096-byte %v", absent, short, long)
	}
}

// Through the production logger the refusal record of a realistic configured secret equals that of an
// absent one, and the configured secret stays in the redaction registry.
func TestWebhookRefusalLogIsTheSameThroughTheProductionLogger(t *testing.T) {
	now := time.Unix(1790000000, 0)
	header := fmt.Sprintf("t=%d,v1=%s", now.Unix(), stripeMAC("guess", now, unverifiableBody))
	logOf := func(secret string) string {
		var out bytes.Buffer
		h := handlers{stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}), webhookSecret: newWebhookCredential(secrets.NewValue(secret)),
			logger: logging.NewJSON(&out, slog.LevelDebug), now: func() time.Time { return now }}
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(unverifiableBody))
		req.Header.Set("Stripe-Signature", header)
		h.stripeWebhook(httptest.NewRecorder(), req)
		return withoutTime(out.String())
	}
	realistic := "whsec_" + hex.EncodeToString(bytes.Repeat([]byte{7, 9, 11}, 12))
	absent := logOf("")
	if configured := logOf(realistic); configured != absent || absent == "" {
		t.Fatalf("refusal log with a configured secret\n%s\nwith an absent one\n%s", configured, absent)
	}
	var echoed bytes.Buffer
	logging.NewJSON(&echoed, slog.LevelDebug).Warn("echoed " + realistic)
	if strings.Contains(echoed.String(), realistic) || !strings.Contains(echoed.String(), "[REDACTED]") {
		t.Fatalf("the configured secret was not blanked: %s", echoed.String())
	}
}

func TestStartupLogNamesEachAbsentCredentialOnce(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, nil))
	startupLogOnce = sync.Once{}
	LogUnconfigured(logger, secrets.NewValue(""), secrets.NewValue("sk"))
	LogUnconfigured(logger, secrets.NewValue(""), secrets.NewValue("sk"))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 || !strings.Contains(out.String(), "STRIPE_WEBHOOK_SECRET") {
		t.Fatalf("log = %q, want one line for the webhook secret", out.String())
	}
}
