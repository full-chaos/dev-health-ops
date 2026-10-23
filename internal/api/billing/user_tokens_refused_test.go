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
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/billing/stripeclient"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// The Stripe webhook (the api listener and the billing edge mount the same handler) is authenticated by
// the Stripe-Signature HMAC. A user token of any kind, as the Authorization bearer and in the signature
// header (bare, and as a v1 item under a fresh timestamp), is refused 400 "Invalid Stripe signature" with
// no database use, no event processed, and no credential in the body or the logs.
func TestStripeWebhookRefusesUserTokens(t *testing.T) {
	const (
		signingSecret = "whsec_user_token_proof"
		body          = `{"id": "evt_user_token_proof", "object": "event", "type": "customer.created", "data": {"object": {}}}`
		refusal       = `{"detail":"Invalid Stripe signature"}`
	)
	now := time.Unix(1790000000, 0)
	tokens := withissuer.All(t)
	pool, connects := usertokens.CountingPool(t)
	var logs bytes.Buffer
	deps := Deps{
		Stripe: stripeclient.New(stripeclient.Options{Key: "sk_test_unit"}), WebhookSecret: secrets.NewValue(signingSecret),
		Pool: pool, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Now: func() time.Time { return now },
	}
	// The api listener and the billing edge both mount the Stripe webhook: prove each mount.
	for _, mount := range []struct {
		name   string
		routes func(Deps) []httpapi.Route
	}{{"api", Routes}, {"edge", EdgeRoutes}} {
		t.Run(mount.name, func(t *testing.T) {
			h := stripeHandler(t, mount.routes(deps))
			refuseUserTokens(t, h, tokens, signingSecret, body, refusal, now, &logs, connects)
		})
	}
}

func stripeHandler(t *testing.T, routes []httpapi.Route) http.Handler {
	t.Helper()
	for _, route := range routes {
		if route.Method == http.MethodPost && route.Pattern == "/api/v1/billing/webhooks/stripe" {
			return route.Handler
		}
	}
	t.Fatal("no Stripe webhook route")
	return nil
}

func refuseUserTokens(t *testing.T, h http.Handler, tokens []usertokens.Token, signingSecret, body, refusal string, now time.Time, logs *bytes.Buffer, connects *usertokens.Counter) {
	t.Helper()
	for _, tok := range tokens {
		for _, header := range []string{tok.Value, fmt.Sprintf("t=%d,v1=%s", now.Unix(), tok.Value)} {
			for _, authorization := range []string{"", "Bearer " + tok.Value} {
				request := httptest.NewRequest(http.MethodPost, "/api/v1/billing/webhooks/stripe", strings.NewReader(body))
				request.Header.Set("Stripe-Signature", header)
				if authorization != "" {
					request.Header.Set("Authorization", authorization)
				}
				recorder := httptest.NewRecorder()
				h.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusBadRequest || recorder.Body.String() != refusal {
					t.Errorf("%s: %d %q, want 400 %q", tok.Kind, recorder.Code, recorder.Body.String(), refusal)
				}
				usertokens.NoLeak(t, "stripe body", recorder.Body.String(), tokens)
			}
		}
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the database: %d connection attempt(s), want 0", got)
	}
	usertokens.NoLeak(t, "the logs", logs.String(), tokens)

	// The control: the real signature passes the gate (the event is a type the route ignores), so the
	// refusals above are the gate's, not a broken fixture's.
	mac := hmac.New(sha256.New, []byte(signingSecret))
	fmt.Fprintf(mac, "%d.%s", now.Unix(), body)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/billing/webhooks/stripe", strings.NewReader(body))
	request.Header.Set("Stripe-Signature", fmt.Sprintf("t=%d,v1=%s", now.Unix(), hex.EncodeToString(mac.Sum(nil))))
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if recorder.Code == http.StatusBadRequest && recorder.Body.String() == refusal {
		t.Fatalf("the genuine signature was refused: the control proves nothing")
	}
}
