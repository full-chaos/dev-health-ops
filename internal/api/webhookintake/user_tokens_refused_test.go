package webhookintake_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/webhookintake"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

const webhookBody = `{"zen":"user tokens open nothing","repository":{"full_name":"acme/repo"}}`

func handlerOf(t *testing.T, deps webhookintake.Deps, pattern string) http.Handler {
	t.Helper()
	for _, candidate := range webhookintake.Routes(deps) {
		if candidate.Method == http.MethodPost && candidate.Pattern == pattern {
			return candidate.Handler
		}
	}
	t.Fatalf("no POST %s route", pattern)
	return nil
}

type delivery struct {
	name    string
	pattern string
	headers func(tok string) map[string]string
	status  int
	detail  string
}

func newDelivery(d delivery, tok, authorization string, serial int) *http.Request {
	path := strings.ReplaceAll(d.pattern, "{binding_id}", "not-a-binding-uuid")
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(webhookBody))
	r.SetPathValue("binding_id", "not-a-binding-uuid")
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("X-Forwarded-For", fmt.Sprintf("10.9.%d.%d", serial/256%256, serial%256))
	for name, value := range d.headers(tok) {
		r.Header.Set(name, value)
	}
	if authorization != "" {
		r.Header.Set("Authorization", authorization)
	}
	return r
}

// GitHub and PagerDuty deliveries are authenticated by the provider's own secret check. A
// user token of any kind, sent as the Authorization bearer and in the place the provider's credential
// goes (the signature or token header), is refused with the route's own 401, before any database lookup,
// without the credential echoed in the body or the logs.
func TestWebhookRoutesRefuseUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	pool, connects := usertokens.CountingPool(t)
	var logs bytes.Buffer
	deps := webhookintake.Deps{
		Pool:    pool,
		Secrets: webhookintake.Secrets{GitHub: "github-test-secret"},
		Logger:  slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	const prefix = "/api/v1/webhooks"
	cases := []delivery{
		{name: "github signature", pattern: prefix + "/github", status: 401, detail: "Invalid signature", headers: func(tok string) map[string]string {
			return map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "d-1", "X-Hub-Signature-256": "sha256=" + tok}
		}},
		{name: "github bare token as signature", pattern: prefix + "/github", status: 401, detail: "Invalid signature", headers: func(tok string) map[string]string {
			return map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "d-1", "X-Hub-Signature-256": tok}
		}},
		{name: "pagerduty signature (no such binding)", pattern: prefix + "/pagerduty/{binding_id}", status: 401, detail: "Invalid signature", headers: func(tok string) map[string]string {
			return map[string]string{"X-PagerDuty-Signature": "v1=" + tok, "X-Webhook-Subscription": "sub-1"}
		}},
	}
	serial := 0
	for _, c := range cases {
		handler := handlerOf(t, deps, c.pattern)
		for _, tok := range tokens {
			for _, authorization := range []string{"", "Bearer " + tok.Value} {
				serial++
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, newDelivery(c, tok.Value, authorization, serial))
				var body struct {
					Detail string `json:"detail"`
				}
				if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || recorder.Code != c.status || !strings.HasPrefix(body.Detail, c.detail) {
					t.Errorf("%s with %s (bearer %v): %d %s, want %d detail %q", c.name, tok.Kind, authorization != "", recorder.Code, recorder.Body.String(), c.status, c.detail)
				}
				usertokens.NoLeak(t, c.name+" body", recorder.Body.String(), tokens)
			}
		}
	}
	if got := connects.Count(); got != 0 {
		t.Errorf("a user token reached the database: %d connection attempt(s), want 0", got)
	}
	usertokens.NoLeak(t, "the logs", logs.String(), tokens)
}

// Controls: the provider's real credential passes the gate (GitHub) and a well-formed
// PagerDuty binding id reaches the binding lookup, so the same counting pool shows a non-zero count. If
// these ever read as refusals or zero, the refusals above would prove nothing.
func TestWebhookControlsTheProvidersOwnCredentialPassesTheGate(t *testing.T) {
	pool, connects := usertokens.CountingPool(t)
	deps := webhookintake.Deps{
		Pool:    pool,
		Secrets: webhookintake.Secrets{GitHub: "github-test-secret"},
		Logger:  slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
	}
	mac := func(secret string) string {
		m := hmac.New(sha256.New, []byte(secret))
		m.Write([]byte(webhookBody))
		return hex.EncodeToString(m.Sum(nil))
	}
	const prefix = "/api/v1/webhooks"
	for _, c := range []delivery{
		{name: "github", pattern: prefix + "/github", headers: func(string) map[string]string {
			return map[string]string{"X-GitHub-Event": "ping", "X-GitHub-Delivery": "d-1", "X-Hub-Signature-256": "sha256=" + mac("github-test-secret")}
		}},
	} {
		recorder := httptest.NewRecorder()
		handlerOf(t, deps, c.pattern).ServeHTTP(recorder, newDelivery(c, "", "", 1))
		if recorder.Code == http.StatusUnauthorized || recorder.Code == http.StatusUnprocessableEntity {
			t.Errorf("%s: the provider's own credential was refused at the gate: %d %s", c.name, recorder.Code, recorder.Body.String())
		}
	}
	lookup := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/11111111-1111-4111-8111-111111111111", strings.NewReader(webhookBody))
	lookup.SetPathValue("binding_id", "11111111-1111-4111-8111-111111111111")
	lookup.Header.Set("X-Webhook-Subscription", "sub-1")
	lookup.Header.Set("X-Forwarded-For", "10.9.9.9")
	handlerOf(t, deps, prefix+"/pagerduty/{binding_id}").ServeHTTP(httptest.NewRecorder(), lookup)
	if connects.Count() == 0 {
		t.Fatal("a well-formed PagerDuty binding id never reached the binding lookup: the counting pool detects nothing")
	}
}
