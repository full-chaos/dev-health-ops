package webhookintake

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// PagerDuty authenticates a delivery against the secret of the addressed binding. With a REAL binding (the
// fixture's lookup seam) a user token placed as the signature, bare or as a v1 item, and as the Authorization
// bearer, reaches the signature check itself and is refused 401 "Invalid signature" with the reason
// invalid_signature in the single audit line: the binding lookup happened (one per request) and the
// signature, not an earlier gate, refused it. Nothing of the token is echoed in the body or the log.
func TestPagerDutyRefusesUserTokensAtTheSignatureCheck(t *testing.T) {
	tokens := withissuer.All(t)
	f := newRefusalFixture(t, "active")
	body := []byte(`{"event":{"id":"E-user-token","event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`)
	requests := 0
	for _, tok := range tokens {
		for _, signature := range []string{tok.Value, "v1=" + tok.Value} {
			for _, authorization := range []string{"", "Bearer " + tok.Value} {
				f.logs.Reset()
				request := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+f.bindingID, bytes.NewReader(body))
				request.SetPathValue("binding_id", f.bindingID)
				request.Header.Set("X-Webhook-Subscription", f.sub)
				request.Header.Set("X-PagerDuty-Signature", signature)
				if authorization != "" {
					request.Header.Set("Authorization", authorization)
				}
				recorder := httptest.NewRecorder()
				f.deps.handlePagerDutyWebhook()(recorder, request)
				requests++
				if recorder.Code != http.StatusUnauthorized || recorder.Body.String() != `{"detail":"Invalid signature"}` {
					t.Errorf("%s: %d %s, want 401 Invalid signature", tok.Kind, recorder.Code, recorder.Body.String())
				}
				if !strings.Contains(f.logs.String(), `"reason":"invalid_signature"`) {
					t.Errorf("%s: the refusal is not the signature check's: %s", tok.Kind, f.logs.String())
				}
				usertokens.NoLeak(t, "the PagerDuty body ("+tok.Kind+")", recorder.Body.String(), tokens)
				usertokens.NoLeak(t, "the PagerDuty log ("+tok.Kind+")", f.logs.String(), tokens)
			}
		}
	}
	if f.loads != requests {
		t.Errorf("%d binding lookups for %d requests: the signature check was not reached each time", f.loads, requests)
	}
	// The control: the binding's own signature passes the check (the answer is no longer the 401 refusal).
	f.logs.Reset()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/webhooks/pagerduty/"+f.bindingID, bytes.NewReader(body))
	request.SetPathValue("binding_id", f.bindingID)
	request.Header.Set("X-Webhook-Subscription", f.sub)
	request.Header.Set("X-PagerDuty-Signature", pagerdutySign(f.secret, body))
	recorder := httptest.NewRecorder()
	f.deps.handlePagerDutyWebhook()(recorder, request)
	if recorder.Code == http.StatusUnauthorized {
		t.Fatalf("the binding's own signature was refused: %s %s", recorder.Body.String(), f.logs.String())
	}
}
