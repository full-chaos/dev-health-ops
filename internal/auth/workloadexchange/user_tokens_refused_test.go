package workloadexchange

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens/withissuer"
)

// TestTheExchangeRefusesUserTokens is the negative proof of the exchange row: a
// user access or refresh token opens nothing. As a bearer it is never read (the
// route reads no Authorization header), and as the subject_token only the
// Kubernetes TokenReview API may vouch for it, which it does not: the answer is
// the uniform refusal, the registry is never read, no token is minted, no
// audit row is written, and the token reaches no answer or log line.
func TestTheExchangeRefusesUserTokens(t *testing.T) {
	tokens := withissuer.All(t)
	h := newHarness(t)
	h.review.set(func(w http.ResponseWriter, _ *http.Request, _ reviewSpec) {
		writeReview(w, http.StatusCreated, false, nil, "", "")
	})
	for _, tok := range tokens {
		req := httptest.NewRequest(http.MethodPost, Pattern, strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "Bearer "+tok.Value)
		rec := httptest.NewRecorder()
		h.server.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_request\"}\n" {
			t.Errorf("%s as a bearer: %d %q, want the 400 invalid_request", tok.Kind, rec.Code, rec.Body.String())
		}
		usertokens.NoLeak(t, tok.Kind+" bearer answer", rec.Body.String(), tokens)

		rec = h.post(form(tok.Value, "auth-service", "").Encode())
		if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
			t.Errorf("%s as the subject_token: %d %q, want the uniform 400 invalid_grant", tok.Kind, rec.Code, rec.Body.String())
		}
		usertokens.NoLeak(t, tok.Kind+" subject answer", rec.Body.String(), tokens)
	}
	if h.registry.resolved != 0 {
		t.Errorf("a user token reached the workload registry %d time(s), want 0", h.registry.resolved)
	}
	if rows := h.auditRows(); len(rows) != 0 {
		t.Errorf("%d audit row(s) for user tokens, want 0", len(rows))
	}
	usertokens.NoLeak(t, "the logs", h.logs.String(), tokens)
	usertokens.NoLeak(t, "the metrics", dumpMetrics(t, h.reader), tokens)
}
