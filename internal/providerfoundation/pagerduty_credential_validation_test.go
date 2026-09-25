package providerfoundation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// The client-credentials token request carries the client secret in its
// body; the default client (no doer given) must not replay it to a redirect
// target, and the redirect is a failed read.
func TestValidatePagerDutyCredentialNeverReplaysClientSecretToARedirect(t *testing.T) {
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	token := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer token.Close()

	_, err := ValidatePagerDutyCredential(context.Background(), nil,
		PagerDutyRevokeConfig{TokenURL: token.URL, APIBaseOverride: target.URL},
		PagerDutyCredentialCandidate{AuthMode: "client_credentials", ClientID: "id", ClientSecret: "the-secret", Subdomain: "acme", Region: "us"},
		PagerDutyReadScopeSet())
	if reached.Load() != 0 {
		t.Errorf("the redirect target received %d request(s): the client secret was replayed", reached.Load())
	}
	var validation *PagerDutyValidationError
	if err == nil || !asValidationError(err, &validation) || validation.Code != "live_read_failed" {
		t.Fatalf("err = %v, want live_read_failed", err)
	}
}

func asValidationError(err error, target **PagerDutyValidationError) bool {
	v, ok := err.(*PagerDutyValidationError)
	if ok {
		*target = v
	}
	return ok
}
