package authapi

import (
	"net/http"
	"strings"
	"testing"
)

const retiredRefusalsCounter = "auth.session.retired_credential_refusals"

// retiredSessionIssuerBearer is a well-shaped svc_authsess_ credential, the session issuer credential these routes
// no longer take.
var retiredSessionIssuerBearer = map[string]string{"Authorization": "Bearer svc_authsess_" + strings.Repeat("A", 43)}

// The session issue, state and delegated routes refuse the session issuer credential from the bearer text alone,
// with the 401 any bad bearer gets, and count each refusal by operation.
func TestSessionRoutesRefuseTheRetiredSessionIssuerCredential(t *testing.T) {
	for _, route := range []struct{ pattern, op string }{
		{PatternIssueSession, opSessionIssue},
		{PatternSessionState, opSessionState},
		{PatternDelegatedSession, opDelegatedStart},
		{PatternDelegatedSessionEnd, opDelegatedEnd},
	} {
		w := newWorkloadIssueFixture(t)
		bad := w.do(http.MethodPost, route.pattern, `{}`, bearerOf("not-a-credential"))
		expectError(t, bad, http.StatusUnauthorized, "invalid_credentials")
		response := w.do(http.MethodPost, route.pattern, `{}`, retiredSessionIssuerBearer)
		expectError(t, response, http.StatusUnauthorized, "invalid_credentials")
		if got, want := refusalWithoutPerRequestFields(t, response.Body.Bytes()), refusalWithoutPerRequestFields(t, bad.Body.Bytes()); got != want {
			t.Errorf("%s: the refusal differs from any bad bearer's:\n%s\n%s", route.pattern, got, want)
		}
		if got, _ := w.counter(retiredRefusalsCounter, map[string]string{"operation": route.op}); got != 1 {
			t.Errorf("%s: retired refusals{operation=%s} = %d, want 1", route.pattern, route.op, got)
		}
		if !strings.Contains(w.logs.String(), `"refusal":"retired_credential_class"`) {
			t.Errorf("%s: the log line does not name the retired class: %s", route.pattern, w.logs.String())
		}
		if w.users.calls != 0 || len(w.sessions.created) != 0 || w.keys.reads != 0 {
			t.Errorf("%s: a refusal read a store: users %d sessions %d key set %d", route.pattern, w.users.calls, len(w.sessions.created), w.keys.reads)
		}
	}
}

// The refusal counter exists at 0 for every operation before any request, with the operation label alone.
func TestTheRetiredRefusalCounterIsDeclaredAtZeroPerOperation(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	for _, op := range []string{opSessionIssue, opSessionState, opDelegatedStart, opDelegatedEnd} {
		if got, series := w.counter(retiredRefusalsCounter, map[string]string{"operation": op}); got != 0 || series != 1 {
			t.Errorf("operation %s: total %d over %d series, want one series at 0", op, got, series)
		}
	}
	if _, series := w.counter(retiredRefusalsCounter, nil); series != 4 {
		t.Errorf("%d series, want 4 (one per operation)", series)
	}
}
