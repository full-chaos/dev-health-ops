package authapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// A workload token issued at t0 is accepted by the session issue route until
// t0+299 s and refused from t0+300 s, on an injected route clock; the route
// reads no registry, so this bound is the only one an issued token has.
func TestSessionIssueWorkloadTokenIsLiveUntilItsFiveMinuteExpiryOnTheRouteClock(t *testing.T) {
	signer, _, _ := fileSigner(t)
	f := newCompatFixture(t)
	keys := &issueCountingKeys{inner: signer}
	subject := session.WirePrincipalID(uuid.New())
	t0 := time.Unix(1_900_000_000, 0).UTC()
	now := t0
	workload, err := NewSessionIssueWorkload(SessionIssueWorkloadConfig{
		IssuerURL: goldenIssuer, Keys: keys, Subject: subject, Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	f.workload = workload
	f.buildCompat()
	w := &workloadIssueFixture{compatFixture: f, signer: signer, keys: keys, subject: subject}
	header, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) {
		c.Iat, c.Nbf, c.Exp = t0.Unix(), t0.Unix(), t0.Unix()+300
	})
	for _, tc := range []struct {
		at   time.Duration
		want int
	}{
		{0, http.StatusOK}, {299 * time.Second, http.StatusOK},
		{300 * time.Second, http.StatusUnauthorized}, {301 * time.Second, http.StatusUnauthorized},
		{time.Hour, http.StatusUnauthorized},
	} {
		now = t0.Add(tc.at)
		before := len(f.sessions.created)
		if got := w.issue(header); got.Code != tc.want {
			t.Fatalf("at t0+%v: status %d, want %d (%s)", tc.at, got.Code, tc.want, got.Body)
		}
		if grew := len(f.sessions.created) - before; (tc.want == http.StatusOK) != (grew == 1) || grew > 1 {
			t.Fatalf("at t0+%v: %d session rows written", tc.at, grew)
		}
	}
}
