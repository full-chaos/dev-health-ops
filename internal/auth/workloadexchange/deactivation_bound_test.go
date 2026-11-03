package workloadexchange

import (
	"net/http"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// A binding is read from the store on every exchange. After a success, a workload, a binding or a principal that
// the store reports off refuses the next exchange at once with the one uniform answer and issues nothing; the
// token issued before lives at most the workload token ceiling of the contract.
func TestDeactivationBound_Workload_Exchange_StoreRefusalEndsAccessAtOnce(t *testing.T) {
	for _, reason := range []string{"disabled", "unknown", "no_audience"} {
		t.Run(reason, func(t *testing.T) {
			h := newHarness(t)
			answer := decodeAnswer(t, h.post(form(h.validSubject(), "auth-service", "").Encode()))
			claims := verifyIssued(t, h, answer.AccessToken, "auth-service")
			if life := time.Duration(claims.ExpiresAt-claims.IssuedAt) * time.Second; life > contracts.WorkloadTokenLifetimeCeiling {
				t.Fatalf("token life %s over the ceiling %s", life, contracts.WorkloadTokenLifetimeCeiling)
			}
			h.registry.err = refusedBecause(reason)
			issuedBefore := len(h.auditRows())
			rec := h.post(form(h.validSubject(), "auth-service", "").Encode())
			if rec.Code != http.StatusBadRequest || rec.Body.String() != "{\"error\":\"invalid_grant\"}\n" {
				t.Fatalf("after %s: %d %q, want the uniform 400", reason, rec.Code, rec.Body.String())
			}
			if got := len(h.auditRows()); got != issuedBefore {
				t.Fatalf("a refused exchange wrote %d audit rows", got-issuedBefore)
			}
			h.registry.err = nil
			if rec := h.post(form(h.validSubject(), "auth-service", "").Encode()); rec.Code != http.StatusOK {
				t.Fatalf("after the store reads the binding live again: %d", rec.Code)
			}
		})
	}
}
