package admin

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/providerrevocation"
)

// The revocation worker must leave a setup row alone exactly as long as the
// callback's own drain does: a younger row's callback may still be in flight.
func TestRevocationWorkerSetupCutoffMatchesTheCallback(t *testing.T) {
	if providerrevocation.SetupStaleAfter != setupRevocationStaleAfter {
		t.Fatalf("worker setup cutoff %s, callback cutoff %s", providerrevocation.SetupStaleAfter, setupRevocationStaleAfter)
	}
}

// Every route sender claims like the worker: a row another sender holds is
// skipped, never waited on and sent twice. The callback reaches this claim
// through retryPagerDutyRevocations (pagerduty_callback.go), the disconnect
// through the same call, and the setup drain directly.
func TestRouteSendersClaimWithSkipLocked(t *testing.T) {
	for _, predicate := range []string{
		`org_id = $1 AND provider = 'pagerduty' AND credential_name = $2 AND status = 'pending'`,
		`org_id = $1 AND provider = 'pagerduty' AND purpose = 'setup' AND status = 'pending'`,
	} {
		query := pagerDutyRevocationClaimSQL(predicate)
		if !strings.HasSuffix(query, "FOR UPDATE SKIP LOCKED") || !strings.Contains(query, "WHERE "+predicate+" ") {
			t.Fatalf("route claim is not the shared SKIP LOCKED claim: %s", query)
		}
	}
}

// Org deletion claims one row per transaction with the same skip-locked claim.
func TestOrgDeletionClaimsOneRevocationWithSkipLocked(t *testing.T) {
	query := pagerDutyRevocationClaimOneSQL(orgDeletionRevocationOrgPredicate)
	if !strings.HasSuffix(query, "LIMIT 1 FOR UPDATE SKIP LOCKED") || !strings.Contains(query, "WHERE "+orgDeletionRevocationOrgPredicate+" ") {
		t.Fatalf("org deletion claim is not the shared one-row SKIP LOCKED claim: %s", query)
	}
}
