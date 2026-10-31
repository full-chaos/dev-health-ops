package admin

import (
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/providerrevocation"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
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

// Every route claim reads the key version with the sealed token: a row the restore fence queued from a stored
// credential (oauth-v1) holds the OAuthTokens JSON, which is decoded before the send, never sent as is.
func TestRouteClaimsReadTheTokenKeyVersion(t *testing.T) {
	for _, query := range []string{pagerDutyRevocationClaimSQL("true"), pagerDutyRevocationClaimOneSQL("true")} {
		if !strings.HasPrefix(query, "SELECT id, token_encrypted, token_key_version FROM provider_oauth_revocations") {
			t.Fatalf("route claim does not read token_key_version: %s", query)
		}
	}
}

type prefixDecryptor struct{}

func (prefixDecryptor) Decrypt(v secrets.Value) ([]byte, error) {
	if text, ok := strings.CutPrefix(v.Reveal(), "sealed:"); ok {
		return []byte(text), nil
	}
	return nil, errors.New("bad cipher")
}

// Every route sender opens a claimed row through one decode by key version: a raw token (v1) as is, a stored
// credential (oauth-v1) to its refresh_token, an unknown version refused (nothing is sent).
func TestRouteSendersDecodeByKeyVersion(t *testing.T) {
	stored := `sealed:{"access_token":"acc-1","refresh_token":"ref-1","expires_at":"2026-01-01T00:00:00Z"}`
	for _, tc := range []struct{ cipher, version, want string }{
		{"sealed:tok-1", providerfoundation.RevocationKeyToken, "tok-1"},
		{stored, providerfoundation.RevocationKeyOAuth, "ref-1"},
		{"sealed:tok-1", "v9", ""},
		{"not-sealed", providerfoundation.RevocationKeyToken, ""},
	} {
		got, err := pagerDutyRevocationTokenOf(prefixDecryptor{}, tc.cipher, tc.version)
		if got != tc.want || (tc.want == "") != (err != nil) {
			t.Errorf("%s/%s: got %q, %v; want %q", tc.cipher, tc.version, got, err, tc.want)
		}
	}
}
