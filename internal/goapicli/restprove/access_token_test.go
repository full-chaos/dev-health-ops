package restprove

import (
	"strings"
	"testing"
)

// Under -service=query-api the candidate is read with a platform admin's user access token from a 0600 file
// (-candidate-access-token-file), never with a minted effective-principal envelope.
func TestTheQueryAPICandidateIsTheUserAccessTokenFile(t *testing.T) {
	base := []string{"-python-api-url", "http://python.test", "-baseline-bearer-exec", `["mint-edge-token","-org","o"]`,
		"-org", "o", "-recorded-by", "w", "-review-evidence", "y", "-artifact-dir", t.TempDir(), "-dry-run"}
	if _, err := parseFlags(append(append([]string(nil), base...), "-candidate-access-token-file", "/run/proof/token")); err != nil {
		t.Fatalf("parseFlags with -candidate-access-token-file: %v", err)
	}
	_, err := parseFlags(append(append([]string(nil), base...), "-candidate-bearer-exec", `["mint-envelope","-org","o"]`))
	if err == nil || !strings.Contains(err.Error(), "-candidate-access-token-file") {
		t.Fatalf("a mint-envelope candidate: %v, want a refusal naming -candidate-access-token-file", err)
	}
	if _, err := parseFlags(base); err == nil || !strings.Contains(err.Error(), "-candidate-access-token-file") {
		t.Fatalf("no candidate credential: %v, want a refusal naming -candidate-access-token-file", err)
	}
}
