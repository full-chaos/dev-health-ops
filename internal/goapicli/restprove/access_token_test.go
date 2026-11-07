package restprove

import (
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
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

func candidateToken(t *testing.T, typ string) string {
	t.Helper()
	segment := func(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
	return segment(`{"alg":"EdDSA","typ":"`+typ+`"}`) + "." + segment(`{"sub":"prn_admin","org_id":"o"}`) + "." + segment("synthetic-signature")
}

// The query-api candidate credential carries the file's user access token; an envelope in the file is refused before
// a request is sent, and the refusal never carries the value.
func TestTheQueryAPICandidateCredentialCarriesTheFileToken(t *testing.T) {
	dir := t.TempDir()
	write := func(name, value string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	token := candidateToken(t, "at+jwt")
	credential, err := candidateCredentialFor(flags{service: goapiproof.RESTServiceQueryAPI, candidateAccessTokenFile: write("token", token)})
	if err != nil {
		t.Fatalf("candidateCredentialFor: %v", err)
	}
	request, _ := http.NewRequest(http.MethodGet, "http://query-api.test/api/v1/home", nil)
	if err := credential.Apply(context.Background(), request); err != nil || request.Header.Get("Authorization") != "Bearer "+token {
		t.Fatalf("the candidate request does not carry the file's token (err %v)", err)
	}
	_, err = candidateCredentialFor(flags{service: goapiproof.RESTServiceQueryAPI, candidateAccessTokenFile: write("envelope", candidateToken(t, "JWT"))})
	if err == nil || !strings.Contains(err.Error(), "not a user access token (typ at+jwt)") || strings.Contains(err.Error(), "synthetic") {
		t.Fatalf("an envelope in the candidate token file: %v, want a refusal that names the class and not the value", err)
	}
}
