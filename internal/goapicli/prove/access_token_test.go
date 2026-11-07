package prove

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func headerToken(t *testing.T, typ string) string {
	t.Helper()
	segment := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return strings.Join([]string{segment(map[string]string{"alg": "EdDSA", "typ": typ}), segment(map[string]string{"sub": "prn_admin"}),
		base64.RawURLEncoding.EncodeToString([]byte("synthetic-signature"))}, ".")
}

// The proof credential (/buildinfo and /query/proof) is a platform admin's user access token the operator stages in a
// 0600 file (-proof-access-token-file), read on each use; nothing mints the effective-principal envelope.
func TestTheProofCredentialIsTheUserAccessTokenFile(t *testing.T) {
	var minted []string
	withFakeMinter(t, func(_ context.Context, helperName string, _ []string) (string, error) {
		minted = append(minted, helperName)
		return headerToken(t, "at+jwt"), nil
	})
	dir := t.TempDir()
	file := filepath.Join(dir, "proof-token")
	token := headerToken(t, "at+jwt")
	if err := os.WriteFile(file, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-dry-run", "-documents", "d.json", "-org", "o", "-artifact-dir", dir, "-recorded-by", "w", "-review-evidence", "y"}
	f, err := parseFlags(append(append([]string(nil), args...), "-proof-access-token-file", file))
	if err != nil {
		t.Fatalf("parseFlags with -proof-access-token-file: %v", err)
	}
	_, proof, err := credentials(f)
	if err != nil {
		t.Fatalf("credentials: %v", err)
	}
	request, _ := http.NewRequest(http.MethodGet, "http://query-api.test/buildinfo", nil)
	if err := proof.Apply(context.Background(), request); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := request.Header.Get("Authorization"); got != "Bearer "+token {
		t.Fatal("the proof request does not carry the user access token of the file")
	}
	for _, name := range minted {
		if name == "mint-envelope" {
			t.Fatal("credentials() minted the effective-principal envelope")
		}
	}
	envelope := filepath.Join(dir, "envelope")
	if err := os.WriteFile(envelope, []byte(headerToken(t, "JWT")), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err = parseFlags(append(append([]string(nil), args...), "-proof-access-token-file", envelope))
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if _, _, err := credentials(f); err == nil || !strings.Contains(err.Error(), "not a user access token (typ at+jwt)") || strings.Contains(err.Error(), "synthetic-signature") {
		t.Fatalf("an envelope in the proof token file: %v, want a refusal that names the class and not the value", err)
	}
	f, err = parseFlags(args)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if _, _, err := credentials(f); err == nil || !strings.Contains(err.Error(), "-proof-access-token-file") {
		t.Fatalf("no proof token file: %v, want a refusal naming -proof-access-token-file", err)
	}
}
