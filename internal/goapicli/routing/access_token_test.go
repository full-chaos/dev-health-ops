package routing

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// tokenWithHeader is a JWT-shaped value whose header names typ; built at run time, so no token literal is in the tree.
func tokenWithHeader(t *testing.T, typ string) string {
	t.Helper()
	segment := func(value any) string {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return base64.RawURLEncoding.EncodeToString(raw)
	}
	return strings.Join([]string{segment(map[string]string{"alg": "EdDSA", "typ": typ}), segment(map[string]string{"sub": "prn_subject"}),
		base64.RawURLEncoding.EncodeToString([]byte("synthetic-signature"))}, ".")
}

func tokenFile(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "access-token")
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// seed and enable read /buildinfo with a platform admin's user access token from a 0600 file the operator stages
// (GO_API_ROUTING_ACCESS_TOKEN_FILE), never an envelope from GO_API_ROUTING_BEARER. The value is never printed.
func TestTheRoutingWriteVerbsTakeTheUserAccessTokenFile(t *testing.T) {
	t.Setenv("POSTGRES_URI", "")
	t.Setenv("GO_API_QUERY_API_URL", "")
	t.Setenv("GO_API_ROUTING_BEARER", tokenWithHeader(t, "JWT"))
	envelope := tokenWithHeader(t, "JWT")
	base := []string{"enable", "-operations", "mcp:hotspots", "-mode", "canary", "-recorded-by", "w", "-review-evidence", "y", "-postgres-uri", "postgres://x"}
	for name, c := range map[string]struct {
		file string
		want string
	}{
		"no token file":            {"", "GO_API_ROUTING_ACCESS_TOKEN_FILE"},
		"an envelope in the file":  {tokenFile(t, envelope), "not a user access token (typ at+jwt)"},
		"an unreadable token file": {filepath.Join(t.TempDir(), "absent"), "GO_API_ROUTING_ACCESS_TOKEN_FILE"},
		"a user access token":      {tokenFile(t, tokenWithHeader(t, "at+jwt")), "no query-api URL"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("GO_API_ROUTING_ACCESS_TOKEN_FILE", c.file)
			err := run(base)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("run(enable) = %v, want a refusal naming %q", err, c.want)
			}
			if strings.Contains(err.Error(), envelope) || strings.Contains(err.Error(), "synthetic-signature") {
				t.Fatal("the refusal carries the credential")
			}
		})
	}
}
