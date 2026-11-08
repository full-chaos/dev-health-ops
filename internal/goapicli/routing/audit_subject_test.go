package routing

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
)

// seed and enable take the audit subject from the token /buildinfo accepted, never from a second read of the token
// file. The fake /buildinfo replaces the file with a value that is not a token before it answers 200, so a verb that
// reads the file again refuses on the token file; a verb that does not goes on to its Postgres step, which refuses the
// unparseable DSN. No database is reached.
func TestSeedAndEnableDoNotReadTheTokenFileAfterBuildinfoAcceptedIt(t *testing.T) {
	const dsn = "postgres://%zz"
	operation := mcpclass.Operation("analytics")
	for name, argv := range map[string][]string{
		"seed":   {"seed", "-operations", operation, "-recorded-by", "w", "-review-evidence", "y", "-postgres-uri", dsn},
		"enable": {"enable", "-operations", operation, "-mode", "canary", "-recorded-by", "w", "-review-evidence", "y", "-postgres-uri", dsn},
	} {
		t.Run(name, func(t *testing.T) {
			path := tokenFile(t, tokenWithHeader(t, "at+jwt"))
			mux := http.NewServeMux()
			mux.HandleFunc("/registry", func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprintf(w, `{"schema_digest": %q, "operations": [{"operation": %q, "document_digest": "d"}]}`, localSchemaDigest(), operation)
			})
			mux.HandleFunc("/buildinfo", func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") == "" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if err := os.WriteFile(path, []byte("rotated-to-not-a-token\n"), 0o600); err != nil {
					t.Error(err)
				}
				_, _ = w.Write([]byte(`{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`))
			})
			server := httptest.NewServer(mux)
			t.Cleanup(server.Close)
			t.Setenv("POSTGRES_URI", "")
			t.Setenv("GO_API_QUERY_API_URL", server.URL)
			t.Setenv("GO_API_ROUTING_ACCESS_TOKEN_FILE", path)

			_, _, err := captureVerb(t, argv...)
			if err == nil || !strings.Contains(err.Error(), "could not be parsed as a Postgres DSN") {
				t.Fatalf("%s = %v, want the Postgres DSN refusal: the audit subject must come from the token /buildinfo accepted", name, err)
			}
			if strings.Contains(err.Error(), "synthetic-signature") || strings.Contains(err.Error(), "rotated-to-not-a-token") {
				t.Fatal("the refusal carries the credential")
			}
		})
	}
}
