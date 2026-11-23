package integrationsadmin

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The integration PATCH replaces the config whole; the CI artifact exclude list stored in it survives a new config
// that does not carry the key, a carried value replaces it after validation, and a refused value is a 422 without it.
func TestIntegrationUpdateKeepsTheArtifactExcludeList(t *testing.T) {
	current := &integration{Provider: "github", Config: `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-x-"]}`}
	next := pyjson.NewObject()
	next.Set("fetch_comments", false)
	if err := keepArtifactExclude(current, next); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(next); got != `{"fetch_comments": false, "ci_artifact_exclude_prefixes": ["ci-x-"]}` {
		t.Fatalf("a config without the key: %s, want the stored list kept", got)
	}
	replaced := pyjson.NewObject()
	replaced.Set("ci_artifact_exclude_prefixes", []pyjson.Value{"ci-z-"})
	if err := keepArtifactExclude(current, replaced); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(replaced); got != `{"ci_artifact_exclude_prefixes": ["ci-z-"]}` {
		t.Fatalf("a carried list: %s, want it as sent", got)
	}
	bad := pyjson.NewObject()
	bad.Set("ci_artifact_exclude_prefixes", []pyjson.Value{"zz9 x"})
	err := keepArtifactExclude(current, bad)
	var denied refusal
	if !errors.As(err, &denied) || denied.status != http.StatusUnprocessableEntity || strings.Contains(denied.message, "zz9") {
		t.Fatalf("an invalid list: %v, want a 422 without the value", err)
	}
	untouched := &integration{Provider: "gitlab", Config: `{}`}
	plain := pyjson.NewObject()
	plain.Set("a", int64(1))
	if err := keepArtifactExclude(untouched, plain); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(plain); got != `{"a": 1}` {
		t.Fatalf("no list anywhere: %s, want the config as sent", got)
	}
}
