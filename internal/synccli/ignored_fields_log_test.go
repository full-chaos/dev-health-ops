package synccli

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func TestShapeRefusalLogsTheIgnoredKeyNamesNeverTheirValues(t *testing.T) {
	log := captureLookupLog(t)
	_, _, err := githubCredentialFromJSON([]byte(`{"org":"acme","zz_extra":"distinctive-secret-9727"}`))
	logCredentialLookupFailure(context.Background(), "org-1", failureOf(t, err))
	out := log.String()
	for _, want := range []string{`"reason":"credential_shape_invalid"`, `"ignored_fields":"org,zz_extra"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("log lacks %q: %s", want, out)
		}
	}
	if strings.Contains(out, "distinctive-secret-9727") {
		t.Fatalf("log carries a payload value: %s", out)
	}
}

func TestIgnoredKeyNamesAreBoundedInTheRefusalLine(t *testing.T) {
	log := captureLookupLog(t)
	var keys []string
	for i := 0; i < 25; i++ {
		keys = append(keys, fmt.Sprintf(`"k%02d":"v"`, i))
	}
	_, _, err := githubCredentialFromJSON([]byte("{" + strings.Join(keys, ",") + "}"))
	logCredentialLookupFailure(context.Background(), "org-1", failureOf(t, err))
	out := log.String()
	if !strings.Contains(out, `,(5 more)"`) {
		t.Fatalf("log lacks the bound marker: %s", out)
	}
	if strings.Contains(out, "k20") {
		t.Fatalf("log carries more than the bounded names: %s", out)
	}
}

func TestNonShapeRefusalsKeepTheirKeySet(t *testing.T) {
	log := captureLookupLog(t)
	logCredentialLookupFailure(context.Background(), "org-1", credentialLookupFailure{Reason: reasonDecryptFailed, IgnoredFields: []string{"org"}})
	_, err := githubCredentialFromDB(context.Background(), "mysql://dho_user:s3cret-pw@127.0.0.1:1/dho", "org-1", cli.Env{})
	if err == nil {
		t.Fatal("want a refusal for a database URL the lookup cannot use")
	}
	if strings.Contains(log.String(), "ignored_fields") {
		t.Fatalf("a non-shape refusal line carries ignored_fields: %s", log.String())
	}
}
