package edgetoken

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

// With no key file the verb refuses naming the key file variable, whatever JWT_SECRET_KEY holds: no HS256 mode.
func TestRunWithoutAKeyFileRefusesAndMintsNothing(t *testing.T) {
	t.Setenv(edgetokenmint.SigningKeyFileEnvVar, "")
	t.Setenv("JWT_SECRET_KEY", testKey)
	var opened, closed bool
	var out bytes.Buffer
	err := run(context.Background(), []string{"-org", testOrg}, &out, io.Discard, opener(&fakeDB{active: true}, &opened, &closed))
	if err == nil || !strings.Contains(err.Error(), edgetokenmint.SigningKeyFileEnvVar) {
		t.Fatalf("err = %v, want a refusal naming %s", err, edgetokenmint.SigningKeyFileEnvVar)
	}
	if opened || out.Len() != 0 {
		t.Fatalf("opened=%v, stdout=%q: a token path ran without a key file", opened, out.String())
	}
}
