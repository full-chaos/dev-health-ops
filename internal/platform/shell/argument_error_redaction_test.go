package shell

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// CHAOS-7937: the argument-error sink redacts. Removing the RedactText call from writeArgumentError turns both tests RED.
func TestWriteArgumentErrorRedactsEveryPrefixedShape(t *testing.T) {
	for _, shape := range credentialshapes.Shapes() {
		if shape.Opaque {
			continue
		}
		var out bytes.Buffer
		writeArgumentError(&out, errors.New("flag provided but not defined: -"+shape.Value()))
		if strings.Contains(out.String(), credentialshapes.Tail(shape.Value())) || !strings.Contains(out.String(), "[REDACTED]") {
			t.Fatalf("%s: %s", shape.ID, out.String())
		}
	}
}

func TestExecuteRedactsAFlagThatQuotesACredentialShape(t *testing.T) {
	shape := credentialshapes.Shapes()[0]
	var stdout, stderr bytes.Buffer
	code := Execute(t.Context(), Spec{Service: "dev-health-worker", RequireQueues: true}, []string{"--" + shape.Value() + "=1"}, testLookup(nil), IO{Stdout: &stdout, Stderr: &stderr})
	if code != 2 || strings.Contains(stderr.String(), credentialshapes.Tail(shape.Value())) {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}
