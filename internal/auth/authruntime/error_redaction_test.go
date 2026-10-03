package authruntime

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// CHAOS-7937: the two error sinks of the auth runtime redact (removing the RedactText call from either turns this RED).
func TestAuthRuntimeErrorSinksRedactEveryPrefixedShape(t *testing.T) {
	for _, shape := range credentialshapes.Shapes() {
		if shape.Opaque {
			continue
		}
		for name, write := range map[string]func(*bytes.Buffer, error){
			"configuration": func(b *bytes.Buffer, err error) { writeConfigurationError(b, err) },
			"argument":      func(b *bytes.Buffer, err error) { writeArgumentError(b, err) },
		} {
			var out bytes.Buffer
			write(&out, errors.New("invalid value "+shape.Value()+" for the setting"))
			if strings.Contains(out.String(), credentialshapes.Tail(shape.Value())) || !strings.Contains(out.String(), "[REDACTED]") {
				t.Fatalf("%s %s: %s", name, shape.ID, out.String())
			}
		}
	}
}
