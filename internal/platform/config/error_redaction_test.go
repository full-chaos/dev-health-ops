package config

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/credentialshapes"
)

// CHAOS-7937: the two config error sinks redact (removing the RedactText call from either turns this RED).
func TestConfigErrorSinksRedactEveryPrefixedShape(t *testing.T) {
	for _, shape := range credentialshapes.Shapes() {
		if shape.Opaque {
			continue
		}
		window := credentialshapes.Tail(shape.Value())
		var out bytes.Buffer
		WriteConfigError(&out, errors.New("setting quoted "+shape.Value()+" back"))
		if strings.Contains(out.String(), window) || !strings.Contains(out.String(), "[REDACTED]") {
			t.Fatalf("WriteConfigError %s: %s", shape.ID, out.String())
		}
		driver := redactDriverError("assembled-dsn-value", errors.New("parse failed near "+shape.Value()+" in assembled-dsn-value"))
		if strings.Contains(driver, window) || strings.Contains(driver, "assembled-dsn-value") {
			t.Fatalf("redactDriverError %s: %s", shape.ID, driver)
		}
	}
}
