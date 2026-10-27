package main

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// auth-service exports spans only when the operator turns tracing on: the local
// stack has no collector, so the pass-through defaults to off like the other
// traced services of the file.
func TestComposeAuthServiceTracingDefaultsOff(t *testing.T) {
	var file composeKeyFile
	if err := yaml.Unmarshal(readCompose(t, "compose.yml"), &file); err != nil {
		t.Fatal(err)
	}
	got, ok := file.Services["auth-service"].Environment["OTEL_ENABLED"]
	if !ok {
		t.Fatal("auth-service: OTEL_ENABLED is not passed through")
	}
	if got != "${OTEL_ENABLED:-false}" {
		t.Errorf("auth-service: OTEL_ENABLED = %q, want the default-off pass-through ${OTEL_ENABLED:-false}", got)
	}
}
