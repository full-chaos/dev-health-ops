package legacyingest

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func mustBody(t *testing.T, text string) pybody.Body {
	t.Helper()
	value, err := pyjson.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return pybody.Body{Value: value}
}

func marshal(t *testing.T, parsed batch) string {
	t.Helper()
	text, err := pyjson.MarshalModel(parsed.Dump)
	if err != nil {
		t.Fatal(err)
	}
	return string(text)
}
