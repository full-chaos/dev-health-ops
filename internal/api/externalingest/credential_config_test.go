package externalingest

import (
	"errors"
	"testing"
)

// TestCredentialConfig pins (decodePyConfig) `credential.config or {}` before `.get`: an
// absent, null or falsy config is empty, a JSON object is itself, and any other
// value is the AttributeError Python raises (a typed refusal here).
func TestCredentialConfig(t *testing.T) {
	for _, c := range []struct {
		name    string
		raw     string
		want    map[string]string
		refused bool
	}{
		{"absent", "", nil, false},
		{"null", "null", nil, false},
		{"empty object", "{}", map[string]string{}, false},
		{"object", `{"url":"https://x.test"}`, map[string]string{"url": "https://x.test"}, false},
		{"empty array", "[]", nil, false},
		{"empty string", `""`, nil, false},
		{"zero", "0", nil, false},
		{"false", "false", nil, false},
		{"array", `["bad"]`, nil, true},
		{"string", `"text"`, nil, true},
		{"number", "5", nil, true},
		{"true", "true", nil, true},
	} {
		got, err := decodePyConfig([]byte(c.raw))
		if c.refused != errors.Is(err, errCredentialConfigNotObject) || (!c.refused && err != nil) {
			t.Errorf("%s: err = %v, refused want %v", c.name, err, c.refused)
			continue
		}
		if len(got) != len(c.want) {
			t.Errorf("%s: config = %v, want %v", c.name, got, c.want)
			continue
		}
		for key, want := range c.want {
			if text, ok := got.str(key); !ok || text != want {
				t.Errorf("%s: config[%q] = (%q, %t), want %q", c.name, key, text, ok, want)
			}
		}
	}
}
