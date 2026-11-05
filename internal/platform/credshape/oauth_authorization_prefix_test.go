package credshape

import (
	"strings"
	"testing"
)

// The OAuth authorization code and the consent anti-CSRF token carry
// credential_prefix markers in credential-classes.json, so the contract-derived
// scanner refuses a value of either shape and probes it.
func TestTheScannerMatchesTheOAuthAuthorizationSecrets(t *testing.T) {
	scanner, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"fcoac_", "fcocs_"} {
		value := prefix + strings.Repeat("Kq", 21) + "Z"
		if rule := scanner.Match(value); rule != "prefix:"+prefix {
			t.Errorf("a %s value matched rule %q, want prefix:%s", prefix, rule, prefix)
		}
		if rule := scanner.Match("redirect code=" + value + "&state=x"); rule == "" {
			t.Errorf("a %s value inside text was not matched", prefix)
		}
		probed := false
		for _, probe := range probes {
			if strings.HasPrefix(probe, prefix) {
				probed = true
			}
		}
		if !probed {
			t.Errorf("no probe carries the %s prefix", prefix)
		}
	}
}
