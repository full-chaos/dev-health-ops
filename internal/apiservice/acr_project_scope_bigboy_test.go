package apiservice

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/bigboyrender"
)

// The bigboy users.d check reads go-api's ACR_ACCESS_PROJECT_SCOPE with its own reader
// (bigboyrender.ProjectScopeValue, CHAOS-12268), because cigate must not link go-api. Two readers of one flag are a
// cross-implementation pair, so this is their differential check: for every value, both must agree on on/off and on
// whether the value is refused. A drift would make bigboy render or compare the posture of a flag go-api does not run
// with, so go-api would be unready with the check green, or the other way round.
func TestTheBigboyProjectScopeReaderAgreesWithGoAPI(t *testing.T) {
	if bigboyrender.ProjectScopeEnv != EnvACRAccessProjectScope {
		t.Fatalf("bigboy reads %q, go-api reads %q", bigboyrender.ProjectScopeEnv, EnvACRAccessProjectScope)
	}
	values := []string{
		"", " ", "\t", "true", "false", "TRUE", "FALSE", "True", "False", "t", "f", "T", "F", "1", "0",
		" true ", "\tfalse\n", "yes", "no", "on", "off", "2", "-1", "tRuE", "truee", "1.0", "y", "enabled",
	}
	for _, raw := range values {
		goAPI, goErr := ParseACRAccessProjectScope(func(key string) (string, bool) {
			if key != EnvACRAccessProjectScope {
				t.Fatalf("go-api read %q", key)
			}
			return raw, true
		})
		bigboy, bigboyErr := bigboyrender.ProjectScopeValue(raw)
		if (goErr != nil) != (bigboyErr != nil) || goAPI != bigboy {
			t.Errorf("value %q: go-api (%v, err %v), bigboy (%v, err %v)", raw, goAPI, goErr, bigboy, bigboyErr)
		}
	}
	// Unset is off for go-api; the bigboy reader takes a missing line as the empty value.
	if on, err := ParseACRAccessProjectScope(func(string) (string, bool) { return "", false }); on || err != nil {
		t.Fatalf("go-api unset: %v, %v", on, err)
	}
	if on, err := bigboyrender.ProjectScopeValue(""); on || err != nil {
		t.Fatalf("bigboy empty: %v, %v", on, err)
	}
}
