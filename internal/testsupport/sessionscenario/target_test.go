package sessionscenario

import "testing"

func TestTargetNeedsLimitReset(t *testing.T) {
	base := Target{BaseURL: "http://target.test", PostgresURI: "postgres://x"}
	if base.Validate() == nil {
		t.Error("a target with no way to open fresh rate-limit windows was accepted")
	}
	withValkey := base
	withValkey.ValkeyURI = "redis://x"
	if err := withValkey.Validate(); err != nil {
		t.Errorf("valkey target refused: %v", err)
	}
	withReset := base
	withReset.ResetLimits = func() {}
	if err := withReset.Validate(); err != nil {
		t.Errorf("reset-func target refused: %v", err)
	}
	if (Target{BaseURL: "http://target.test", ValkeyURI: "redis://x"}).Validate() == nil {
		t.Error("a target with no Postgres URI was accepted")
	}
}
