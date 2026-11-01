package authconfig

import "testing"

func TestSSOJITIsOffUnlessSetToTrue(t *testing.T) {
	for raw, want := range map[string]bool{"": false, "false": false, "true": true} {
		env := baseEnv()
		env[EnvSSOJIT] = raw
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if cfg.SSO.JIT != want {
			t.Errorf("%q: JIT = %v, want %v", raw, cfg.SSO.JIT, want)
		}
	}
	env := baseEnv()
	env[EnvSSOJIT] = "yes"
	if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err == nil {
		t.Fatal(`"yes" was accepted`)
	}
}
