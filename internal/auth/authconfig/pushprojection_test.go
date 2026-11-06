package authconfig

import (
	"strings"
	"testing"
)

func TestThePushProjectionIsOffByDefaultAndOnlyOnOrOffAreValues(t *testing.T) {
	projectionSwitchIsOffByDefaultAndOnlyOnOrOff(t, EnvPushProjection, func(c Config) bool { return c.PushProjection })
}

func TestTheRepoIdentityProjectionIsOffByDefaultAndOnlyOnOrOffAreValues(t *testing.T) {
	projectionSwitchIsOffByDefaultAndOnlyOnOrOff(t, EnvRepoIdentityProjection, func(c Config) bool { return c.RepoIdentityProjection })
	cfg, err := Load(Spec{LookupEnv: lookupFrom(baseEnvWith(EnvRepoIdentityProjection, "on"))})
	if err != nil || cfg.PushProjection {
		t.Fatalf("the repository identity switch moved the push switch: %v, %v", cfg.PushProjection, err)
	}
}

func projectionSwitchIsOffByDefaultAndOnlyOnOrOff(t *testing.T, name string, read func(Config) bool) {
	t.Helper()
	for value, want := range map[string]bool{"": false, "off": false, "OFF": false, " off ": false, "on": true, "ON": true, " On ": true} {
		env := baseEnv()
		if value != "" {
			env[name] = value
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Fatalf("%s=%q: %v", name, value, err)
		}
		if read(cfg) != want {
			t.Errorf("%s=%q: switch = %v, want %v", name, value, read(cfg), want)
		}
	}
	for _, bad := range []string{"true", "false", "1", "yes", "enabled", "onn"} {
		env := baseEnv()
		env[name] = bad
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Errorf("%s=%q: err %v, want a refusal naming %s", name, bad, err, name)
		}
		if err != nil && strings.Contains(err.Error(), bad+"\"") {
			t.Errorf("the refusal quotes the value: %v", err)
		}
	}
}

func baseEnvWith(name, value string) map[string]string {
	env := baseEnv()
	env[name] = value
	return env
}
