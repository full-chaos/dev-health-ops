package authconfig

import (
	"strings"
	"testing"
)

func TestThePushProjectionIsOffByDefaultAndOnlyOnOrOffAreValues(t *testing.T) {
	for value, want := range map[string]bool{"": false, "off": false, "OFF": false, " off ": false, "on": true, "ON": true, " On ": true} {
		env := baseEnv()
		if value != "" {
			env[EnvPushProjection] = value
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Fatalf("%s=%q: %v", EnvPushProjection, value, err)
		}
		if cfg.PushProjection != want {
			t.Errorf("%s=%q: PushProjection = %v, want %v", EnvPushProjection, value, cfg.PushProjection, want)
		}
	}
	for _, bad := range []string{"true", "false", "1", "yes", "enabled", "onn"} {
		env := baseEnv()
		env[EnvPushProjection] = bad
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), EnvPushProjection) {
			t.Errorf("%s=%q: err %v, want a refusal naming %s", EnvPushProjection, bad, err, EnvPushProjection)
		}
		if err != nil && strings.Contains(err.Error(), bad+"\"") {
			t.Errorf("the refusal quotes the value: %v", err)
		}
	}
}
