package authconfig

import (
	"strings"
	"testing"
)

func TestTheRevisionFeedIsOffByDefaultAndOnlyOnOrOffAreValues(t *testing.T) {
	for value, want := range map[string]bool{"": false, "off": false, "OFF": false, " off ": false, "on": true, "ON": true, " On ": true} {
		env := withValkey(baseEnv(), testLimitedValkeyURI)
		if value != "" {
			env[EnvRevisionFeed] = value
		}
		cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err != nil {
			t.Fatalf("%s=%q: %v", EnvRevisionFeed, value, err)
		}
		if cfg.RevisionFeed != want {
			t.Errorf("%s=%q: RevisionFeed = %v, want %v", EnvRevisionFeed, value, cfg.RevisionFeed, want)
		}
	}
	for _, bad := range []string{"true", "false", "1", "yes", "enabled", "onn"} {
		env := withValkey(baseEnv(), testLimitedValkeyURI)
		env[EnvRevisionFeed] = bad
		_, err := Load(Spec{LookupEnv: lookupFrom(env)})
		if err == nil || !strings.Contains(err.Error(), EnvRevisionFeed) {
			t.Errorf("%s=%q: err %v, want a refusal naming %s", EnvRevisionFeed, bad, err, EnvRevisionFeed)
		}
		if err != nil && strings.Contains(err.Error(), bad+"\"") && bad != "" {
			t.Errorf("the refusal quotes the value: %v", err)
		}
	}
}

func TestTheRevisionFeedOnNeedsTheValkeyURIEvenWithNoLimitedRoute(t *testing.T) {
	env := baseEnv()
	env[EnvRevisionFeed] = "on"
	_, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err == nil || !strings.Contains(err.Error(), EnvValkeyURI) || !strings.Contains(err.Error(), EnvRevisionFeed) {
		t.Fatalf("on without the URI: err %v, want a refusal naming %s and %s", err, EnvRevisionFeed, EnvValkeyURI)
	}
	env = withValkey(baseEnv(), testLimitedValkeyURI)
	env[EnvRevisionFeed] = "on"
	cfg, err := Load(Spec{LookupEnv: lookupFrom(env)})
	if err != nil || !cfg.RevisionFeed || cfg.LimitedRoutesMounted() {
		t.Fatalf("on with the URI and no limited route: err %v, feed %v, limited %v", err, cfg.RevisionFeed, cfg.LimitedRoutesMounted())
	}
}

func TestTheRevisionFeedOffChangesNothingAboutTheValkeyRequirement(t *testing.T) {
	env := baseEnv()
	env[EnvRevisionFeed] = "off"
	if cfg, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil || cfg.ValkeyURI.Configured() {
		t.Fatalf("off without a URI: err %v", err)
	}
}
