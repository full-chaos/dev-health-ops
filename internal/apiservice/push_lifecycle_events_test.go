package apiservice

import "testing"

func TestParsePushLifecycleEvents(t *testing.T) {
	for _, tc := range []struct {
		raw, set string
		want     bool
		fails    bool
	}{
		{"", "unset", false, false}, {"  ", "set", false, false}, {"true", "set", true, false}, {" TRUE ", "set", true, false},
		{"false", "set", false, false}, {"1", "set", true, false}, {"yes", "set", false, true}, {"on", "set", false, true},
	} {
		lookup := func(string) (string, bool) { return tc.raw, tc.set == "set" }
		got, err := ParsePushLifecycleEvents(lookup)
		if (err != nil) != tc.fails || got != tc.want {
			t.Errorf("%q (%s): got %v, %v; want %v, fails=%v", tc.raw, tc.set, got, err, tc.want, tc.fails)
		}
	}
	if EnvPushLifecycleEvents != "AUTH_PUSH_LIFECYCLE_EVENTS" {
		t.Fatalf("EnvPushLifecycleEvents = %q", EnvPushLifecycleEvents)
	}
}
