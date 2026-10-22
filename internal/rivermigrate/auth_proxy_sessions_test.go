package rivermigrate

import "testing"

func TestAuthProxySessionsIsReadAsGoAPIReadsIt(t *testing.T) {
	for raw, want := range map[string]bool{"": false, "  ": false, "false": false, "0": false, "true": true, "1": true, " TRUE ": true} {
		got, err := authProxySessions(func(string) (string, bool) { return raw, true })
		if err != nil || got != want {
			t.Errorf("%q: %v %v, want %v", raw, got, err, want)
		}
	}
	if got, err := authProxySessions(func(string) (string, bool) { return "", false }); err != nil || got {
		t.Errorf("unset: %v %v, want off", got, err)
	}
	if _, err := authProxySessions(func(string) (string, bool) { return "yes", true }); err == nil {
		t.Error("an unparsable value was accepted")
	}
}
