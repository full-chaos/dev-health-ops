package teamid

import (
	"errors"
	"testing"
)

func TestOfPrefixesEveryProviderOnce(t *testing.T) {
	cases := []struct {
		provider, id, want string
	}{
		{"github", "platform", "gh:platform"},
		{"github", "gh:platform", "gh:platform"},
		{"gitlab", " acme/ops ", "gl:acme/ops"},
		{"gitlab", "gl:acme/ops", "gl:acme/ops"},
		{"linear", "ENG", "linear:ENG"},
		{"linear", "linear:ENG", "linear:ENG"},
		{"jira", "9b1c2d3e-0000-4000-8000-00000000000a", "jira:9b1c2d3e-0000-4000-8000-00000000000a"},
		{"jira", "jira:9b1c2d3e-0000-4000-8000-00000000000a", "jira:9b1c2d3e-0000-4000-8000-00000000000a"},
		{"ms-teams", "abc", "ms-teams:abc"},
		{"custom", "squad-7", "custom:squad-7"},
		{"pagerduty", "P123", "pagerduty:P123"},
		// An id that already carries a known provider key stays, whatever
		// provider writes it.
		{"linear", "gh:ENG", "gh:ENG"},
		{"custom", "gh:x", "gh:x"},
		{"custom", "gl:acme/ops", "gl:acme/ops"},
		{"custom", "linear:ENG", "linear:ENG"},
		{"custom", "jira:abc", "jira:abc"},
		{"custom", "pagerduty:P1", "pagerduty:P1"},
		{"pagerduty", "custom:x", "custom:x"},
		{"github", "ms-teams:abc", "ms-teams:abc"},
		// A custom id with no known key gets the system's prefix.
		{"custom", "squad:7", "custom:squad:7"},
		{"custom", "unknown:7", "custom:unknown:7"},
		// A pushed Atlassian team is the native Atlassian team: jira prefix,
		// and the atlassian: form folds into it.
		{"atlassian", "u-1", "jira:u-1"},
		{"atlassian", "atlassian:u-1", "jira:u-1"},
		{"atlassian", "jira:u-1", "jira:u-1"},
		{"jira", "atlassian:u-1", "jira:u-1"},
		{"", " ENG ", "ENG"},
	}
	for _, c := range cases {
		got := Of(c.provider, c.id)
		if got != c.want {
			t.Errorf("Of(%q, %q) = %q, want %q", c.provider, c.id, got, c.want)
		}
		if again := Of(c.provider, got); again != got {
			t.Errorf("Of(%q, Of(..)) = %q, not idempotent on %q", c.provider, again, got)
		}
	}
}

func TestCheckRefusesABareOrEmptyProviderTeamID(t *testing.T) {
	refused := []struct{ provider, id string }{
		{"linear", "ENG"},
		{"linear", "linear:"},
		{"linear", "linear: "},
		{"linear", " linear:ENG"},
		{"jira", "9b1c2d3e-0000-4000-8000-00000000000a"},
		{"github", "platform"},
		{"gitlab", "acme/ops"},
		{"custom", "squad-7"},
		{"", "ENG"},
		{" ", "ENG"},
		{"atlassian", "atlassian:u-1"},
	}
	for _, c := range refused {
		if err := Check(c.provider, c.id); !errors.Is(err, ErrBareTeamID) {
			t.Errorf("Check(%q, %q) = %v, want ErrBareTeamID", c.provider, c.id, err)
		}
	}
	accepted := []struct{ provider, id string }{
		{"linear", "linear:ENG"},
		{"jira", "jira:9b1c2d3e-0000-4000-8000-00000000000a"},
		{"github", "gh:platform"},
		{"gitlab", "gl:acme/ops"},
		{"custom", "custom:squad-7"},
		{" linear ", "linear:ENG"},
	}
	for _, c := range accepted {
		if err := Check(c.provider, c.id); err != nil {
			t.Errorf("Check(%q, %q) = %v, want nil", c.provider, c.id, err)
		}
	}
}

// providerMatrix is every native provider with the prefix its team ids carry.
var providerMatrix = []struct{ provider, prefix string }{
	{"jira", "jira:"}, {"gitlab", "gl:"}, {"github", "gh:"}, {"linear", "linear:"},
}

func TestNativeKeyGivesTheKeyOfTheProvidersOwnID(t *testing.T) {
	for _, p := range providerMatrix {
		for _, id := range []string{p.prefix + "ENG", " " + p.prefix + " ENG "} {
			if key, ok := NativeKey(p.provider, id); !ok || key != "ENG" {
				t.Errorf("NativeKey(%q, %q) = (%q, %v), want (ENG, true)", p.provider, id, key, ok)
			}
		}
		if key, ok := NativeKey(p.provider, "ENG"); !ok || key != "ENG" {
			t.Errorf("NativeKey(%q, bare ENG) = (%q, %v), want (ENG, true)", p.provider, key, ok)
		}
	}
	if key, ok := NativeKey("atlassian", Of("atlassian", "atlassian:u-1")); !ok || key != "u-1" {
		t.Errorf("NativeKey(atlassian, pushed atlassian id) = (%q, %v), want (u-1, true)", key, ok)
	}
	if key, ok := NativeKey("jira", "atlassian:u-1"); !ok || key != "u-1" {
		t.Errorf("NativeKey(jira, atlassian:u-1) = (%q, %v), want (u-1, true)", key, ok)
	}
	if key, ok := NativeKey("custom", "custom:x"); !ok || key != "x" {
		t.Errorf("NativeKey(custom, custom:x) = (%q, %v), want (x, true)", key, ok)
	}
}

func TestNativeKeyRefusesAnotherProvidersID(t *testing.T) {
	for _, p := range providerMatrix {
		for _, other := range providerMatrix {
			if other.provider == p.provider {
				continue
			}
			if key, ok := NativeKey(p.provider, other.prefix+"ENG"); ok || key != "" {
				t.Errorf("NativeKey(%q, %q) = (%q, %v), want refused", p.provider, other.prefix+"ENG", key, ok)
			}
			if _, ok := NativeKey(p.provider, other.prefix); ok {
				t.Errorf("NativeKey(%q, %q) accepted a bare foreign prefix", p.provider, other.prefix)
			}
		}
		if _, ok := NativeKey(p.provider, "custom:x"); ok {
			t.Errorf("NativeKey(%q, custom:x) accepted a pushed custom id", p.provider)
		}
		for _, id := range []string{"", "  ", p.prefix, p.prefix + "  "} {
			if _, ok := NativeKey(p.provider, id); ok {
				t.Errorf("NativeKey(%q, %q) accepted an id with no key", p.provider, id)
			}
		}
	}
	if _, ok := NativeKey("", "linear:ENG"); ok {
		t.Error("NativeKey with no provider accepted an id")
	}
	if _, ok := NativeKey("", "ENG"); ok {
		t.Error("NativeKey with no provider accepted a bare id")
	}
}

func TestCheckPushedAcceptsAnyKnownKeyAndRefusesAnEmptyOne(t *testing.T) {
	for _, c := range []struct{ system, id string }{
		{"custom", "custom:x"}, {"custom", "gh:x"}, {"linear", "gl:a/b"}, {"atlassian", "jira:u"},
	} {
		if err := CheckPushed(c.system, c.id); err != nil {
			t.Errorf("CheckPushed(%q, %q) = %v, want nil", c.system, c.id, err)
		}
	}
	for _, c := range []struct{ system, id string }{
		{"custom", "custom:"}, {"custom", "gh:"}, {"custom", "x"}, {"", "gh:"}, {"custom", ""},
		{"custom", "jira:"}, {"custom", "atlassian:"}, {"custom", "custom:jira:"}, {"github", "gh:jira:"}, {"jira", "linear:"}, {"atlassian", "atlassian:"},
	} {
		if err := CheckPushed(c.system, c.id); !errors.Is(err, ErrBareTeamID) {
			t.Errorf("CheckPushed(%q, %q) = %v, want ErrBareTeamID", c.system, c.id, err)
		}
	}
}
