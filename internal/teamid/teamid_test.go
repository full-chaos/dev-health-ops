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

func TestNativeStripsOnlyTheProvidersOwnPrefix(t *testing.T) {
	cases := []struct{ provider, id, want string }{
		{"linear", "linear:ENG", "ENG"},
		{"linear", "ENG", "ENG"},
		{"jira", "jira:abc", "abc"},
		{"github", "gh:platform", "platform"},
		{"linear", "gh:platform", "gh:platform"},
		{"", "linear:ENG", "linear:ENG"},
	}
	for _, c := range cases {
		if got := Native(c.provider, c.id); got != c.want {
			t.Errorf("Native(%q, %q) = %q, want %q", c.provider, c.id, got, c.want)
		}
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
	} {
		if err := CheckPushed(c.system, c.id); !errors.Is(err, ErrBareTeamID) {
			t.Errorf("CheckPushed(%q, %q) = %v, want ErrBareTeamID", c.system, c.id, err)
		}
	}
}

func TestNativeOfAPushedAtlassianIDIsTheBareUUID(t *testing.T) {
	if got := Native("atlassian", Of("atlassian", "atlassian:u-1")); got != "u-1" {
		t.Fatalf("got %q, want u-1", got)
	}
}
