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
		{"atlassian", "x", "atlassian:x"},
		// A Linear key that looks like another provider's prefix is still a
		// Linear key.
		{"linear", "gh:ENG", "linear:gh:ENG"},
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
		{"linear", "gh:ENG"},
		{"", "ENG"},
		{" ", "ENG"},
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
