package externalingest

import "testing"

func TestMatchesInstance(t *testing.T) {
	cases := []struct {
		name     string
		system   string
		instance string
		source   integrationSource
		family   string
		config   map[string]any
		want     bool
	}{
		{"github external_id case-insensitive", "github", "Acme/API",
			integrationSource{ExternalID: "acme/api"}, legacyEntityFamily, nil, true},
		{"github full_name fallback", "github", "acme/api",
			integrationSource{FullName: "acme/api"}, legacyEntityFamily, nil, true},
		{"github no match", "github", "acme/api",
			integrationSource{ExternalID: "other/repo"}, legacyEntityFamily, nil, false},
		{"gitlab path_with_namespace", "gitlab", "group/sub/project",
			integrationSource{Metadata: map[string]any{"path_with_namespace": "group/sub/project"}}, legacyEntityFamily, nil, true},
		{"gitlab numeric external_id", "gitlab", "123",
			integrationSource{ExternalID: "123"}, legacyEntityFamily, nil, true},
		{"linear org-wide placeholder literal", "linear", "any-team-uuid",
			integrationSource{ExternalID: "linear"}, legacyEntityFamily, nil, true},
		{"linear org-wide placeholder metadata flag", "linear", "any-team-uuid",
			integrationSource{Metadata: map[string]any{"org_wide_placeholder": true}}, legacyEntityFamily, nil, true},
		{"linear specific team id", "linear", "team-uuid-1",
			integrationSource{ExternalID: "team-uuid-1"}, legacyEntityFamily, nil, true},
		{"custom system never matches", "custom", "x", integrationSource{ExternalID: "x"}, legacyEntityFamily, nil, false},
		{"empty instance never matches", "github", "", integrationSource{ExternalID: ""}, legacyEntityFamily, nil, false},
		{"operational github default host matches github.com", "github", "github.com",
			integrationSource{}, operationalEntityFamily, nil, true},
		{"operational github default host does not match a different host", "github", "git.example.com",
			integrationSource{}, operationalEntityFamily, nil, false},
		{"operational github explicit configured host matches", "github", "git.example.com",
			integrationSource{}, operationalEntityFamily, map[string]any{"github_instance_url": "https://git.example.com"}, true},
		{"operational github explicit configured host is case-insensitive", "github", "GIT.EXAMPLE.COM",
			integrationSource{}, operationalEntityFamily, map[string]any{"github_instance_url": "git.example.com"}, true},
		{"operational gitlab explicit configured host matches", "gitlab", "gitlab.example.org",
			integrationSource{}, operationalEntityFamily, map[string]any{"gitlab_instance_url": "https://gitlab.example.org:443/"}, true},
		{"operational github api.github.com aliases to github.com", "github", "api.github.com",
			integrationSource{}, operationalEntityFamily, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchesInstance(c.system, c.instance, c.source, c.family, c.config); got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestNormalizedOperationalHost(t *testing.T) {
	cases := []struct {
		system, raw, want string
	}{
		{"github", "github.com", "github.com"},
		{"github", "api.github.com", "github.com"},
		{"github", "https://GitHub.com/", "github.com"},
		{"gitlab", "gitlab.example.org", "gitlab.example.org"},
		{"gitlab", "https://gitlab.example.org:443", "gitlab.example.org"},
		{"gitlab", "gitlab.example.org:8443", "gitlab.example.org:8443"},
		{"github", "", ""},
	}
	for _, c := range cases {
		if got := normalizedOperationalHost(c.system, c.raw); got != c.want {
			t.Errorf("normalizedOperationalHost(%q, %q) = %q, want %q", c.system, c.raw, got, c.want)
		}
	}
}

func TestOwnershipErrorMapsEveryNonCustomerPushMode(t *testing.T) {
	cases := map[effectiveMode]string{
		modeUnclaimed:     "source_not_registered",
		modeDisabled:      "source_disabled",
		modeFullchaosSync: "source_owned_by_fullchaos_sync",
	}
	for mode, code := range cases {
		err := ownershipError(mode, "github", "acme/api")
		if err.Code != code || err.Status != 403 {
			t.Errorf("%s: got %d %s, want 403 %s", mode, err.Status, err.Code, code)
		}
	}
}
