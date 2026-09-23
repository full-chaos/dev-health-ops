package externalingest

import "testing"

func TestMatchesInstance(t *testing.T) {
	cases := []struct {
		name     string
		system   string
		instance string
		source   integrationSource
		family   string
		want     bool
	}{
		{"github external_id case-insensitive", "github", "Acme/API",
			integrationSource{ExternalID: "acme/api"}, legacyEntityFamily, true},
		{"github full_name fallback", "github", "acme/api",
			integrationSource{FullName: "acme/api"}, legacyEntityFamily, true},
		{"github no match", "github", "acme/api",
			integrationSource{ExternalID: "other/repo"}, legacyEntityFamily, false},
		{"gitlab path_with_namespace", "gitlab", "group/sub/project",
			integrationSource{Metadata: map[string]any{"path_with_namespace": "group/sub/project"}}, legacyEntityFamily, true},
		{"gitlab numeric external_id", "gitlab", "123",
			integrationSource{ExternalID: "123"}, legacyEntityFamily, true},
		{"linear org-wide placeholder literal", "linear", "any-team-uuid",
			integrationSource{ExternalID: "linear"}, legacyEntityFamily, true},
		{"linear org-wide placeholder metadata flag", "linear", "any-team-uuid",
			integrationSource{Metadata: map[string]any{"org_wide_placeholder": true}}, legacyEntityFamily, true},
		{"linear specific team id", "linear", "team-uuid-1",
			integrationSource{ExternalID: "team-uuid-1"}, legacyEntityFamily, true},
		{"custom system never matches", "custom", "x", integrationSource{ExternalID: "x"}, legacyEntityFamily, false},
		{"empty instance never matches", "github", "", integrationSource{ExternalID: ""}, legacyEntityFamily, false},
		{"operational github is the named limit", "github", "acme/api",
			integrationSource{ExternalID: "acme/api"}, operationalEntityFamily, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := matchesInstance(c.system, c.instance, c.source, c.family); got != c.want {
				t.Errorf("got %v, want %v", got, c.want)
			}
		})
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
