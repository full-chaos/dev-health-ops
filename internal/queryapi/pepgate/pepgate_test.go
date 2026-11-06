package pepgate

import (
	"slices"
	"testing"
)

// P16: the zero value is no scope: it names no organization and is restricted. Nothing reads it as unrestricted.
func TestZeroScopeIsNoScope(t *testing.T) {
	var zero Scope
	if org, ok := zero.OrgID(); ok || org != "" {
		t.Fatalf("zero scope OrgID = (%q, %v), want (\"\", false)", org, ok)
	}
	if !zero.Restricted() {
		t.Fatal("zero scope is unrestricted")
	}
	if zero.RepoKeys() != nil || zero.Kind() != KindNone {
		t.Fatalf("zero scope keys %v kind %v", zero.RepoKeys(), zero.Kind())
	}
}

// An empty organization builds no scope for an enforced decision (organization, allowlist).
func TestEmptyOrganizationIsNoScope(t *testing.T) {
	for name, s := range map[string]Scope{
		"organization": Organization(""),
		"allowlist":    Allowlist("", []string{"acme/a"}),
	} {
		if _, ok := s.OrgID(); ok || !s.Restricted() || s.Kind() != KindNone {
			t.Errorf("%s(\"\") = kind %v restricted %v, want no scope", name, s.Kind(), s.Restricted())
		}
	}
}

// Product rule 1 (D6773): a legacy request with no org (a platform admin) keeps today's path: a Legacy scope that
// reads the empty org, unrestricted, as the claims organization did.
func TestEmptyOrganizationLegacyKeepsTodaysPath(t *testing.T) {
	s := Legacy("")
	if org, ok := s.OrgID(); !ok || org != "" || s.Restricted() || s.Kind() != KindLegacy || s.RepoKeys() != nil {
		t.Fatalf("Legacy(\"\") = org (%q, %v) kind %v restricted %v keys %v, want the legacy scope of the empty org",
			org, ok, s.Kind(), s.Restricted(), s.RepoKeys())
	}
}

func TestConstructors(t *testing.T) {
	const org = "22222222-2222-2222-2222-222222222222"
	for name, tc := range map[string]struct {
		scope      Scope
		kind       Kind
		restricted bool
		keys       []string
	}{
		"legacy":          {Legacy(org), KindLegacy, false, nil},
		"organization":    {Organization(org), KindOrganization, false, nil},
		"allowlist":       {Allowlist(org, []string{"acme/a", "acme/b"}), KindAllowlist, true, []string{"acme/a", "acme/b"}},
		"empty allowlist": {Allowlist(org, []string{}), KindAllowlist, true, []string{}},
		"nil allowlist":   {Allowlist(org, nil), KindAllowlist, true, []string{}},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := tc.scope.OrgID()
			if !ok || got != org {
				t.Fatalf("OrgID = (%q, %v), want (%q, true)", got, ok, org)
			}
			if tc.scope.Kind() != tc.kind || tc.scope.Restricted() != tc.restricted {
				t.Fatalf("kind %v restricted %v, want %v %v", tc.scope.Kind(), tc.scope.Restricted(), tc.kind, tc.restricted)
			}
			keys := tc.scope.RepoKeys()
			if (keys == nil) != (tc.keys == nil) || !slices.Equal(keys, tc.keys) {
				t.Fatalf("RepoKeys = %#v, want %#v", keys, tc.keys)
			}
		})
	}
}

// An allowlist scope owns its keys: neither the caller's slice nor the returned copy can widen it later.
func TestAllowlistKeysAreCopies(t *testing.T) {
	keys := []string{"acme/a"}
	s := Allowlist("org", keys)
	keys[0] = "*"
	out := s.RepoKeys()
	if len(out) != 1 || out[0] != "acme/a" {
		t.Fatalf("the caller's slice changed the scope: %v", out)
	}
	out[0] = "*"
	if again := s.RepoKeys(); len(again) != 1 || again[0] != "acme/a" {
		t.Fatalf("a returned slice changed the scope: %v", again)
	}
}

// Scope is comparable, so authctx.Claims stays comparable.
func TestScopeIsComparable(t *testing.T) {
	const org = "22222222-2222-2222-2222-222222222222"
	if Legacy(org) != Legacy(org) || Organization(org) != Organization(org) || Legacy(org) == Organization(org) {
		t.Fatal("scope equality is not by kind and organization")
	}
}
