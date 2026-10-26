package identity

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestIsSSORelinkable(t *testing.T) {
	s := func(v string) *string { return &v }
	row := uuid.New()
	cases := []struct {
		name                        string
		provider, subject, protocol *string
		bound                       *uuid.UUID
		want                        bool
	}{
		{"oidc bound to an oidc row", s("oidc"), s("sub"), s("oidc"), &row, true},
		{"saml bound to a saml row", s("saml"), s("nameid"), s("saml"), &row, true},
		{"oidc unbound", s("oidc"), s("sub"), nil, nil, false},
		{"oidc bound, row gone", s("oidc"), s("sub"), nil, &row, false},
		{"saml bound to an oidc row", s("saml"), s("nameid"), s("oidc"), &row, false},
		{"oidc empty subject", s("oidc"), s(""), s("oidc"), &row, false},
		{"oidc NULL subject", s("oidc"), nil, s("oidc"), &row, false},
		{"another case of the protocol", s("OIDC"), s("sub"), s("OIDC"), &row, false},
		{"oauth-type login", s("github"), s("910001"), s("github"), &row, false},
		{"service", s("service"), nil, nil, nil, false},
		{"local", s("local"), nil, nil, nil, false},
		{"no provider", nil, s("sub"), s("oidc"), &row, false},
	}
	for _, c := range cases {
		if got := IsSSORelinkable(c.provider, c.subject, c.bound, c.protocol); got != c.want {
			t.Errorf("%s: IsSSORelinkable = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestSSORelinkableSQLNamesTheAliasAndTheRule(t *testing.T) {
	sql := SSORelinkableSQL("u")
	for _, part := range []string{"coalesce(u.auth_provider, '') IN ('oidc', 'saml')", "coalesce(u.auth_provider_id, '') <> ''",
		"u.sso_provider_id IS NOT NULL", "sp_relink.id = u.sso_provider_id", "sp_relink.protocol = u.auth_provider"} {
		if !strings.Contains(sql, part) {
			t.Errorf("SSORelinkableSQL lacks %q:\n%s", part, sql)
		}
	}
	if strings.Count(sql, "(") != strings.Count(sql, ")") {
		t.Fatalf("unbalanced predicate: %s", sql)
	}
}

// The refusal is the ambiguous-identity answer every flow already maps (OIDC 409 identity_conflict, SAML refuse).
func TestErrRelinkRefusedIsAnAmbiguousIdentity(t *testing.T) {
	if !errors.Is(ErrRelinkRefused, ErrMultipleUsers) {
		t.Fatal("ErrRelinkRefused must be ErrMultipleUsers")
	}
}
