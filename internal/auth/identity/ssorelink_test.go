package identity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// ssoRelinkShape is one public.users row for the relink rule. bound names the protocol of the provider row the
// account is bound to ("" = unbound). relinkable is the readiness waiver (IsSSORelinkable); unverifiable is the
// bound SAML account with a keyable value whose NameID format go-api never stored.
type ssoRelinkShape struct {
	name              string
	provider, subject *string
	bound             string
	relinkable        bool
	unverifiable      bool
}

// ssoRelinkShapes is the ONE row table both the Go rule and its SQL twin are run over (TestIsSSORelinkable here,
// TestSSORelinkGoAndSQLAgreeOverOneShapeTable against PostgreSQL). Each subject shape is a key the sign-in path
// accepts or refuses (ExternalKey.Validate).
func ssoRelinkShapes() []ssoRelinkShape {
	s := func(v string) *string { return &v }
	oidc := func(name, subject string, want bool) ssoRelinkShape {
		return ssoRelinkShape{name: name, provider: s("oidc"), subject: s(subject), bound: "oidc", relinkable: want}
	}
	return []ssoRelinkShape{
		oidc("oidc bound to an oidc row", "sub", true),
		oidc("oidc subject with an inner space", "su b", true),
		oidc("oidc subject with a zero-width space", "s\u200bub", true),
		oidc("oidc subject of 1024 bytes", strings.Repeat("a", 1024), true),
		oidc("oidc subject of 1025 bytes", strings.Repeat("a", 1025), false),
		oidc("oidc subject of 1025 bytes ending in a two-byte rune", strings.Repeat("a", 1023)+"\u00e9", false),
		oidc("oidc subject with a leading space", " sub", false),
		oidc("oidc subject with a trailing space", "sub ", false),
		oidc("oidc subject with a trailing no-break space", "sub\u00a0", false),
		oidc("oidc subject with a leading ideographic space", "\u3000sub", false),
		oidc("oidc subject with a trailing line separator", "sub\u2028", false),
		oidc("oidc subject with a trailing tab", "sub\t", false),
		oidc("oidc subject with a C0 control", "s\x01ub", false),
		oidc("oidc subject with a C1 control", "s\u0085ub", false),
		oidc("oidc subject with a replacement character", "s\ufffdub", false),
		oidc("oidc empty subject", "", false),
		{name: "oidc NULL subject", provider: s("oidc"), bound: "oidc"},
		{name: "oidc unbound", provider: s("oidc"), subject: s("sub")},
		{name: "oidc bound to a saml row", provider: s("oidc"), subject: s("sub"), bound: "saml"},
		{name: "oidc bound to an oauth row", provider: s("oidc"), subject: s("sub"), bound: "oauth"},
		{name: "saml bound to a saml row: no stored NameID format", provider: s("saml"), subject: s("nameid@example.com"), bound: "saml", unverifiable: true},
		{name: "saml transient-looking NameID value", provider: s("saml"), subject: s("_8f2c1e0a9b7d"), bound: "saml", unverifiable: true},
		{name: "saml NameID with a trailing space", provider: s("saml"), subject: s("nameid@example.com "), bound: "saml"},
		{name: "saml empty NameID", provider: s("saml"), subject: s(""), bound: "saml"},
		{name: "saml unbound", provider: s("saml"), subject: s("nameid@example.com")},
		{name: "saml bound to an oidc row", provider: s("saml"), subject: s("nameid@example.com"), bound: "oidc"},
		{name: "another case of the protocol", provider: s("OIDC"), subject: s("sub"), bound: "oidc"},
		{name: "oauth-type login", provider: s("github"), subject: s("910001"), bound: "oidc"},
		{name: "service", provider: s("service")},
		{name: "local", provider: s("local")},
		{name: "no provider", subject: s("sub"), bound: "oidc"},
	}
}

// goArgs is the shape as IsSSORelinkable's arguments; a bound shape names one provider row of its protocol.
func (c ssoRelinkShape) goArgs(row uuid.UUID) (provider, subject *string, bound *uuid.UUID, protocol *string) {
	if c.bound != "" {
		p := c.bound
		return c.provider, c.subject, &row, &p
	}
	return c.provider, c.subject, nil, nil
}

func TestIsSSORelinkable(t *testing.T) {
	row := uuid.New()
	for _, c := range ssoRelinkShapes() {
		if got := IsSSORelinkable(c.goArgs(row)); got != c.relinkable {
			t.Errorf("%s: IsSSORelinkable = %v, want %v", c.name, got, c.relinkable)
		}
		if got := IsSSOSubjectUnverifiable(c.goArgs(row)); got != c.unverifiable {
			t.Errorf("%s: IsSSOSubjectUnverifiable = %v, want %v", c.name, got, c.unverifiable)
		}
		// The waiver keys exactly what the sign-in path keys.
		if c.relinkable && !ValidSubject(*c.subject) {
			t.Errorf("%s: relinkable with a subject ValidSubject refuses", c.name)
		}
		if c.subject != nil && c.provider != nil && *c.provider == "oidc" && c.bound == "oidc" &&
			IsSSORelinkable(c.goArgs(row)) != (ExternalKey{Issuer: "https://idp.example.com", Provider: Provider(row.String()), Subject: *c.subject}.Validate() == nil) {
			t.Errorf("%s: IsSSORelinkable disagrees with ExternalKey.Validate", c.name)
		}
	}
	s := func(v string) *string { return &v }
	if IsSSORelinkable(s("oidc"), s("sub"), &row, nil) {
		t.Error("oidc bound to a provider row that is gone: IsSSORelinkable = true, want false")
	}
}

// A subject with white space at either end, which the sign-in path cannot key, is never waived by readiness.
func TestWhitespaceSubjectIsNotRelinkable(t *testing.T) {
	s := func(v string) *string { return &v }
	row := uuid.New()
	for _, subject := range []string{" sub", "sub ", "sub\u00a0"} {
		if IsSSORelinkable(s("oidc"), s(subject), &row, s("oidc")) {
			t.Errorf("%q: IsSSORelinkable = true, want false", subject)
		}
		_, found, err := (&Store{}).IdentityBySubject(context.Background(), "https://idp.example.com", Provider(row.String()), subject, &LegacyMatch{AuthProvider: "oidc"})
		if found || !errors.Is(err, ErrInvalidExternalKey) {
			t.Errorf("%q: IdentityBySubject = %v, %v; want ErrInvalidExternalKey", subject, found, err)
		}
	}
}

func TestSSORelinkableSQLNamesTheAliasAndTheRule(t *testing.T) {
	for name, sql := range map[string]string{"SSORelinkableSQL": SSORelinkableSQL("u"), "SSOSubjectUnverifiableSQL": SSOSubjectUnverifiableSQL("u")} {
		for _, part := range []string{"coalesce(u.auth_provider, '') IN ('oidc', 'saml')", "u.sso_provider_id IS NOT NULL",
			"sp_relink.id = u.sso_provider_id", "sp_relink.protocol = u.auth_provider", SubjectKeyableSQL("u.auth_provider_id")} {
			if !strings.Contains(sql, part) {
				t.Errorf("%s lacks %q:\n%s", name, part, sql)
			}
		}
		if strings.Count(sql, "(") != strings.Count(sql, ")") {
			t.Fatalf("%s: unbalanced predicate: %s", name, sql)
		}
	}
	if !strings.Contains(SSORelinkableSQL("u"), "u.auth_provider = 'oidc'") || !strings.Contains(SSOSubjectUnverifiableSQL("u"), "u.auth_provider = 'saml'") {
		t.Fatal("the waiver is OIDC only and the unverifiable bucket SAML only")
	}
}

// The refusal is the ambiguous-identity answer every flow already maps (OIDC 409 identity_conflict, SAML refuse).
func TestErrRelinkRefusedIsAnAmbiguousIdentity(t *testing.T) {
	if !errors.Is(ErrRelinkRefused, ErrMultipleUsers) {
		t.Fatal("ErrRelinkRefused must be ErrMultipleUsers")
	}
}
