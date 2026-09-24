package githubapp

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var testSigner = Signer{Secret: "unit-test-secret-unit-test-secret-1", Issuer: "dev-health-ops", Audience: "dev-health-api"}

func TestMintedStateVerifies(t *testing.T) {
	now := time.Now()
	returnTo := "/auth/onboard/integration"
	state, err := testSigner.Mint("org-1", &returnTo, now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := testSigner.Verify(state, now)
	if err != nil || got.OrgID != "org-1" || got.JTI == "" || got.ReturnTo == nil || *got.ReturnTo != returnTo {
		t.Fatalf("Verify = %+v, %v", got, err)
	}
	if again, _ := testSigner.Mint("org-1", nil, now); again == state {
		t.Fatal("two states are identical")
	}
	if noReturn, _ := testSigner.Mint("org-1", nil, now); func() bool {
		verified, err := testSigner.Verify(noReturn, now)
		return err != nil || verified.ReturnTo != nil
	}() {
		t.Fatal("a state without return_to verified with one")
	}
}

func TestVerifyRefusals(t *testing.T) {
	now := time.Now()
	sign := func(method jwt.SigningMethod, secret string, mutate func(jwt.MapClaims)) string {
		claims := jwt.MapClaims{"org_id": "org-1", "jti": "j", "purpose": installPurpose, "iss": "dev-health-ops", "aud": "dev-health-api",
			"iat": now.Unix(), "exp": now.Add(time.Minute).Unix()}
		if mutate != nil {
			mutate(claims)
		}
		signed, err := jwt.NewWithClaims(method, claims).SignedString([]byte(secret))
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}
	hs256 := func(mutate func(jwt.MapClaims)) string {
		return sign(jwt.SigningMethodHS256, testSigner.Secret, mutate)
	}
	cases := []struct {
		name  string
		state string
		want  error
	}{
		{"garbage", "nope", errInvalidState},
		{"other secret", sign(jwt.SigningMethodHS256, "another-secret-another-secret-another", nil), errInvalidState},
		{"other algorithm", sign(jwt.SigningMethodHS512, testSigner.Secret, nil), errInvalidState},
		{"expired", hs256(func(c jwt.MapClaims) { c["exp"] = now.Add(-time.Minute).Unix() }), errInvalidState},
		{"future iat", hs256(func(c jwt.MapClaims) { c["iat"] = now.Add(time.Hour).Unix() }), errInvalidState},
		{"not before", hs256(func(c jwt.MapClaims) { c["nbf"] = now.Add(time.Hour).Unix() }), errInvalidState},
		{"wrong issuer", hs256(func(c jwt.MapClaims) { c["iss"] = "x" }), errInvalidState},
		{"no issuer", hs256(func(c jwt.MapClaims) { delete(c, "iss") }), errInvalidState},
		{"wrong audience", hs256(func(c jwt.MapClaims) { c["aud"] = "x" }), errInvalidState},
		{"no audience", hs256(func(c jwt.MapClaims) { delete(c, "aud") }), errInvalidState},
		{"numeric jti", hs256(func(c jwt.MapClaims) { c["jti"] = 5 }), errInvalidState},
		{"numeric subject", hs256(func(c jwt.MapClaims) { c["sub"] = 5 }), errInvalidState},
		{"wrong purpose", hs256(func(c jwt.MapClaims) { c["purpose"] = "access" }), errInvalidPurpose},
		{"no org", hs256(func(c jwt.MapClaims) { delete(c, "org_id") }), errInvalidOrganization},
		{"empty org", hs256(func(c jwt.MapClaims) { c["org_id"] = "" }), errInvalidOrganization},
		{"no jti", hs256(func(c jwt.MapClaims) { delete(c, "jti") }), errInvalidIdentifier},
		{"empty jti", hs256(func(c jwt.MapClaims) { c["jti"] = "" }), errInvalidIdentifier},
	}
	for _, c := range cases {
		if _, err := testSigner.Verify(c.state, now); err != c.want {
			t.Errorf("%s: Verify error = %v, want %v", c.name, err, c.want)
		}
	}
	if _, err := testSigner.Verify(hs256(func(c jwt.MapClaims) { c["return_to"] = 5 }), now); err != nil {
		t.Errorf("a non-string return_to is ignored, got %v", err)
	}
}

func TestInstallURL(t *testing.T) {
	cases := []struct{ slug, state, callback, want string }{
		{"my-app", "abc.def", "", "https://github.com/apps/my-app/installations/new?state=abc.def"},
		{"dev health app/v2", "s", "https://app.example.test/cb?x=1&y=é", "https://github.com/apps/dev%20health%20app%2Fv2/installations/new?state=s&redirect_uri=https%3A%2F%2Fapp.example.test%2Fcb%3Fx%3D1%26y%3D%C3%A9"},
		{"a+b", "s p", "http://h/a b", "https://github.com/apps/a%2Bb/installations/new?state=s+p&redirect_uri=http%3A%2F%2Fh%2Fa+b"},
	}
	for _, c := range cases {
		if got := installURL(c.slug, c.state, c.callback); got != c.want {
			t.Errorf("installURL(%q, %q, %q) = %q, want %q", c.slug, c.state, c.callback, got, c.want)
		}
	}
}

func TestCanonicalizeReturnTo(t *testing.T) {
	yes := func(s string) *string { return &s }
	for raw, want := range map[string]string{
		"/auth/onboard/integration":       "/auth/onboard/integration",
		"/org/admin/integrations/github":  "/org/admin/integrations/github",
		"/org/admin/integrations/github/": "/org/admin/integrations/github",
		"//evil.example":                  "/org/admin/integrations/github",
		"https://evil.example":            "/org/admin/integrations/github",
		"":                                "/org/admin/integrations/github",
	} {
		if got := canonicalizeReturnTo(yes(raw)); got != want {
			t.Errorf("canonicalizeReturnTo(%q) = %q, want %q", raw, got, want)
		}
	}
	if got := canonicalizeReturnTo(nil); got != "/org/admin/integrations/github" {
		t.Errorf("canonicalizeReturnTo(nil) = %q", got)
	}
}
