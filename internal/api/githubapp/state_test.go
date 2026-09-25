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

// TestVerifyReadsTimeClaimsLikePyJWT pins the int() reading of exp, nbf and
// iat: numeric strings (whitespace, sign, underscores), booleans and an exp
// of zero (the epoch, expired) behave as PyJWT does; the venue oracle
// compares the same shapes against the real Python api.
func TestVerifyReadsTimeClaimsLikePyJWT(t *testing.T) {
	signer := Signer{Secret: "unit-secret-unit-secret-unit-secret-1", Issuer: "iss", Audience: "aud"}
	now := time.Unix(1_800_000_000, 0)
	sign := func(mutate func(jwt.MapClaims)) string {
		claims := jwt.MapClaims{"org_id": "org", "jti": "j", "purpose": installPurpose, "iss": "iss", "aud": "aud",
			"iat": now.Unix() - 10, "exp": now.Unix() + 600}
		mutate(claims)
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(signer.Secret))
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	for _, tc := range []struct {
		name   string
		claim  string
		value  any
		accept bool
	}{
		{"exp numeric string", "exp", "1800000600", true},
		{"exp padded string", "exp", " \t1800000600\n", true},
		{"exp underscored string", "exp", "1_800_000_600", true},
		{"exp signed string", "exp", "+1800000600", true},
		{"exp huge string", "exp", "99999999999999999999999999", true},
		{"exp past string", "exp", "1799999999", false},
		{"exp zero", "exp", 0, false},
		{"exp zero string", "exp", "0", false},
		{"exp false", "exp", false, false},
		{"exp negative string", "exp", "-5", false},
		{"exp fractional string", "exp", "1800000600.5", false},
		{"exp exponent string", "exp", "1e10", false},
		{"exp double underscore", "exp", "1__0", false},
		{"exp leading underscore", "exp", "_10", false},
		{"nbf numeric string", "nbf", "1799999990", true},
		{"nbf true", "nbf", true, true},
		{"nbf future string", "nbf", "1800000600", false},
		{"iat numeric string", "iat", "1799999990", true},
		{"iat false", "iat", false, true},
		{"iat future string", "iat", "1800000600", false},
		{"nbf null", "nbf", nil, false},
		{"exp Arabic-Indic digits", "exp", "١٨٠٠٠٠٠٦٠٠", true},
		{"exp fullwidth digits", "exp", "１８００００００６００", true},
		{"exp mathematical bold digits", "exp", "𝟏𝟖𝟎𝟎𝟎𝟎𝟎𝟔𝟎𝟎", true},
		{"exp mixed-script digits", "exp", "١٨٠٠" + "०००६००", true},
		{"exp ASCII separator padded is refused by int()", "exp", "\x1f1800000600\x1c", false},
		{"exp next-line padded", "exp", "\u00851800000600\u0085", true},
		{"exp no-break space padded", "exp", "\u00a01800000600\u2003", true},
		{"exp Arabic-Indic digits past", "exp", "١٧٩٩٩٩٩٩٩٩", false},
		{"exp superscript two is no decimal digit", "exp", "\u00b21800000600", false},
		{"iat list", "iat", []int{1}, false},
	} {
		_, err := signer.Verify(sign(func(c jwt.MapClaims) { c[tc.claim] = tc.value }), now)
		if (err == nil) != tc.accept {
			t.Errorf("%s: accepted=%v, want %v (err %v)", tc.name, err == nil, tc.accept, err)
		}
	}
}
