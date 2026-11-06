package livee2e_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
)

const (
	loginIssuer = "https://localhost:8443"
	loginKID    = "e2e-auth"
)

func loginAnswer(t *testing.T, method jwt.SigningMethod, key any, kid string, claims jwt.MapClaims) []byte {
	t.Helper()
	tok := jwt.NewWithClaims(method, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return mustJSON(t, m{"access_token": signed, "refresh_token": "r", "token_type": "bearer", "expires_in": 3600})
}

func goodClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss": loginIssuer, "aud": "dev-health-api", "sub": "prn_x",
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
}

func TestAssertLoginToken(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	with := func(edit func(jwt.MapClaims)) jwt.MapClaims {
		c := goodClaims(now)
		edit(c)
		return c
	}
	cases := []struct {
		name    string
		answer  []byte
		wantErr string
	}{
		{"eddsa ok", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, loginKID, goodClaims(now)), ""},
		{"audience list ok", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, loginKID, with(func(c jwt.MapClaims) { c["aud"] = []string{"x", "dev-health-api"} })), ""},
		{"hs256 refused", loginAnswer(t, jwt.SigningMethodHS256, []byte("0123456789abcdef0123456789abcdef"), loginKID, goodClaims(now)), "alg"},
		{"wrong issuer refused", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, loginKID, with(func(c jwt.MapClaims) { c["iss"] = "https://other.example" })), "iss"},
		{"wrong audience refused", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, loginKID, with(func(c jwt.MapClaims) { c["aud"] = "auth-service" })), "aud"},
		{"wrong kid refused", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, "other", goodClaims(now)), "kid"},
		{"no kid refused", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, "", goodClaims(now)), "kid"},
		{"long lifetime refused", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, loginKID, with(func(c jwt.MapClaims) { c["exp"] = now.Add(2 * time.Hour).Unix() })), "lifetime"},
		{"expired refused", loginAnswer(t, jwt.SigningMethodEdDSA, edKey, loginKID, with(func(c jwt.MapClaims) {
			c["iat"] = now.Add(-2 * time.Hour).Unix()
			c["exp"] = now.Add(-time.Hour).Unix()
		})), "expired"},
		{"alg none refused", mustJSON(t, m{"access_token": algNoneToken()}), "alg"},
		{"no token refused", mustJSON(t, m{"refresh_token": "r"}), "access_token"},
		{"not json refused", []byte("nope"), "answer"},
		{"not a jwt refused", mustJSON(t, m{"access_token": "abc"}), "three"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			line, err := livee2e.AssertLoginToken(tc.answer, loginIssuer, loginKID, now)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("FAIL: refused a good token: %v", err)
				}
				for _, want := range []string{"alg=EdDSA", "kid=" + loginKID, "iss=" + loginIssuer, "aud=dev-health-api"} {
					if !strings.Contains(line, want) {
						t.Fatalf("FAIL: the proof line %q lacks %q", line, want)
					}
				}
				if strings.Contains(line, "eyJ") {
					t.Fatalf("FAIL: the proof line carries token text: %q", line)
				}
				return
			}
			if err == nil {
				t.Fatalf("FAIL: accepted a token that must be refused (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("FAIL: refused for the wrong reason: %v, want a mention of %q", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), "eyJ") {
				t.Fatalf("FAIL: the error carries token text: %v", err)
			}
		})
	}
}

// algNoneToken is built here so no token literal sits in the file: header alg none, the e2e kid, an unsigned tail.
func algNoneToken() string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"alg":"none","kid":"`+loginKID+`"}`)) + "." + enc([]byte(`{"iss":"`+loginIssuer+`"}`)) + "."
}
