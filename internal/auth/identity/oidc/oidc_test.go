package oidc

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/oauth2"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// testStateSecret is built at run time: no key material in the source.
var testStateSecret = uuid.NewString()

func testFlowState(providerID string) flowState {
	return flowState{ProviderID: providerID, OrgID: uuid.NewString(), Nonce: "n-1", CodeVerifier: "v-1",
		RedirectURI: "https://app.example/oidc/cb", NonceHash: hashLoginNonce("cookie-1")}
}

func TestStateOpensOnlyForItsProviderAndBeforeExpiry(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	provider := uuid.NewString()
	token, sealed, err := sealState(testStateSecret, testFlowState(provider), now)
	if err != nil {
		t.Fatal(err)
	}
	got, err := openState(testStateSecret, token, provider, now.Add(stateTTL-time.Second))
	if err != nil || got.ID != sealed.ID || got.CodeVerifier != "v-1" {
		t.Fatalf("open = %+v, %v", got, err)
	}
	if _, err := openState(testStateSecret, token, uuid.NewString(), now); !errors.Is(err, errStateInvalid) {
		t.Fatalf("another provider: %v, want errStateInvalid", err)
	}
	if _, err := openState(uuid.NewString(), token, provider, now); !errors.Is(err, errStateInvalid) {
		t.Fatalf("another secret: %v, want errStateInvalid", err)
	}
	if _, err := openState(testStateSecret, token, provider, now.Add(stateTTL+time.Second)); !errors.Is(err, errStateExpired) {
		t.Fatalf("expired: %v, want errStateExpired", err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(token)
	for i := range raw {
		tampered := append([]byte(nil), raw...)
		tampered[i] ^= 0x01
		if _, err := openState(testStateSecret, base64.RawURLEncoding.EncodeToString(tampered), provider, now); err == nil {
			t.Fatalf("byte %d flipped: state still opened", i)
		}
	}
	if _, err := openState(testStateSecret, strings.Repeat("A", maxStateBytes+1), provider, now); !errors.Is(err, errStateInvalid) {
		t.Fatalf("oversized: %v", err)
	}
}

func TestStateHidesItsSecrets(t *testing.T) {
	token, _, err := sealState(testStateSecret, testFlowState(uuid.NewString()), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(token)
	for _, secret := range []string{"v-1", "n-1", "code_verifier"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("sealed state carries %q in clear", secret)
		}
	}
}

func compact(header, payload string, sig string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(header)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(payload)) + "." + sig
}

func TestJOSEHeaderRefusesUnsafeForms(t *testing.T) {
	cases := []struct {
		name, token string
		want        error
	}{
		{"rs256", compact(`{"alg":"RS256","kid":"k1"}`, `{}`, "c2ln"), nil},
		{"es256", compact(`{"alg":"ES256"}`, `{}`, "c2ln"), nil},
		{"none", compact(`{"alg":"none"}`, `{}`, "c2ln"), errAlgNotAllowed},
		{"none empty signature", compact(`{"alg":"none"}`, `{}`, ""), errMalformedToken},
		{"hs256", compact(`{"alg":"HS256"}`, `{}`, "c2ln"), errAlgNotAllowed},
		{"hs512", compact(`{"alg":"HS512"}`, `{}`, "c2ln"), errAlgNotAllowed},
		{"lower case alg", compact(`{"alg":"rs256"}`, `{}`, "c2ln"), errAlgNotAllowed},
		{"duplicate alg", compact(`{"alg":"RS256","alg":"none"}`, `{}`, "c2ln"), errMalformedToken},
		{"crit", compact(`{"alg":"RS256","crit":["exp"]}`, `{}`, "c2ln"), errMalformedToken},
		{"embedded jwk", compact(`{"alg":"RS256","jwk":{}}`, `{}`, "c2ln"), errMalformedToken},
		{"jku", compact(`{"alg":"RS256","jku":"https://untrusted.example/k"}`, `{}`, "c2ln"), errMalformedToken},
		{"x5u", compact(`{"alg":"RS256","x5u":"https://untrusted.example/c"}`, `{}`, "c2ln"), errMalformedToken},
		{"kid not a string", compact(`{"alg":"RS256","kid":7}`, `{}`, "c2ln"), errMalformedToken},
		{"alg missing", compact(`{"kid":"k1"}`, `{}`, "c2ln"), errMalformedToken},
		{"header not an object", compact(`["RS256"]`, `{}`, "c2ln"), errMalformedToken},
		{"two parts", "eyJhbGciOiJSUzI1NiJ9.e30", errMalformedToken},
		{"four parts", compact(`{"alg":"RS256"}`, `{}`, "c2ln") + ".x", errMalformedToken},
		{"padded header", base64.URLEncoding.EncodeToString([]byte(`{"alg":"RS256"} `)) + ".e30.c2ln", errMalformedToken},
		{"empty", "", errMalformedToken},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseJOSEHeader(tc.token)
			if !errors.Is(err, tc.want) && !(tc.want == nil && err == nil) {
				t.Fatalf("parseJOSEHeader = %v, want %v", err, tc.want)
			}
		})
	}
}

func claimsJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	now := time.Unix(1_800_000_000, 0)
	c := map[string]any{
		"iss": "https://idp.example", "aud": "client-1", "sub": "subject-1", "nonce": "nonce-1",
		"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(),
	}
	if mutate != nil {
		mutate(c)
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func testRules() claimRules {
	return claimRules{Issuer: "https://idp.example", ClientID: "client-1", Nonce: "nonce-1", Now: time.Unix(1_800_000_000, 0)}
}

func TestClaimsAreCheckedExplicitly(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cases := []struct {
		name   string
		mutate func(map[string]any)
		want   Reason
	}{
		{"valid", nil, ReasonAccepted},
		{"other issuer", func(c map[string]any) { c["iss"] = "https://untrusted.example" }, ReasonIssuer},
		{"issuer differs by a slash", func(c map[string]any) { c["iss"] = "https://idp.example/" }, ReasonIssuer},
		{"issuer missing", func(c map[string]any) { delete(c, "iss") }, ReasonIssuer},
		{"other audience", func(c map[string]any) { c["aud"] = "client-2" }, ReasonAudience},
		{"audience list with ours", func(c map[string]any) { c["aud"] = []string{"client-1"} }, ReasonAccepted},
		{"several audiences no azp", func(c map[string]any) { c["aud"] = []string{"client-1", "client-2"} }, ReasonAudience},
		{"several audiences our azp", func(c map[string]any) {
			c["aud"] = []string{"client-1", "client-2"}
			c["azp"] = "client-1"
		}, ReasonAccepted},
		{"foreign azp", func(c map[string]any) { c["azp"] = "client-2" }, ReasonAudience},
		{"audience not a string", func(c map[string]any) { c["aud"] = 7 }, ReasonAudience},
		{"expired beyond skew", func(c map[string]any) { c["exp"] = now.Add(-61 * time.Second).Unix() }, ReasonExpired},
		{"expired within skew", func(c map[string]any) { c["exp"] = now.Add(-30 * time.Second).Unix() }, ReasonAccepted},
		{"exp missing", func(c map[string]any) { delete(c, "exp") }, ReasonMalformed},
		{"exp a string", func(c map[string]any) { c["exp"] = "1900000000" }, ReasonMalformed},
		{"iat missing", func(c map[string]any) { delete(c, "iat") }, ReasonMalformed},
		{"iat in the future", func(c map[string]any) { c["iat"] = now.Add(2 * time.Minute).Unix() }, ReasonNotYetValid},
		{"iat too old", func(c map[string]any) { c["iat"] = now.Add(-time.Hour).Unix() }, ReasonExpired},
		{"nbf in the future", func(c map[string]any) { c["nbf"] = now.Add(2 * time.Minute).Unix() }, ReasonNotYetValid},
		{"nbf within skew", func(c map[string]any) { c["nbf"] = now.Add(30 * time.Second).Unix() }, ReasonAccepted},
		{"negative exp", func(c map[string]any) { c["exp"] = -1 }, ReasonMalformed},
		{"nonce wrong", func(c map[string]any) { c["nonce"] = "nonce-2" }, ReasonNonceMismatch},
		{"nonce missing", func(c map[string]any) { delete(c, "nonce") }, ReasonNonceMismatch},
		{"sub missing", func(c map[string]any) { delete(c, "sub") }, ReasonMissingSub},
		{"sub empty", func(c map[string]any) { c["sub"] = "" }, ReasonMissingSub},
		{"sub padded", func(c map[string]any) { c["sub"] = " subject-1" }, ReasonMissingSub},
		{"sub control char", func(c map[string]any) { c["sub"] = "sub\x00ject" }, ReasonMissingSub},
		{"sub too long", func(c map[string]any) { c["sub"] = strings.Repeat("s", maxSubjectBytes+1) }, ReasonMissingSub},
		{"sub a number", func(c map[string]any) { c["sub"] = 12345 }, ReasonMissingSub},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := checkClaims(claimsJSON(t, tc.mutate), testRules())
			if tc.want == ReasonAccepted {
				if err != nil {
					t.Fatalf("checkClaims = %v, want accepted", err)
				}
				if got.Subject == "" || got.Issuer != "https://idp.example" {
					t.Fatalf("claims = %+v", got)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkClaims accepted, want %s", tc.want)
			}
			if r := reasonOf(err); r != tc.want {
				t.Fatalf("reason = %s, want %s (%v)", r, tc.want, err)
			}
		})
	}
}

func TestClaimsRefuseDuplicateMembers(t *testing.T) {
	payload := []byte(`{"iss":"https://idp.example","aud":"client-1","sub":"subject-1","sub":"subject-2","nonce":"nonce-1","iat":1800000000,"exp":1800000300}`)
	if _, err := checkClaims(payload, testRules()); err == nil || reasonOf(err) != ReasonMalformed {
		t.Fatalf("duplicate sub: %v, want malformed", err)
	}
}

func TestEmailVerifiedNeedsAnExplicitTrue(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  bool
	}{{true, true}, {"true", true}, {false, false}, {"false", false}, {"TRUE", false}, {1, false}, {nil, false}} {
		claims := map[string]any{"email": "a@example.com"}
		if tc.value != nil {
			claims["email_verified"] = tc.value
		}
		if got := readProfile(claims, nil).EmailVerified; got != tc.want {
			t.Errorf("email_verified=%#v: %v, want %v", tc.value, got, tc.want)
		}
	}
}

func TestClaimMappingIsGoAPIs(t *testing.T) {
	p := readProfile(map[string]any{"mail": "  m@example.com ", "email": "e@example.com", "display": "Ann"},
		map[string]string{"email": "mail", "full_name": "display"})
	if p.Email != "m@example.com" || p.FullName != "Ann" {
		t.Fatalf("profile = %+v", p)
	}
}

func TestCallbackBodyParsing(t *testing.T) {
	cases := []struct {
		body     string
		wantErrs int
	}{
		{`{"code":"c","state":"s"}`, 0},
		{`{"code":"c","state":"s","code_verifier":null}`, 0},
		{`{"code":"c","state":"s","code_verifier":"caller-chosen"}`, 0},
		{`{"code":"c"}`, 1},
		{`{}`, 2},
		{`{"code":1,"state":"s"}`, 1},
		{`{"code":"c","state":"s","code":"d"}`, 1},
		{`[]`, 1},
		{``, 1},
		{`{"code":"c","state":"s"} {}`, 1},
	}
	for _, tc := range cases {
		_, errs := parseCallbackBody([]byte(tc.body))
		if len(errs) != tc.wantErrs {
			t.Errorf("%q: %d errors %+v, want %d", tc.body, len(errs), errs, tc.wantErrs)
		}
	}
}

func TestRedirectRule(t *testing.T) {
	for raw, ok := range map[string]bool{
		"https://app.example/cb": true, "http://localhost:3000/cb": true, "http://127.0.0.1/cb": true,
		"HTTP://untrusted.example/cb": false, "//untrusted.example/cb": false, "https:opaque": false,
		"https://user@app.example/cb": false, "javascript:alert(1)": false,
	} {
		if got := validateHTTPSOrLoopback(raw) == nil; got != ok {
			t.Errorf("%q: accepted=%v, want %v", raw, got, ok)
		}
	}
}

func TestReasonsAreAuthOutcomeCodes(t *testing.T) {
	contract := map[string]bool{}
	for _, code := range contracts.AuthOutcomeReasonCodes {
		contract[code] = true
	}
	for _, r := range allReasons {
		if !contract[string(r)] {
			t.Errorf("reason %q is not an auth-outcome.v1 reason code", r)
		}
	}
}

func TestReasonsAreDistinct(t *testing.T) {
	seen := map[Reason]bool{}
	for _, r := range allReasons {
		if seen[r] {
			t.Fatalf("duplicate reason %s", r)
		}
		seen[r] = true
		if r == "" || strings.ToLower(string(r)) != string(r) {
			t.Fatalf("reason %q is not a lower-case code", r)
		}
	}
}

func ExampleError() {
	err := error(deny(409, ReasonEmailCollision, "conflict"))
	var failure *Error
	fmt.Println(errors.As(err, &failure), failure.Status, failure.Reason)
	// Output: true 409 email_collision
}

func TestLoggedCausesCarryAClassNotTheirText(t *testing.T) {
	secret := "q" + uuid.NewString()
	queryURL := "https://idp.example/token?client_secret=" + secret + "&code=" + secret
	cases := map[string]error{
		"refused":  &url.Error{Op: "Post", URL: queryURL, Err: syscall.ECONNREFUSED},
		"deadline": &url.Error{Op: "Post", URL: queryURL, Err: context.DeadlineExceeded},
		"canceled": fmt.Errorf("discovery: %w", &url.Error{Op: "Get", URL: queryURL, Err: context.Canceled}),
		"tls":      &url.Error{Op: "Post", URL: queryURL, Err: x509.UnknownAuthorityError{}},
		"decode":   fmt.Errorf("userinfo %s: %w", queryURL, &json.SyntaxError{}),
		"postgres": fmt.Errorf("provision: %w", &pgconn.PgError{Code: "23505",
			Detail: "Key (email_lower)=(" + secret + "@example.com) already exists."}),
		"other": &oauth2.RetrieveError{Response: &http.Response{StatusCode: 400}, Body: []byte(secret)},
	}
	for want, cause := range cases {
		t.Run(want, func(t *testing.T) {
			var buf bytes.Buffer
			slog.New(slog.NewTextHandler(&buf, nil)).Info("refused", logging.ErrorAttr(authFailed(ReasonDependency, cause)))
			line := buf.String()
			if !strings.Contains(line, "error.class="+want) {
				t.Errorf("log %q lacks error.class=%s", line, want)
			}
			if strings.Contains(line, secret) || strings.Contains(line, "idp.example") {
				t.Errorf("log holds the cause text: %q", line)
			}
		})
	}
	if got := (authFailed(ReasonDependency, errors.New(queryURL))).Error(); strings.Contains(got, secret) {
		t.Errorf("Error() holds the cause text: %q", got)
	}
}
