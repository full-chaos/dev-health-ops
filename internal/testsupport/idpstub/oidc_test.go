package idpstub_test

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

const (
	clientID    = "client-1"
	redirectURI = "https://sp.example.test/cb"
)

func b64u(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func pkce(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return b64u(sum[:])
}

func startOIDC(t *testing.T, opts idpstub.OIDCOptions) (*idpstub.OIDC, *httptest.Server) {
	t.Helper()
	o, err := idpstub.NewOIDC(opts)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewTLSServer(o.Handler())
	t.Cleanup(srv.Close)
	o.SetBaseURL(srv.URL)
	return o, srv
}

func getJSON(t *testing.T, c *http.Client, url, bearer string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

// authorize runs /authorize without following the redirect and returns the code and state it carries.
func authorize(t *testing.T, srv *httptest.Server, extra url.Values) (code, state string, status int) {
	t.Helper()
	c := *srv.Client()
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	q := url.Values{"response_type": {"code"}, "client_id": {clientID}, "redirect_uri": {redirectURI}, "state": {"st-1"},
		"nonce": {"nonce-1"}, "code_challenge": {pkce("verifier-1")}, "code_challenge_method": {"S256"}}
	for k, v := range extra {
		q[k] = v
	}
	res, err := c.Get(srv.URL + "/authorize?" + q.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusFound {
		return "", "", res.StatusCode
	}
	loc, err := url.Parse(res.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return loc.Query().Get("code"), loc.Query().Get("state"), res.StatusCode
}

func exchange(t *testing.T, srv *httptest.Server, code, verifier string) (int, map[string]any) {
	t.Helper()
	res, err := srv.Client().PostForm(srv.URL+"/token", url.Values{"grant_type": {"authorization_code"}, "code": {code},
		"code_verifier": {verifier}, "client_id": {clientID}, "redirect_uri": {redirectURI}})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(res.Body).Decode(&out)
	return res.StatusCode, out
}

func jwtParts(t *testing.T, token string) (header, claims map[string]any, signed, sig []byte) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("id_token has %d parts", len(parts))
	}
	for i, dst := range []*map[string]any{&header, &claims} {
		raw, err := base64.RawURLEncoding.DecodeString(parts[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, dst); err != nil {
			t.Fatal(err)
		}
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	return header, claims, []byte(parts[0] + "." + parts[1]), sig
}

func TestOIDCDiscoveryDocument(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{})
	status, doc := getJSON(t, srv.Client(), srv.URL+"/tenant-a/.well-known/openid-configuration", "")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	want := map[string]any{"issuer": srv.URL + "/tenant-a", "authorization_endpoint": srv.URL + "/authorize", "token_endpoint": srv.URL + "/token",
		"jwks_uri": srv.URL + "/jwks", "userinfo_endpoint": srv.URL + "/userinfo"}
	for k, v := range want {
		if doc[k] != v {
			t.Errorf("%s = %v, want %v", k, doc[k], v)
		}
	}
	for _, key := range []string{"id_token_signing_alg_values_supported", "code_challenge_methods_supported"} {
		list, _ := doc[key].([]any)
		if len(list) != 1 || (list[0] != "RS256" && list[0] != "S256") {
			t.Errorf("%s = %v", key, doc[key])
		}
	}
	if status, _ := getJSON(t, srv.Client(), srv.URL+"/nothing", ""); status != http.StatusNotFound {
		t.Errorf("a path that is not discovery answered %d", status)
	}
}

func TestOIDCMinimalDiscoveryHasOnlyThreeEndpoints(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{MinimalDiscovery: true})
	_, doc := getJSON(t, srv.Client(), srv.URL+"/.well-known/openid-configuration", "")
	if len(doc) != 3 || doc["token_endpoint"] == nil || doc["jwks_uri"] == nil || doc["issuer"] == nil {
		t.Fatalf("minimal discovery = %v", doc)
	}
}

func TestOIDCJWKSMatchesIDTokenSignature(t *testing.T) {
	o, srv := startOIDC(t, idpstub.OIDCOptions{})
	code, _, _ := authorize(t, srv, nil)
	_, tok := exchange(t, srv, code, "verifier-1")
	header, _, signed, sig := jwtParts(t, tok["id_token"].(string))
	if header["alg"] != "RS256" || header["kid"] != o.Keys().KID {
		t.Fatalf("header = %v", header)
	}
	_, jwks := getJSON(t, srv.Client(), srv.URL+"/jwks", "")
	keys := jwks["keys"].([]any)
	if len(keys) != 1 {
		t.Fatalf("jwks = %v", jwks)
	}
	k := keys[0].(map[string]any)
	n, _ := base64.RawURLEncoding.DecodeString(k["n"].(string))
	e, _ := base64.RawURLEncoding.DecodeString(k["e"].(string))
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
	digest := sha256.Sum256(signed)
	if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("id_token does not verify against the served JWKS: %v", err)
	}
	if k["kid"] != header["kid"] {
		t.Fatalf("kid %v != %v", k["kid"], header["kid"])
	}
}

func TestOIDCAuthorizeRedirectsWithCodeAndState(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{ClientID: clientID, RedirectURIs: []string{redirectURI}})
	code, state, status := authorize(t, srv, nil)
	if status != http.StatusFound || code == "" || state != "st-1" {
		t.Fatalf("authorize = %d code=%q state=%q", status, code, state)
	}
}

func TestOIDCAuthorizeRefusesUnknownClientOrRedirect(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{ClientID: clientID, RedirectURIs: []string{redirectURI}})
	for name, extra := range map[string]url.Values{
		"client":       {"client_id": {"other"}},
		"redirect":     {"redirect_uri": {"https://evil.example.test/cb"}},
		"no state":     {"state": {""}},
		"no challenge": {"code_challenge": {""}},
		"plain":        {"code_challenge_method": {"plain"}},
	} {
		if _, _, status := authorize(t, srv, extra); status != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, status)
		}
	}
}

func TestOIDCTokenChecksPKCE(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{})
	code, _, _ := authorize(t, srv, nil)
	if status, body := exchange(t, srv, code, "not-the-verifier"); status != http.StatusBadRequest || body["error"] != "invalid_grant" {
		t.Fatalf("wrong verifier = %d %v", status, body)
	}
	code, _, _ = authorize(t, srv, nil)
	if status, _ := exchange(t, srv, code, "verifier-1"); status != http.StatusOK {
		t.Fatalf("right verifier = %d", status)
	}
}

func TestOIDCTokenCodeIsSingleUse(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{})
	code, _, _ := authorize(t, srv, nil)
	if status, _ := exchange(t, srv, code, "verifier-1"); status != http.StatusOK {
		t.Fatalf("first use = %d", status)
	}
	if status, _ := exchange(t, srv, code, "verifier-1"); status != http.StatusBadRequest {
		t.Fatalf("second use = %d, want 400", status)
	}
}

func TestOIDCTokenChecksClientSecretWhenSet(t *testing.T) {
	o, srv := startOIDC(t, idpstub.OIDCOptions{ClientSecret: "s3"})
	code, _, _ := authorize(t, srv, nil)
	res, err := srv.Client().PostForm(srv.URL+"/token", url.Values{"grant_type": {"authorization_code"}, "code": {code},
		"code_verifier": {"verifier-1"}, "client_secret": {"wrong"}})
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong secret = %d", res.StatusCode)
	}
	if v, s := o.Received(); v != "verifier-1" || s != "wrong" {
		t.Fatalf("Received = %q %q", v, s)
	}
}

func TestOIDCIDTokenClaimsForTwoUsers(t *testing.T) {
	frozen := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	_, srv := startOIDC(t, idpstub.OIDCOptions{Clock: func() time.Time { return frozen }})
	for _, tc := range []struct{ hint, sub, email string }{{"", "alice", "alice@idp.example.test"}, {"bob", "bob", "bob@idp.example.test"}} {
		extra := url.Values{}
		if tc.hint != "" {
			extra.Set("login_hint", tc.hint)
		}
		code, _, _ := authorize(t, srv, extra)
		_, tok := exchange(t, srv, code, "verifier-1")
		_, claims, _, _ := jwtParts(t, tok["id_token"].(string))
		want := map[string]any{"iss": srv.URL, "aud": clientID, "sub": tc.sub, "nonce": "nonce-1", "email": tc.email,
			"email_verified": true, "iat": float64(frozen.Unix()), "exp": float64(frozen.Add(5 * time.Minute).Unix())}
		for k, v := range want {
			if claims[k] != v {
				t.Errorf("%s: %s = %v, want %v", tc.sub, k, claims[k], v)
			}
		}
	}
}

func TestOIDCUserinfoRequiresBearer(t *testing.T) {
	_, srv := startOIDC(t, idpstub.OIDCOptions{})
	code, _, _ := authorize(t, srv, url.Values{"login_hint": {"bob"}})
	_, tok := exchange(t, srv, code, "verifier-1")
	if status, _ := getJSON(t, srv.Client(), srv.URL+"/userinfo", ""); status != http.StatusUnauthorized {
		t.Fatalf("no bearer = %d", status)
	}
	status, info := getJSON(t, srv.Client(), srv.URL+"/userinfo", tok["access_token"].(string))
	if status != http.StatusOK || info["sub"] != "bob" || info["email"] != "bob@idp.example.test" {
		t.Fatalf("userinfo = %d %v", status, info)
	}
}

func TestOIDCFailureSwitches(t *testing.T) {
	o, srv := startOIDC(t, idpstub.OIDCOptions{})
	o.SetFailDiscovery(true)
	if status, _ := getJSON(t, srv.Client(), srv.URL+"/.well-known/openid-configuration", ""); status != http.StatusBadGateway {
		t.Fatalf("failed discovery = %d", status)
	}
	o.SetFailDiscovery(false)
	other := "https://other.example.test"
	o.SetDocIssuer(&other)
	if _, doc := getJSON(t, srv.Client(), srv.URL+"/.well-known/openid-configuration", ""); doc["issuer"] != other {
		t.Fatalf("doc issuer = %v", doc["issuer"])
	}
	o.SetFailUserinfo(true)
	if status, _ := getJSON(t, srv.Client(), srv.URL+"/userinfo", "x"); status != http.StatusBadGateway {
		t.Fatalf("failed userinfo = %d", status)
	}
}

func TestOIDCIssueCodeAndOverrideBypassAuthorize(t *testing.T) {
	o, srv := startOIDC(t, idpstub.OIDCOptions{})
	o.SetIssuer(srv.URL + "/tenant/x")
	user, _ := o.User("alice")
	code := o.IssueCode(idpstub.CodeRequest{Challenge: pkce("v"), Nonce: "n9", ClientID: clientID, User: user,
		Claims: func(c map[string]any) { delete(c, "sub") }})
	status, tok := exchange(t, srv, code, "v")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	_, claims, _, _ := jwtParts(t, tok["id_token"].(string))
	if _, has := claims["sub"]; has || claims["iss"] != srv.URL+"/tenant/x" || claims["nonce"] != "n9" {
		t.Fatalf("claims = %v", claims)
	}
	if o.TokenCalls() != 1 {
		t.Fatalf("TokenCalls = %d", o.TokenCalls())
	}
}

func TestOIDCAcceptAnyCodeAnswersTheNextUser(t *testing.T) {
	o, srv := startOIDC(t, idpstub.OIDCOptions{AcceptAnyCode: true, NoPKCE: true})
	o.SetNext(idpstub.OIDCUser{Sub: "s-7", Email: "x@example.test", Name: "X"}, "nonce-7")
	status, tok := exchange(t, srv, "any-code", "")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	_, claims, _, _ := jwtParts(t, tok["id_token"].(string))
	if claims["sub"] != "s-7" || claims["nonce"] != "nonce-7" || claims["name"] != "X" {
		t.Fatalf("claims = %v", claims)
	}
	if _, has := claims["email_verified"]; has {
		t.Fatalf("email_verified present for a user that left it nil: %v", claims)
	}
}
