package oidc

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"math/big"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity/ssoaudit"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// providerMarker stands for a value that comes from the provider or the request: it must never reach a record.
const providerMarker = "ZZ-provider-marker-ZZ"

const (
	unitClient = "client-1"
	unitNonce  = "nonce-1"
)

// unitIdP is an OIDC provider whose answers a case can break.
type unitIdP struct {
	server                                     *httptest.Server
	key                                        *rsa.PrivateKey
	tokenStatus, discoveryStatus, userinfoCode int
	tokenBody, discoveryBody, userinfoBody     string
	idToken                                    string
	noIDToken                                  bool
}

func (i *unitIdP) sign(t *testing.T, header, claims map[string]any) string {
	t.Helper()
	input := unitSegment(t, header) + "." + unitSegment(t, claims)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, i.key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func unitSegment(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

var (
	unitKeyOnce sync.Once
	unitKeyPair *rsa.PrivateKey
)

func unitKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	unitKeyOnce.Do(func() {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		unitKeyPair = key
	})
	return unitKeyPair
}

func startUnitIdP(t *testing.T) *unitIdP {
	t.Helper()
	key := unitKey(t)
	idp := &unitIdP{key: key}
	mux := http.NewServeMux()
	answer := func(w http.ResponseWriter, status int, body string) {
		w.Header().Set("Content-Type", "application/json")
		if status != 0 {
			w.WriteHeader(status)
		}
		_, _ = w.Write([]byte(body))
	}
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, _ *http.Request) {
		if idp.tokenBody != "" {
			answer(w, idp.tokenStatus, idp.tokenBody)
			return
		}
		body := map[string]any{"access_token": "at-1", "token_type": "Bearer", "expires_in": 300}
		if !idp.noIDToken {
			body["id_token"] = idp.idToken
		}
		raw, _ := json.Marshal(body)
		answer(w, idp.tokenStatus, string(raw))
	})
	mux.HandleFunc("/userinfo", func(w http.ResponseWriter, _ *http.Request) {
		answer(w, idp.userinfoCode, idp.userinfoBody)
	})
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		answer(w, idp.discoveryStatus, idp.discoveryBody)
	})
	idp.server = httptest.NewTLSServer(mux)
	t.Cleanup(idp.server.Close)
	return idp
}

// unitCase is one exchangeAndValidate run: the provider config, how the provider answers, and the ID token.
type unitCase struct {
	config func(c *ProviderConfig, idp *unitIdP)
	idp    func(idp *unitIdP)
	claims func(c map[string]any)
	header func(h map[string]any)
	raw    func(t *testing.T, idp *unitIdP, signed string) string
}

func (i *unitIdP) claims(now time.Time) map[string]any {
	return map[string]any{"iss": i.server.URL, "aud": unitClient, "sub": "sub-1", "nonce": unitNonce,
		"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(), "email": "u@example.test", "email_verified": true}
}

func runUnitCase(t *testing.T, c unitCase, failures ssoaudit.ProviderFailure) (Provider, *Error) {
	t.Helper()
	idp := startUnitIdP(t)
	now := time.Unix(1_800_000_000, 0)
	cfg := ProviderConfig{ClientID: unitClient, Issuer: idp.server.URL, TokenEndpoint: idp.server.URL + "/token", JWKSURI: idp.server.URL + "/jwks"}
	claims := idp.claims(now)
	if c.claims != nil {
		c.claims(claims)
	}
	header := map[string]any{"alg": "RS256", "kid": "k1", "typ": "JWT"}
	if c.header != nil {
		c.header(header)
	}
	idp.idToken = idp.sign(t, header, claims)
	if c.raw != nil {
		idp.idToken = c.raw(t, idp, idp.idToken)
	}
	if c.config != nil {
		c.config(&cfg, idp)
	}
	if c.idp != nil {
		c.idp(idp)
	}
	svc := &Service{cfg: Config{HTTPClient: idp.server.Client(), Now: func() time.Time { return now }, ProviderFailure: failures}}
	row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "oidc", AutoProvision: true, Config: cfg}
	_, failure := svc.exchangeAndValidate(context.Background(), row, flowState{Nonce: unitNonce, RedirectURI: "https://app.example/cb"}, "code-1")
	return row, failure
}

func withDiscovery(doc string, status int) func(*ProviderConfig, *unitIdP) {
	return func(c *ProviderConfig, idp *unitIdP) {
		c.TokenEndpoint, c.JWKSURI = "", ""
		idp.discoveryBody, idp.discoveryStatus = strings.ReplaceAll(doc, "ISSUER", idp.server.URL), status
	}
}

func withUserinfo(body string, status int) func(*ProviderConfig, *unitIdP) {
	return func(c *ProviderConfig, idp *unitIdP) {
		c.UserinfoEndpoint = idp.server.URL + "/userinfo"
		idp.userinfoBody, idp.userinfoCode = body, status
	}
}

func setIssuer(issuer string) func(*ProviderConfig, *unitIdP) {
	return func(c *ProviderConfig, _ *unitIdP) { c.TokenEndpoint, c.JWKSURI, c.Issuer = "", "", issuer }
}

func dropClaim(name string) func(map[string]any) {
	return func(c map[string]any) { delete(c, name) }
}

func setClaim(name string, value any) func(map[string]any) {
	return func(c map[string]any) { c[name] = value }
}

// Each provider failure go-api records (recordSSOFailure, protocol oidc, no stage) writes one record with go-api's
// fixed text, naming the provider row, and the answer stays the 400 authentication failure.
func TestOIDCProviderFailuresRecordGoAPIsText(t *testing.T) {
	expired := int64(1_800_000_000 - 3600)
	for name, tc := range map[string]struct {
		c    unitCase
		text string
	}{
		"no issuer":         {unitCase{config: setIssuer("")}, "OIDC issuer is required"},
		"issuer unparsable": {unitCase{config: setIssuer("https://[bad")}, "Invalid OIDC issuer URL"},
		"issuer not https":  {unitCase{config: setIssuer("http://issuer.example")}, "OIDC issuer must use HTTPS"},
		"issuer no host":    {unitCase{config: setIssuer("https://")}, "OIDC issuer must be a valid URL"},
		"discovery 502":     {unitCase{config: withDiscovery(providerMarker, http.StatusBadGateway)}, "Failed to fetch OIDC discovery document"},
		"discovery 404":     {unitCase{config: withDiscovery(providerMarker, http.StatusNotFound)}, "Failed to fetch OIDC discovery document"},
		"discovery no jwks": {unitCase{config: withDiscovery(`{"issuer":"ISSUER","token_endpoint":"https://t.example/token"}`, 0)}, "OIDC discovery missing required endpoints"},
		"no client id":      {unitCase{config: func(c *ProviderConfig, _ *unitIdP) { c.ClientID = "" }}, "OIDC configuration missing issuer/client_id/jwks"},
		"exchange refused": {unitCase{idp: func(i *unitIdP) {
			i.tokenStatus, i.tokenBody = 400, `{"error":"invalid_grant","error_description":"`+providerMarker+`"}`
		}}, "OIDC token exchange failed"},
		"exchange 502":      {unitCase{idp: func(i *unitIdP) { i.tokenStatus, i.tokenBody = 502, providerMarker }}, "OIDC token exchange failed"},
		"no access token":   {unitCase{idp: func(i *unitIdP) { i.tokenBody = `{"token_type":"Bearer","id_token":"x"}` }}, "OIDC token exchange failed"},
		"no id token":       {unitCase{idp: func(i *unitIdP) { i.noIDToken = true }}, "OIDC token response missing id_token"},
		"two parts":         {unitCase{raw: func(*testing.T, *unitIdP, string) string { return "a.b" }}, "OIDC id_token validation failed"},
		"header not base64": {unitCase{raw: func(*testing.T, *unitIdP, string) string { return "!!!.b.c" }}, "OIDC id_token validation failed"},
		"alg none":          {unitCase{header: func(h map[string]any) { h["alg"] = "none" }}, "OIDC id_token validation failed"},
		"bad signature":     {unitCase{raw: func(_ *testing.T, _ *unitIdP, s string) string { return s[:strings.LastIndex(s, ".")+1] + "AAAA" }}, "OIDC id_token validation failed"},
		"issuer differs":    {unitCase{claims: setClaim("iss", "https://other.example")}, "OIDC id_token validation failed"},
		"audience differs":  {unitCase{claims: setClaim("aud", "other")}, "OIDC id_token validation failed"},
		"exp missing":       {unitCase{claims: dropClaim("exp")}, "OIDC id_token validation failed"},
		"exp not a number":  {unitCase{claims: setClaim("exp", "soon")}, "OIDC id_token validation failed"},
		"expired":           {unitCase{claims: setClaim("exp", expired)}, "OIDC id_token validation failed"},
		"iat missing":       {unitCase{claims: dropClaim("iat")}, "OIDC id_token validation failed"},
		"iat not a number":  {unitCase{claims: setClaim("iat", "now")}, "OIDC id_token validation failed"},
		"nbf not a number":  {unitCase{claims: setClaim("nbf", "later")}, "OIDC id_token validation failed"},
		"nbf in the future": {unitCase{claims: setClaim("nbf", int64(1_800_000_000+3600))}, "OIDC id_token validation failed"},
		"nonce differs":     {unitCase{claims: setClaim("nonce", providerMarker)}, "OIDC nonce mismatch"},
		"nonce missing":     {unitCase{claims: dropClaim("nonce")}, "OIDC nonce mismatch"},
		"userinfo 502":      {unitCase{config: withUserinfo(providerMarker, http.StatusBadGateway)}, "OIDC userinfo request failed"},
		"userinfo 401":      {unitCase{config: withUserinfo(providerMarker, http.StatusUnauthorized)}, "OIDC userinfo request failed"},
		"userinfo not JSON": {unitCase{config: withUserinfo(providerMarker, 0)}, "OIDC userinfo request failed"},
	} {
		t.Run(name, func(t *testing.T) {
			without := func() *Error { _, f := runUnitCase(t, tc.c, nil); return f }()
			failures := &oidcFailures{}
			row, failure := runUnitCase(t, tc.c, failures)
			if failure == nil || without == nil || failure.Status != http.StatusBadRequest || failure.Detail != detailAuthFailed || failure.Reason != without.Reason {
				t.Fatalf("answer %+v, without the record %+v, want the 400 authentication failure", failure, without)
			}
			want := ssoaudit.Row{OrgID: row.OrgID, ProviderID: row.ID, Protocol: "oidc", ErrorMessage: tc.text}
			if len(failures.rows) != 1 || failures.rows[0] != want {
				t.Fatalf("records %+v, want exactly %+v", failures.rows, want)
			}
			if strings.Contains(failures.rows[0].ErrorMessage, providerMarker) {
				t.Fatalf("a provider value reached the record: %+v", failures.rows[0])
			}
		})
	}
}

// The token response texts, in go-api's order. oauth2 turns a response without an access token into an exchange
// error, in go-api too, so that text is reached only here.
func TestMissingTokenTextsAreGoAPIs(t *testing.T) {
	for _, tc := range []struct{ access, id, want string }{
		{"", "", "OIDC token response missing access_token"},
		{"", "x", "OIDC token response missing access_token"},
		{"x", "", "OIDC token response missing id_token"},
		{"x", "x", ""},
	} {
		if got := missingTokenText(tc.access, tc.id); got != tc.want {
			t.Errorf("missingTokenText(%q, %q) = %q, want %q", tc.access, tc.id, got, tc.want)
		}
	}
}

type oidcFailures struct {
	rows []ssoaudit.Row
	err  error
}

func (r *oidcFailures) Failed(_ context.Context, row ssoaudit.Row) error {
	r.rows = append(r.rows, row)
	return r.err
}

// A check go-api does not make, and a refusal go-api answers without its recorder, records nothing.
func TestOIDCRefusalsGoAPIDoesNotRecordWriteNoRecord(t *testing.T) {
	now := int64(1_800_000_000)
	big4K := strings.Repeat("a", maxIdPResponseBytes+1)
	for name, c := range map[string]unitCase{
		"issuer with userinfo":      {config: setIssuer("https://user@issuer.example")},
		"document not JSON":         {config: withDiscovery(providerMarker, 0)},
		"document issuer differs":   {config: withDiscovery(`{"issuer":"https://other.example","token_endpoint":"https://t.example/token","jwks_uri":"https://t.example/jwks"}`, 0)},
		"document too large":        {config: withDiscovery(big4K, 0)},
		"document trailing data":    {config: withDiscovery(`{"issuer":"ISSUER"} x`, 0)},
		"userinfo too large":        {config: withUserinfo(big4K, 0)},
		"userinfo trailing data":    {config: withUserinfo(`{"sub":"sub-1"} x`, 0)},
		"userinfo subject differs":  {config: withUserinfo(`{"sub":"other"}`, 0)},
		"azp differs":               {claims: setClaim("azp", "other")},
		"several audiences, no azp": {claims: setClaim("aud", []string{unitClient, "other"})},
		"issued in the future":      {claims: setClaim("iat", now+3600)},
		"issued too long ago":       {claims: func(c map[string]any) { c["iat"] = now - 7200; c["exp"] = now + 300 }},
		"no subject":                {claims: dropClaim("sub")},
		"subject not a key":         {claims: setClaim("sub", "s\x01")},
		"token too long":            {raw: func(*testing.T, *unitIdP, string) string { return strings.Repeat("a", maxIDTokenBytes+1) }},
		"header too long":           {header: func(h map[string]any) { h["x"] = strings.Repeat("a", maxHeaderBytes*2) }},
		"crit member":               {header: func(h map[string]any) { h["crit"] = []string{"exp"} }},
		"embedded jwk":              {header: func(h map[string]any) { h["jwk"] = map[string]any{} }},
		"jku":                       {header: func(h map[string]any) { h["jku"] = "https://k.example" }},
		"x5u":                       {header: func(h map[string]any) { h["x5u"] = "https://k.example" }},
		"x5c":                       {header: func(h map[string]any) { h["x5c"] = []string{"a"} }},
		"alg not text":              {header: func(h map[string]any) { h["alg"] = 7 }},
		"kid not text":              {header: func(h map[string]any) { h["kid"] = 7 }},
		"duplicate header member": {raw: func(t *testing.T, _ *unitIdP, s string) string {
			return base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","alg":"RS256"}`)) + s[strings.Index(s, "."):]
		}},
		"header not an object": {raw: func(t *testing.T, _ *unitIdP, s string) string {
			return base64.RawURLEncoding.EncodeToString([]byte(`["RS256"]`)) + s[strings.Index(s, "."):]
		}},
	} {
		t.Run(name, func(t *testing.T) {
			failures := &oidcFailures{}
			_, failure := runUnitCase(t, c, failures)
			if failure == nil {
				t.Fatal("the case was not refused")
			}
			if len(failures.rows) != 0 {
				t.Fatalf("records %+v, want none", failures.rows)
			}
		})
	}
	failures := &oidcFailures{}
	if _, failure := runUnitCase(t, unitCase{}, failures); failure != nil || len(failures.rows) != 0 {
		t.Fatalf("a good token: %+v, records %+v", failure, failures.rows)
	}
}

// A record that cannot be written is go-api's 500, for every class.
func TestOIDCRecordWriteErrorIsThe500(t *testing.T) {
	for name, c := range map[string]unitCase{
		"discovery": {config: setIssuer("")},
		"exchange":  {idp: func(i *unitIdP) { i.tokenStatus, i.tokenBody = 400, `{"error":"invalid_grant"}` }},
		"id token":  {idp: func(i *unitIdP) { i.noIDToken = true }},
		"nonce":     {claims: setClaim("nonce", "other")},
		"userinfo":  {config: withUserinfo("", http.StatusBadGateway)},
	} {
		_, failure := runUnitCase(t, c, &oidcFailures{err: errors.New("permission denied for table sso_providers")})
		if failure == nil || failure.Status != http.StatusInternalServerError || failure.Reason != ReasonStoreUnavailable {
			t.Errorf("%s: %+v, want the 500 store_unavailable", name, failure)
		}
	}
}

func resolveRecorded(t *testing.T, protocol string, autoProvision bool, p profile, ids *resolveIdentities, failures ssoaudit.ProviderFailure) (Provider, *Error) {
	t.Helper()
	svc := &Service{cfg: Config{Identities: ids, Memberships: resolveMembers{}, Sessions: &resolveSessions{}, MembershipJIT: &resolveJIT{}, ProviderFailure: failures}}
	row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: autoProvision}
	_, failure := svc.resolve(context.Background(), row, validated{profile: p}, nil)
	return row, failure
}

// OIDC claims without an email: go-api's record, and its 400 authentication failure, for a linked and a new key.
func TestOIDCNoEmailRecordsAndAnswersGoAPIs(t *testing.T) {
	for name, ids := range map[string]*resolveIdentities{"new": {err: ErrIdentityNotFound}, "linked": {}} {
		failures := &oidcFailures{}
		row, failure := resolveRecorded(t, "oidc", true, profile{EmailVerified: true}, ids, failures)
		if failure == nil || failure.Status != http.StatusBadRequest || failure.Detail != detailAuthFailed || failure.Reason != ReasonMalformed {
			t.Fatalf("%s: %+v", name, failure)
		}
		want := ssoaudit.Row{OrgID: row.OrgID, ProviderID: row.ID, Protocol: "oidc", ErrorMessage: "OIDC claims missing email"}
		if len(failures.rows) != 1 || failures.rows[0] != want {
			t.Fatalf("%s: records %+v, want %+v", name, failures.rows, want)
		}
	}
}

// OIDC auto-provisioning off: go-api's provisioning record and its 400; the reason code stays.
func TestOIDCAutoProvisionOffRecordsAndAnswersGoAPIs(t *testing.T) {
	failures := &oidcFailures{}
	row, failure := resolveRecorded(t, "oidc", false, profile{Email: "u@example.test", EmailVerified: true}, &resolveIdentities{err: ErrIdentityNotFound}, failures)
	if failure == nil || failure.Status != http.StatusBadRequest || failure.Detail != "OIDC user provisioning failed" || failure.Reason != ReasonProvisioningDisabled {
		t.Fatalf("%+v", failure)
	}
	want := ssoaudit.Row{OrgID: row.OrgID, ProviderID: row.ID, Protocol: "oidc", Stage: "provisioning", ErrorMessage: "User not found and auto-provisioning disabled"}
	if len(failures.rows) != 1 || failures.rows[0] != want {
		t.Fatalf("records %+v, want %+v", failures.rows, want)
	}
	_, failure = resolveRecorded(t, "oidc", false, profile{Email: "u@example.test", EmailVerified: true}, &resolveIdentities{err: ErrIdentityNotFound}, &oidcFailures{err: errors.New("denied")})
	if failure == nil || failure.Status != http.StatusInternalServerError {
		t.Fatalf("a write error: %+v, want the 500", failure)
	}
}

// OAuth keeps its answers: the 403 and 12040's denial row, no provider failure.
func TestOAuthKeepsItsAnswersOnTheSharedChecks(t *testing.T) {
	failures := &oidcFailures{}
	_, failure := resolveRecorded(t, "oauth_gitlab", false, profile{Email: "u@example.test", EmailVerified: true}, &resolveIdentities{err: ErrIdentityNotFound}, failures)
	if failure == nil || failure.Status != http.StatusForbidden || failure.Reason != ReasonProvisioningDisabled || len(failures.rows) != 0 {
		t.Fatalf("auto-provisioning off: %+v, records %+v", failure, failures.rows)
	}
	_, failure = resolveRecorded(t, "oauth_gitlab", true, profile{EmailVerified: true}, &resolveIdentities{}, failures)
	if failure == nil || failure.Detail != "Email address is missing from OIDC claims" || len(failures.rows) != 0 {
		t.Fatalf("no email: %+v, records %+v", failure, failures.rows)
	}
}

// A check is unrecorded unless it says otherwise: every claim check and header return of idtoken.go is classified
// here, so a new check has to be classified, and a default check records nothing.
func TestEveryIDTokenCheckIsClassified(t *testing.T) {
	claimChecks := map[string]string{
		"payload is not a JSON object":              "",
		"payload does not decode":                   "",
		"issuer mismatch":                           "msgIDTokenInvalid",
		"audience mismatch":                         "msgIDTokenInvalid",
		"authorized party mismatch":                 "",
		"several audiences and no authorized party": "",
		"exp missing or not a number":               "msgIDTokenInvalid",
		"iat missing or not a number":               "msgIDTokenInvalid",
		"expired":                                   "msgIDTokenInvalid",
		"issued in the future":                      "",
		"issued too long ago":                       "",
		"nbf not a number":                          "msgIDTokenInvalid",
		"not yet valid":                             "msgIDTokenInvalid",
		"nonce mismatch":                            "msgNonceMismatch",
		"subject missing or invalid":                "",
	}
	headerReturns := map[string]int{
		"errMalformedToken": 7,
		"recordAs(msgIDTokenInvalid, errMalformedToken)": 2,
		"recordAs(msgIDTokenInvalid, errAlgNotAllowed)":  1,
	}
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filepath.Join(root, "internal", "auth", "identity", "oidc", "idtoken.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	gotClaims := map[string]string{}
	seen := map[*ast.CallExpr]bool{}
	claimMessage := func(call *ast.CallExpr) (string, bool) {
		if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "claimFail" {
			return "", false
		}
		lit, ok := call.Args[1].(*ast.BasicLit)
		if !ok {
			t.Fatalf("a claimFail message is not a literal: %s", fset.Position(call.Pos()))
		}
		text, err := strconv.Unquote(lit.Value)
		if err != nil {
			t.Fatal(err)
		}
		return text, true
	}
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "recordAs" {
			if inner, ok := call.Args[1].(*ast.CallExpr); ok {
				if msg, ok := claimMessage(inner); ok {
					gotClaims[msg] = call.Args[0].(*ast.Ident).Name
					seen[inner] = true
				}
			}
			return true
		}
		if msg, ok := claimMessage(call); ok && !seen[call] {
			gotClaims[msg] = ""
		}
		return true
	})
	for msg, text := range gotClaims {
		want, ok := claimChecks[msg]
		if !ok {
			t.Errorf("claim check %q is not classified here", msg)
		} else if want != text {
			t.Errorf("claim check %q records %q, classified %q", msg, text, want)
		}
	}
	for msg := range claimChecks {
		if _, ok := gotClaims[msg]; !ok {
			t.Errorf("classified claim check %q is gone", msg)
		}
	}
	gotHeader := map[string]int{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "parseJOSEHeader" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			ret, ok := n.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 2 {
				return true
			}
			var text strings.Builder
			if err := printer.Fprint(&text, fset, ret.Results[1]); err != nil {
				t.Fatal(err)
			}
			if text.String() != "nil" {
				gotHeader[text.String()]++
			}
			return true
		})
	}
	if len(gotHeader) != len(headerReturns) {
		t.Errorf("header returns %v, classified %v", gotHeader, headerReturns)
	}
	for expr, count := range headerReturns {
		if gotHeader[expr] != count {
			t.Errorf("header return %s: %d, classified %d", expr, gotHeader[expr], count)
		}
	}
	if _, ok := goAPIText(errors.New("any other error")); ok {
		t.Error("an unmarked error records")
	}
	if _, ok := goAPIText(claimFail(ReasonMalformed, "x")); ok {
		t.Error("a default claim check records")
	}
}
