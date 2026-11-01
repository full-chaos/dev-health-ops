package idpstub

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	oidcAccessPrefix = "idpstub-access-"
	discoverySuffix  = "/.well-known/openid-configuration"
)

// OIDCUser is one account the stub signs in.
type OIDCUser struct {
	Sub, Email, Name string
	// EmailVerified is the email_verified claim; nil leaves the claim out.
	EmailVerified *bool
	Extra         map[string]any
}

// OIDCOptions configures NewOIDC. The zero value is a usable provider with two default users.
type OIDCOptions struct {
	Keys  *Keys
	Clock func() time.Time
	// Issuer is the iss claim of every id_token. Empty means the base URL.
	Issuer string
	// ClientID and RedirectURIs, when set, are checked at /authorize; ClientSecret, when set, is checked at /token.
	ClientID     string
	ClientSecret string
	RedirectURIs []string
	// NoPKCE accepts a token request without a matching code_verifier.
	NoPKCE bool
	// AcceptAnyCode makes /token answer an unknown code with the user set by SetNext.
	AcceptAnyCode bool
	// MinimalDiscovery serves only issuer, token_endpoint and jwks_uri in the discovery document.
	MinimalDiscovery bool
	Users            []OIDCUser
	// Override edits the claims of every id_token and every userinfo answer.
	Override func(map[string]any)
}

type pendingCode struct {
	challenge, nonce, clientID string
	user                       OIDCUser
	claims                     func(map[string]any)
	idToken                    *string
}

// CodeRequest registers an authorization code without the /authorize hop.
type CodeRequest struct {
	Challenge, Nonce, ClientID string
	User                       OIDCUser
	Claims                     func(map[string]any)
	// IDToken, when set, is returned as the id_token exactly as given (empty means an empty id_token), so a test
	// can sign its own header and claims.
	IDToken *string
}

// OIDC is an OpenID provider: discovery, JWKS, authorize, token and userinfo.
type OIDC struct {
	opts OIDCOptions
	keys *Keys
	now  func() time.Time

	mu            sync.Mutex
	baseURL       string
	issuer        string
	codes         map[string]pendingCode
	tokens        map[string]OIDCUser
	next          OIDCUser
	nextNonce     string
	tokenCalls    int
	verifier      string
	secret        string
	userinfo      map[string]any
	docIssuer     *string
	failDiscovery bool
	failUserinfo  bool
}

// NewOIDC builds the provider; keys are generated unless opts.Keys is set.
func NewOIDC(opts OIDCOptions) (*OIDC, error) {
	keys := opts.Keys
	if keys == nil {
		var err error
		if keys, err = NewKeys("idpstub oidc"); err != nil {
			return nil, err
		}
	}
	now := opts.Clock
	if now == nil {
		now = time.Now
	}
	if len(opts.Users) == 0 {
		yes := true
		opts.Users = []OIDCUser{
			{Sub: "alice", Email: "alice@idp.example.test", Name: "Alice Example", EmailVerified: &yes},
			{Sub: "bob", Email: "bob@idp.example.test", Name: "Bob Example", EmailVerified: &yes},
		}
	}
	return &OIDC{opts: opts, keys: keys, now: now, issuer: opts.Issuer, codes: map[string]pendingCode{},
		tokens: map[string]OIDCUser{}, next: opts.Users[0]}, nil
}

// Keys returns the signing key material.
func (o *OIDC) Keys() *Keys { return o.keys }

// SetBaseURL sets the absolute URL the endpoints are served under (no trailing slash).
func (o *OIDC) SetBaseURL(base string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.baseURL = strings.TrimSuffix(base, "/")
}

// SetIssuer sets the iss claim of later id_tokens.
func (o *OIDC) SetIssuer(issuer string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.issuer = issuer
}

// SetNext sets the user and nonce an unknown code is answered with (AcceptAnyCode).
func (o *OIDC) SetNext(user OIDCUser, nonce string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.next, o.nextNonce = user, nonce
}

// SetUserinfo makes /userinfo answer exactly this object, with no bearer check; nil restores the per-user answer.
func (o *OIDC) SetUserinfo(answer map[string]any) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.userinfo = answer
}

// SetDocIssuer makes the discovery document name a different issuer; nil restores the served one.
func (o *OIDC) SetDocIssuer(issuer *string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.docIssuer = issuer
}

// SetFailDiscovery makes discovery answer 502.
func (o *OIDC) SetFailDiscovery(fail bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failDiscovery = fail
}

// SetFailUserinfo makes /userinfo answer 502.
func (o *OIDC) SetFailUserinfo(fail bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.failUserinfo = fail
}

// TokenCalls is the number of /token requests so far.
func (o *OIDC) TokenCalls() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.tokenCalls
}

// Received is the code_verifier and client_secret of the last /token request.
func (o *OIDC) Received() (verifier, clientSecret string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.verifier, o.secret
}

// User returns the configured user with this sub or email.
func (o *OIDC) User(subOrEmail string) (OIDCUser, bool) {
	for _, u := range o.opts.Users {
		if u.Sub == subOrEmail || u.Email == subOrEmail {
			return u, true
		}
	}
	return OIDCUser{}, false
}

// IssueCode registers an authorization code and returns it.
func (o *OIDC) IssueCode(req CodeRequest) string {
	code := randomToken()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.codes[code] = pendingCode{challenge: req.Challenge, nonce: req.Nonce, clientID: req.ClientID, user: req.User, claims: req.Claims, idToken: req.IDToken}
	return code
}

// IDTokenClaims is the claim set /token signs for this user.
func (o *OIDC) IDTokenClaims(user OIDCUser, clientID, nonce string) map[string]any {
	o.mu.Lock()
	issuer := o.issuer
	if issuer == "" {
		issuer = o.baseURL
	}
	o.mu.Unlock()
	now := o.now()
	claims := map[string]any{"iss": issuer, "aud": clientID, "sub": user.Sub, "nonce": nonce,
		"iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix()}
	if user.Email != "" {
		claims["email"] = user.Email
	}
	if user.Name != "" {
		claims["name"] = user.Name
	}
	if user.EmailVerified != nil {
		claims["email_verified"] = *user.EmailVerified
	}
	for k, v := range user.Extra {
		claims[k] = v
	}
	return claims
}

// SignIDToken signs header and claims as an RS256 JWT with the stub's key.
func (o *OIDC) SignIDToken(header, claims map[string]any) (string, error) {
	if header == nil {
		header = map[string]any{"alg": "RS256", "kid": o.keys.KID, "typ": "JWT"}
	}
	h, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	c, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := b64(h) + "." + b64(c)
	digest := sha256.Sum256([]byte(input))
	sig, err := rsa.SignPKCS1v15(rand.Reader, o.keys.Key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return input + "." + b64(sig), nil
}

// Handler serves the provider. Discovery answers any path that ends in /.well-known/openid-configuration.
func (o *OIDC) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/jwks", o.serveJWKS)
	mux.HandleFunc("/authorize", o.serveAuthorize)
	mux.HandleFunc("/token", o.serveToken)
	mux.HandleFunc("/userinfo", o.serveUserinfo)
	mux.HandleFunc("/", o.serveDiscovery)
	return mux
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func randomToken() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (o *OIDC) serveJWKS(w http.ResponseWriter, _ *http.Request) {
	pub := o.keys.Key.PublicKey
	writeJSON(w, http.StatusOK, map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "kid": o.keys.KID, "use": "sig", "alg": "RS256",
		"n": b64(pub.N.Bytes()), "e": b64(big.NewInt(int64(pub.E)).Bytes()),
	}}})
}

func (o *OIDC) serveDiscovery(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, discoverySuffix) {
		http.NotFound(w, r)
		return
	}
	o.mu.Lock()
	base, fail, docIssuer := o.baseURL, o.failDiscovery, o.docIssuer
	o.mu.Unlock()
	if fail {
		http.Error(w, "unavailable", http.StatusBadGateway)
		return
	}
	issuer := base + strings.TrimSuffix(r.URL.Path, discoverySuffix)
	if docIssuer != nil {
		issuer = *docIssuer
	}
	doc := map[string]any{"issuer": issuer, "token_endpoint": base + "/token", "jwks_uri": base + "/jwks"}
	if !o.opts.MinimalDiscovery {
		doc["authorization_endpoint"] = base + "/authorize"
		doc["userinfo_endpoint"] = base + "/userinfo"
		doc["response_types_supported"] = []string{"code"}
		doc["subject_types_supported"] = []string{"public"}
		doc["id_token_signing_alg_values_supported"] = []string{"RS256"}
		doc["code_challenge_methods_supported"] = []string{"S256"}
	}
	writeJSON(w, http.StatusOK, doc)
}

func (o *OIDC) serveAuthorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	redirect := q.Get("redirect_uri")
	if q.Get("response_type") != "code" || redirect == "" || q.Get("state") == "" {
		http.Error(w, "invalid_request", http.StatusBadRequest)
		return
	}
	if o.opts.ClientID != "" && q.Get("client_id") != o.opts.ClientID {
		http.Error(w, "unauthorized_client", http.StatusBadRequest)
		return
	}
	if len(o.opts.RedirectURIs) > 0 && !contains(o.opts.RedirectURIs, redirect) {
		http.Error(w, "invalid redirect_uri", http.StatusBadRequest)
		return
	}
	if !o.opts.NoPKCE && (q.Get("code_challenge") == "" || q.Get("code_challenge_method") != "S256") {
		http.Error(w, "S256 code_challenge required", http.StatusBadRequest)
		return
	}
	target, err := url.Parse(redirect)
	if err != nil || (target.Scheme != "https" && target.Scheme != "http") {
		http.Error(w, "invalid redirect_uri", http.StatusBadRequest)
		return
	}
	user := o.opts.Users[0]
	for _, key := range []string{q.Get("login_hint"), q.Get("user")} {
		if u, ok := o.User(key); ok && key != "" {
			user = u
			break
		}
	}
	code := o.IssueCode(CodeRequest{Challenge: q.Get("code_challenge"), Nonce: q.Get("nonce"), ClientID: q.Get("client_id"), User: user})
	out := target.Query()
	out.Set("code", code)
	out.Set("state", q.Get("state"))
	target.RawQuery = out.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func (o *OIDC) serveToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.ParseForm() != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	form := r.PostForm
	o.mu.Lock()
	o.tokenCalls++
	o.verifier, o.secret = form.Get("code_verifier"), form.Get("client_secret")
	pending, ok := o.codes[form.Get("code")]
	delete(o.codes, form.Get("code"))
	next, nextNonce := o.next, o.nextNonce
	o.mu.Unlock()

	invalid := func() { writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid_grant"}) }
	if form.Get("grant_type") != "authorization_code" && !o.opts.AcceptAnyCode {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
		return
	}
	if !ok {
		if !o.opts.AcceptAnyCode {
			invalid()
			return
		}
		pending = pendingCode{user: next, clientID: form.Get("client_id"), nonce: nextNonce}
	} else if !o.opts.NoPKCE {
		sum := sha256.Sum256([]byte(form.Get("code_verifier")))
		if subtle.ConstantTimeCompare([]byte(b64(sum[:])), []byte(pending.challenge)) != 1 {
			invalid()
			return
		}
	}
	if o.opts.ClientSecret != "" && subtle.ConstantTimeCompare([]byte(form.Get("client_secret")), []byte(o.opts.ClientSecret)) != 1 {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid_client"})
		return
	}
	clientID := pending.clientID
	if clientID == "" {
		clientID = form.Get("client_id")
	}
	var idToken string
	if pending.idToken != nil {
		idToken = *pending.idToken
	} else {
		claims := o.IDTokenClaims(pending.user, clientID, pending.nonce)
		if pending.claims != nil {
			pending.claims(claims)
		}
		if o.opts.Override != nil {
			o.opts.Override(claims)
		}
		var err error
		if idToken, err = o.SignIDToken(nil, claims); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	access := oidcAccessPrefix + randomToken()
	o.mu.Lock()
	o.tokens[access] = pending.user
	o.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"access_token": access, "token_type": "Bearer", "expires_in": 300, "id_token": idToken})
}

func (o *OIDC) serveUserinfo(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	fail, override := o.failUserinfo, o.userinfo
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	user, known := o.tokens[token]
	o.mu.Unlock()
	if fail {
		http.Error(w, "unavailable", http.StatusBadGateway)
		return
	}
	if override != nil {
		writeJSON(w, http.StatusOK, override)
		return
	}
	if !known {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	answer := map[string]any{"sub": user.Sub, "email": user.Email, "name": user.Name}
	if o.opts.Override != nil {
		o.opts.Override(answer)
	}
	writeJSON(w, http.StatusOK, answer)
}
