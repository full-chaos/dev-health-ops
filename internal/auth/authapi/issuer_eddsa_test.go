package authapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// goldenIssuer is the issuer of the access-token golden corpus.
const goldenIssuer = "https://auth.example.test"

func writeEd25519Key(t *testing.T, dir, kid string) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, kid+".pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeKeySet(t *testing.T, dir string, pairs ...string) string {
	t.Helper()
	var keys []map[string]string
	for i := 0; i+1 < len(pairs); i += 2 {
		keys = append(keys, map[string]string{"kid": pairs[i], "state": pairs[i+1], "file": pairs[i] + ".pem"})
	}
	body, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(dir, ".staged")
	if err := os.WriteFile(staged, body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(staged, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "keyset.json")
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	return path
}

func fileSigner(t *testing.T) (*token.Signer, string, string) {
	t.Helper()
	dir := t.TempDir()
	writeEd25519Key(t, dir, "k1")
	writeEd25519Key(t, dir, "k2")
	manifest := writeKeySet(t, dir, "k1", "active", "k2", "pending")
	return token.NewFileSigner(token.FileConfig{KeySetPath: manifest}), dir, manifest
}

func newEdDSAIssuer(t *testing.T, signer AccessSigner, audience string) *Issuer {
	t.Helper()
	issuer, err := NewEdDSAIssuer(testSecret, testIssuer, audience, signer, goldenIssuer)
	if err != nil {
		t.Fatal(err)
	}
	return issuer
}

type decoded struct {
	header map[string]any
	claims map[string]any
	raw    []byte
}

// verifyEdDSA checks compact as a verifier does: EdDSA pinned before key
// lookup, kid required, exact iss and aud, no leeway.
func verifyEdDSA(t *testing.T, compact string, set token.KeySet, issuer, audience string, now time.Time) decoded {
	t.Helper()
	parsed, err := jwt.Parse(compact, func(tok *jwt.Token) (any, error) {
		kid, _ := tok.Header["kid"].(string)
		for _, key := range set.Published() {
			if key.KeyID == kid {
				return key.PublicKey, nil
			}
		}
		return nil, errors.New("unknown kid")
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(issuer), jwt.WithAudience(audience),
		jwt.WithTimeFunc(func() time.Time { return now }), jwt.WithLeeway(0), jwt.WithExpirationRequired())
	if err != nil || !parsed.Valid {
		t.Fatalf("token does not verify: %v", err)
	}
	parts := strings.Split(compact, ".")
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	return decoded{header: parsed.Header, claims: claims, raw: payload}
}

func accessDocument(t *testing.T, d decoded) []byte {
	t.Helper()
	document, err := json.Marshal(map[string]any{"header": d.header, "claims": d.claims})
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func TestEdDSAAccessTokenRoundTripsUnderBothAudiences(t *testing.T) {
	for audience, scope := range map[string]string{"dev-health-api": "api:access", "auth-service": "auth:session"} {
		t.Run(audience, func(t *testing.T) {
			signer, _, _ := fileSigner(t)
			issuer := newEdDSAIssuer(t, signer, audience)
			now := time.Now().UTC().Truncate(time.Second)
			issuer.now = func() time.Time { return now }
			org, principal, sessionID := uuid.New(), uuid.New(), uuid.New()
			compact, err := issuer.AccessContext(context.Background(), Grant{
				PrincipalID: principal, OrganizationID: &org, SessionID: sessionID, Role: "admin",
				User: UserState{Email: "ada@example.com", IsSuperuser: true, TokenVersion: 4},
			})
			if err != nil {
				t.Fatal(err)
			}
			set, err := signer.KeySet(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			got := verifyEdDSA(t, compact, set, goldenIssuer, audience, now)
			if got.header["alg"] != "EdDSA" || got.header["typ"] != "at+jwt" || got.header["kid"] != "k1" || len(got.header) != 3 {
				t.Fatalf("header %v", got.header)
			}
			parsed, err := contracts.ParseAccessToken(repoRoot(t), accessDocument(t, got))
			if err != nil {
				t.Fatalf("not access-token.v1: %v", err)
			}
			c := parsed.Claims
			if c.Iss != goldenIssuer || c.Aud != audience || c.Scope != scope || c.PrincipalType != "human" ||
				c.Sub != session.WirePrincipalID(principal) || c.Sid != session.WireSessionID(sessionID) ||
				c.OrgID == nil || *c.OrgID != org.String() || c.Iat != now.Unix() || c.Nbf != c.Iat ||
				c.Exp-c.Iat != 3600 {
				t.Fatalf("claims %+v", c)
			}
			for _, refused := range []string{"role", "email", "username", "full_name", "is_superuser", "tv", "type"} {
				if _, has := got.claims[refused]; has {
					t.Errorf("claim %q was minted into an access-token.v1 token", refused)
				}
			}
		})
	}
}

func TestEdDSAAccessTokenWithoutAnOrganizationOmitsOrgID(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	compact, err := issuer.AccessContext(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	set, _ := signer.KeySet(context.Background())
	got := verifyEdDSA(t, compact, set, goldenIssuer, testAudience, time.Now())
	if _, has := got.claims["org_id"]; has {
		t.Fatal("org_id minted for a grant without an organization")
	}
	if _, err := contracts.ParseAccessToken(repoRoot(t), accessDocument(t, got)); err != nil {
		t.Fatal(err)
	}
}

func TestEdDSAIssuerIsByteExactAboutIss(t *testing.T) {
	signer, _, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	compact, err := issuer.AccessContext(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	set, _ := signer.KeySet(context.Background())
	if got := verifyEdDSA(t, compact, set, goldenIssuer, testAudience, time.Now()); got.claims["iss"] != goldenIssuer {
		t.Fatalf("iss = %v", got.claims["iss"])
	}
	for _, other := range []string{goldenIssuer + "/", "HTTPS://auth.example.test", "https://auth.example.test:443", "http://auth.example.test"} {
		_, err := jwt.Parse(compact, func(*jwt.Token) (any, error) {
			return set.Active().PublicKey, nil
		}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(other))
		if err == nil {
			t.Errorf("a verifier pinned to %q accepted a token whose iss is %q", other, goldenIssuer)
		}
	}
	slashed, err := NewEdDSAIssuer(testSecret, testIssuer, testAudience, signer, goldenIssuer+"/")
	if err != nil {
		t.Fatal(err)
	}
	compact, err = slashed.AccessContext(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	got := verifyEdDSA(t, compact, set, goldenIssuer+"/", testAudience, time.Now())
	if got.claims["iss"] != goldenIssuer+"/" {
		t.Fatalf("the issuer was normalised: iss = %v", got.claims["iss"])
	}
}

func TestEdDSALifetimeIsTheInclusiveCeiling(t *testing.T) {
	base := contracts.AccessTokenClaims{
		Iss: goldenIssuer, Aud: "dev-health-api", Sub: "x", PrincipalType: "human", Scope: "api:access",
		Iat: 1_760_000_000, Nbf: 1_760_000_000,
	}
	for lifetime, wantOK := range map[int64]bool{3599: true, 3600: true, 3601: false} {
		c := base
		c.Exp = c.Iat + lifetime
		if err := checkAccessClaims(c); (err == nil) != wantOK {
			t.Errorf("lifetime %d: checkAccessClaims err = %v, want ok=%v", lifetime, err, wantOK)
		}
		// The schema-level parser agrees on the boundary.
		doc := accessDocumentFor(t, c)
		if _, err := contracts.ParseAccessToken(repoRoot(t), doc); (err == nil) != wantOK {
			t.Errorf("lifetime %d: ParseAccessToken err = %v, want ok=%v", lifetime, err, wantOK)
		}
	}
	if EdDSAAccessLifetime != time.Hour || AccessExpiresIn != 3600 {
		t.Fatalf("EdDSA lifetime %v, expires_in %d; want 1h and 3600", EdDSAAccessLifetime, AccessExpiresIn)
	}
	c := base
	c.Exp, c.Nbf = c.Iat+60, c.Iat-1
	if checkAccessClaims(c) == nil {
		t.Error("nbf before iat accepted")
	}
	c = base
	c.Exp = c.Nbf
	if checkAccessClaims(c) == nil {
		t.Error("exp == nbf accepted")
	}
	c = base
	c.Exp, c.Scope = c.Iat+60, "auth:session"
	if checkAccessClaims(c) == nil {
		t.Error("scope of the other audience accepted")
	}
}

func accessDocumentFor(t *testing.T, c contracts.AccessTokenClaims) []byte {
	t.Helper()
	c.Sub, c.Sid, c.Jti = session.WirePrincipalID(uuid.New()), session.WireSessionID(uuid.New()), uuid.NewString()
	document, err := json.Marshal(contracts.AccessToken{
		Header: contracts.AccessTokenHeader{Alg: "EdDSA", Typ: "at+jwt", Kid: "k1"}, Claims: c,
	})
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func TestEdDSAIssuerFollowsTheKeySetAcrossMints(t *testing.T) {
	signer, dir, _ := fileSigner(t)
	issuer := newEdDSAIssuer(t, signer, testAudience)
	kid := func() string {
		compact, err := issuer.AccessContext(context.Background(), Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
		if err != nil {
			t.Fatal(err)
		}
		header, err := base64.RawURLEncoding.DecodeString(strings.Split(compact, ".")[0])
		if err != nil {
			t.Fatal(err)
		}
		var h map[string]string
		if err := json.Unmarshal(header, &h); err != nil {
			t.Fatal(err)
		}
		return h["kid"]
	}
	if got := kid(); got != "k1" {
		t.Fatalf("first kid %q", got)
	}
	writeKeySet(t, dir, "k1", "retiring", "k2", "active")
	if got := kid(); got != "k2" {
		t.Fatalf("kid after the swap %q, want k2", got)
	}
}

type countingSigner struct {
	calls int
	err   error
	inner AccessSigner
}

func (c *countingSigner) Sign(ctx context.Context, typ string, payload []byte) (string, string, error) {
	c.calls++
	if c.err != nil {
		return "", "", c.err
	}
	return c.inner.Sign(ctx, typ, payload)
}

// NEVER BOTH: one issuer builds one class. The EdDSA issuer holds no HS256
// signer and signs through its key set only; the HS256 issuer holds no
// EdDSA minter and never calls a key-set signer.
func TestAnIssuerMintsExactlyOneClassNeverBoth(t *testing.T) {
	signer, _, _ := fileSigner(t)
	counter := &countingSigner{inner: signer}
	grant := Grant{PrincipalID: uuid.New(), SessionID: uuid.New(), Role: "member"}

	ed := newEdDSAIssuer(t, counter, testAudience)
	if ed.signer != nil || ed.eddsa == nil {
		t.Fatal("an EdDSA issuer holds an HS256 signer")
	}
	compact, err := ed.AccessContext(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	if counter.calls != 1 {
		t.Fatalf("key-set signer called %d times, want 1", counter.calls)
	}
	if _, err := jwt.Parse(compact, func(*jwt.Token) (any, error) { return []byte(testSecret), nil },
		jwt.WithValidMethods([]string{"HS256"})); err == nil {
		t.Fatal("the EdDSA issuer's token verifies as HS256")
	}
	if _, verifyErr := ed.verifier.Verify(compact); verifyErr == nil {
		t.Fatal("go-api's HS256 verifier accepted the EdDSA token")
	}

	hs, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	if hs.eddsa != nil || hs.signer == nil {
		t.Fatal("an HS256 issuer holds an EdDSA minter")
	}
	counter.calls = 0
	compact, err = hs.AccessContext(context.Background(), grant)
	if err != nil {
		t.Fatal(err)
	}
	if counter.calls != 0 {
		t.Fatal("the HS256 issuer called a key-set signer")
	}
	if _, verifyErr := hs.verifier.Verify(compact); verifyErr != nil {
		t.Fatalf("HS256 token refused: %v", verifyErr)
	}

	both := &Issuer{signer: hs.signer, eddsa: ed.eddsa, verifier: hs.verifier, now: time.Now}
	counter.calls = 0
	if token, err := both.AccessContext(context.Background(), grant); err == nil || token != "" || counter.calls != 0 {
		t.Fatalf("an issuer with both classes minted (%d bytes, err %v, %d signer calls)", len(token), err, counter.calls)
	}
	none := &Issuer{verifier: hs.verifier, now: time.Now}
	if token, err := none.AccessContext(context.Background(), grant); err == nil || token != "" {
		t.Fatalf("an issuer with no class minted (%d bytes, err %v)", len(token), err)
	}
}

// NO FALLBACK: a custody or signing failure under EdDSA mints nothing and
// never a compatibility HS256 token.
func TestEdDSACustodyFailureMintsNothingAndNeverFallsBackToHS256(t *testing.T) {
	grant := Grant{PrincipalID: uuid.New(), SessionID: uuid.New(), Role: "member"}
	failures := map[string]AccessSigner{
		"signer error": &countingSigner{err: errors.New("custody unavailable")},
		"key set gone": token.NewFileSigner(token.FileConfig{KeySetPath: filepath.Join(t.TempDir(), "missing.json")}),
		"key file gone": func() AccessSigner {
			signer, dir, _ := fileSigner(t)
			if err := os.Remove(filepath.Join(dir, "k1.pem")); err != nil {
				t.Fatal(err)
			}
			return signer
		}(),
	}
	for name, signer := range failures {
		t.Run(name, func(t *testing.T) {
			issuer := newEdDSAIssuer(t, signer, testAudience)
			compact, err := issuer.AccessContext(context.Background(), grant)
			if err == nil || compact != "" || !errors.Is(err, errIssue) {
				t.Fatalf("minted %d bytes, err %v; want no token and errIssue", len(compact), err)
			}
		})
	}

	f := newFixtureWith(t, newEdDSAIssuer(t, &countingSigner{err: errors.New("custody unavailable")}, testAudience))
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(time.Hour),
	}}
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	if response.Code == http.StatusOK || strings.Contains(response.Body.String(), "access_token") {
		t.Fatalf("refresh under failing custody answered %d: %s", response.Code, response.Body)
	}
}

func TestEdDSAIssuerRefusesWhatCannotFormAUserAccessToken(t *testing.T) {
	signer, _, _ := fileSigner(t)
	for _, audience := range []string{"", "query-api", "dev-health-api ", "Dev-Health-API", "other"} {
		if _, err := NewEdDSAIssuer(testSecret, testIssuer, audience, signer, goldenIssuer); err == nil {
			t.Errorf("audience %q accepted", audience)
		}
	}
	if _, err := NewEdDSAIssuer(testSecret, testIssuer, testAudience, nil, goldenIssuer); err == nil {
		t.Error("no signer accepted")
	}
	if _, err := NewEdDSAIssuer(testSecret, testIssuer, testAudience, signer, ""); err == nil {
		t.Error("no issuer URL accepted")
	}
	issuer := newEdDSAIssuer(t, signer, testAudience)
	for name, grant := range map[string]Grant{
		"no session":   {PrincipalID: uuid.New()},
		"no principal": {SessionID: uuid.New()},
	} {
		if compact, err := issuer.AccessContext(context.Background(), grant); err == nil || compact != "" {
			t.Errorf("%s: minted %d bytes", name, len(compact))
		}
	}
}

// The refresh route mints only the selected class.
func TestRefreshRouteMintsOnlyTheSelectedClass(t *testing.T) {
	signer, _, _ := fileSigner(t)
	for class, issuer := range map[string]*Issuer{
		"hs256_compat": func() *Issuer { i, _ := NewIssuer(testSecret, testIssuer, testAudience); return i }(),
		"eddsa":        newEdDSAIssuer(t, signer, testAudience),
	} {
		t.Run(class, func(t *testing.T) {
			f := newFixtureWith(t, issuer)
			f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
				Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(7*24*time.Hour - time.Second),
			}}
			response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
			if response.Code != http.StatusOK {
				t.Fatalf("status %d: %s", response.Code, response.Body)
			}
			// refresh-response.v1 still pins the HS256 header, so the EdDSA
			// answer is read as plain JSON here.
			var parsed struct {
				AccessToken string `json:"access_token"`
				ExpiresIn   int    `json:"expires_in"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
				t.Fatal(err)
			}
			if parsed.ExpiresIn != 3600 {
				t.Fatalf("expires_in %d", parsed.ExpiresIn)
			}
			compact := parsed.AccessToken
			header, err := base64.RawURLEncoding.DecodeString(strings.Split(compact, ".")[0])
			if err != nil {
				t.Fatal(err)
			}
			var h map[string]string
			if err := json.Unmarshal(header, &h); err != nil {
				t.Fatal(err)
			}
			verifier, err := edgetoken.New(testSecret, testIssuer, testAudience)
			if err != nil {
				t.Fatal(err)
			}
			_, hsVerifyErr := verifier.Verify(compact)
			switch class {
			case "hs256_compat":
				if h["alg"] != "HS256" || hsVerifyErr != nil {
					t.Fatalf("hs256_compat minted header %v, HS256 verify err %v", h, hsVerifyErr)
				}
			case "eddsa":
				if h["alg"] != "EdDSA" || h["typ"] != "at+jwt" || h["kid"] != "k1" || hsVerifyErr == nil {
					t.Fatalf("eddsa minted header %v, HS256 verify err %v", h, hsVerifyErr)
				}
			}
		})
	}
}

func TestEveryAccessAudienceTheMinterKnowsIsAUserTokenRowOfTheVocabulary(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(contracts.ContractsDir(repoRoot(t)), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary struct {
		Audiences []struct {
			Audience string   `json:"audience"`
			Classes  []string `json:"classes"`
		} `json:"audiences"`
	}
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	user := map[string]bool{}
	for _, row := range vocabulary.Audiences {
		for _, class := range row.Classes {
			if class == contracts.UserAccessTokenClass {
				user[row.Audience] = true
			}
		}
	}
	if len(user) != 2 {
		t.Fatalf("vocabulary user-token audiences %v; this minter knows two", user)
	}
	for audience := range user {
		if _, ok := contracts.AccessTokenScope(audience); !ok {
			t.Errorf("vocabulary audience %q has no scope", audience)
		}
	}
	for _, audience := range []string{"query-api", "", "x"} {
		if _, ok := contracts.AccessTokenScope(audience); ok {
			t.Errorf("audience %q has a user access token scope", audience)
		}
	}
}
