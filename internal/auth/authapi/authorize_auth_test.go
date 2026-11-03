package authapi

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

type authorizeFixture struct {
	t       *testing.T
	signer  *token.Signer
	keys    *issueCountingKeys
	subject string
	now     time.Time
}

func newAuthorizeFixture(t *testing.T) *authorizeFixture {
	t.Helper()
	signer, _, _ := fileSigner(t)
	return &authorizeFixture{
		t: t, signer: signer, keys: &issueCountingKeys{inner: signer},
		subject: session.WirePrincipalID(uuid.New()), now: time.Now(),
	}
}

func (f *authorizeFixture) callers(subjects ...string) *AuthorizeCallers {
	f.t.Helper()
	if len(subjects) == 0 {
		subjects = []string{f.subject}
	}
	gate, err := NewAuthorizeCallers(AuthorizeCallersConfig{IssuerURL: goldenIssuer, Keys: f.keys, Subjects: subjects, Now: func() time.Time { return f.now }})
	if err != nil {
		f.t.Fatal(err)
	}
	return gate
}

func (f *authorizeFixture) references() *PrincipalReferences {
	f.t.Helper()
	resolver, err := NewPrincipalReferences(PrincipalReferencesConfig{IssuerURL: goldenIssuer, Keys: f.keys, Now: func() time.Time { return f.now }})
	if err != nil {
		f.t.Fatal(err)
	}
	return resolver
}

func (f *authorizeFixture) sign(typ string, claims any) string {
	f.t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		f.t.Fatal(err)
	}
	compact, _, err := f.signer.Sign(context.Background(), typ, payload)
	if err != nil {
		f.t.Fatal(err)
	}
	return compact
}

func (f *authorizeFixture) workloadToken(mutate func(*contracts.WorkloadTokenClaims)) string {
	now := f.now.Unix()
	claims := contracts.WorkloadTokenClaims{
		Iss: goldenIssuer, Aud: "auth-service", Sub: f.subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now, Nbf: now, Exp: now + 300,
	}
	if mutate != nil {
		mutate(&claims)
	}
	return f.sign(contracts.WorkloadTokenTyp, claims)
}

func (f *authorizeFixture) userToken(principal uuid.UUID, mutate func(*contracts.AccessTokenClaims)) string {
	now := f.now.Unix()
	org := uuid.NewString()
	claims := contracts.AccessTokenClaims{
		Iss: goldenIssuer, Aud: contracts.PlatformAPIAudience, Sub: session.WirePrincipalID(principal), PrincipalType: "human",
		Sid: session.WireSessionID(uuid.New()), Jti: uuid.NewString(), Iat: now, Nbf: now, Exp: now + 600,
		Scope: "api:access", OrgID: &org,
	}
	if mutate != nil {
		mutate(&claims)
	}
	return f.sign(accessTokenTyp, claims)
}

// unsignedJWT builds a compact token from header and payload text with a junk
// signature: enough for every check that runs before the key set is read.
func unsignedJWT(header, payload string) string {
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(header)) + "." + enc.EncodeToString([]byte(payload)) + "." + enc.EncodeToString(make([]byte, 64))
}

func TestAuthorizeCallerAcceptsAListedWorkloadToken(t *testing.T) {
	f := newAuthorizeFixture(t)
	var jti string
	bearer := f.workloadToken(func(c *contracts.WorkloadTokenClaims) { jti = c.Jti })
	caller, reason, err := f.callers().Authenticate(bearer)
	if err != nil || reason != "" {
		t.Fatalf("Authenticate = %q, %v; want accepted", reason, err)
	}
	if caller.Subject != f.subject || caller.TokenID != jti || session.WirePrincipalID(caller.Principal) != f.subject {
		t.Fatalf("caller = %+v, want subject %s and jti %s", caller, f.subject, jti)
	}
}

func TestAuthorizeCallerRefusesAWrongAudienceBeforeTheKeySet(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	for _, aud := range []string{"dev-health-api", "query-api", "auth-service ", "Auth-Service", ""} {
		bearer := f.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Aud = aud })
		f.keys.reads = 0
		if _, reason, err := gate.Authenticate(bearer); reason != "audience" || err != nil {
			t.Errorf("aud %q: Authenticate = %q, %v; want audience", aud, reason, err)
		}
		if f.keys.reads != 0 {
			t.Errorf("aud %q: %d key-set reads before the audience refusal, want 0", aud, f.keys.reads)
		}
	}
}

func TestAuthorizeCallerRefusesAnUnlistedSubjectBeforeTheKeySet(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	other := session.WirePrincipalID(uuid.New())
	bearer := f.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Sub = other })
	if _, reason, err := gate.Authenticate(bearer); reason != "subject" || err != nil {
		t.Fatalf("Authenticate = %q, %v; want subject", reason, err)
	}
	if f.keys.reads != 0 {
		t.Fatalf("%d key-set reads before the subject refusal, want 0", f.keys.reads)
	}
}

func TestAuthorizeCallerRefusesEveryOtherCredentialClassFromItsText(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	userBearer := f.userToken(uuid.New(), nil)
	hs256 := unsignedJWT(`{"alg":"HS256","typ":"JWT"}`, `{"sub":"`+f.subject+`","aud":"auth-service"}`)
	untyped := unsignedJWT(`{"alg":"EdDSA","kid":"k1"}`, `{"sub":"`+f.subject+`","aud":"auth-service"}`)
	for name, bearer := range map[string]string{
		"authsess compat": "svc_authsess_" + strings.Repeat("a", 40),
		"service":         "svc_acr_" + strings.Repeat("a", 40),
		"acr client":      "fcacr_" + strings.Repeat("a", 40),
		"push":            "fcpush_" + strings.Repeat("a", 40),
		"bootstrap":       "fcwlb_" + strings.Repeat("a", 40),
		"refresh":         "dhr1_" + strings.Repeat("a", 40),
		"empty":           "",
		"oversized":       strings.Repeat("a", maxIssueBearerBytes+1),
		"user token":      userBearer,
		"hs256":           hs256,
		"no typ":          untyped,
		"two segments":    "eyJhbGciOiJFZERTQSJ9.e30",
	} {
		f.keys.reads = 0
		caller, reason, err := gate.Authenticate(bearer)
		if reason == "" || err != nil || caller != (AuthorizeCaller{}) {
			t.Errorf("%s: Authenticate = %+v, %q, %v; want a refusal", name, caller, reason, err)
		}
		if f.keys.reads != 0 {
			t.Errorf("%s: %d key-set reads, want 0", name, f.keys.reads)
		}
	}
}

func TestAuthorizeCallerRefusesACaseVariantClaimBeforeTheKeySet(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	for _, payload := range []string{
		`{"aud":"auth-service","Aud":"auth-service","sub":"` + f.subject + `"}`,
		`{"aud":"auth-service","sub":"` + f.subject + `","SUB":"` + f.subject + `"}`,
		`{"aud":"auth-service","aud":"auth-service","sub":"` + f.subject + `"}`,
		`{"aud":["auth-service"],"sub":"` + f.subject + `"}`,
	} {
		f.keys.reads = 0
		bearer := unsignedJWT(`{"alg":"EdDSA","typ":"workload+jwt","kid":"k1"}`, payload)
		if _, reason, err := gate.Authenticate(bearer); reason != "payload_not_strict" || err != nil {
			t.Errorf("payload %s: Authenticate = %q, %v; want payload_not_strict", payload, reason, err)
		}
		if f.keys.reads != 0 {
			t.Errorf("payload %s: %d key-set reads, want 0", payload, f.keys.reads)
		}
	}
}

func TestAuthorizeCallerVerifiesTheSignatureAndTimes(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	expired := f.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Iat -= 600; c.Nbf -= 600; c.Exp -= 600 })
	if _, reason, _ := gate.Authenticate(expired); reason != "expired" {
		t.Errorf("expired: reason %q, want expired", reason)
	}
	wrongIss := f.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Iss = "https://other.example.test" })
	if _, reason, _ := gate.Authenticate(wrongIss); reason != "issuer" {
		t.Errorf("wrong iss: reason %q, want issuer", reason)
	}
	good := f.workloadToken(nil)
	parts := strings.Split(good, ".")
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	sig[0] ^= 1
	tampered := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(sig)
	if _, reason, _ := gate.Authenticate(tampered); reason != "signature" {
		t.Errorf("tampered: reason %q, want signature", reason)
	}
}

func TestAuthorizeCallerAnswersUnavailableWhenTheKeySetFails(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	bearer := f.workloadToken(nil)
	f.keys.err = errors.New("keystore down")
	_, reason, err := gate.Authenticate(bearer)
	if reason != "unavailable" || err == nil {
		t.Fatalf("Authenticate = %q, %v; want unavailable with an error", reason, err)
	}
}

func TestAuthorizeCallerNilOrMisconfiguredRefusesEverything(t *testing.T) {
	f := newAuthorizeFixture(t)
	var gate *AuthorizeCallers
	if _, reason, err := gate.Authenticate(f.workloadToken(nil)); reason != "not_configured" || err != nil {
		t.Fatalf("nil gate = %q, %v; want not_configured", reason, err)
	}
	for name, cfg := range map[string]AuthorizeCallersConfig{
		"no keys":     {IssuerURL: goldenIssuer, Subjects: []string{f.subject}},
		"no subjects": {IssuerURL: goldenIssuer, Keys: f.keys},
		"not a prn":   {IssuerURL: goldenIssuer, Keys: f.keys, Subjects: []string{"api"}},
		"nil prn":     {IssuerURL: goldenIssuer, Keys: f.keys, Subjects: []string{session.WirePrincipalID(uuid.Nil)}},
		"repeated":    {IssuerURL: goldenIssuer, Keys: f.keys, Subjects: []string{f.subject, f.subject}},
		"no issuer":   {Keys: f.keys, Subjects: []string{f.subject}},
	} {
		if _, err := NewAuthorizeCallers(cfg); err == nil {
			t.Errorf("%s: NewAuthorizeCallers accepted the config", name)
		} else if strings.Contains(err.Error(), f.subject) {
			t.Errorf("%s: error carries the subject: %v", name, err)
		}
	}
}

func TestPrincipalReferenceResolvesThePlatformAPIUserToken(t *testing.T) {
	f := newAuthorizeFixture(t)
	principal := uuid.New()
	var sid, jti string
	reference := f.userToken(principal, func(c *contracts.AccessTokenClaims) { sid, jti = c.Sid, c.Jti })
	got, reason, err := f.references().Resolve(reference)
	if err != nil || reason != "" {
		t.Fatalf("Resolve = %q, %v; want resolved", reason, err)
	}
	if got.ID != principal || got.SessionID != sid || got.TokenID != jti {
		t.Fatalf("Resolve = %+v, want principal %s sid %s jti %s", got, principal, sid, jti)
	}
}

func TestPrincipalReferenceRefusesTheSessionRouteAudienceBeforeTheKeySet(t *testing.T) {
	f := newAuthorizeFixture(t)
	reference := f.userToken(uuid.New(), func(c *contracts.AccessTokenClaims) {
		c.Aud, c.Scope = contracts.SessionRouteAudience, "auth:session"
	})
	if _, reason, err := f.references().Resolve(reference); reason != "audience" || err != nil {
		t.Fatalf("Resolve = %q, %v; want audience", reason, err)
	}
	if f.keys.reads != 0 {
		t.Fatalf("%d key-set reads before the audience refusal, want 0", f.keys.reads)
	}
}

func TestPrincipalReferenceRefusesEveryOtherTokenClassBeforeTheKeySet(t *testing.T) {
	f := newAuthorizeFixture(t)
	resolver := f.references()
	userPayload := `{"aud":"dev-health-api","sub":"` + session.WirePrincipalID(uuid.New()) + `"}`
	for name, c := range map[string]struct{ reference, reason string }{
		"workload token":  {f.workloadToken(func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" }), "credential_class"},
		"hs256 legacy":    {unsignedJWT(`{"alg":"HS256","typ":"JWT"}`, userPayload), "credential_class"},
		"hs256 at+jwt":    {unsignedJWT(`{"alg":"HS256","typ":"at+jwt"}`, userPayload), "credential_class"},
		"eddsa no typ":    {unsignedJWT(`{"alg":"EdDSA","kid":"k1"}`, userPayload), "credential_class"},
		"alg none":        {unsignedJWT(`{"alg":"none","typ":"at+jwt"}`, userPayload), "credential_class"},
		"space":           {"abcdefghijklmnop qrstuvwxyz", "malformed"},
		"too short":       {"abc.def.ghi", "malformed"},
		"too long":        {strings.Repeat("a", 1001), "malformed"},
		"refresh":         {"dhr1_" + strings.Repeat("a", 43), "contract"},
		"opaque prf":      {"prf_EXAMPLE0000000000000001", "contract"},
		"case aud member": {unsignedJWT(`{"alg":"EdDSA","typ":"at+jwt","kid":"k1"}`, `{"aud":"dev-health-api","AUD":"x","sub":"prn_x"}`), "payload_not_strict"},
	} {
		f.keys.reads = 0
		got, reason, err := resolver.Resolve(c.reference)
		if reason != c.reason || err != nil || got != (ReferencedPrincipal{}) {
			t.Errorf("%s: Resolve = %+v, %q, %v; want %s", name, got, reason, err, c.reason)
		}
		if f.keys.reads != 0 {
			t.Errorf("%s: %d key-set reads, want 0", name, f.keys.reads)
		}
	}
}

func TestPrincipalReferenceVerifiesTheSignatureAndTimes(t *testing.T) {
	f := newAuthorizeFixture(t)
	resolver := f.references()
	expired := f.userToken(uuid.New(), func(c *contracts.AccessTokenClaims) { c.Iat -= 1200; c.Nbf -= 1200; c.Exp -= 1200 })
	if _, reason, _ := resolver.Resolve(expired); reason != "expired" {
		t.Errorf("expired: reason %q, want expired", reason)
	}
	wrongIss := f.userToken(uuid.New(), func(c *contracts.AccessTokenClaims) { c.Iss = "https://other.example.test" })
	if _, reason, _ := resolver.Resolve(wrongIss); reason != "issuer" {
		t.Errorf("wrong iss: reason %q, want issuer", reason)
	}
	f.keys.err = errors.New("keystore down")
	if _, reason, err := resolver.Resolve(f.userToken(uuid.New(), nil)); reason != "unavailable" || err == nil {
		t.Errorf("key outage: Resolve = %q, %v; want unavailable with an error", reason, err)
	}
	var nilResolver *PrincipalReferences
	if _, reason, _ := nilResolver.Resolve(f.userToken(uuid.New(), nil)); reason != "not_configured" {
		t.Errorf("nil resolver: reason %q, want not_configured", reason)
	}
	if _, err := NewPrincipalReferences(PrincipalReferencesConfig{IssuerURL: goldenIssuer}); err == nil {
		t.Error("NewPrincipalReferences accepted a config with no key set")
	}
}

// goldenKeys is a key set built from a golden corpus's public JWKS.
type goldenKeys struct{ set token.KeySet }

func (g goldenKeys) KeySet(context.Context) (token.KeySet, error) { return g.set, nil }

type goldenCorpus struct {
	Issuer  string `json:"issuer"`
	Vectors []struct {
		File     string `json:"file"`
		Now      int64  `json:"now"`
		Audience string `json:"verify_as_audience"`
		Expect   string `json:"expect"`
	} `json:"vectors"`
}

type goldenVector struct {
	Header    string `json:"header_json"`
	Payload   string `json:"payload_json"`
	Signature string `json:"signature_hex"`
}

func loadGolden(t *testing.T, name string) (string, goldenCorpus, goldenKeys) {
	t.Helper()
	dir := filepath.Join(contracts.ContractsDir(repoRoot(t)), "golden", name)
	var corpus goldenCorpus
	readJSON(t, filepath.Join(dir, "manifest.json"), &corpus)
	var jwks struct {
		Keys []struct {
			Kid string `json:"kid"`
			X   string `json:"x"`
		} `json:"keys"`
	}
	readJSON(t, filepath.Join(dir, "jwks.json"), &jwks)
	var keys []token.Key
	for i, k := range jwks.Keys {
		pub, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			t.Fatal(err)
		}
		state := token.StatePending
		if i == 0 {
			state = token.StateActive
		}
		keys = append(keys, token.Key{KeyID: k.Kid, State: state, Algorithm: token.Algorithm, PublicKey: ed25519.PublicKey(pub)})
	}
	set, err := token.NewKeySet(keys)
	if err != nil {
		t.Fatal(err)
	}
	return dir, corpus, goldenKeys{set: set}
}

func readJSON(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func goldenCompact(t *testing.T, path string) string {
	t.Helper()
	var v goldenVector
	readJSON(t, path, &v)
	sig, err := hex.DecodeString(v.Signature)
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(v.Header)) + "." + enc.EncodeToString([]byte(v.Payload)) + "." + enc.EncodeToString(sig)
}

// The golden subjects are the nil principal, which no real principal is: an
// accepted vector therefore ends at the subject check, after every other one.
func TestPrincipalReferenceDecidesTheAccessTokenGoldenCorpus(t *testing.T) {
	dir, corpus, keys := loadGolden(t, "access-token")
	for _, v := range corpus.Vectors {
		now := time.Unix(v.Now, 0)
		resolver, err := NewPrincipalReferences(PrincipalReferencesConfig{IssuerURL: corpus.Issuer, Keys: keys, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		_, reason, err := resolver.Resolve(goldenCompact(t, filepath.Join(dir, "vectors", v.File)))
		if err != nil {
			t.Errorf("%s: Resolve error %v", v.File, err)
			continue
		}
		switch {
		case v.Expect == "accept" && v.Audience == contracts.PlatformAPIAudience:
			if reason != "subject" {
				t.Errorf("%s: accepted vector refused as %q, want only the nil-subject refusal", v.File, reason)
			}
		case v.Expect == "accept":
			if reason != "audience" {
				t.Errorf("%s: %s vector refused as %q, want audience", v.File, v.Audience, reason)
			}
		case reason == "":
			t.Errorf("%s: rejected vector resolved", v.File)
		}
	}
}

func TestAuthorizeCallerDecidesTheWorkloadTokenGoldenCorpus(t *testing.T) {
	dir, corpus, keys := loadGolden(t, "workload-token")
	listed := session.WirePrincipalID(uuid.New())
	for _, v := range corpus.Vectors {
		now := time.Unix(v.Now, 0)
		gate, err := NewAuthorizeCallers(AuthorizeCallersConfig{IssuerURL: corpus.Issuer, Keys: keys, Subjects: []string{listed}, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		_, reason, err := gate.Authenticate(goldenCompact(t, filepath.Join(dir, "vectors", v.File)))
		if err != nil {
			t.Errorf("%s: Authenticate error %v", v.File, err)
			continue
		}
		switch {
		case v.Expect == "accept" && v.Audience == authorizeCallerAudience:
			if reason != "subject" {
				t.Errorf("%s: accepted vector refused as %q, want only the unlisted-subject refusal", v.File, reason)
			}
		case v.Expect == "accept":
			if reason != "audience" {
				t.Errorf("%s: %s vector refused as %q, want audience", v.File, v.Audience, reason)
			}
		case reason == "":
			t.Errorf("%s: rejected vector accepted", v.File)
		}
	}
}
