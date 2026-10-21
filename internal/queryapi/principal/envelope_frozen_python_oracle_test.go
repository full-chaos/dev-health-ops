package principal

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// envelopeGoldens is the set of this package's frozen Python answers: the structure of the envelope the real
// issuer mints. The producer is dev_health_ops.api.graphql.principal_envelope over PyJWT and cryptography. A golden
// recorded by another producer is refused.
var envelopeGoldens = programoracle.Set{
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	Pins: map[string]string{
		"envelope-claims.golden.json": "66c1422e1f3672fe173fd2e860767e9fb62bf141178f7e18e05e3d55e4f4fd17",
	},
}

// TestVerifierMatchesFrozenPythonIssuedEnvelopeStructure proves query-api's Go verifier accepts an envelope with
// the claim structure the Python edge's issuer actually produces and a JWKS with the attributes its key export
// actually produces. The frozen answer holds the structure of four real issuances (header, claims, lifetime): a
// plain one, one with is_superuser alone, one with is_superuser_verified alone, one under an active impersonation;
// and the attributes of the real key export. Go builds, for each, an envelope with the recorded claims, its own key
// and the clock of the run (the issued and expiry times are per run); the verifier must accept it and read every
// recorded claim field by field, so two claim names crossed in the verifier are seen. What the golden cannot pin:
// the signature bytes of the Python issuer (the key is generated per issuance and never recorded).
func TestVerifierMatchesFrozenPythonIssuedEnvelopeStructure(t *testing.T) {
	output := []byte(envelopeGoldens.Outputs(t, "envelope-claims.golden.json",
		programoracle.Program{Name: "issued envelope structure"})[0])
	type issued struct {
		Header          map[string]any `json:"header"`
		Claims          map[string]any `json:"claims"`
		TTLSeconds      int            `json:"ttl_seconds"`
		JTIIsRandomUUID bool           `json:"jti_is_random_uuid"`
	}
	var want struct {
		Envelopes    map[string]issued `json:"envelopes"`
		JWKSKey      map[string]any    `json:"jwks_key"`
		JWKSKeyCount int               `json:"jwks_key_count"`
		JWKSXLength  int               `json:"jwks_x_length"`
		Issuer       string            `json:"issuer"`
		Audience     string            `json:"audience"`
	}
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode the frozen envelope structure: %v: %s", err, output)
	}
	for _, name := range []string{"plain", "superuser only", "superuser verified only", "impersonation active"} {
		if _, found := want.Envelopes[name]; !found {
			t.Fatalf("the golden holds no %q envelope", name)
		}
	}

	// What the issuer's own key export promises the verifier.
	kid, _ := want.JWKSKey["kid"].(string)
	for name, value := range map[string]string{"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA"} {
		if want.JWKSKey[name] != value {
			t.Errorf("the key export %s = %v, want %s", name, want.JWKSKey[name], value)
		}
	}
	if want.JWKSKeyCount != 1 || want.JWKSXLength != base64.RawURLEncoding.EncodedLen(ed25519.PublicKeySize) {
		t.Errorf("the key export holds %d keys with x of %d characters, want one key with x of %d", want.JWKSKeyCount, want.JWKSXLength, base64.RawURLEncoding.EncodedLen(ed25519.PublicKeySize))
	}

	// Go's side: its own key, the recorded key attributes.
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	jwk := map[string]any{}
	for name, value := range want.JWKSKey {
		jwk[name] = value
	}
	jwk["x"] = base64.RawURLEncoding.EncodeToString(pub)
	jwks, err := json.Marshal(map[string]any{"keys": []map[string]any{jwk}})
	if err != nil {
		t.Fatal(err)
	}
	jwksPath := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(jwksPath, jwks, 0o600); err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(jwksPath, want.Issuer, want.Audience)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	verifyWith := func(label string, header map[string]any, ttlSeconds int, recorded map[string]any) *Claims {
		t.Helper()
		now := time.Now()
		claims := jwt.MapClaims{}
		for name, value := range recorded {
			claims[name] = value
		}
		claims["iat"] = now.Unix()
		claims["exp"] = now.Add(time.Duration(ttlSeconds) * time.Second).Unix()
		claims["jti"] = uuid.NewString()
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		for name, value := range header {
			if name != "alg" && name != "typ" {
				token.Header[name] = value
			}
		}
		signed, err := token.SignedString(priv)
		if err != nil {
			t.Fatal(err)
		}
		got, err := verifier.Verify(context.Background(), signed)
		if err != nil {
			t.Fatalf("%s: Verify rejected an envelope of the structure the Python issuer produces: %v", label, err)
		}
		return got
	}
	toStrings := func(value any) []string {
		var out []string
		for _, item := range value.([]any) {
			out = append(out, item.(string))
		}
		return out
	}

	for label, envelope := range want.Envelopes {
		// What the issuer's own header and lifetime promise the verifier.
		if envelope.Header["alg"] != "EdDSA" || envelope.Header["typ"] != "JWT" || envelope.Header["kid"] != kid || kid == "" {
			t.Errorf("%s: the issuer's token header = %v, want alg EdDSA, typ JWT and the key export's kid %q", label, envelope.Header, kid)
		}
		if envelope.TTLSeconds != 60 || !envelope.JTIIsRandomUUID {
			t.Errorf("%s: the issuer's lifetime = %d seconds, random jti = %v, want 60 and true", label, envelope.TTLSeconds, envelope.JTIIsRandomUUID)
		}
		got := verifyWith(label, envelope.Header, envelope.TTLSeconds, envelope.Claims)
		c := envelope.Claims
		if got.SchemaVersion != int(c["v"].(float64)) {
			t.Errorf("%s: SchemaVersion = %d, recorded %v", label, got.SchemaVersion, c["v"])
		}
		if got.UserID() != c["sub"] || got.OrgID != c["org_id"] || got.Role != c["role"] || got.Tier != c["tier"] {
			t.Errorf("%s: identity = (%q, %q, %q, %q), recorded (%v, %v, %v, %v)", label, got.UserID(), got.OrgID, got.Role, got.Tier, c["sub"], c["org_id"], c["role"], c["tier"])
		}
		// The recorded claims are what the issuer wrote; the verifier then applies
		// the superuser discipline (CHAOS-4738): superuser only with both flags
		// and no impersonation, a verified flag only where nothing contradicts it.
		recSU, _ := c["is_superuser"].(bool)
		recVer, _ := c["is_superuser_verified"].(bool)
		recImp, _ := c["impersonation_active"].(bool)
		if by, ok := c["impersonated_by"].(string); ok && by != "" {
			recImp = true
		}
		wantSU := recSU && recVer && !recImp
		wantVer := recVer && (wantSU || (!recSU && !recImp))
		if got.IsSuperuser != wantSU {
			t.Errorf("%s: IsSuperuser = %v, want %v (recorded %v)", label, got.IsSuperuser, wantSU, c["is_superuser"])
		}
		if got.IsSuperuserVerified != wantVer {
			t.Errorf("%s: IsSuperuserVerified = %v, want %v (recorded %v)", label, got.IsSuperuserVerified, wantVer, c["is_superuser_verified"])
		}
		if got.ImpersonationActive != c["impersonation_active"] {
			t.Errorf("%s: ImpersonationActive = %v, recorded %v", label, got.ImpersonationActive, c["impersonation_active"])
		}
		if float64(got.TokenVersion) != c["token_version"] {
			t.Errorf("%s: TokenVersion = %d, recorded %v", label, got.TokenVersion, c["token_version"])
		}
		if !reflect.DeepEqual(got.Permissions, toStrings(c["permissions"])) || len(got.Permissions) == 0 {
			t.Errorf("%s: Permissions = %v, recorded %v", label, got.Permissions, c["permissions"])
		}
		if !reflect.DeepEqual(got.LicensedFeatures, toStrings(c["licensed_features"])) {
			t.Errorf("%s: LicensedFeatures = %v, recorded %v", label, got.LicensedFeatures, c["licensed_features"])
		}
		recordedBy, present := c["impersonated_by"]
		if !present || (got.ImpersonatedBy == nil) != (recordedBy == nil) || (got.ImpersonatedBy != nil && *got.ImpersonatedBy != recordedBy) {
			t.Errorf("%s: ImpersonatedBy = %v, recorded %v (present %v)", label, got.ImpersonatedBy, recordedBy, present)
		}
	}

	// The issuer never sets impersonation_active without impersonated_by, so the recorded envelopes cannot tell a
	// verifier that derives one from the other. This envelope is TEST-SET (not recorded): the recorded plain claims
	// with only impersonation_active true and impersonated_by null; the verifier must read the flag as it is and
	// the other two flags as false.
	plain := want.Envelopes["plain"]
	testSet := map[string]any{}
	for name, value := range plain.Claims {
		testSet[name] = value
	}
	testSet["impersonation_active"] = true
	testSet["impersonated_by"] = nil
	got := verifyWith("test-set: impersonation_active alone", plain.Header, plain.TTLSeconds, testSet)
	if got.IsSuperuser || got.IsSuperuserVerified || !got.ImpersonationActive || got.ImpersonatedBy != nil {
		t.Errorf("test-set envelope with only impersonation_active: flags (%v, %v, %v), impersonated_by %v; want (false, false, true) and nil",
			got.IsSuperuser, got.IsSuperuserVerified, got.ImpersonationActive, got.ImpersonatedBy)
	}

	// The three flags are recorded one at a time: each recorded envelope must set exactly its own flag.
	flagsOf := func(label string) [3]bool {
		c := want.Envelopes[label].Claims
		return [3]bool{c["is_superuser"].(bool), c["is_superuser_verified"].(bool), c["impersonation_active"].(bool)}
	}
	for label, flags := range map[string][3]bool{
		"plain":                   {false, false, false},
		"superuser only":          {true, false, false},
		"superuser verified only": {false, true, false},
		"impersonation active":    {false, false, true},
	} {
		if flagsOf(label) != flags {
			t.Errorf("the recorded %q envelope has the flags %v, want %v", label, flagsOf(label), flags)
		}
	}
	if by, _ := want.Envelopes["impersonation active"].Claims["impersonated_by"].(string); by == "" {
		t.Error("the recorded impersonation envelope names no impersonator")
	}
}
