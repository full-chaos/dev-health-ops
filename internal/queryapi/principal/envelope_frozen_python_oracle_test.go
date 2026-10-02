package principal

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
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
	Package:  "./internal/queryapi/principal/",
	Build:    "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity: "python 3.14.7\nunicodedata 16.0.0",
	// The goldenrecord verb writes each digest when it promotes a recording; a new golden starts as
	// "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"envelope-claims.golden.json": "aeffe76ce5fe14958d41786b760c8991c04384a488f9da8a800c5a8c5164db24",
	},
}

// pythonEnvelopeStructureProgram mints a REAL effective-principal envelope with
// principal_envelope.issue_effective_principal_envelope and exports its REAL JWKS via build_envelope_jwks, as the
// live oracle does, and answers only the STRUCTURE of both: the token header, the claims without the three
// per-run values (iat, exp, jti), the lifetime, whether the id is a random UUID, the attributes of the public key
// without the key and its length. The answer holds no token, no key and no signature: the key is generated in the
// program and never leaves it. The app logs to stdout while it loads: that goes to stderr, so the answer is the
// only stdout.
const pythonEnvelopeStructureProgram = `
import json
import os
import sys
import uuid

answer_stream = sys.stdout
sys.stdout = sys.stderr

import jwt
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat

key = Ed25519PrivateKey.generate()
os.environ["GO_API_ENVELOPE_PRIVATE_KEY"] = key.private_bytes(
    encoding=Encoding.PEM,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption(),
).decode("utf-8")

from dev_health_ops.api.graphql import principal_envelope
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.types import LicenseTier

user = AuthenticatedUser(
    user_id="11111111-1111-4111-8111-111111111111",
    email="dev@example.com",
    org_id="org-1",
    role="admin",
    is_superuser=False,
    is_superuser_verified=False,
    token_version=3,
)
token = principal_envelope.issue_effective_principal_envelope(
    user, tier=LicenseTier.TEAM, licensed_features=["ai_review"],
)
jwks = principal_envelope.build_envelope_jwks()
header = jwt.get_unverified_header(token)
payload = jwt.decode(token, options={"verify_signature": False, "verify_aud": False})
public = dict(jwks["keys"][0])
x = public.pop("x")
answer_stream.write(json.dumps({
    "header": header,
    "claims": {name: value for name, value in payload.items() if name not in ("iat", "exp", "jti")},
    "ttl_seconds": payload["exp"] - payload["iat"],
    "jti_is_random_uuid": str(uuid.UUID(payload["jti"], version=4)) == payload["jti"],
    "jwks_key": public,
    "jwks_key_count": len(jwks["keys"]),
    "jwks_x_length": len(x),
    "issuer": principal_envelope.ENVELOPE_ISSUER,
    "audience": principal_envelope.ENVELOPE_AUDIENCE,
}, sort_keys=True) + "\n")
`

// TestVerifierMatchesFrozenPythonIssuedEnvelopeStructure proves query-api's Go verifier accepts an envelope with
// the claim structure the Python edge's issuer actually produces and a JWKS with the attributes its key export
// actually produces. The frozen answer is the structure of a real issuance (header, claims, lifetime, key
// attributes); Go builds a same-shape envelope with its own key and the clock of the run (the issued and expiry
// times are per run), and the verifier must accept it and read every recorded claim. What the golden cannot pin:
// the signature bytes of the Python issuer (the key is generated per issuance and never recorded); the live
// TestVerifierMatchesLivePythonIssuedEnvelope stays until the Python delete (CHAOS-7308) for that.
func TestVerifierMatchesFrozenPythonIssuedEnvelopeStructure(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve principal package path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	output := []byte(envelopeGoldens.Outputs(t, root, "envelope-claims.golden.json",
		programoracle.Program{Name: "issued envelope structure", Text: pythonEnvelopeStructureProgram})[0])
	var want struct {
		Header          map[string]any `json:"header"`
		Claims          map[string]any `json:"claims"`
		TTLSeconds      int            `json:"ttl_seconds"`
		JTIIsRandomUUID bool           `json:"jti_is_random_uuid"`
		JWKSKey         map[string]any `json:"jwks_key"`
		JWKSKeyCount    int            `json:"jwks_key_count"`
		JWKSXLength     int            `json:"jwks_x_length"`
		Issuer          string         `json:"issuer"`
		Audience        string         `json:"audience"`
	}
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode the frozen envelope structure: %v: %s", err, output)
	}

	// What the issuer's own key export and header promise the verifier.
	if want.Header["alg"] != "EdDSA" || want.Header["typ"] != "JWT" {
		t.Errorf("the issuer's token header = %v, want alg EdDSA, typ JWT", want.Header)
	}
	kid, _ := want.Header["kid"].(string)
	if kid == "" || want.JWKSKey["kid"] != kid {
		t.Fatalf("the token kid %q and the key export kid %v must be the same non-empty id", kid, want.JWKSKey["kid"])
	}
	for name, value := range map[string]string{"kty": "OKP", "crv": "Ed25519", "use": "sig", "alg": "EdDSA"} {
		if want.JWKSKey[name] != value {
			t.Errorf("the key export %s = %v, want %s", name, want.JWKSKey[name], value)
		}
	}
	if want.JWKSKeyCount != 1 || want.JWKSXLength != base64.RawURLEncoding.EncodedLen(ed25519.PublicKeySize) {
		t.Errorf("the key export holds %d keys with x of %d characters, want one key with x of %d", want.JWKSKeyCount, want.JWKSXLength, base64.RawURLEncoding.EncodedLen(ed25519.PublicKeySize))
	}
	if want.TTLSeconds != 60 || !want.JTIIsRandomUUID {
		t.Errorf("the issuer's lifetime = %d seconds, random jti = %v, want 60 and true", want.TTLSeconds, want.JTIIsRandomUUID)
	}

	// Go builds the envelope of that structure: the recorded claims, the clock of this run, its own key.
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
	verifyWith := func(label string, recorded map[string]any) *Claims {
		t.Helper()
		now := time.Now()
		claims := jwt.MapClaims{}
		for name, value := range recorded {
			claims[name] = value
		}
		claims["iat"] = now.Unix()
		claims["exp"] = now.Add(time.Duration(want.TTLSeconds) * time.Second).Unix()
		claims["jti"] = uuid.NewString()
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		for name, value := range want.Header {
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

	// 1. The claims exactly as the issuer recorded them.
	got := verifyWith("recorded claims", want.Claims)
	if got.SchemaVersion != int(want.Claims["v"].(float64)) {
		t.Errorf("SchemaVersion = %d, recorded %v", got.SchemaVersion, want.Claims["v"])
	}
	if got.UserID() != want.Claims["sub"] || got.OrgID != want.Claims["org_id"] || got.Role != want.Claims["role"] || got.Tier != want.Claims["tier"] {
		t.Errorf("identity = (%q, %q, %q, %q), recorded (%v, %v, %v, %v)", got.UserID(), got.OrgID, got.Role, got.Tier,
			want.Claims["sub"], want.Claims["org_id"], want.Claims["role"], want.Claims["tier"])
	}
	if got.IsSuperuser != want.Claims["is_superuser"] || got.IsSuperuserVerified != want.Claims["is_superuser_verified"] ||
		got.ImpersonationActive != want.Claims["impersonation_active"] {
		t.Errorf("flags = (%v, %v, %v), recorded (%v, %v, %v)", got.IsSuperuser, got.IsSuperuserVerified, got.ImpersonationActive,
			want.Claims["is_superuser"], want.Claims["is_superuser_verified"], want.Claims["impersonation_active"])
	}
	if float64(got.TokenVersion) != want.Claims["token_version"] {
		t.Errorf("TokenVersion = %d, recorded %v", got.TokenVersion, want.Claims["token_version"])
	}
	toStrings := func(value any) []string {
		var out []string
		for _, item := range value.([]any) {
			out = append(out, item.(string))
		}
		return out
	}
	if !reflect.DeepEqual(got.Permissions, toStrings(want.Claims["permissions"])) || len(got.Permissions) == 0 {
		t.Errorf("Permissions = %v, recorded %v", got.Permissions, want.Claims["permissions"])
	}
	if !reflect.DeepEqual(got.LicensedFeatures, toStrings(want.Claims["licensed_features"])) {
		t.Errorf("LicensedFeatures = %v, recorded %v", got.LicensedFeatures, want.Claims["licensed_features"])
	}
	if recorded, present := want.Claims["impersonated_by"]; (got.ImpersonatedBy == nil) != (recorded == nil) || !present {
		t.Errorf("ImpersonatedBy = %v, recorded %v (present %v)", got.ImpersonatedBy, recorded, present)
	}

	// 2. The recorded envelope carries zero values for the flags and the nullable claim (false, false, false,
	// null): a verifier that read none of those names would return the same zeros. The same recorded NAMES with a
	// non-zero value each must come back non-zero, so each name is read.
	changed := map[string]any{}
	for name, value := range want.Claims {
		changed[name] = value
	}
	for _, name := range []string{"is_superuser", "is_superuser_verified", "impersonation_active"} {
		if _, present := want.Claims[name]; !present {
			t.Fatalf("the recorded envelope has no %s claim", name)
		}
		changed[name] = true
	}
	const impersonator = "22222222-2222-4222-8222-222222222222"
	if _, present := want.Claims["impersonated_by"]; !present {
		t.Fatal("the recorded envelope has no impersonated_by claim")
	}
	changed["impersonated_by"] = impersonator
	got = verifyWith("non-zero flags", changed)
	if !got.IsSuperuser || !got.IsSuperuserVerified || !got.ImpersonationActive {
		t.Errorf("the verifier returned flags (%v, %v, %v) for an envelope that sets all three", got.IsSuperuser, got.IsSuperuserVerified, got.ImpersonationActive)
	}
	if got.ImpersonatedBy == nil || *got.ImpersonatedBy != impersonator {
		t.Errorf("ImpersonatedBy = %v, want %s", got.ImpersonatedBy, impersonator)
	}
}
