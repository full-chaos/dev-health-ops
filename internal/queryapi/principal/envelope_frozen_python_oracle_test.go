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
		"envelope-claims.golden.json": "66c1422e1f3672fe173fd2e860767e9fb62bf141178f7e18e05e3d55e4f4fd17",
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
from dev_health_ops.api.services import auth
from dev_health_ops.api.services.auth import AuthenticatedUser
from dev_health_ops.licensing.types import LicenseTier


def user(**changes):
    fields = dict(
        user_id="11111111-1111-4111-8111-111111111111",
        email="dev@example.com",
        org_id="org-1",
        role="admin",
        is_superuser=False,
        is_superuser_verified=False,
        token_version=3,
    )
    fields.update(changes)
    return AuthenticatedUser(**fields)


def structure(token):
    header = jwt.get_unverified_header(token)
    payload = jwt.decode(token, options={"verify_signature": False, "verify_aud": False})
    return {
        "header": header,
        "claims": {name: value for name, value in payload.items() if name not in ("iat", "exp", "jti")},
        "ttl_seconds": payload["exp"] - payload["iat"],
        "jti_is_random_uuid": str(uuid.UUID(payload["jti"], version=4)) == payload["jti"],
    }


def issue(account):
    return structure(principal_envelope.issue_effective_principal_envelope(
        account, tier=LicenseTier.TEAM, licensed_features=["ai_review"],
    ))


envelopes = {"plain": issue(user())}
# One envelope per flag, each set alone, from the real issuer.
envelopes["superuser only"] = issue(user(is_superuser=True))
envelopes["superuser verified only"] = issue(user(is_superuser_verified=True))
reset = auth.set_impersonation_context(
    target_user_id="33333333-3333-4333-8333-333333333333",
    target_org_id="org-2",
    target_role="viewer",
    real_user_id="11111111-1111-4111-8111-111111111111",
)
try:
    envelopes["impersonation active"] = issue(user())
finally:
    auth._impersonation_ctx.reset(reset)

jwks = principal_envelope.build_envelope_jwks()
public = dict(jwks["keys"][0])
x = public.pop("x")
answer_stream.write(json.dumps({
    "envelopes": envelopes,
    "jwks_key": public,
    "jwks_key_count": len(jwks["keys"]),
    "jwks_x_length": len(x),
    "issuer": principal_envelope.ENVELOPE_ISSUER,
    "audience": principal_envelope.ENVELOPE_AUDIENCE,
}, sort_keys=True) + "\n")
`

// TestVerifierMatchesFrozenPythonIssuedEnvelopeStructure proves query-api's Go verifier accepts an envelope with
// the claim structure the Python edge's issuer actually produces and a JWKS with the attributes its key export
// actually produces. The frozen answer holds the structure of four real issuances (header, claims, lifetime): a
// plain one, one with is_superuser alone, one with is_superuser_verified alone, one under an active impersonation;
// and the attributes of the real key export. Go builds, for each, an envelope with the recorded claims, its own key
// and the clock of the run (the issued and expiry times are per run); the verifier must accept it and read every
// recorded claim field by field, so two claim names crossed in the verifier are seen. What the golden cannot pin:
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
		if got.IsSuperuser != c["is_superuser"] {
			t.Errorf("%s: IsSuperuser = %v, recorded %v", label, got.IsSuperuser, c["is_superuser"])
		}
		if got.IsSuperuserVerified != c["is_superuser_verified"] {
			t.Errorf("%s: IsSuperuserVerified = %v, recorded %v", label, got.IsSuperuserVerified, c["is_superuser_verified"])
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
