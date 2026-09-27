// Package processlicense is licensing/gating.py's LicenseManager.initialize
// and licensing/validator.py's LicenseValidator for the Go api: it verifies
// the process-wide (self-hosted install) license once at start-up and
// installs the result with licensing.SetProcessLicense.
//
// It lives apart from package licensing because it validates the payload
// with pybody's pydantic-lax helpers, and pybody links the HTTP layer the
// worker binaries that import licensing do not link.
package processlicense

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Reason names why a license is not in force. It is logged; it never
// carries the key, the license or any part of either.
type Reason string

const (
	ReasonInvalidPublicKey Reason = "invalid_public_key"
	ReasonFormat           Reason = "invalid_format"
	ReasonBase64           Reason = "invalid_base64"
	ReasonSignature        Reason = "invalid_signature"
	ReasonEncoding         Reason = "invalid_utf8"
	ReasonJSON             Reason = "invalid_json"
	ReasonSchema           Reason = "invalid_payload_schema"
	ReasonExpired          Reason = "expired_past_grace"
)

// Verifier is LicenseValidator(public_key_base64): the Ed25519 public key.
type Verifier struct{ key ed25519.PublicKey }

// NewVerifier is LicenseValidator.__init__: base64.b64decode (lenient) then
// nacl VerifyKey, which takes exactly 32 bytes. Any failure is
// ReasonInvalidPublicKey (Python raises LicenseValidationError, which the
// api lifespan swallows into the community tier).
func NewVerifier(publicKeyB64 string) (*Verifier, error) {
	raw, err := licensing.B64Decode(publicKeyB64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New(string(ReasonInvalidPublicKey))
	}
	return &Verifier{key: ed25519.PublicKey(raw)}, nil
}

// Result is ValidationResult: License is the payload in force (nil when
// the license is refused), Reason why it is refused.
type Result struct {
	License *licensing.ProcessLicense
	Reason  Reason
}

// Validate is LicenseValidator.validate(license_str, current_time=now):
// "<base64 payload>.<base64 signature>", the Ed25519 signature checked as
// libsodium checks it, the payload validated as pydantic validates
// LicensePayload, then the expiry: in force up to exp, in the grace period
// up to exp + grace_days days, refused after.
//
// Python raises (rather than returns invalid) for a signature that is not
// 64 bytes and for a payload that is not UTF-8; the lifespan swallows both
// into the community tier, which is what a refusal here means too.
func (v *Verifier) Validate(license string, now int64) Result {
	parts := strings.Split(pythonparity.Strip(license), ".")
	if len(parts) != 2 {
		return Result{Reason: ReasonFormat}
	}
	payload, err := licensing.B64Decode(parts[0])
	if err != nil {
		return Result{Reason: ReasonBase64}
	}
	signature, err := licensing.B64Decode(parts[1])
	if err != nil {
		return Result{Reason: ReasonBase64}
	}
	if !libsodiumVerify(v.key, payload, signature) {
		return Result{Reason: ReasonSignature}
	}
	// payload_bytes.decode("utf-8") then json.loads(str): strict UTF-8,
	// and no encoding detection or BOM skipping (pyjson.Decode is
	// json.loads(bytes), which does both: a UTF-16 document is valid UTF-8
	// full of NULs that Python refuses and json.loads(bytes) would accept).
	if !utf8.Valid(payload) {
		return Result{Reason: ReasonEncoding}
	}
	decoded, err := pyjson.DecodeString(string(payload))
	if err != nil {
		return Result{Reason: ReasonJSON}
	}
	parsed, ok := validatePayload(decoded)
	if !ok {
		return Result{Reason: ReasonSchema}
	}
	current := big.NewInt(now)
	if current.Cmp(parsed.exp) <= 0 {
		return Result{License: parsed.license(false)}
	}
	graceEnd := new(big.Int).Mul(parsed.graceDays, big.NewInt(24*60*60))
	graceEnd.Add(graceEnd, parsed.exp)
	if current.Cmp(graceEnd) <= 0 {
		return Result{License: parsed.license(true)}
	}
	return Result{Reason: ReasonExpired}
}

// smallOrder is libsodium's ge25519_has_small_order blocklist
// (ed25519_ref10.c): the encodings of the points of order 1, 2, 4 and 8,
// with the non-canonical encodings of y = 0 and y = 1, compared with the
// sign bit of the last byte masked off. crypto_sign_verify_detached refuses
// a public key or a signature R on this list; Go's crypto/ed25519 does not,
// and with a small-order public key it accepts forged signatures
// (TestValidateRefusesSmallOrderPoints).
var smallOrder = func() [][32]byte {
	p := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(19))
	order8, _ := new(big.Int).SetString("2707385501144840649318225287225658788936804267575313519463743609750303402022", 10)
	values := []*big.Int{
		big.NewInt(0),                      // order 4
		big.NewInt(1),                      // order 1
		order8,                             // order 8
		new(big.Int).Sub(p, order8),        // order 8
		new(big.Int).Sub(p, big.NewInt(1)), // p-1, order 2
		new(big.Int).Set(p),                // p (= 0), order 4
		new(big.Int).Add(p, big.NewInt(1)), // p+1 (= 1), order 1
	}
	out := make([][32]byte, len(values))
	for index, value := range values {
		be := value.FillBytes(make([]byte, 32))
		for i := range be {
			out[index][i] = be[31-i]
		}
	}
	return out
}()

func hasSmallOrder(point []byte) bool {
	for _, blocked := range smallOrder {
		if bytes.Equal(point[:31], blocked[:31]) && point[31]&0x7f == blocked[31] {
			return true
		}
	}
	return false
}

// libsodiumVerify is nacl VerifyKey.verify(message, signature): libsodium's
// crypto_sign_verify_detached. Beyond Go's ed25519.Verify (which already
// refuses a non-canonical S and compares R byte for byte) libsodium refuses
// a small-order R and a small-order public key. libsodium also refuses a
// non-canonical public key encoding (y >= p); that check is not ported:
// no signer can produce a signature that verifies under such an encoding
// (it needs the discrete log of the point), so the two answers cannot
// differ on any license that exists.
func libsodiumVerify(key ed25519.PublicKey, message, signature []byte) bool {
	if len(signature) != ed25519.SignatureSize {
		return false
	}
	if hasSmallOrder(signature[:32]) || hasSmallOrder(key) {
		return false
	}
	return ed25519.Verify(key, message, signature)
}

// payloadFields is LicensePayload after validation, the fields Go reads.
type payloadFields struct {
	exp, graceDays *big.Int
	tier           string
	features       map[string]bool
}

func (p payloadFields) license(grace bool) *licensing.ProcessLicense {
	return &licensing.ProcessLicense{Tier: p.tier, Features: p.features, InGracePeriod: grace}
}

// validatePayload is LicensePayload.model_validate(json.loads(payload)) in
// pydantic's lax python mode: a dict; iss the literal "fullchaos.studio";
// sub a str; iat and exp ints; tier a LicenseTier value; features a dict of
// str to bool; limits a LicenseLimits dict (users, repos, api_rate ints,
// backfill_days int or None, default None); grace_days an int >= 0;
// org_name, contact_email and license_id str or None (default None). Keys
// the model does not name are ignored. Any failure refuses the whole
// payload, as the ValidationError does.
func validatePayload(value pyjson.Value) (payloadFields, bool) {
	object, ok := value.(*pyjson.Object)
	if !ok {
		return payloadFields{}, false
	}
	var out payloadFields
	valid := true
	get := func(name string) (pyjson.Value, bool) { return object.Get(name) }
	requireInt := func(name string) *big.Int {
		raw, present := get(name)
		if !present {
			valid = false
			return nil
		}
		number, errType, _ := pybody.PydanticInt(raw)
		if errType != "" {
			valid = false
			return nil
		}
		return number
	}
	requireStr := func(raw pyjson.Value, present bool) (string, bool) {
		if !present {
			return "", false
		}
		text, isString := raw.(string)
		return text, isString
	}
	optionalStr := func(name string) {
		raw, present := get(name)
		if !present || raw == nil {
			return
		}
		if _, isString := raw.(string); !isString {
			valid = false
		}
	}

	if iss, ok := requireStr(get("iss")); !ok || iss != "fullchaos.studio" {
		valid = false
	}
	if _, ok := requireStr(get("sub")); !ok {
		valid = false
	}
	requireInt("iat")
	out.exp = requireInt("exp")
	if tier, ok := requireStr(get("tier")); ok {
		if _, member := licensing.TierRank(tier); member {
			out.tier = tier
		} else {
			valid = false
		}
	} else {
		valid = false
	}
	out.features, ok = validateFeatures(get("features"))
	if !ok {
		valid = false
	}
	if !validateLimits(get("limits")) {
		valid = false
	}
	out.graceDays = requireInt("grace_days")
	if out.graceDays != nil && out.graceDays.Sign() < 0 {
		valid = false
	}
	optionalStr("org_name")
	optionalStr("contact_email")
	optionalStr("license_id")
	return out, valid
}

// validateFeatures is dict[str, bool]: every value a lax bool.
func validateFeatures(raw pyjson.Value, present bool) (map[string]bool, bool) {
	if !present {
		return nil, false
	}
	object, ok := raw.(*pyjson.Object)
	if !ok {
		return nil, false
	}
	features := make(map[string]bool, object.Len())
	valid := true
	for _, key := range object.Keys() {
		item, _ := object.Get(key)
		enabled, errType, _ := pybody.PydanticBool(item)
		if errType != "" {
			valid = false
			continue
		}
		features[key] = enabled
	}
	return features, valid
}

// validateLimits is LicenseLimits: users, repos and api_rate required lax
// ints, backfill_days a lax int or None (absent is None).
func validateLimits(raw pyjson.Value, present bool) bool {
	if !present {
		return false
	}
	object, ok := raw.(*pyjson.Object)
	if !ok {
		return false
	}
	valid := true
	for _, name := range []string{"users", "repos", "api_rate"} {
		item, present := object.Get(name)
		if !present {
			valid = false
			continue
		}
		if _, errType, _ := pybody.PydanticInt(item); errType != "" {
			valid = false
		}
	}
	if item, present := object.Get("backfill_days"); present && item != nil {
		if _, errType, _ := pybody.PydanticInt(item); errType != "" {
			valid = false
		}
	}
	return valid
}
