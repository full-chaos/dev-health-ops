package processlicense

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

// testKey is a throwaway Ed25519 key generated for this test run only.
type testKey struct {
	public ed25519.PublicKey
	seed   []byte
}

func newTestKey(t *testing.T) testKey {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return testKey{public: public, seed: private.Seed()}
}

func (k testKey) publicB64() string { return base64.StdEncoding.EncodeToString(k.public) }
func (k testKey) seedB64() string   { return base64.StdEncoding.EncodeToString(k.seed) }

// signRaw is sign_payload over arbitrary payload bytes.
func (k testKey) signRaw(payload []byte) string {
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(k.seed), payload)
	return base64.StdEncoding.EncodeToString(payload) + "." + base64.StdEncoding.EncodeToString(signature)
}

// license is licensing.SignLicense (the Go port of sign_license, held to
// Python by TestSignLicenseMatchesLivePython) for tier, issued at issued,
// lasting days.
func (k testKey) license(t *testing.T, tier string, issued int64, days int64) string {
	t.Helper()
	text, err := licensing.SignLicense(k.seedB64(), licensing.LicenseRequest{
		OrgID: "org-6663", Tier: tier, IssuedAt: issued, LicenseID: "lic-6663", DurationDays: big.NewInt(days),
	})
	if err != nil {
		t.Fatal(err)
	}
	return text
}

const (
	issued = int64(1_790_000_000)
	day    = int64(86_400)
)

func validate(t *testing.T, publicB64, license string, now int64) Result {
	t.Helper()
	verifier, err := NewVerifier(publicB64)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return verifier.Validate(license, now)
}

func TestValidateAcceptsASignedLicense(t *testing.T) {
	key := newTestKey(t)
	for _, tier := range []string{"community", "team", "enterprise"} {
		result := validate(t, key.publicB64(), key.license(t, tier, issued, 365), issued+day)
		if result.License == nil {
			t.Fatalf("%s: refused: %s", tier, result.Reason)
		}
		if result.License.Tier != tier || result.License.InGracePeriod {
			t.Fatalf("%s: got %+v", tier, result.License)
		}
	}
	// Python strips the license text before it splits it.
	if result := validate(t, key.publicB64(), " \n"+key.license(t, "team", issued, 365)+"\t\n", issued); result.License == nil {
		t.Fatalf("surrounding whitespace refused: %s", result.Reason)
	}
}

func TestValidateRefusesATamperedLicense(t *testing.T) {
	key := newTestKey(t)
	good := key.license(t, "team", issued, 365)
	payloadB64, signatureB64, _ := strings.Cut(good, ".")
	payload, _ := base64.StdEncoding.DecodeString(payloadB64)
	signature, _ := base64.StdEncoding.DecodeString(signatureB64)

	upgraded := bytes.Replace(payload, []byte(`"tier":"team"`), []byte(`"tier":"enterprise"`), 1)
	if bytes.Equal(upgraded, payload) {
		t.Fatal("the payload holds no team tier to tamper with")
	}
	flipped := append([]byte(nil), signature...)
	flipped[10] ^= 0x01
	for name, license := range map[string]string{
		"payload upgraded to enterprise": base64.StdEncoding.EncodeToString(upgraded) + "." + signatureB64,
		"signature bit flipped":          payloadB64 + "." + base64.StdEncoding.EncodeToString(flipped),
		"signature truncated":            payloadB64 + "." + base64.StdEncoding.EncodeToString(signature[:63]),
	} {
		if result := validate(t, key.publicB64(), license, issued); result.License != nil || result.Reason != ReasonSignature {
			t.Errorf("%s: got %+v, want %s", name, result, ReasonSignature)
		}
	}
	for name, license := range map[string]string{
		"no dot":      payloadB64,
		"three parts": good + ".x",
		"empty":       "",
	} {
		if result := validate(t, key.publicB64(), license, issued); result.License != nil || result.Reason != ReasonFormat {
			t.Errorf("%s: got %+v, want %s", name, result, ReasonFormat)
		}
	}
	if result := validate(t, key.publicB64(), "\u00e9"+good, issued); result.Reason != ReasonBase64 {
		t.Errorf("non-ASCII: got %+v, want %s", result, ReasonBase64)
	}
}

func TestValidateRefusesAnotherKeysLicense(t *testing.T) {
	signer, other := newTestKey(t), newTestKey(t)
	if result := validate(t, other.publicB64(), signer.license(t, "enterprise", issued, 365), issued); result.License != nil || result.Reason != ReasonSignature {
		t.Fatalf("got %+v, want %s", result, ReasonSignature)
	}
}

func TestValidateExpiryAndGrace(t *testing.T) {
	key := newTestKey(t)
	license := key.license(t, "team", issued, 10) // team: 14 grace days
	exp := issued + 10*day
	for name, testCase := range map[string]struct {
		now          int64
		inForce      bool
		grace        bool
		wantedReason Reason
	}{
		"at expiry":            {exp, true, false, ""},
		"one second after":     {exp + 1, true, true, ""},
		"at the end of grace":  {exp + 14*day, true, true, ""},
		"one second past that": {exp + 14*day + 1, false, false, ReasonExpired},
	} {
		result := validate(t, key.publicB64(), license, testCase.now)
		if (result.License != nil) != testCase.inForce || result.Reason != testCase.wantedReason {
			t.Errorf("%s: got %+v", name, result)
			continue
		}
		if result.License != nil && result.License.InGracePeriod != testCase.grace {
			t.Errorf("%s: grace = %v, want %v", name, result.License.InGracePeriod, testCase.grace)
		}
	}
}

func TestNewVerifierRefusesAKeyThatIsNot32Bytes(t *testing.T) {
	for name, publicKey := range map[string]string{
		"31 bytes":      base64.StdEncoding.EncodeToString(make([]byte, 31)),
		"33 bytes":      base64.StdEncoding.EncodeToString(make([]byte, 33)),
		"not base64":    "A",
		"not ASCII":     "\u00e9" + base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"empty payload": "",
	} {
		if _, err := NewVerifier(publicKey); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// Go's ed25519.Verify accepts what libsodium (Python's nacl) refuses: every
// signature under a small-order public key R = identity and S = 0 verifies,
// so anyone could mint an enterprise license for such a key; and a real key
// can sign with R = identity (S = h*a), a small-order R libsodium refuses.
func TestValidateRefusesSmallOrderPoints(t *testing.T) {
	payload := []byte(`{"iss":"fullchaos.studio","sub":"o","iat":0,"exp":99999999999,"tier":"enterprise","features":{"sso_saml":true},"limits":{"users":-1,"repos":-1,"api_rate":-1,"backfill_days":null},"grace_days":30}`)
	identity := make([]byte, 32)
	identity[0] = 1
	forged := append(append([]byte(nil), identity...), make([]byte, 32)...)
	if !ed25519.Verify(ed25519.PublicKey(identity), payload, forged) {
		t.Fatal("precondition: Go's ed25519.Verify no longer accepts the small-order forgery; this guard's rationale changed")
	}
	license := base64.StdEncoding.EncodeToString(payload) + "." + base64.StdEncoding.EncodeToString(forged)
	if result := validate(t, base64.StdEncoding.EncodeToString(identity), license, 0); result.License != nil || result.Reason != ReasonSignature {
		t.Fatalf("small-order public key: got %+v", result)
	}

	// An ordinary R under every small-order key encoding, sign bit clear and
	// set: this reaches the public-key check alone (the R check passes), and
	// the sign-bit-set encodings reach the blocklist's sign-bit mask.
	goAccepted, signSetAccepted := 0, 0
	for index, blocked := range smallOrder {
		for _, sign := range []byte{0, 0x80} {
			point := blocked
			point[31] |= sign
			forged, ok := forgeUnderSmallOrderKey(point[:], payload)
			if !ok || !ed25519.Verify(ed25519.PublicKey(point[:]), payload, forged) {
				continue
			}
			if hasSmallOrder(forged[:32]) {
				t.Fatalf("entry %d sign %x: the forgery's R is small-order; it does not isolate the key check", index, sign)
			}
			goAccepted++
			if sign != 0 {
				signSetAccepted++
			}
			license := base64.StdEncoding.EncodeToString(payload) + "." + base64.StdEncoding.EncodeToString(forged)
			if result := validate(t, base64.StdEncoding.EncodeToString(point[:]), license, 0); result.License != nil {
				t.Errorf("entry %d sign %x: forged license accepted as %+v", index, sign, result.License)
			}
		}
	}
	if goAccepted < 8 || signSetAccepted < 3 {
		t.Fatalf("precondition: Go accepted only %d forgeries (%d sign-bit-set); the guard is not exercised", goAccepted, signSetAccepted)
	}

	key := newTestKey(t)
	identityR := identityRSignature(key, payload)
	if !ed25519.Verify(key.public, payload, identityR) {
		t.Fatal("precondition: Go's ed25519.Verify refuses R = identity; this guard's rationale changed")
	}
	license = base64.StdEncoding.EncodeToString(payload) + "." + base64.StdEncoding.EncodeToString(identityR)
	if result := validate(t, key.publicB64(), license, 0); result.License != nil || result.Reason != ReasonSignature {
		t.Fatalf("small-order R: got %+v", result)
	}
}

// identityRSignature signs payload with R = the identity point (nonce 0):
// S = h*a mod L, h = SHA-512(R || A || M).
func identityRSignature(key testKey, payload []byte) []byte {
	digest := sha512.Sum512(key.seed)
	scalar := digest[:32]
	scalar[0] &= 248
	scalar[31] &= 127
	scalar[31] |= 64
	a := new(big.Int).SetBytes(reverse(scalar))
	r := make([]byte, 32)
	r[0] = 1
	hash := sha512.New()
	hash.Write(r)
	hash.Write(key.public)
	hash.Write(payload)
	h := new(big.Int).SetBytes(reverse(hash.Sum(nil)))
	s := new(big.Int).Mul(h, a)
	s.Mod(s, groupOrder)
	return append(r, reverse(s.FillBytes(make([]byte, 32)))...)
}

var groupOrder, _ = new(big.Int).SetString("7237005577332262213973186563042994240857116359379907606001950938285454250989", 10)

func reverse(in []byte) []byte {
	out := make([]byte, len(in))
	for i := range in {
		out[i] = in[len(in)-1-i]
	}
	return out
}

// The payload is validated as pydantic validates LicensePayload; the
// feature map in force is the payload's own, not the tier's defaults.
func TestValidatePayloadSchema(t *testing.T) {
	key := newTestKey(t)
	base := `"iss":"fullchaos.studio","sub":"o","iat":1,"exp":99999999999,"tier":"team","limits":{"users":1,"repos":1,"api_rate":1},"grace_days":0`
	accepted := map[string]string{
		"own feature map":      `{` + base + `,"features":{"sso_saml":true,"x":false}}`,
		"lax ints and bools":   `{"iss":"fullchaos.studio","sub":"o","iat":"1","exp":99999999999.0,"tier":"team","features":{"sso_saml":"yes"},"limits":{"users":"5","repos":true,"api_rate":1.0,"backfill_days":null},"grace_days":"0"}`,
		"extra keys ignored":   `{` + base + `,"features":{},"extra":[1,2]}`,
		"nullable strs null":   `{` + base + `,"features":{},"org_name":null,"contact_email":null,"license_id":null}`,
		"duplicate key (last)": `{` + base + `,"features":{},"tier":"enterprise"}`,
	}
	for name, payload := range accepted {
		if result := validate(t, key.publicB64(), key.signRaw([]byte(payload)), 0); result.License == nil {
			t.Errorf("%s: refused: %s", name, result.Reason)
		}
	}
	result := validate(t, key.publicB64(), key.signRaw([]byte(accepted["own feature map"])), 0)
	if !result.License.Features["sso_saml"] || result.License.Features["x"] {
		t.Errorf("features = %v", result.License.Features)
	}
	if got := validate(t, key.publicB64(), key.signRaw([]byte(accepted["duplicate key (last)"])), 0).License.Tier; got != "enterprise" {
		t.Errorf("duplicate tier key: %q, want the last value", got)
	}
	refused := map[string]string{
		"wrong issuer":          `{"iss":"example.com",` + base[len(`"iss":"fullchaos.studio",`):] + `,"features":{}}`,
		"unknown tier":          strings.Replace(`{`+base+`,"features":{}}`, `"team"`, `"gold"`, 1),
		"tier wrong case":       strings.Replace(`{`+base+`,"features":{}}`, `"team"`, `"Team"`, 1),
		"features missing":      `{` + base + `}`,
		"feature not a bool":    `{` + base + `,"features":{"sso_saml":"maybe"}}`,
		"feature int 2":         `{` + base + `,"features":{"sso_saml":2}}`,
		"negative grace":        strings.Replace(`{`+base+`,"features":{}}`, `"grace_days":0`, `"grace_days":-1`, 1),
		"fractional exp":        strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `1.5`, 1),
		"sub not a str":         strings.Replace(`{`+base+`,"features":{}}`, `"sub":"o"`, `"sub":5`, 1),
		"limits missing users":  strings.Replace(`{`+base+`,"features":{}}`, `"users":1,`, ``, 1),
		"org_name not a str":    `{` + base + `,"features":{},"org_name":1}`,
		"not an object":         `[1]`,
		"not json":              `{`,
		"not utf-8":             "{" + base + ",\"features\":{\"\xff\":true}}",
		"leading BOM":           "\ufeff{" + base + `,"features":{}}`,
		"backfill_days a float": strings.Replace(`{`+base+`,"features":{}}`, `"api_rate":1`, `"api_rate":1,"backfill_days":0.5`, 1),
	}
	for name, payload := range refused {
		if result := validate(t, key.publicB64(), key.signRaw([]byte(payload)), 0); result.License != nil {
			t.Errorf("%s: accepted %+v", name, result.License)
		}
	}
}

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := env[key]; return value, ok }
}

func install(t *testing.T, env map[string]string, now int64) (*licensing.ProcessLicense, Reason, string) {
	t.Helper()
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
	var logs bytes.Buffer
	license, reason := Install(context.Background(), lookupFrom(env), slog.New(slog.NewJSONHandler(&logs, nil)), now)
	output := logs.String()
	for name, value := range env {
		if len(value) >= 8 && strings.Contains(output, value) {
			t.Fatalf("the value of %s reached the log:\n%s", name, output)
		}
	}
	if !strings.Contains(output, `"reason":"`+string(reason)+`"`) && license == nil {
		t.Fatalf("the community outcome was not logged with its reason %q:\n%s", reason, output)
	}
	return license, reason, output
}

func TestInstall(t *testing.T) {
	key := newTestKey(t)
	good := key.license(t, "enterprise", issued, 365)

	license, reason, _ := install(t, map[string]string{"LICENSE_KEY": good, "LICENSE_PUBLIC_KEY": key.publicB64()}, issued)
	if license == nil || reason != "" || licensing.ProcessTier() != "enterprise" || !licensing.ProcessHasFeature("sso_saml") {
		t.Fatalf("valid license: %+v %q tier=%s", license, reason, licensing.ProcessTier())
	}
	if got := licensing.FeatureNotLicensedDetail("f", nil); !strings.Contains(string(mustMarshal(t, got)), `"current_tier":"enterprise"`) {
		t.Fatalf("402 detail does not carry the process tier: %s", mustMarshal(t, got))
	}

	for name, testCase := range map[string]struct {
		env  map[string]string
		now  int64
		want Reason
		warn bool
	}{
		"unset":           {map[string]string{}, issued, ReasonUnset, false},
		"both empty":      {map[string]string{"LICENSE_KEY": "", "LICENSE_PUBLIC_KEY": ""}, issued, ReasonUnset, false},
		"key only":        {map[string]string{"LICENSE_KEY": good}, issued, ReasonKeyOnly, true},
		"public key only": {map[string]string{"LICENSE_PUBLIC_KEY": key.publicB64()}, issued, ReasonPublicKeyOnly, true},
		"wrong key":       {map[string]string{"LICENSE_KEY": good, "LICENSE_PUBLIC_KEY": newTestKey(t).publicB64()}, issued, ReasonSignature, true},
		"expired":         {map[string]string{"LICENSE_KEY": good, "LICENSE_PUBLIC_KEY": key.publicB64()}, issued + 396*day, ReasonExpired, true},
		"bad public key":  {map[string]string{"LICENSE_KEY": good, "LICENSE_PUBLIC_KEY": "bm90LWEta2V5LXZhbHVl"}, issued, ReasonInvalidPublicKey, true},
		"both sources":    {map[string]string{"LICENSE_KEY": good, "LICENSE_KEY_FILE": "/nonexistent", "LICENSE_PUBLIC_KEY": key.publicB64()}, issued, ReasonResolveFailed, true},
	} {
		licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise"})
		license, reason, output := install(t, testCase.env, testCase.now)
		if license != nil || reason != testCase.want || licensing.ProcessTier() != "community" || licensing.ProcessHasFeature("sso_saml") {
			t.Errorf("%s: got %+v %q tier=%s, want community %q", name, license, reason, licensing.ProcessTier(), testCase.want)
		}
		if strings.Contains(output, `"level":"WARN"`) != testCase.warn {
			t.Errorf("%s: warning logged = %v, want %v:\n%s", name, !testCase.warn, testCase.warn, output)
		}
	}
}

func TestInstallReadsFileVariables(t *testing.T) {
	key := newTestKey(t)
	dir := t.TempDir()
	keyFile, publicFile := filepath.Join(dir, "license"), filepath.Join(dir, "public")
	if err := os.WriteFile(keyFile, []byte(key.license(t, "team", issued, 365)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(publicFile, []byte(key.publicB64()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	license, reason, _ := install(t, map[string]string{"LICENSE_KEY_FILE": keyFile, "LICENSE_PUBLIC_KEY_FILE": publicFile}, issued)
	if license == nil || license.Tier != "team" {
		t.Fatalf("got %+v %q", license, reason)
	}
}

func mustMarshal(t *testing.T, value interface{ MarshalJSON() ([]byte, error) }) []byte {
	t.Helper()
	out, err := value.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return out
}
