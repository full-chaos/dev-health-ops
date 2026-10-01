package processlicense

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonLicenseProgram is the real producer and the real verifier. Stdin is
// {"sign": [[seed_b64, org, tier, issued_at, days], ...], "judge": [[public_b64,
// license, now], ...]}; licenses naming "@<n>" are the n-th license the sign
// step produced. The answer names each signed license by its digest only
// ({"sha256", "length"}): the text is a signed token and is not stored.
// Signing is licensing.generator.sign_license; judging is
// what LicenseManager.initialize does with the pair (LicenseValidator(public)
// then .validate(license)), at a fixed clock, with any exception -- which the
// api lifespan swallows into the community tier -- answered as not in force.
// Each judgement is [in_force, tier, in_grace_period, features].
const pythonLicenseProgram = `
import hashlib, json, sys
from dev_health_ops.licensing.generator import sign_license
from dev_health_ops.licensing.validator import LicenseValidator
request = json.loads(sys.stdin.read())
signed = [sign_license(seed, org_id=org, tier=tier, issued_at=issued, duration_days=days, license_id="lic")
          for seed, org, tier, issued, days in request["sign"]]
judged = []
for public, license, now in request["judge"]:
    if license.startswith("@"):
        license = signed[int(license[1:])]
    try:
        result = LicenseValidator(public).validate(license, current_time=now)
    except Exception:
        judged.append([False, None, False, None])
        continue
    if not result.valid:
        judged.append([False, None, False, None])
        continue
    judged.append([True, result.payload.tier.value, result.in_grace_period, result.payload.features])
digests = [{"sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), "length": len(text.encode("utf-8"))} for text in signed]
print(json.dumps({"signed": digests, "judged": judged}))
`

type judgeCase struct {
	name, public, license string
	now                   int64
}

// TestVerifierMatchesFrozenPythonLicenseValidator holds Validate to the Python
// LicenseValidator: licenses signed by the real sign_license with throwaway
// keys, judged at clocks before expiry, in grace and past it; the same
// licenses tampered with, under another key, and re-signed payloads that
// probe pydantic's lax validation and libsodium's small-order refusals. The
// verdict, tier, grace flag and feature map must agree case for case.
func TestVerifierMatchesFrozenPythonLicenseValidator(t *testing.T) {
	random := rand.New(rand.NewSource(6663))
	newKey := func() testKey {
		seed := make([]byte, 32)
		random.Read(seed)
		private := ed25519.NewKeyFromSeed(seed)
		return testKey{public: private.Public().(ed25519.PublicKey), seed: seed}
	}
	keys := []testKey{newKey(), newKey()}
	stranger := newKey()

	type signCase struct {
		seed, org, tier string
		issued, days    int64
	}
	var signs []signCase
	for _, key := range keys {
		for _, tier := range []string{"community", "team", "enterprise"} {
			for _, days := range []int64{1, 30, 365} {
				signs = append(signs, signCase{key.seedB64(), "org-" + tier, tier, issued, days})
			}
		}
	}

	var cases []judgeCase
	add := func(name, public, license string, now int64) {
		cases = append(cases, judgeCase{name, public, license, now})
	}
	for index, s := range signs {
		key := keys[index/(len(signs)/len(keys))]
		ref := fmt.Sprintf("@%d", index)
		exp := s.issued + s.days*day
		// The cases are the program's input: their order is the same in every run.
		for _, clock := range []struct {
			label string
			now   int64
		}{{"issued", s.issued}, {"at exp", exp}, {"exp+1", exp + 1}, {"grace end", exp + 14*day}, {"grace end+1", exp + 14*day + 1}, {"past enterprise grace", exp + 30*day + 1}} {
			add(fmt.Sprintf("%s/%d %s", s.tier, s.days, clock.label), key.publicB64(), ref, clock.now)
		}
		add(fmt.Sprintf("%s/%d another key", s.tier, s.days), stranger.publicB64(), ref, s.issued)
	}

	// Raw payloads signed with a throwaway key (Ed25519 is deterministic:
	// crypto/ed25519 and nacl produce the same signature).
	key := keys[0]
	base := `"iss":"fullchaos.studio","sub":"o","iat":1,"exp":99999999999,"tier":"team","limits":{"users":1,"repos":1,"api_rate":1},"grace_days":3`
	raw := map[string]string{
		"minimal":                 `{` + base + `,"features":{"sso_saml":true}}`,
		"extra keys":              `{` + base + `,"features":{},"x":{"y":[1]}}`,
		"duplicate tier":          `{` + base + `,"features":{},"tier":"enterprise"}`,
		"duplicate features":      `{` + base + `,"features":{"a":true},"features":{"b":true}}`,
		"feature str yes":         `{` + base + `,"features":{"a":"yes","b":"OFF","c":"t","d":"N"}}`,
		"feature ints":            `{` + base + `,"features":{"a":1,"b":0}}`,
		"feature floats":          `{` + base + `,"features":{"a":1.0,"b":0.0}}`,
		"feature 2":               `{` + base + `,"features":{"a":2}}`,
		"feature 0.5":             `{` + base + `,"features":{"a":0.5}}`,
		"feature null":            `{` + base + `,"features":{"a":null}}`,
		"feature list":            `{` + base + `,"features":{"a":[]}}`,
		"feature str maybe":       `{` + base + `,"features":{"a":"maybe"}}`,
		"feature str padded":      `{` + base + `,"features":{"a":" true"}}`,
		"features list":           `{` + base + `,"features":[]}`,
		"features missing":        `{` + base + `}`,
		"iss padded":              strings.Replace(`{`+base+`,"features":{}}`, `"fullchaos.studio"`, `"fullchaos.studio "`, 1),
		"iss upper":               strings.Replace(`{`+base+`,"features":{}}`, `"fullchaos.studio"`, `"Fullchaos.studio"`, 1),
		"iss missing":             strings.Replace(`{`+base+`,"features":{}}`, `"iss":"fullchaos.studio",`, ``, 1),
		"tier upper":              strings.Replace(`{`+base+`,"features":{}}`, `"tier":"team"`, `"tier":"TEAM"`, 1),
		"tier gold":               strings.Replace(`{`+base+`,"features":{}}`, `"tier":"team"`, `"tier":"gold"`, 1),
		"tier null":               strings.Replace(`{`+base+`,"features":{}}`, `"tier":"team"`, `"tier":null`, 1),
		"tier int":                strings.Replace(`{`+base+`,"features":{}}`, `"tier":"team"`, `"tier":1`, 1),
		"sub int":                 strings.Replace(`{`+base+`,"features":{}}`, `"sub":"o"`, `"sub":5`, 1),
		"sub empty":               strings.Replace(`{`+base+`,"features":{}}`, `"sub":"o"`, `"sub":""`, 1),
		"sub null":                strings.Replace(`{`+base+`,"features":{}}`, `"sub":"o"`, `"sub":null`, 1),
		"sub surrogate":           strings.Replace(`{`+base+`,"features":{}}`, `"sub":"o"`, `"sub":"\ud800"`, 1),
		"iat str":                 strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":"1"`, 1),
		"iat str underscore":      strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":"1_000"`, 1),
		"iat str padded":          strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":" 7 "`, 1),
		"iat str 5.0":             strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":"5.0"`, 1),
		"iat str junk":            strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":"x"`, 1),
		"iat bool":                strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":true`, 1),
		"iat null":                strings.Replace(`{`+base+`,"features":{}}`, `"iat":1`, `"iat":null`, 1),
		"exp float":               strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `99999999999.0`, 1),
		"exp fraction":            strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `99999999999.5`, 1),
		"exp huge":                strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `1`+strings.Repeat("0", 40), 1),
		"exp huge float":          strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `1e40`, 1),
		"exp NaN":                 strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `NaN`, 1),
		"exp Infinity":            strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `Infinity`, 1),
		"exp past, in grace":      strings.Replace(`{`+base+`,"features":{"z":true}}`, `99999999999`, `1790000000`, 1),
		"exp past, grace str":     strings.Replace(strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `1790000000`, 1), `"grace_days":3`, `"grace_days":"3"`, 1),
		"exp past, grace true":    strings.Replace(strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `1790000000`, 1), `"grace_days":3`, `"grace_days":true`, 1),
		"exp past, grace huge":    strings.Replace(strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `1790000000`, 1), `"grace_days":3`, `"grace_days":1`+strings.Repeat("0", 30), 1),
		"exp negative":            strings.Replace(`{`+base+`,"features":{}}`, `99999999999`, `-5`, 1),
		"grace negative":          strings.Replace(`{`+base+`,"features":{}}`, `"grace_days":3`, `"grace_days":-1`, 1),
		"grace missing":           strings.Replace(`{`+base+`,"features":{}}`, `,"grace_days":3`, ``, 1),
		"limits missing":          strings.Replace(`{`+base+`,"features":{}}`, `"limits":{"users":1,"repos":1,"api_rate":1},`, ``, 1),
		"limits users missing":    strings.Replace(`{`+base+`,"features":{}}`, `"users":1,`, ``, 1),
		"limits str ints":         strings.Replace(`{`+base+`,"features":{}}`, `"users":1`, `"users":"-1"`, 1),
		"limits backfill str":     strings.Replace(`{`+base+`,"features":{}}`, `"api_rate":1`, `"api_rate":1,"backfill_days":"7"`, 1),
		"limits backfill frac":    strings.Replace(`{`+base+`,"features":{}}`, `"api_rate":1`, `"api_rate":1,"backfill_days":7.5`, 1),
		"limits backfill null":    strings.Replace(`{`+base+`,"features":{}}`, `"api_rate":1`, `"api_rate":1,"backfill_days":null`, 1),
		"limits list":             strings.Replace(`{`+base+`,"features":{}}`, `{"users":1,"repos":1,"api_rate":1}`, `[]`, 1),
		"limits extra":            strings.Replace(`{`+base+`,"features":{}}`, `"users":1`, `"users":1,"seats":"x"`, 1),
		"org_name int":            `{` + base + `,"features":{},"org_name":1}`,
		"contact_email null":      `{` + base + `,"features":{},"contact_email":null}`,
		"license_id str":          `{` + base + `,"features":{},"license_id":"x"}`,
		"license_id list":         `{` + base + `,"features":{},"license_id":[]}`,
		"array":                   `[1,2]`,
		"string":                  `"x"`,
		"null":                    `null`,
		"truncated":               `{` + base,
		"trailing garbage":        `{` + base + `,"features":{}}x`,
		"surrounding whitespace":  " \n{" + base + `,"features":{}}` + "\n ",
		"bom":                     "\ufeff{" + base + `,"features":{}}`,
		"not utf-8":               "{" + base + ",\"features\":{\"\xff\":true}}",
		"utf-8 encoded surrogate": "{" + base + ",\"features\":{\"\xed\xa0\x80\":true}}",
		"overlong utf-8":          "{" + base + ",\"features\":{\"\xc0\xaf\":true}}",
		"empty":                   ``,
		"unicode feature key":     `{` + base + `,"features":{"f\u00e9":true,"\u2028":false}}`,
		"python sorted keys only": `{"exp":99999999999,"features":{"sso_saml":true},"grace_days":0,"iat":1,"iss":"fullchaos.studio","limits":{"api_rate":1,"repos":1,"users":1},"sub":"o","tier":"enterprise"}`,
	}
	document := `{` + base + `,"features":{"sso_saml":true}}`
	raw["utf-16-le"] = string(utf16Bytes(document, false))
	raw["utf-16-be"] = string(utf16Bytes(document, true))
	raw["utf-16-le with BOM"] = "\xff\xfe" + string(utf16Bytes(document, false))
	raw["utf-32-le"] = string(utf32LE(document))
	raw["utf-8 BOM then utf-8"] = "\xef\xbb\xbf" + document
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		add("raw "+name, key.publicB64(), key.signRaw([]byte(raw[name])), 1_795_000_000)
	}

	// Tampering, framing and key shapes around one Python-signed license
	// (its text arrives only after the sign step, so these are built from a
	// Go-signed twin: licensing.SignLicense is byte-identical to sign_license,
	// pinned by TestSignLicenseMatchesLivePython).
	good := key.license(t, "enterprise", issued, 365)
	payloadB64, signatureB64, _ := strings.Cut(good, ".")
	payload, _ := base64.StdEncoding.DecodeString(payloadB64)
	signature, _ := base64.StdEncoding.DecodeString(signatureB64)
	for i := 0; i < 24; i++ {
		tampered := append([]byte(nil), payload...)
		tampered[random.Intn(len(tampered))] ^= byte(1 << random.Intn(8))
		add(fmt.Sprintf("payload bit %d", i), key.publicB64(), base64.StdEncoding.EncodeToString(tampered)+"."+signatureB64, issued)
		flipped := append([]byte(nil), signature...)
		flipped[random.Intn(len(flipped))] ^= byte(1 << random.Intn(8))
		add(fmt.Sprintf("signature bit %d", i), key.publicB64(), payloadB64+"."+base64.StdEncoding.EncodeToString(flipped), issued)
	}
	add("good", key.publicB64(), good, issued)
	add("padded", key.publicB64(), " \t\n"+good+"\r\n\u3000", issued)
	add("inner newline", key.publicB64(), payloadB64[:10]+"\n"+payloadB64[10:]+"."+signatureB64, issued)
	add("unpadded", key.publicB64(), strings.TrimRight(payloadB64, "=")+"."+strings.TrimRight(signatureB64, "="), issued)
	add("urlsafe", key.publicB64(), strings.NewReplacer("+", "-", "/", "_").Replace(good), issued)
	add("extra padding", key.publicB64(), payloadB64+"===."+signatureB64, issued)
	add("three parts", key.publicB64(), good+".", issued)
	add("no dot", key.publicB64(), payloadB64, issued)
	add("empty", key.publicB64(), "", issued)
	add("non-ASCII", key.publicB64(), "\u00e9"+good, issued)
	add("signature 63 bytes", key.publicB64(), payloadB64+"."+base64.StdEncoding.EncodeToString(signature[:63]), issued)
	add("signature 65 bytes", key.publicB64(), payloadB64+"."+base64.StdEncoding.EncodeToString(append(append([]byte(nil), signature...), 0)), issued)
	add("S + L (non-canonical)", key.publicB64(), payloadB64+"."+base64.StdEncoding.EncodeToString(addGroupOrderToS(signature)), issued)
	add("public key padded", " "+key.publicB64()+"\n", good, issued)
	add("public key 31 bytes", base64.StdEncoding.EncodeToString(key.public[:31]), good, issued)
	add("public key 33 bytes", base64.StdEncoding.EncodeToString(append(append([]byte(nil), key.public...), 0)), good, issued)
	add("public key not base64", "!!!!", good, issued)
	add("public key empty", "", good, issued)

	// libsodium's small-order refusals: the forged license under each
	// small-order public key (sign bit clear and set), and a real key's
	// signature with R = identity.
	forgedPayload := []byte(`{"iss":"fullchaos.studio","sub":"o","iat":0,"exp":99999999999,"tier":"enterprise","features":{"sso_saml":true},"limits":{"users":-1,"repos":-1,"api_rate":-1},"grace_days":30}`)
	for index, blocked := range smallOrder {
		for _, sign := range []byte{0, 0x80} {
			point := blocked
			point[31] |= sign
			forged := append(append([]byte(nil), point[:]...), make([]byte, 32)...)
			add(fmt.Sprintf("small-order key %d sign %x R=A S=0", index, sign), base64.StdEncoding.EncodeToString(point[:]),
				base64.StdEncoding.EncodeToString(forgedPayload)+"."+base64.StdEncoding.EncodeToString(forged), 0)
			identity := make([]byte, 64)
			identity[0] = 1
			add(fmt.Sprintf("small-order key %d sign %x R=identity S=0", index, sign), base64.StdEncoding.EncodeToString(point[:]),
				base64.StdEncoding.EncodeToString(forgedPayload)+"."+base64.StdEncoding.EncodeToString(identity), 0)
		}
	}
	forgedCount := 0
	for index, blocked := range smallOrder {
		for _, sign := range []byte{0, 0x80} {
			point := blocked
			point[31] |= sign
			if forged, ok := forgeUnderSmallOrderKey(point[:], forgedPayload); ok {
				forgedCount++
				add(fmt.Sprintf("small-order key %d sign %x ordinary R", index, sign), base64.StdEncoding.EncodeToString(point[:]),
					base64.StdEncoding.EncodeToString(forgedPayload)+"."+base64.StdEncoding.EncodeToString(forged), 0)
			}
		}
	}
	if forgedCount < 8 {
		t.Fatalf("only %d small-order forgeries built", forgedCount)
	}
	add("R = identity, real key", key.publicB64(),
		base64.StdEncoding.EncodeToString(forgedPayload)+"."+base64.StdEncoding.EncodeToString(identityRSignature(key, forgedPayload)), 0)

	signInput := make([][]any, len(signs))
	for index, s := range signs {
		signInput[index] = []any{s.seed, s.org, s.tier, s.issued, s.days}
	}
	judgeInput := make([][]any, len(cases))
	for index, c := range cases {
		judgeInput[index] = []any{c.public, c.license, c.now}
	}
	stdin, err := json.Marshal(map[string]any{"sign": signInput, "judge": judgeInput})
	if err != nil {
		t.Fatal(err)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", ".."))
	output := verifierGoldens.Outputs(t, root, "license-verifier.golden.json", programoracle.Program{Name: "license verifier", Text: pythonLicenseProgram, Stdin: stdin})[0]
	var answer struct {
		Signed []struct {
			SHA256 string `json:"sha256"`
			Length int    `json:"length"`
		} `json:"signed"`
		Judged [][]json.RawMessage `json:"judged"`
	}
	if err := json.Unmarshal([]byte(output), &answer); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(answer.Signed) != len(signs) || len(answer.Judged) != len(cases) {
		t.Fatalf("python answered %d/%d signs and %d/%d judgements", len(answer.Signed), len(signs), len(answer.Judged), len(cases))
	}
	// The golden holds each Python-signed license as its digest only. The
	// text the Go verifier judges is the Go-signed twin, and it is the same
	// text: the same SHA-256 and length.
	signedTexts := make([]string, len(signs))
	for index, s := range signs {
		text, err := licensing.SignLicense(s.seed, licensing.LicenseRequest{OrgID: s.org, Tier: s.tier, IssuedAt: s.issued, LicenseID: "lic", DurationDays: big.NewInt(s.days)})
		if err != nil {
			t.Fatalf("sign case %d: %v", index, err)
		}
		sum := sha256.Sum256([]byte(text))
		if hex.EncodeToString(sum[:]) != answer.Signed[index].SHA256 || len(text) != answer.Signed[index].Length {
			t.Fatalf("sign case %d (%s, %d days): the Go-signed license is not the Python-signed license (go %s, length %d; python sha256 %s, length %d)",
				index, s.tier, s.days, text, len(text), answer.Signed[index].SHA256, answer.Signed[index].Length)
		}
		signedTexts[index] = text
	}

	mismatches, inForce, graced, refused := 0, 0, 0, 0
	for index, c := range cases {
		license := c.license
		if strings.HasPrefix(license, "@") {
			var n int
			fmt.Sscanf(license, "@%d", &n)
			license = signedTexts[n]
		}
		got := goJudgement(c.public, license, c.now)
		want := canonicalJudgement(t, answer.Judged[index])
		if got != want {
			mismatches++
			t.Errorf("%s:\n go     %s\n python %s", c.name, got, want)
		}
		switch {
		case strings.HasPrefix(want, "[true,") && strings.Contains(want, ",true,"):
			graced++
			inForce++
		case strings.HasPrefix(want, "[true,"):
			inForce++
		default:
			refused++
		}
	}
	// A comparison where Python refused (almost) everything measured nothing.
	if inForce < 60 || graced < 10 || refused < 60 {
		t.Fatalf("too little measured: %d in force (%d in grace), %d refused", inForce, graced, refused)
	}
	t.Logf("%d cases: %d in force (%d in grace), %d refused, %d mismatches", len(cases), inForce, graced, refused, mismatches)
}

// verifierGoldens is the set of this package's frozen Python answers. The
// producers are sign_license and LicenseValidator of the pinned build and the
// distributions under them, so Identity names those distributions. A golden
// recorded by another producer is refused.
var verifierGoldens = programoracle.Set{
	Package:       "./internal/api/licensing/processlicense/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\npydantic 2.13.5\npydantic-core 2.46.5\npynacl 1.6.2",
	Distributions: []string{"pydantic", "pydantic-core", "pynacl"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"license-verifier.golden.json": "25c843bc72fdcf01f38e74e208b479dacd3f9c303eb30b3dad0bd49414dda4c7",
	},
}

// goJudgement is Install's verification for one pair, in the oracle's shape.
func goJudgement(public, license string, now int64) string {
	verifier, err := NewVerifier(public)
	if err != nil {
		return `[false,null,false,null]`
	}
	result := verifier.Validate(license, now)
	if result.License == nil {
		return `[false,null,false,null]`
	}
	features, _ := json.Marshal(result.License.Features)
	tier, _ := json.Marshal(result.License.Tier)
	return fmt.Sprintf("[true,%s,%v,%s]", tier, result.License.InGracePeriod, features)
}

// canonicalJudgement re-encodes Python's judgement with sorted feature keys.
func canonicalJudgement(t *testing.T, judged []json.RawMessage) string {
	t.Helper()
	var inForce, grace bool
	var tier *string
	var features map[string]bool
	if err := json.Unmarshal(judged[0], &inForce); err != nil {
		t.Fatal(err)
	}
	if !inForce {
		return `[false,null,false,null]`
	}
	_ = json.Unmarshal(judged[1], &tier)
	_ = json.Unmarshal(judged[2], &grace)
	if err := json.Unmarshal(judged[3], &features); err != nil {
		t.Fatal(err)
	}
	encodedFeatures, _ := json.Marshal(features)
	encodedTier, _ := json.Marshal(*tier)
	return fmt.Sprintf("[true,%s,%v,%s]", encodedTier, grace, encodedFeatures)
}

// addGroupOrderToS is the signature with S replaced by S + L: the same
// point, a non-canonical scalar both libsodium and Go must refuse.
func addGroupOrderToS(signature []byte) []byte {
	s := new(big.Int).SetBytes(reverse(signature[32:]))
	s.Add(s, groupOrder)
	out := append([]byte(nil), signature[:32]...)
	return append(out, reverse(s.FillBytes(make([]byte, 32)))...)
}

// utf16Bytes encodes an ASCII document as UTF-16 (big-endian when be).
func utf16Bytes(document string, be bool) []byte {
	var out []byte
	for i := 0; i < len(document); i++ {
		if be {
			out = append(out, 0, document[i])
		} else {
			out = append(out, document[i], 0)
		}
	}
	return out
}

// utf32LE encodes an ASCII document as UTF-32LE.
func utf32LE(document string) []byte {
	var out []byte
	for i := 0; i < len(document); i++ {
		out = append(out, document[i], 0, 0, 0)
	}
	return out
}
