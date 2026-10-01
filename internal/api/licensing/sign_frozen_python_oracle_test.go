package licensing

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// pythonSignProgram signs each case with the real sign_license: stdin is a
// JSON list of [key, org_id, tier, issued_at, license_id, duration_days as decimal text ("" = the default)]; each answer is
// the digest of the license ({"sha256", "length"}: the text is a signed
// token and is not stored) or "error: <ValueError text>".
const pythonSignProgram = `
import hashlib, json, sys
from dev_health_ops.licensing.generator import sign_license
out = []
for key, org, tier, issued, license_id, days in json.loads(sys.stdin.read()):
    try:
        extra = {} if days == "" else {"duration_days": int(days)}
        text = sign_license(key, org_id=org, tier=tier, issued_at=issued, license_id=license_id, **extra).encode("utf-8")
        out.append({"sha256": hashlib.sha256(text).hexdigest(), "length": len(text)})
    except ValueError as exc:
        out.append("error: " + str(exc))
print(json.dumps(out))
`

// digestOf is the digest a golden holds for text.
func digestOf(text string) textDigest {
	sum := sha256.Sum256([]byte(text))
	return textDigest{SHA256: hex.EncodeToString(sum[:]), Length: len(text)}
}

type signCase struct {
	key, org, tier string
	issued         int64
	licenseID      string
	days           string
}

func signCases() []signCase {
	random := rand.New(rand.NewSource(6258))
	seed := func() string {
		raw := make([]byte, 32)
		random.Read(raw)
		return base64.StdEncoding.EncodeToString(raw)
	}
	zero := base64.StdEncoding.EncodeToString(make([]byte, 32))
	keys := []string{zero, seed(), seed(), seed()}
	orgs := []string{"8d0b9f0e-0000-4000-8000-000000000001", "org-abc", "", "Ünïcødé org \u2028 \"quoted\" \\ / <tag>", "\U0001F600"}
	tiers := []string{"community", "team", "enterprise"}
	issued := []int64{0, 1, 1790200000, 4102444800, -86400}
	var cases []signCase
	for index, key := range keys {
		for _, org := range orgs {
			for _, tier := range tiers {
				cases = append(cases, signCase{key, org, tier, issued[(index+len(org))%len(issued)], fmt.Sprintf("lic-%d-%s", index, tier), ""})
			}
		}
	}
	// Tier spelling and key shapes that sign_license refuses or reads
	// leniently.
	cases = append(cases,
		signCase{zero, "o", "Team", 5, "l", ""},
		signCase{zero, "o", "gold", 5, "l", ""},
		signCase{" " + zero[:10] + "\n" + zero[10:] + " ", "o", "team", 5, "l", ""},
		signCase{base64.StdEncoding.EncodeToString(make([]byte, 31)), "o", "team", 5, "l", ""},
		signCase{base64.StdEncoding.EncodeToString(make([]byte, 33)), "o", "team", 5, "l", ""},
		signCase{"not base64 at all!", "o", "team", 5, "l", ""},
		signCase{zero, "o", "team", 5, "", ""},
		// Excess padding, non-ASCII text and one stray character: the
		// review round's inputs (binascii's lenient loop).
		signCase{"AAAA====", "o", "team", 5, "l", ""},
		signCase{zero + "====", "o", "team", 5, "l", ""},
		signCase{zero[:43] + "=" + "=", "o", "team", 5, "l", ""},
		signCase{"\u00e9" + zero, "o", "team", 5, "l", ""},
		signCase{"AAAAA", "o", "team", 5, "l", ""},
		signCase{"", "o", "team", 5, "l", ""},
	)
	// Python's integers are unbounded: durations beyond 64 bits (and the
	// review round's inputs) are signed with an exact expiry; a duration that
	// is not positive is refused whatever its size.
	for _, days := range []string{"1", "365", "106751991167301", "106751991167302", "9223372036854775807", "9223372036854775808", "18446744073709551616", "1" + strings.Repeat("0", 40), "0", "-1", "-9223372036854775809", "-1" + strings.Repeat("0", 40)} {
		for _, issued := range []int64{0, 5, 1790200000, -86400, math.MaxInt64} {
			cases = append(cases, signCase{zero, "o", "team", issued, "l", days})
		}
	}
	return cases
}

// TestSignLicenseMatchesFrozenPython holds SignLicense to the frozen answers
// of the Python sign_license: the SHA-256 and the length of every license
// text, over keys, tiers, org ids (non-ASCII, JSON-escaped characters), issue
// times and license ids, and the text of every refusal.
func TestSignLicenseMatchesFrozenPython(t *testing.T) {
	cases := signCases()
	input := make([][]any, len(cases))
	for index, c := range cases {
		licenseID := any(c.licenseID)
		if c.licenseID == "" {
			licenseID = "0"
		}
		input[index] = []any{c.key, c.org, c.tier, c.issued, licenseID, c.days}
	}
	stdin, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "sign-license.golden.json", programoracle.Program{Name: "sign license", Text: pythonSignProgram, Stdin: stdin})[0]
	var answers []json.RawMessage
	if err := json.Unmarshal([]byte(output), &answers); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(answers) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(answers), len(cases))
	}
	// Each answer is a refusal text or the digest of a license.
	refusals, digests := make([]string, len(cases)), make([]*textDigest, len(cases))
	for index, raw := range answers {
		if err := json.Unmarshal(raw, &refusals[index]); err == nil {
			if !strings.HasPrefix(refusals[index], "error: ") {
				t.Fatalf("answer %d is the text %q: a license is stored as its digest, never as text", index, refusals[index])
			}
			continue
		}
		digest := new(textDigest)
		if err := json.Unmarshal(raw, digest); err != nil || len(digest.SHA256) != 64 || digest.Length == 0 {
			t.Fatalf("answer %d is neither a refusal nor a digest: %s", index, raw)
		}
		digests[index] = digest
	}
	mismatches, signed := 0, 0
	for index, c := range cases {
		licenseID := c.licenseID
		if licenseID == "" {
			licenseID = "0"
		}
		request := LicenseRequest{OrgID: c.org, Tier: c.tier, IssuedAt: c.issued, LicenseID: licenseID}
		if c.days != "" {
			request.DurationDays, _ = new(big.Int).SetString(c.days, 10)
		}
		got, err := SignLicense(c.key, request)
		pythonRefused := digests[index] == nil
		switch {
		case err != nil && pythonRefused:
			if want := strings.TrimPrefix(refusals[index], "error: "); err.Error() != want {
				mismatches++
				t.Errorf("case %d %+v: go error %q, python error %q", index, c, err.Error(), want)
			}
		case err == nil && !pythonRefused:
			signed++
			if digestOf(got) != *digests[index] {
				mismatches++
				t.Errorf("case %d %+v: the Go license is not the Python license:\n go     %s (%+v)\n python %+v", index, c, got, digestOf(got), *digests[index])
			}
		default:
			mismatches++
			t.Errorf("case %d %+v: go (%q, %v), python %s", index, c, got, err, answers[index])
		}
	}
	if signed < 60 {
		t.Fatalf("only %d cases signed on both planes; the comparison measured too little", signed)
	}
	t.Logf("%d cases, %d signed on both planes, %d mismatches", len(cases), signed, mismatches)
}

// pythonDecodeProgram decodes each text with the real base64.b64decode: stdin is
// a JSON list of strings; each answer is the bytes as hex, or "error: <text>".
const pythonDecodeProgram = `
import base64, json, sys
out = []
for text in json.loads(sys.stdin.read()):
    try:
        out.append(base64.b64decode(text).hex())
    except Exception as exc:
        out.append("error: " + str(exc))
print(json.dumps(out))
`

// TestPythonB64DecodeMatchesFrozenPython holds pythonB64Decode to the frozen
// answers of base64.b64decode over every text of up to five characters drawn
// from a set that covers the alphabet, padding, skipped characters and
// non-ASCII, plus random longer ones.
func TestPythonB64DecodeMatchesFrozenPython(t *testing.T) {
	pieces := []string{"A", "Q", "/", "=", " ", "\n", "-", "\u00e9"}
	var texts []string
	var build func(prefix string, depth int)
	build = func(prefix string, depth int) {
		texts = append(texts, prefix)
		if depth == 0 {
			return
		}
		for _, piece := range pieces {
			build(prefix+piece, depth-1)
		}
	}
	build("", 5)
	random := rand.New(rand.NewSource(6675))
	for index := 0; index < 4000; index++ {
		var text strings.Builder
		for length := random.Intn(14); length > 0; length-- {
			text.WriteString(pieces[random.Intn(len(pieces))])
		}
		texts = append(texts, text.String())
	}
	stdin, err := json.Marshal(texts)
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "b64decode.golden.json", programoracle.Program{Name: "b64decode", Text: pythonDecodeProgram, Stdin: stdin})[0]
	var want []string
	if err := json.Unmarshal([]byte(output), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(texts) {
		t.Fatalf("python answered %d of %d texts", len(want), len(texts))
	}
	mismatches, decoded := 0, 0
	for index, text := range texts {
		got, err := pythonB64Decode(text)
		answer := fmt.Sprintf("%x", got)
		if err != nil {
			answer = "error: " + err.Error()
		} else {
			decoded++
		}
		if answer != want[index] {
			mismatches++
			if mismatches <= 20 {
				t.Errorf("%q: go %q, python %q", text, answer, want[index])
			}
		}
	}
	if decoded < 1000 {
		t.Fatalf("only %d texts decoded on the Go side; the comparison measured too little", decoded)
	}
	t.Logf("%d texts, %d decoded, %d mismatches", len(texts), decoded, mismatches)
}

// pythonSignAndJudgeProgram signs each case with the real sign_license and
// judges the license with the real LicenseValidator, under the public key of
// the case's seed and at a fixed clock. Stdin is {"now", "cases": [[key,
// org_id, tier, issued_at, license_id, duration_days as decimal text], ...]};
// each answer is [digest of the license, valid, error, payload JSON,
// in_grace_period].
const pythonSignAndJudgeProgram = `
import base64, hashlib, json, sys
from nacl.signing import SigningKey
from dev_health_ops.licensing.generator import sign_license
from dev_health_ops.licensing.validator import LicenseValidator
request = json.loads(sys.stdin.read())
out = []
for key, org, tier, issued, license_id, days in request["cases"]:
    text = sign_license(key, org_id=org, tier=tier, issued_at=issued, license_id=license_id, duration_days=int(days))
    public = base64.b64encode(bytes(SigningKey(base64.b64decode(key)).verify_key)).decode()
    result = LicenseValidator(public).validate(text, current_time=request["now"])
    raw = text.encode("utf-8")
    out.append([{"sha256": hashlib.sha256(raw).hexdigest(), "length": len(raw)}, result.valid, result.error,
                result.payload.model_dump_json() if result.payload else None, result.in_grace_period])
print(json.dumps(out))
`

// TestBigDurationLicensesAreThePythonLicensesAndPythonAcceptsThem: a license
// whose expiry is beyond 64 bits is a capability only if the verifier accepts
// it. The Go-signed license text is the Python-signed text (the same SHA-256
// and length), and the frozen verdict of the real Python LicenseValidator for
// that text is "valid", with the tier and the exact expiry in its payload.
// (The Go verifier, processlicense, is held to that validator by
// TestVerifierMatchesFrozenPythonLicenseValidator.)
func TestBigDurationLicensesAreThePythonLicensesAndPythonAcceptsThem(t *testing.T) {
	zero := base64.StdEncoding.EncodeToString(make([]byte, 32))
	const issued, now = 1790000000, 1790200000
	days := []string{"365", "106751991167301", "9223372036854775808", "1" + strings.Repeat("0", 40)}
	cases := make([][]any, len(days))
	for index, d := range days {
		cases[index] = []any{zero, "o", "team", issued, "l", d}
	}
	stdin, err := json.Marshal(map[string]any{"now": now, "cases": cases})
	if err != nil {
		t.Fatal(err)
	}
	output := frozenPython(t, "big-duration-license.golden.json", programoracle.Program{Name: "sign and judge", Text: pythonSignAndJudgeProgram, Stdin: stdin})[0]
	var answers []struct {
		digest  textDigest
		valid   bool
		failure *string
		payload *string
		grace   bool
	}
	var raw [][]json.RawMessage
	if err := json.Unmarshal([]byte(output), &raw); err != nil || len(raw) != len(days) {
		t.Fatalf("decode: %v (%d of %d answers)", err, len(raw), len(days))
	}
	answers = make([]struct {
		digest  textDigest
		valid   bool
		failure *string
		payload *string
		grace   bool
	}, len(raw))
	for index, fields := range raw {
		if len(fields) != 5 {
			t.Fatalf("answer %d has %d fields, want 5", index, len(fields))
		}
		for position, target := range []any{&answers[index].digest, &answers[index].valid, &answers[index].failure, &answers[index].payload, &answers[index].grace} {
			if err := json.Unmarshal(fields[position], target); err != nil {
				t.Fatalf("answer %d field %d: %v", index, position, err)
			}
		}
	}
	for index, d := range days {
		duration, _ := new(big.Int).SetString(d, 10)
		license, err := SignLicense(zero, LicenseRequest{OrgID: "o", Tier: "team", IssuedAt: issued, LicenseID: "l", DurationDays: duration})
		if err != nil {
			t.Fatalf("%s days: %v", d, err)
		}
		answer := answers[index]
		if digestOf(license) != answer.digest {
			t.Errorf("%s days: the Go-signed license is not the Python-signed license: go %s (%+v), python %+v", d, license, digestOf(license), answer.digest)
		}
		if !answer.valid || answer.failure != nil || answer.payload == nil || answer.grace {
			t.Errorf("%s days: the Python verifier did not accept the license as in force: valid %v, error %v, grace %v", d, answer.valid, answer.failure, answer.grace)
			continue
		}
		// The payload the verifier read holds the exact expiry: issued + days * 86400.
		expiry := new(big.Int).Add(big.NewInt(issued), new(big.Int).Mul(duration, big.NewInt(86400)))
		if !strings.Contains(*answer.payload, `"exp":`+expiry.String()) || !strings.Contains(*answer.payload, `"tier":"team"`) {
			t.Errorf("%s days: the payload Python read is %s, want the tier team and the expiry %s", d, *answer.payload, expiry)
		}
	}
}
