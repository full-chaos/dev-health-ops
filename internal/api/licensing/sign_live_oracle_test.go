package licensing

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonSignProgram signs each case with the real sign_license: stdin is a
// JSON list of [key, org_id, tier, issued_at, license_id, duration_days as decimal text ("" = the default)]; each answer is
// the license or "error: <ValueError text>".
const pythonSignProgram = `
import json, sys
from dev_health_ops.licensing.generator import sign_license
out = []
for key, org, tier, issued, license_id, days in json.loads(sys.stdin.read()):
    try:
        extra = {} if days == "" else {"duration_days": int(days)}
        out.append(sign_license(key, org_id=org, tier=tier, issued_at=issued, license_id=license_id, **extra))
    except ValueError as exc:
        out.append("error: " + str(exc))
print(json.dumps(out))
`

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

// TestSignLicenseMatchesLivePython holds SignLicense to the Python
// sign_license, license text for license text, over keys, tiers, org ids
// (non-ASCII, JSON-escaped characters), issue times and license ids, and
// the refused inputs.
func TestSignLicenseMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := signCases()
	input := make([][]any, len(cases))
	for index, c := range cases {
		licenseID := any(c.licenseID)
		if c.licenseID == "" {
			licenseID = "0"
		}
		input[index] = []any{c.key, c.org, c.tier, c.issued, licenseID, c.days}
	}
	stdin, _ := json.Marshal(input)
	command := exec.Command(python, "-c", pythonSignProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(stdin))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(cases) {
		t.Fatalf("python answered %d of %d cases", len(want), len(cases))
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
		pythonRefused := strings.HasPrefix(want[index], "error: ")
		switch {
		case err != nil && pythonRefused:
			if want := strings.TrimPrefix(want[index], "error: "); err.Error() != want {
				mismatches++
				t.Errorf("case %d %+v: go error %q, python error %q", index, c, err.Error(), want)
			}
		case err == nil && !pythonRefused:
			signed++
			if got != want[index] {
				mismatches++
				t.Errorf("case %d %+v:\n go     %s\n python %s", index, c, got, want[index])
			}
		default:
			mismatches++
			t.Errorf("case %d %+v: go (%q, %v), python %q", index, c, got, err, want[index])
		}
	}
	if signed < 60 {
		t.Fatalf("only %d cases signed on both planes; the comparison measured too little", signed)
	}
	t.Logf("%d cases, %d signed on both planes, %d mismatches", len(cases), signed, mismatches)
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-licensing-sign"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
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

// TestPythonB64DecodeMatchesLivePython holds pythonB64Decode to base64.b64decode
// over every text of up to five characters drawn from a set that covers the
// alphabet, padding, skipped characters and non-ASCII, plus random longer ones.
func TestPythonB64DecodeMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
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
	stdin, _ := json.Marshal(texts)
	command := exec.Command(python, "-c", pythonDecodeProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(stdin))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
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
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-licensing-b64decode"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// pythonVerifyProgram runs the real LicenseValidator over each license text on
// stdin (a JSON list) with the public key of the seed in argv[1] and a fixed
// clock: each answer is [valid, error, payload JSON, in_grace_period].
const pythonVerifyProgram = `
import base64, json, sys
from nacl.signing import SigningKey
from dev_health_ops.licensing.validator import LicenseValidator
seed = base64.b64decode(sys.argv[1])
public = base64.b64encode(bytes(SigningKey(seed).verify_key)).decode()
validator = LicenseValidator(public)
out = []
for text in json.loads(sys.stdin.read()):
    result = validator.validate(text, current_time=1790200000)
    out.append([result.valid, result.error, result.payload.model_dump_json() if result.payload else None, result.in_grace_period])
print(json.dumps(out))
`

// TestBigDurationLicensesVerifyIdenticallyGoSignedAndPythonSigned: a license
// whose expiry is beyond 64 bits is a capability only if the verifier accepts
// it. The Go-signed and the Python-signed license text is byte-identical, and the
// real Python LicenseValidator (the only license verifier: the Go api refuses
// LICENSE_KEY) gives the same verdict, payload and grace flag for both.
func TestBigDurationLicensesVerifyIdenticallyGoSignedAndPythonSigned(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	zero := base64.StdEncoding.EncodeToString(make([]byte, 32))
	days := []string{"365", "106751991167301", "9223372036854775808", "1" + strings.Repeat("0", 40)}
	var signInput [][]any
	var goSigned []string
	for _, d := range days {
		duration, _ := new(big.Int).SetString(d, 10)
		license, err := SignLicense(zero, LicenseRequest{OrgID: "o", Tier: "team", IssuedAt: 1790000000, LicenseID: "l", DurationDays: duration})
		if err != nil {
			t.Fatalf("%s days: %v", d, err)
		}
		goSigned = append(goSigned, license)
		signInput = append(signInput, []any{zero, "o", "team", 1790000000, "l", d})
	}
	run := func(program string, stdin any, args ...string) [][]any {
		raw, _ := json.Marshal(stdin)
		command := exec.Command(python, append([]string{"-c", program}, args...)...)
		command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
		command.Stdin = strings.NewReader(string(raw))
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
		}
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		var answers [][]any
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &answers); err != nil {
			// The sign program answers a flat list of strings.
			answers = nil
			var flat []any
			if err := json.Unmarshal([]byte(lines[len(lines)-1]), &flat); err != nil {
				t.Fatalf("decode: %v", err)
			}
			for _, item := range flat {
				answers = append(answers, []any{item})
			}
		}
		return answers
	}
	pythonSigned := run(pythonSignProgram, signInput)
	var pythonTexts []string
	for index, answer := range pythonSigned {
		text, _ := answer[0].(string)
		if text != goSigned[index] {
			t.Errorf("%s days: go-signed and python-signed license text differ", days[index])
		}
		pythonTexts = append(pythonTexts, text)
	}
	forGo := run(pythonVerifyProgram, goSigned, zero)
	forPython := run(pythonVerifyProgram, pythonTexts, zero)
	for index, d := range days {
		if fmt.Sprint(forGo[index]) != fmt.Sprint(forPython[index]) {
			t.Errorf("%s days: verifier verdict differs: go-signed %v, python-signed %v", d, forGo[index], forPython[index])
		}
		if forGo[index][0] != true {
			t.Errorf("%s days: the Python verifier refused the license: %v", d, forGo[index])
		}
	}
}
