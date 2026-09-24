package licensing

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
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
// JSON list of [key, org_id, tier, issued_at, license_id]; each answer is
// the license or "error: <ValueError text>".
const pythonSignProgram = `
import json, sys
from dev_health_ops.licensing.generator import sign_license
out = []
for key, org, tier, issued, license_id in json.loads(sys.stdin.read()):
    try:
        out.append(sign_license(key, org_id=org, tier=tier, issued_at=issued, license_id=license_id))
    except ValueError as exc:
        out.append("error: " + str(exc))
print(json.dumps(out))
`

type signCase struct {
	key, org, tier string
	issued         int64
	licenseID      string
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
				cases = append(cases, signCase{key, org, tier, issued[(index+len(org))%len(issued)], fmt.Sprintf("lic-%d-%s", index, tier)})
			}
		}
	}
	// Tier spelling and key shapes that sign_license refuses or reads
	// leniently.
	cases = append(cases,
		signCase{zero, "o", "Team", 5, "l"},
		signCase{zero, "o", "gold", 5, "l"},
		signCase{" " + zero[:10] + "\n" + zero[10:] + " ", "o", "team", 5, "l"},
		signCase{base64.StdEncoding.EncodeToString(make([]byte, 31)), "o", "team", 5, "l"},
		signCase{base64.StdEncoding.EncodeToString(make([]byte, 33)), "o", "team", 5, "l"},
		signCase{"not base64 at all!", "o", "team", 5, "l"},
		signCase{zero, "o", "team", 5, ""},
	)
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
		input[index] = []any{c.key, c.org, c.tier, c.issued, licenseID}
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
		got, err := SignLicense(c.key, LicenseRequest{OrgID: c.org, Tier: c.tier, IssuedAt: c.issued, LicenseID: licenseID})
		pythonRefused := strings.HasPrefix(want[index], "error: ")
		switch {
		case err != nil && pythonRefused:
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
