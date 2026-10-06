//go:build integration

package server

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const chaos8509HomePolicyNegativeChildEnv = "DHO_CHAOS8509_POLICY_NEGATIVE_CHILD"

func TestCHAOS8509HomeCapturePolicyGenerated(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "run", "-trimpath", "./internal/queryapi/server/testdata/chaos-8509/home_coverage_policy_generate.go", "-root", ".", "-check")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated CHAOS-8509 Home coverage policy is stale: %v\n%s", err, output)
	}
}

func TestCHAOS8509HomeCapturePolicyRecordedPairs(t *testing.T) {
	for _, key := range chaos8509HomeCapturePolicyOrder {
		key := key
		t.Run(key.Oracle+"/"+key.Case, func(t *testing.T) {
			policy, ok := chaos8509HomeCapturePolicies[key]
			if !ok {
				t.Fatalf("CHAOS-8509 generated policy has no %s/%s pair", key.Oracle, key.Case)
			}
			python, goBody, ok := chaos8509HomeCapturePolicyBodies(t, key, policy)
			if !ok {
				return
			}
			assertCHAOS8509HomeCapturePolicy(t, key, policy, python, goBody)
		})
	}
}

// chaos8509HomeCapturePolicyBodies makes the source measurement explicit. A
// missing capture, a stale raw-file digest, an altered inner body, or wrong
// hosted provenance fails before the bounded comparison is considered.
func chaos8509HomeCapturePolicyBodies(t *testing.T, key chaos8169HomeNoDataLedgerKey, policy chaos8509HomeCapturePolicy) (string, string, bool) {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Errorf("CHAOS-8509 find module root: %v", err)
		return "", "", false
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/queryapi/server/testdata/chaos-8509", policy.File))
	if err != nil {
		t.Errorf("CHAOS-8509 read recorded Home capture %s: %v", policy.File, err)
		return "", "", false
	}
	if got := sha256.Sum256(raw); fmtSHA256(got) != policy.FixtureSHA256 {
		t.Errorf("CHAOS-8509 recorded Home capture %s digest = %s, want %s", policy.File, fmtSHA256(got), policy.FixtureSHA256)
		return "", "", false
	}
	var capture chaos8509HomeCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Errorf("CHAOS-8509 decode recorded Home capture %s: %v", policy.File, err)
		return "", "", false
	}
	if capture.Oracle != key.Oracle || capture.Case != key.Case {
		t.Errorf("CHAOS-8509 recorded Home capture %s identity = %s/%s, want %s/%s", policy.File, capture.Oracle, capture.Case, key.Oracle, key.Case)
		return "", "", false
	}
	if capture.PythonSHA256 != policy.PythonSHA256 || capture.GoSHA256 != policy.GoSHA256 {
		t.Errorf("CHAOS-8509 recorded Home capture %s inner digests = Python %s Go %s, want Python %s Go %s", policy.File, capture.PythonSHA256, capture.GoSHA256, policy.PythonSHA256, policy.GoSHA256)
		return "", "", false
	}
	if got := chaos8509HomeCaptureDigest(capture.Python); got != policy.PythonSHA256 {
		t.Errorf("CHAOS-8509 recorded Home capture %s Python body digest = %s, want %s", policy.File, got, policy.PythonSHA256)
		return "", "", false
	}
	if got := chaos8509HomeCaptureDigest(capture.Go); got != policy.GoSHA256 {
		t.Errorf("CHAOS-8509 recorded Home capture %s Go body digest = %s, want %s", policy.File, got, policy.GoSHA256)
		return "", "", false
	}
	if !json.Valid([]byte(capture.Python)) || !json.Valid([]byte(capture.Go)) {
		t.Errorf("CHAOS-8509 recorded Home capture %s has invalid body JSON", policy.File)
		return "", "", false
	}
	if policy.SourceHead != "9c438f9ca4e59eaecfb790448731a7e72177aaf3" || policy.SourceRun != "37413067348" ||
		policy.SourceJob != "112105594511" || policy.SourceArtifact != "11390701993" ||
		policy.SourceReceiptSHA256 != "08a379f6ccf122291b1ff0997aaba0589cc750958663a6f6d36f0ba057c84dd1" {
		t.Errorf("CHAOS-8509 generated source provenance is not the recorded 37413067348/112105594511 capture")
		return "", "", false
	}
	if policy.ExpectedStatus != 200 || !policy.HeadersEqual {
		t.Errorf("CHAOS-8509 generated transport contract = status %d headers_equal %t, want status 200 and equal headers", policy.ExpectedStatus, policy.HeadersEqual)
		return "", "", false
	}
	return capture.Python, capture.Go, true
}

// assertCHAOS8509HomeCapturePolicy composes the unchanged CHAOS-8169 root
// ledger with the five generated coverage leaves. It rejects every other
// changed path, value, count, strict root, or missing measurement.
func assertCHAOS8509HomeCapturePolicy(t *testing.T, key chaos8169HomeNoDataLedgerKey, policy chaos8509HomeCapturePolicy, pythonBody, goBody string) bool {
	t.Helper()
	python := chaos8169HomeObject(t, pythonBody)
	goResponse := chaos8169HomeObject(t, goBody)
	ledger := chaos8169HomeLedgerFor(t, key)

	legacyLeaves := 0
	legacyEntries := make(map[string]chaos8169HomeNoDataLedgerEntry, len(ledger.Entries))
	for _, entry := range ledger.Entries {
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, python, entry.Path)); got != entry.Python {
			t.Errorf("CHAOS-8509 legacy Python %s = %s, want %s", entry.Path, got, entry.Python)
			return false
		}
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, goResponse, entry.Path)); got != entry.Go {
			t.Errorf("CHAOS-8509 legacy Go %s = %s, want %s", entry.Path, got, entry.Go)
			return false
		}
		legacyLeaves += entry.Leaves
		legacyEntries[entry.Path] = entry
	}
	if legacyLeaves != policy.LegacyDifferenceLeaves {
		t.Errorf("CHAOS-8509 legacy difference leaves = %d, want %d", legacyLeaves, policy.LegacyDifferenceLeaves)
		return false
	}
	for _, root := range ledger.StrictRoots {
		if !chaos8509ExistingStrictRoot(t, python, goResponse, root) {
			return false
		}
	}

	expected := make(map[string]chaos8509HomeCaptureEntry, len(policy.Entries))
	for _, entry := range policy.Entries {
		if _, duplicate := expected[entry.Path]; duplicate {
			t.Errorf("CHAOS-8509 generated policy has duplicate leaf %s", entry.Path)
			return false
		}
		expected[entry.Path] = entry
	}
	if len(expected) != 5 {
		t.Errorf("CHAOS-8509 generated policy leaves = %d, want exact five", len(expected))
		return false
	}

	differences := chaos8169HomeDifferenceLeaves(python, true, goResponse, true, "")
	if got, want := len(differences), policy.DifferenceLeaves; got != want {
		t.Errorf("CHAOS-8509 difference leaves = %d, want %d", got, want)
		return false
	}
	seen := make(map[string]int, len(expected))
	legacyCounts := make(map[string]int, len(legacyEntries))
	for _, path := range differences {
		if _, ok := expected[path]; ok {
			seen[path]++
			continue
		}
		root := chaos8509HomeCaptureRoot(path)
		if _, ok := legacyEntries[root]; !ok {
			t.Errorf("CHAOS-8509 unapproved difference at %s", path)
			return false
		}
		legacyCounts[root]++
	}
	for _, entry := range ledger.Entries {
		if got := legacyCounts[entry.Path]; got != entry.Leaves {
			t.Errorf("CHAOS-8509 legacy difference leaves at %s = %d, want %d", entry.Path, got, entry.Leaves)
			return false
		}
	}
	for _, entry := range policy.Entries {
		if got := seen[entry.Path]; got != 1 {
			t.Errorf("CHAOS-8509 approved difference at %s = %d, want 1", entry.Path, got)
			return false
		}
		pythonValue, pythonOK := chaos8509HomeCaptureValue(python, entry.Path)
		goValue, goOK := chaos8509HomeCaptureValue(goResponse, entry.Path)
		if !pythonOK || !goOK {
			t.Errorf("CHAOS-8509 approved path %s is missing", entry.Path)
			return false
		}
		if got := chaos8169JSONText(t, pythonValue); got != entry.Python {
			t.Errorf("CHAOS-8509 capture Python %s = %s, want %s", entry.Path, got, entry.Python)
			return false
		}
		if got := chaos8169JSONText(t, goValue); got != entry.Go {
			t.Errorf("CHAOS-8509 capture Go %s = %s, want %s", entry.Path, got, entry.Go)
			return false
		}
	}
	return true
}

func TestCHAOS8509HomeCapturePolicyNegativeGuards(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{name: "modified_root", want: "CHAOS-8509 capture existing strict root /tiles changed"},
		{name: "modified_value", want: "CHAOS-8509 difference leaves = 198, want 197"},
		{name: "modified_count", want: "CHAOS-8509 difference leaves = 197, want 196"},
		{name: "stale_capture", want: "CHAOS-8509 recorded Home capture dict-order-home-no-data.json digest"},
		{name: "missing_capture", want: "CHAOS-8509 read recorded Home capture missing-capture.json"},
	}
	for policyIndex, key := range chaos8509HomeCapturePolicyOrder {
		policy := chaos8509HomeCapturePolicies[key]
		for entryIndex, entry := range policy.Entries {
			cases = append(cases,
				struct{ name, want string }{name: fmt.Sprintf("leaf_python_%d_%d", policyIndex, entryIndex), want: "CHAOS-8509 capture Python " + entry.Path},
				struct{ name, want string }{name: fmt.Sprintf("leaf_go_%d_%d", policyIndex, entryIndex), want: "CHAOS-8509 capture Go " + entry.Path},
				struct{ name, want string }{name: fmt.Sprintf("leaf_path_%d_%d", policyIndex, entryIndex), want: "CHAOS-8509 unapproved difference at " + entry.Path},
			)
		}
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestCHAOS8509HomeCapturePolicyNegativeChild$")
			command.Env = append(os.Environ(), chaos8509HomePolicyNegativeChildEnv+"="+tc.name)
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("negative %s passed; expected its policy guard to fail\n%s", tc.name, output)
			}
			if !strings.Contains(string(output), tc.want) {
				t.Fatalf("negative %s failed without the expected guard %q:\n%s", tc.name, tc.want, output)
			}
		})
	}
}

func TestCHAOS8509HomeCapturePolicyNegativeChild(t *testing.T) {
	negative := os.Getenv(chaos8509HomePolicyNegativeChildEnv)
	if negative == "" {
		t.Skip("runs only as an expected-failure child of TestCHAOS8509HomeCapturePolicyNegativeGuards")
	}
	key := chaos8509HomeCapturePolicyOrder[0]
	policy := chaos8509CloneHomeCapturePolicy(chaos8509HomeCapturePolicies[key])
	python, goBody, ok := chaos8509HomeCapturePolicyBodies(t, key, policy)
	if !ok {
		return
	}
	switch negative {
	case "modified_root":
		goBody = chaos8169ReplaceOnce(t, goBody, `"title":"Understand"`, `"title":"Changed"`)
	case "modified_value":
		goBody = chaos8169ReplaceOnce(t, goBody, `"level":"low"`, `"level":"high"`)
	case "modified_count":
		policy.DifferenceLeaves--
	case "stale_capture":
		policy.FixtureSHA256 = strings.Repeat("0", len(policy.FixtureSHA256))
		chaos8509HomeCapturePolicyBodies(t, key, policy)
		return
	case "missing_capture":
		policy.File = "missing-capture.json"
		chaos8509HomeCapturePolicyBodies(t, key, policy)
		return
	default:
		parts := strings.Split(negative, "_")
		if len(parts) != 4 || parts[0] != "leaf" {
			t.Fatalf("unknown CHAOS-8509 negative %q", negative)
		}
		policyIndex, err := strconv.Atoi(parts[2])
		if err != nil || policyIndex < 0 || policyIndex >= len(chaos8509HomeCapturePolicyOrder) {
			t.Fatalf("invalid policy index in %q", negative)
		}
		entryIndex, err := strconv.Atoi(parts[3])
		if err != nil {
			t.Fatalf("invalid entry index in %q", negative)
		}
		key = chaos8509HomeCapturePolicyOrder[policyIndex]
		policy = chaos8509CloneHomeCapturePolicy(chaos8509HomeCapturePolicies[key])
		python, goBody, ok = chaos8509HomeCapturePolicyBodies(t, key, policy)
		if !ok {
			return
		}
		if entryIndex < 0 || entryIndex >= len(policy.Entries) {
			t.Fatalf("entry index %d is outside policy %s/%s", entryIndex, key.Oracle, key.Case)
		}
		switch parts[1] {
		case "python":
			policy.Entries[entryIndex].Python = "null"
		case "go":
			policy.Entries[entryIndex].Go = "0"
		case "path":
			policy.Entries[entryIndex].Path += "-moved"
		default:
			t.Fatalf("unknown leaf negative %q", negative)
		}
	}
	if assertCHAOS8509HomeCapturePolicy(t, key, policy, python, goBody) {
		t.Fatalf("negative %s passed; expected policy guard to fail", negative)
	}
}

func chaos8509CloneHomeCapturePolicy(policy chaos8509HomeCapturePolicy) chaos8509HomeCapturePolicy {
	policy.Entries = append([]chaos8509HomeCaptureEntry(nil), policy.Entries...)
	return policy
}
