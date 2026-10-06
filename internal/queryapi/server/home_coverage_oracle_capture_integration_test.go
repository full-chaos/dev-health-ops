//go:build integration

package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// chaos8509HomeCaptureDirEnv is set only by the venue workflow. It names an
// existing, runner-private directory where the failing fixture pairs are kept
// for the generator; the test never prints either body.
const chaos8509HomeCaptureDirEnv = "DHO_CHAOS8509_CAPTURE_DIR"

type chaos8509HomeCaptureEntry struct {
	Path   string
	Python string
	Go     string
}

type chaos8509HomeCapturePolicy struct {
	Entries          []chaos8509HomeCaptureEntry
	DifferenceLeaves int
	File             string
}

type chaos8509HomeCapture struct {
	Oracle           string `json:"oracle"`
	Case             string `json:"case"`
	Python           string `json:"python"`
	Go               string `json:"go"`
	PythonSHA256     string `json:"python_sha256"`
	GoSHA256         string `json:"go_sha256"`
	DifferenceLeaves int    `json:"difference_leaves"`
}

// chaos8509HomeCapturePolicies names the settled Home boundary. It is
// intentionally separate from the CHAOS-8169 ledger: the latter's business
// roots remain unchanged, and this capture cannot approve any other leaf.
var chaos8509HomeCapturePolicies = map[chaos8169HomeNoDataLedgerKey]chaos8509HomeCapturePolicy{
	chaos8169DictOrderHomeLedgerKey: {
		Entries: []chaos8509HomeCaptureEntry{
			{Path: "/freshness/coverage/repos_covered_pct", Python: "0.0", Go: "null"},
			{Path: "/freshness/coverage/prs_linked_to_issues_pct", Python: "0.0", Go: "null"},
			{Path: "/freshness/coverage/issues_with_cycle_states_pct", Python: "0.0", Go: "null"},
			{Path: "/data_confidence/coverage_pct", Python: "0.0", Go: "null"},
			{Path: "/data_confidence/caveats/0", Python: `"Coverage appears partial; treat cockpit signals as directional."`, Go: `"Coverage could not be computed from available lineage fields."`},
		},
		DifferenceLeaves: 197,
		File:             "dict-order-home-no-data.json",
	},
	chaos8169GraphQLEdgeHomeLedgerKeys["POST home"]: {
		Entries: []chaos8509HomeCaptureEntry{
			{Path: "/freshness/coverage/reposCoveredPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/prsLinkedToIssuesPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/issuesWithCycleStatesPct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/coveragePct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/caveats/0", Python: `"Coverage appears partial; treat cockpit signals as directional."`, Go: `"Coverage could not be computed from available lineage fields."`},
		},
		DifferenceLeaves: 204,
		File:             "graphql-edge-post-home.json",
	},
	chaos8169GraphQLEdgeHomeLedgerKeys["GET home"]: {
		Entries: []chaos8509HomeCaptureEntry{
			{Path: "/freshness/coverage/reposCoveredPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/prsLinkedToIssuesPct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/coveragePct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/caveats/0", Python: `"Coverage appears partial; treat cockpit signals as directional."`, Go: `"Coverage could not be computed from available lineage fields."`},
			{Path: "/freshness/coverage/issuesWithCycleStatesPct", Python: "0", Go: "null"},
		},
		DifferenceLeaves: 204,
		File:             "graphql-edge-get-home.json",
	},
}

// captureCHAOS8509HomePair stores a fixture-only pair from the existing real
// handler oracle after the unchanged CHAOS-8169 assertion has failed closed.
// It writes no default path, never replaces a previous capture, and refuses
// any pair whose values or complete difference set are not the settled five.
func captureCHAOS8509HomePair(t *testing.T, key chaos8169HomeNoDataLedgerKey, pythonBody, goBody string) {
	t.Helper()
	captureDir, enabled := os.LookupEnv(chaos8509HomeCaptureDirEnv)
	if !enabled {
		return
	}
	if strings.TrimSpace(captureDir) == "" || !filepath.IsAbs(captureDir) {
		t.Errorf("CHAOS-8509 capture directory must be a non-empty absolute path")
		return
	}
	policy, ok := chaos8509HomeCapturePolicies[key]
	if !ok {
		return
	}
	if t.Failed() {
		return
	}
	info, err := os.Stat(captureDir)
	if err != nil || !info.IsDir() {
		t.Errorf("CHAOS-8509 capture directory %q is not an existing directory: %v", captureDir, err)
		return
	}

	python := chaos8169HomeObject(t, pythonBody)
	goResponse := chaos8169HomeObject(t, goBody)
	differences := chaos8169HomeDifferenceLeaves(python, true, goResponse, true, "")
	ledger := chaos8169HomeLedgerFor(t, key)
	expected := make(map[string]chaos8509HomeCaptureEntry, len(policy.Entries))
	for _, entry := range policy.Entries {
		expected[entry.Path] = entry
	}
	ledgerEntries := make(map[string]chaos8169HomeNoDataLedgerEntry, len(ledger.Entries))
	wantLeaves := policy.DifferenceLeaves
	for _, entry := range ledger.Entries {
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, python, entry.Path)); got != entry.Python {
			t.Errorf("CHAOS-8509 capture existing Python ledger %s = %s, want %s", entry.Path, got, entry.Python)
			return
		}
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, goResponse, entry.Path)); got != entry.Go {
			t.Errorf("CHAOS-8509 capture existing Go ledger %s = %s, want %s", entry.Path, got, entry.Go)
			return
		}
		ledgerEntries[entry.Path] = entry
		wantLeaves += entry.Leaves
	}
	for _, root := range ledger.StrictRoots {
		if !chaos8509ExistingStrictRoot(t, python, goResponse, root) {
			return
		}
	}
	if len(differences) != wantLeaves {
		t.Errorf("CHAOS-8509 capture %s/%s difference leaves = %d, want %d", key.Oracle, key.Case, len(differences), wantLeaves)
		return
	}
	seen := make(map[string]int, len(expected))
	ledgerCounts := make(map[string]int, len(ledgerEntries))
	for _, path := range differences {
		if _, ok := expected[path]; ok {
			seen[path]++
			continue
		}
		root := chaos8509HomeCaptureRoot(path)
		if _, ok := ledgerEntries[root]; ok {
			ledgerCounts[root]++
			continue
		}
		t.Errorf("CHAOS-8509 capture %s/%s has unapproved difference at %s", key.Oracle, key.Case, path)
		return
	}
	for _, entry := range ledger.Entries {
		if got := ledgerCounts[entry.Path]; got != entry.Leaves {
			t.Errorf("CHAOS-8509 capture existing ledger leaves at %s = %d, want %d", entry.Path, got, entry.Leaves)
			return
		}
	}
	for _, entry := range policy.Entries {
		if seen[entry.Path] != 1 {
			t.Errorf("CHAOS-8509 capture %s/%s difference at %s = %d, want 1", key.Oracle, key.Case, entry.Path, seen[entry.Path])
			return
		}
		pythonValue, pythonOK := chaos8509HomeCaptureValue(python, entry.Path)
		goValue, goOK := chaos8509HomeCaptureValue(goResponse, entry.Path)
		if !pythonOK || !goOK {
			t.Errorf("CHAOS-8509 capture %s/%s path %s is absent", key.Oracle, key.Case, entry.Path)
			return
		}
		if got := chaos8169JSONText(t, pythonValue); got != entry.Python {
			t.Errorf("CHAOS-8509 capture Python %s = %s, want %s", entry.Path, got, entry.Python)
			return
		}
		if got := chaos8169JSONText(t, goValue); got != entry.Go {
			t.Errorf("CHAOS-8509 capture Go %s = %s, want %s", entry.Path, got, entry.Go)
			return
		}
	}

	payload := chaos8509HomeCapture{
		Oracle:           key.Oracle,
		Case:             key.Case,
		Python:           pythonBody,
		Go:               goBody,
		PythonSHA256:     chaos8509HomeCaptureDigest(pythonBody),
		GoSHA256:         chaos8509HomeCaptureDigest(goBody),
		DifferenceLeaves: len(differences),
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("CHAOS-8509 encode fixture capture: %v", err)
	}
	path := filepath.Join(captureDir, policy.File)
	if filepath.Dir(path) != filepath.Clean(captureDir) {
		t.Fatalf("CHAOS-8509 capture path escapes configured directory: %q", path)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Errorf("CHAOS-8509 write fixture capture %q: %v", path, err)
		return
	}
	if _, err := file.Write(append(raw, '\n')); err != nil {
		_ = file.Close()
		t.Errorf("CHAOS-8509 write fixture capture %q: %v", path, err)
		return
	}
	if err := file.Close(); err != nil {
		t.Errorf("CHAOS-8509 close fixture capture %q: %v", path, err)
		return
	}
	t.Logf("CHAOS-8509 wrote fixture Home capture oracle=%s case=%s python_sha256=%s go_sha256=%s", key.Oracle, key.Case, payload.PythonSHA256, payload.GoSHA256)
}

func chaos8509ExistingStrictRoot(t *testing.T, python, goResponse *pyjson.Object, root chaos8169HomeNoDataStrictRoot) bool {
	t.Helper()
	pythonValue := chaos8169HomeValue(t, python, root.Path)
	goValue := chaos8169HomeValue(t, goResponse, root.Path)
	if got, want := chaos8169JSONText(t, goValue), chaos8169JSONText(t, pythonValue); got != want {
		t.Errorf("CHAOS-8509 capture existing strict root %s changed: Go %s, Python %s", root.Path, got, want)
		return false
	}
	switch typed := pythonValue.(type) {
	case []pyjson.Value:
		if root.Kind != "array" || len(typed) != root.Length {
			t.Errorf("CHAOS-8509 capture existing strict root %s = array length %d, want %s length %d", root.Path, len(typed), root.Kind, root.Length)
			return false
		}
	case *pyjson.Object:
		if root.Kind != "object" || typed.Len() != root.Length {
			t.Errorf("CHAOS-8509 capture existing strict root %s = object length %d, want %s length %d", root.Path, typed.Len(), root.Kind, root.Length)
			return false
		}
	default:
		t.Errorf("CHAOS-8509 capture existing strict root %s is %T, want %s", root.Path, pythonValue, root.Kind)
		return false
	}
	return true
}

func chaos8509HomeCaptureRoot(path string) string {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return "/"
	}
	return "/" + strings.Split(trimmed, "/")[0]
}

func chaos8509HomeCaptureValue(object *pyjson.Object, path string) (pyjson.Value, bool) {
	var value pyjson.Value = object
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		switch typed := value.(type) {
		case *pyjson.Object:
			item, ok := typed.Get(component)
			if !ok {
				return nil, false
			}
			value = item
		case []pyjson.Value:
			index, err := strconv.Atoi(component)
			if err != nil || index < 0 || index >= len(typed) {
				return nil, false
			}
			value = typed[index]
		default:
			return nil, false
		}
	}
	return value, true
}

func chaos8509HomeCaptureDigest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}
