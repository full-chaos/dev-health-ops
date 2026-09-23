package orgs

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonRegistryProgram prints the REAL registry constants the entitlements
// read uses besides the decision engine.
const pythonRegistryProgram = `
import json
from dev_health_ops.licensing.registry import STANDARD_FEATURES
from dev_health_ops.licensing.types import TIER_ORDER
from dev_health_ops.models.licensing import TIER_LIMITS
print(json.dumps({
    "standard": [row[0] for row in STANDARD_FEATURES],
    "tier_order": [t.value for t in TIER_ORDER],
    "limits": {t.value: [[k, v, type(v).__name__] for k, v in limits.items()] for t, limits in TIER_LIMITS.items()},
}))
`

func TestRegistryMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, "-c", pythonRegistryProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live Python registry: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var result struct {
		Standard  []string            `json:"standard"`
		TierOrder []string            `json:"tier_order"`
		Limits    map[string][][3]any `json:"limits"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &result); err != nil {
		t.Fatalf("decode: %v: %s", err, output)
	}
	if strings.Join(result.Standard, ",") != strings.Join(StandardFeatureKeys, ",") {
		t.Fatalf("standard features: Python %v, Go %v", result.Standard, StandardFeatureKeys)
	}
	if strings.Join(result.TierOrder, ",") != strings.Join(TierOrder, ",") {
		t.Fatalf("tier order: Python %v, Go %v", result.TierOrder, TierOrder)
	}
	if len(result.Limits) != len(TierLimits) {
		t.Fatalf("limits tiers: Python %d, Go %d", len(result.Limits), len(TierLimits))
	}
	for tier, limits := range result.Limits {
		goLimits := TierLimits[tier]
		if len(goLimits) != len(limits) {
			t.Fatalf("%s: Python %d limits, Go %d", tier, len(limits), len(goLimits))
		}
		for index, row := range limits {
			goKind := "NoneType"
			var goNumber any
			switch value := goLimits[index].Value.(type) {
			case int64:
				goKind, goNumber = "int", float64(value)
			case float64:
				goKind, goNumber = "float", value
			}
			if goLimits[index].Key != row[0] || goNumber != row[1] || goKind != row[2] {
				t.Errorf("%s[%d]: Go %s=%v (%s), Python %v=%v (%v)", tier, index, goLimits[index].Key, goNumber, goKind, row[0], row[1], row[2])
			}
		}
	}
	proofDir := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proofDir == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proofDir, "api-orgs-registry"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
