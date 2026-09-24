package providersync

import (
	"reflect"
	"testing"
)

// TestSupportedLegacyTargetsMatchPython pins the per-provider lists
// sync/datasets.py's supported_legacy_targets returns (printed from the
// Python module), including their _LEGACY_TARGET_ORDER order.
func TestSupportedLegacyTargetsMatchPython(t *testing.T) {
	want := map[string][]string{
		"github":       {"git", "prs", "blame", "cicd", "deployments", "security", "tests", "work-items"},
		"gitlab":       {"git", "prs", "blame", "cicd", "deployments", "incidents", "security", "tests", "work-items", "feature-flags"},
		"jira":         {"work-items", "operational"},
		"linear":       {"work-items"},
		"launchdarkly": {"feature-flags"},
		"pagerduty":    {"operational"},
		"unknown":      {},
	}
	for provider, targets := range want {
		if got := SupportedLegacyTargets(provider); !reflect.DeepEqual(got, targets) {
			t.Errorf("%s: %v, want %v", provider, got, targets)
		}
	}
}
