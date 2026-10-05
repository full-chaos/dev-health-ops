package daily

import (
	"strings"
	"testing"
)

// CHAOS-8710: only a run that computes the whole organization may write a
// marker. The class survives a partition recompute ("#recompute:<nonce>") and a
// finalize redrive ("redrive-full:<nonce>"). Explicit-list runs never qualify.
func TestIsFullOrgGeneration(t *testing.T) {
	const nonce = "00000000-0000-4000-8000-0000000000aa"
	fanout := ScheduledFanoutGenerationPrefix + "2026-09-01T01:00:00Z"
	for name, test := range map[string]struct {
		generation string
		want       bool
	}{
		"scheduled fan-out":             {fanout, true},
		"post-sync":                     {"post-sync:" + nonce, true},
		"fan-out after a recompute":     {fanout + recomputeGenerationMarker + nonce, true},
		"post-sync after a recompute":   {"post-sync:" + nonce + recomputeGenerationMarker + nonce, true},
		"finalize-redriven full-org":    {redriveFullGenerationPrefix + nonce, true},
		"finalize-redriven partial":     {"redrive:" + nonce, false},
		"manual run":                    {ManualDailyGenerationPrefix + nonce, false},
		"external recompute":            {ExternalRecomputeGenerationPrefix + nonce, false},
		"unknown":                       {"something-else:" + nonce, false},
		"empty":                         {"", false},
		"overlong redrive-full":         {redriveFullGenerationPrefix + strings.Repeat("a", 60), false},
		"fan-out prefix as a substring": {"x" + fanout, false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := isFullOrgGeneration(test.generation); got != test.want {
				t.Fatalf("isFullOrgGeneration(%q) = %v, want %v", test.generation, got, test.want)
			}
		})
	}
}
