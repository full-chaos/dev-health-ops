package session

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
)

// The store's refresh lifetime is the rule's one value, and that value is 7 days (ACP-ADR-03).
func TestDefaultRefreshTTLIsTheRulesOneValue(t *testing.T) {
	for name, got := range map[string]time.Duration{"session.DefaultRefreshTTL": DefaultRefreshTTL, "sessionrule.DefaultRefreshTTL": sessionrule.DefaultRefreshTTL} {
		if got != 7*24*time.Hour {
			t.Errorf("%s = %v, want 168h", name, got)
		}
	}
}
