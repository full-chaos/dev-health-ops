package admin

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// BudgetLockKey exposes the per-org budget advisory-lock key to the
// external test package, whose venue test holds that lock itself.
var BudgetLockKey = llmbudget.LockKey

// SetSetupCallbackTimeout shortens the deadline a PagerDuty OAuth callback
// runs under after its setup row is written, and returns the restore.
func SetSetupCallbackTimeout(d time.Duration) (restore func()) {
	previous := setupCallbackTimeout
	setupCallbackTimeout = d
	return func() { setupCallbackTimeout = previous }
}
