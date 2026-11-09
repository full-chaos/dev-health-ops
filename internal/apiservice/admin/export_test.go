package admin

import (
	"context"
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

// ReadinessFingerprint exposes this port's credential-only readiness
// fingerprint so the status venue oracle can seed records that match the
// org's CURRENT BYO config, as a real certification would.
var ReadinessFingerprint = readinessFingerprint

// SetOrgDeletionDeleteHook runs hook after each ALTER DELETE of an org
// deletion's ClickHouse purge, and returns the restore.
func SetOrgDeletionDeleteHook(hook func(ctx context.Context, table string, pass int)) (restore func()) {
	previous := orgDeletionDeleteHook
	orgDeletionDeleteHook = hook
	return func() { orgDeletionDeleteHook = previous }
}
