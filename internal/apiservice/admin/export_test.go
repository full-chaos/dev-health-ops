package admin

import "github.com/full-chaos/dev-health-ops/internal/llmbudget"

// BudgetLockKey exposes the per-org budget advisory-lock key to the
// external test package, whose venue test holds that lock itself.
var BudgetLockKey = llmbudget.LockKey
