package syncdispatchruntime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/jackc/pgx/v5"
)

// budgetAdvisoryLockKey ports _advisory_lock_key verbatim: SHA-256 of the
// raw budget-key string, truncated to 63 bits so it fits a signed Postgres
// bigint. Same algorithm as dispatch_guard.go's bucketAdvisoryLockKey, just
// hashing budget_key's own string form directly rather than a composed
// dispatchBucket tuple -- Python duplicates this helper too (guard.py's
// _bucket_advisory_lock_key and budget_guard.py's _advisory_lock_key are
// separately defined, byte-identical algorithms over different input
// shapes), so this is ported as its own function rather than folded into
// bucketAdvisoryLockKey.
func budgetAdvisoryLockKey(budgetKey string) int64 {
	digest := sha256.Sum256([]byte(budgetKey))
	keyUint := binary.BigEndian.Uint64(digest[:8])
	return int64(keyUint & ((1 << 63) - 1))
}

// acquireBudgetAdvisoryLocks ports _acquire_budget_advisory_locks verbatim.
// budgetKeys MUST already be sorted (callers pass sort.Strings'd keys, same
// as enforce_run's sorted(budget_keys)) -- a consistent global lock order
// across every concurrent pass is the whole deadlock defence, same
// reasoning as acquireBucketAdvisoryLocks's own doc comment.
func acquireBudgetAdvisoryLocks(ctx context.Context, tx pgx.Tx, budgetKeys []string) error {
	keys := make([]int64, len(budgetKeys))
	for index, budgetKey := range budgetKeys {
		keys[index] = budgetAdvisoryLockKey(budgetKey)
	}
	return acquireAdvisoryLocksBounded(ctx, tx, keys, "budget", dispatchLockWaitBudget)
}
