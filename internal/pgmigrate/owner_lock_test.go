package pgmigrate

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/ownermove"
)

// The ops owner move takes this walk's lock so no migrate hook runs while the owners change.
func TestTheOwnerMoveTakesTheWalkLock(t *testing.T) {
	if ownermove.PostgresWalkLockKey != lockKey {
		t.Fatalf("ownermove.PostgresWalkLockKey = %d, the walk's lockKey = %d", ownermove.PostgresWalkLockKey, lockKey)
	}
}
