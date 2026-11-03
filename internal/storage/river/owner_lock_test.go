package riverstore

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/ownermove"
)

// The ops owner move takes the River migration lock so no River migration runs while the owners change.
func TestTheOwnerMoveTakesTheRiverMigrationLock(t *testing.T) {
	if ownermove.RiverMigrationLockKey != migrationLockKey {
		t.Fatalf("ownermove.RiverMigrationLockKey = %d, migrationLockKey = %d", ownermove.RiverMigrationLockKey, migrationLockKey)
	}
}
