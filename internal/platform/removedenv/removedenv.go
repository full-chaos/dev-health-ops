// Package removedenv reports an environment key that no binary reads any more.
package removedenv

import (
	"context"
	"log/slog"
)

// OrgsOwnedKey is the removed organizations-owned switch.
const OrgsOwnedKey = "AUTH_ORGS_OWNED"

// ObserveOrgsOwned reports whether OrgsOwnedKey is set.
func ObserveOrgsOwned(ctx context.Context, lookup func(string) (string, bool), logger *slog.Logger) bool {
	return false
}
