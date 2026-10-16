package providersync

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/teamprovider"
)

// logTeamProviderIdentity writes how the provider's team ids compared with the
// stored ones in one catalog write (CHAOS-9432): Debug, and Warn when a stored
// id changed (the key is now held by another team). Counts only: no team id,
// key or name leaves the store through it.
func logTeamProviderIdentity(ctx context.Context, tally teamprovider.Tally, provider, orgID string) {
	level := slog.LevelDebug
	if tally.Changed > 0 {
		level = slog.LevelWarn
	}
	slog.Default().Log(ctx, level, "team_provider_identity_written",
		"provider", provider, "org_id", orgID, "filled", tally.Filled,
		"unchanged", tally.Unchanged, "changed", tally.Changed, "unread", tally.Unread)
}
