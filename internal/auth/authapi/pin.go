package authapi

import (
	"context"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// pinVerdict is a refreshed session's pin against the user's token_version.
type pinVerdict int

const (
	pinCurrent pinVerdict = iota
	// pinMissing is a session row from before migration 0015: it is refused
	// and left as it is.
	pinMissing
	// pinStale is a session created before the user's token_version moved (a
	// go-api password reset): it is revoked and refused.
	pinStale
)

// checkPin decides a refresh by the session's legacy_token_version. An access
// token carries tv = users.token_version, so without this a session created
// before a password reset would mint tokens that pass go-api's tv check.
func checkPin(ctx context.Context, logger *slog.Logger, record session.Record, user UserState) pinVerdict {
	switch {
	case record.LegacyTokenVersion == nil:
		logger.WarnContext(ctx, "refresh refused: session has no token_version pin",
			"session_id", record.ID.String(), "reason_code", reasonRevisionStale)
		return pinMissing
	case *record.LegacyTokenVersion != user.TokenVersion:
		return pinStale
	default:
		return pinCurrent
	}
}
