package usersbackfill

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// PurgeOptions are the knobs of one legacy link-token purge run.
type PurgeOptions struct {
	DryRun bool
	RunID  string
}

// PurgeReport is the counts of one run, per table.
type PurgeReport struct {
	DryRun                  bool
	EmailVerificationTokens int64
	PasswordResetTokens     int64
}

// Lines renders the report as stable key=value lines: counts only.
func (r PurgeReport) Lines() []string {
	mode, suffix := "purge", "purged"
	if r.DryRun {
		mode, suffix = "dry-run", "would_purge"
	}
	return []string{
		"mode=" + mode,
		fmt.Sprintf("email_verification_tokens_%s=%d", suffix, r.EmailVerificationTokens),
		fmt.Sprintf("password_reset_tokens_%s=%d", suffix, r.PasswordResetTokens),
	}
}

// PurgeLegacyLinks is not implemented yet.
func PurgeLegacyLinks(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, opts PurgeOptions) (PurgeReport, error) {
	return PurgeReport{DryRun: opts.DryRun}, nil
}
