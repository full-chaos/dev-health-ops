package usersbackfill

import (
	"context"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// runIDPattern is the audit allowlist's run_id rule.
var runIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,62}$`)

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

const (
	purgeVerificationSQL = `WITH d AS (DELETE FROM public.email_verification_tokens RETURNING id) SELECT count(*) FROM d`
	purgeResetSQL        = `WITH d AS (DELETE FROM public.password_reset_tokens RETURNING id) SELECT count(*) FROM d`
	countVerificationSQL = `SELECT count(id) FROM public.email_verification_tokens`
	countResetSQL        = `SELECT count(id) FROM public.password_reset_tokens`
)

// PurgeLegacyLinks deletes every row of go-api's public link-token tables
// (email_verification_tokens, password_reset_tokens) and writes one audit row
// (counts and run id) in the same transaction. It never names an auth table
// except security_audit_events through audit. It is the caller's job to run it
// only where users are owned (the verb refuses otherwise); the runtime role's
// DELETE grant is itself switch-gated. A dry run counts and writes nothing.
func PurgeLegacyLinks(ctx context.Context, pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, opts PurgeOptions) (PurgeReport, error) {
	rep := PurgeReport{DryRun: opts.DryRun}
	if opts.DryRun {
		if err := pool.QueryRow(ctx, countVerificationSQL).Scan(&rep.EmailVerificationTokens); err != nil {
			return rep, fmt.Errorf("counting public.email_verification_tokens: %w", err)
		}
		if err := pool.QueryRow(ctx, countResetSQL).Scan(&rep.PasswordResetTokens); err != nil {
			return rep, fmt.Errorf("counting public.password_reset_tokens: %w", err)
		}
		return rep, nil
	}
	apply := func(ctx context.Context, tx audit.TxOps) (map[string]any, error) {
		var verification, reset int64
		if err := tx.QueryRow(ctx, purgeVerificationSQL).Scan(&verification); err != nil {
			return nil, fmt.Errorf("purging public.email_verification_tokens: %w", err)
		}
		if err := tx.QueryRow(ctx, purgeResetSQL).Scan(&reset); err != nil {
			return nil, fmt.Errorf("purging public.password_reset_tokens: %w", err)
		}
		rep.EmailVerificationTokens, rep.PasswordResetTokens = verification, reset
		attributes := map[string]any{"purged_email_verification_tokens": verification, "purged_password_reset_tokens": reset}
		if opts.RunID != "" {
			attributes["run_id"] = opts.RunID
		}
		return attributes, nil
	}
	if err := audit.CommitPublicPurge(ctx, pool, schema, audit.EventLegacyLinkTokensPurged, apply); err != nil {
		return PurgeReport{DryRun: false}, err
	}
	return rep, nil
}
