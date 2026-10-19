package llmorgsettings

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SavedQueryer is the read surface HasSavedBYO needs; *pgxpool.Pool has it.
type SavedQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// HasSavedBYO reports whether the org has a saved BYO LLM configuration: a
// stored, non-empty provider setting that is not auto, mock or none (the
// values OrgConfig reads as "no BYO"). The writer stores the provider in
// plain text. The byo_llm gate is NOT applied: a configuration behind a closed
// gate is still saved, and its consumer refuses it with its own reason.
func HasSavedBYO(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	var provider string
	err := queryer.QueryRow(ctx, `
SELECT coalesce(value, '') FROM settings
WHERE org_id = $1 AND category = $2 AND key = $3 AND coalesce(value, '') <> ''
ORDER BY updated_at DESC LIMIT 1`, orgID, settingsCategoryLLM, keyProvider).Scan(&provider)
	switch {
	case err == pgx.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("llmorgsettings: saved BYO provider for org %q: %w", orgID, err)
	}
	switch normalizeProvider(provider) {
	case "auto", "mock", "none":
		return false, nil
	default:
		return true, nil
	}
}
