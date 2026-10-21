package llmorgsettings

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// KeyOwnModelEnabled is the llm settings row an org admin writes to turn the
// org's saved BYO configuration off ("false") and on ("true") without changing
// it. No row reads as on.
const KeyOwnModelEnabled = "own_model_enabled"

func ownModelTurnedOff(settings orgSettings) bool {
	return settings[KeyOwnModelEnabled] == "false"
}

// OwnModelEnabled reads the org's own-model toggle: false only when the row
// says "false".
func OwnModelEnabled(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	var value *string
	err := queryer.QueryRow(ctx, `SELECT value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, settingsCategoryLLM, KeyOwnModelEnabled).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("llmorgsettings: own model toggle for org %q: %w", orgID, err)
	}
	return value == nil || *value != "false", nil
}
