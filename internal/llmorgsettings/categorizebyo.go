package llmorgsettings

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// KeyCategorizeBYOEnabled is the llm settings row an org admin writes to opt
// the org in to categorization on its own model ("true"). No row, or any other
// value, reads as off. It is read nowhere else and has no env or platform
// default.
const KeyCategorizeBYOEnabled = "categorize_byo_enabled"

// PurposeCategorize names the categorization override of an org's BYO config
// ("purpose:categorize:<field>"). It is not in Purposes: the admin API accepts
// it only for an org that opted in, and nothing reads it for an org that did
// not.
const PurposeCategorize Purpose = "categorize"

// CategorizeBYOEnabled reads the org's opt-in: true only when the row says "true".
func CategorizeBYOEnabled(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	var value *string
	err := queryer.QueryRow(ctx, `SELECT value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, settingsCategoryLLM, KeyCategorizeBYOEnabled).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("llmorgsettings: categorize byo opt-in for org %q: %w", orgID, err)
	}
	return value != nil && *value == "true", nil
}

// CodeCategorizeBYOUnavailable is the one code for every refusal reason outside the closed set.
const CodeCategorizeBYOUnavailable = "categorize_byo_unavailable"

// CodeCategorizeBYONotConfigured is the refusal of an opted-in org whose saved
// configuration is not its own model (the gate answered platform or none).
const CodeCategorizeBYONotConfigured = string(ConfigNotConfigured)

// categorizeRefusalCodes is the closed set of reasons a categorization refusal
// may carry: the model gate's codes and the config reasons. Nothing else leaves
// this package.
var categorizeRefusalCodes = map[string]struct{}{
	CodeOwnModelGateClosed:           {},
	CodeServiceModelNotEntitled:      {},
	CodeAIFeaturesTurnedOff:          {},
	CodeOwnModelTurnedOff:            {},
	string(ConfigNotConfigured):      {},
	string(ConfigUnknownProvider):    {},
	string(ConfigMissingCredentials): {},
	string(ConfigInvalidBaseURL):     {},
	string(ConfigUnreadableRow):      {},
	CodeCategorizeBYOUnavailable:     {},
	"model_required":                 {},
}

// CategorizeRefusalCodes returns the closed set, for a test that pins it.
func CategorizeRefusalCodes() []string {
	out := make([]string, 0, len(categorizeRefusalCodes))
	for code := range categorizeRefusalCodes {
		out = append(out, code)
	}
	return out
}

// SafeCategorizeRefusalCode maps any reason to the closed set: an unknown code
// (a free-text error, a provider message, a URL) becomes the generic code.
func SafeCategorizeRefusalCode(code string) string {
	if _, ok := categorizeRefusalCodes[code]; ok {
		return code
	}
	return CodeCategorizeBYOUnavailable
}
