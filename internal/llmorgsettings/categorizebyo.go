package llmorgsettings

import (
	"context"
	"fmt"
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

// categorizeOptInSQL reads the opt-in through the definer function of pgmigrate
// 0156: the worker's role holds EXECUTE on it and nothing on settings. The
// function answers true only when the row's value is exactly "true".
const categorizeOptInSQL = `SELECT public.categorize_byo_opted_in($1)`

// CategorizeBYOEnabled reads the org's opt-in: true only when the row says "true".
// Any fault, a missing answer included, is an error, never "off".
func CategorizeBYOEnabled(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	var optedIn *bool
	if err := queryer.QueryRow(ctx, categorizeOptInSQL, orgID).Scan(&optedIn); err != nil {
		return false, fmt.Errorf("llmorgsettings: categorize byo opt-in for org %q: %w", orgID, err)
	}
	if optedIn == nil {
		return false, fmt.Errorf("llmorgsettings: categorize byo opt-in for org %q: no answer", orgID)
	}
	return *optedIn, nil
}

// CodeCategorizeBYOUnavailable is the one code for every refusal reason outside the closed set.
const CodeCategorizeBYOUnavailable = "categorize_byo_unavailable"

// CodeCategorizeBYOOptInUnreadable is the refusal of a run whose org opt-in could
// not be read: the run fails closed, never on the platform model.
const CodeCategorizeBYOOptInUnreadable = "categorize_byo_optin_unreadable"

// CodeCategorizeBYOModelUnreported is the refusal of an answer that names no model:
// the org's provider reported none, so the answer cannot be tied to the configured model.
const CodeCategorizeBYOModelUnreported = "categorize_byo_model_unreported"

// CodeCategorizeBYOModelUnsupported is the refusal of a saved model the clef
// backend does not serve (it serves clef and clef-flash).
const CodeCategorizeBYOModelUnsupported = "categorize_byo_model_unsupported"

// CodeCategorizeBYONotConfigured is the refusal of an opted-in org whose saved
// configuration is not its own model (the gate answered platform or none).
const CodeCategorizeBYONotConfigured = string(ConfigNotConfigured)

// categorizeRefusalCodes is the closed set of reasons a categorization refusal
// may carry: the model gate's codes and the config reasons. Nothing else leaves
// this package.
var categorizeRefusalCodes = map[string]struct{}{
	CodeOwnModelGateClosed:            {},
	CodeServiceModelNotEntitled:       {},
	CodeAIFeaturesTurnedOff:           {},
	CodeOwnModelTurnedOff:             {},
	string(ConfigNotConfigured):       {},
	string(ConfigUnknownProvider):     {},
	string(ConfigMissingCredentials):  {},
	string(ConfigInvalidBaseURL):      {},
	string(ConfigUnreadableRow):       {},
	CodeCategorizeBYOUnavailable:      {},
	CodeCategorizeBYOModelUnreported:  {},
	CodeCategorizeBYOModelUnsupported: {},
	CodeCategorizeBYOOptInUnreadable:  {},
	"model_required":                  {},
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
