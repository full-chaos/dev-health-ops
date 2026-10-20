package llmorgsettings

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/jackc/pgx/v5"
)

// SavedQueryer is the read surface HasSavedBYO needs; *pgxpool.Pool and a
// pgx.Tx have it.
type SavedQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// savedProviderKeys are the provider keys of every BYO config an org can save:
// the default row and each purpose override.
func savedProviderKeys() []string {
	keys := []string{keyProvider}
	for _, purpose := range Purposes {
		keys = append(keys, PurposeKey(purpose, keyProvider))
	}
	return keys
}

// HasSavedBYO reports whether the org has a saved BYO LLM configuration of
// ANY purpose: a stored default or purpose-override provider that is not
// auto, mock or none (the values OrgConfig reads as "no BYO"). The writer
// stores the provider in plain text. The byo_llm gate is NOT applied: a
// configuration behind a closed gate is still saved, and its consumer
// refuses it with its own reason.
func HasSavedBYO(ctx context.Context, queryer SavedQueryer, orgID string) (bool, error) {
	var encoded []byte
	err := queryer.QueryRow(ctx, `
SELECT coalesce(json_agg(coalesce(value, '')), '[]'::json) FROM settings
WHERE org_id = $1 AND category = $2 AND key = ANY($3) AND coalesce(value, '') <> ''`,
		orgID, settingsCategoryLLM, savedProviderKeys()).Scan(&encoded)
	if err != nil {
		return false, fmt.Errorf("llmorgsettings: saved BYO providers for org %q: %w", orgID, err)
	}
	var providers []string
	if err := json.Unmarshal(encoded, &providers); err != nil {
		return false, fmt.Errorf("llmorgsettings: saved BYO providers for org %q: %w", orgID, err)
	}
	for _, provider := range providers {
		switch normalizeProvider(provider) {
		case "auto", "mock", "none":
		default:
			return true, nil
		}
	}
	return false, nil
}

// EffectiveServiceModel is the acr_service_model every reader serves (the acr
// entitlement route and the admin service-model GET), from the licensing
// decision for the key: OFF when the feature is globally disabled or in an
// invalid state (Decide checks that before any override), the org's explicit
// override when one is active, else ON for an org with a saved BYO LLM
// configuration of any purpose (auto-enrolment), else the plan or license
// decision.
func EffectiveServiceModel(ctx context.Context, queryer SavedQueryer, orgID string, decision licensing.Decision) (bool, error) {
	switch decision.Reason {
	case licensing.ReasonEnabledByOrgOverride, licensing.ReasonOrgOverrideDisabled,
		licensing.ReasonGlobalDisabled, licensing.ReasonInvalidFeatureState:
		return decision.Allowed, nil
	}
	saved, err := HasSavedBYO(ctx, queryer, orgID)
	if err != nil {
		return false, err
	}
	return saved || decision.Allowed, nil
}
