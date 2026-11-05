package admin

import (
	"context"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A stored key never serves a provider it was not written for (CHAOS-13096).
// The write side refuses or clears it on a provider change; the readers
// withhold a key row older than its provider row (llmorgsettings.KeyBound).

const llmProviderChangeNeedsKey = "provider_change_requires_api_key"

func providerChangeRefusal() *llmRefusal {
	return newLLMRefusal(http.StatusBadRequest, "error", llmProviderChangeNeedsKey, "feature", byoLLMFeature,
		"message", "A new provider needs its own API key.")
}

// scopeKeyRows are the provider and api_key rows of one stored config.
type scopeKeyRows struct {
	provider   string // "" when absent or empty
	providerAt time.Time
	keyStored  bool
	keyAt      time.Time
}

// keyBound is whether the stored key may be served with the stored provider.
func (s scopeKeyRows) keyBound() bool {
	return !s.keyStored || s.provider == "" || llmorgsettings.KeyBound(s.keyAt, s.providerAt)
}

// readScopeKeyRows reads one scope's rows; lock takes their row locks first, so
// a concurrent write cannot change them before the caller's transaction ends.
func readScopeKeyRows(ctx context.Context, db pgExecer, orgID string, scope llmorgsettings.KeyScope, lock bool) (scopeKeyRows, error) {
	if lock {
		if _, err := db.Exec(ctx, `
SELECT 1 FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($3) FOR UPDATE`,
			orgID, llmCategory, []string{scope.ProviderKey, scope.APIKeyKey}); err != nil {
			return scopeKeyRows{}, err
		}
	}
	var provider *string
	var providerAt, keyAt *time.Time
	if err := db.QueryRow(ctx, `
SELECT
  (SELECT value FROM settings WHERE org_id = $1 AND category = $2 AND key = $3),
  (SELECT updated_at FROM settings WHERE org_id = $1 AND category = $2 AND key = $3),
  (SELECT updated_at FROM settings WHERE org_id = $1 AND category = $2 AND key = $4 AND coalesce(value, '') <> '')`,
		orgID, llmCategory, scope.ProviderKey, scope.APIKeyKey).Scan(&provider, &providerAt, &keyAt); err != nil {
		return scopeKeyRows{}, err
	}
	var out scopeKeyRows
	if provider != nil && *provider != "" && providerAt != nil {
		out.provider, out.providerAt = *provider, *providerAt
	}
	if keyAt != nil {
		out.keyStored, out.keyAt = true, *keyAt
	}
	return out, nil
}

// keyStep is what a write does to the scope's stored api_key row.
type keyStep int

const (
	keyStepKeep   keyStep = iota // the row stays as it is
	keyStepWrite                 // the body's api_key replaces it
	keyStepDelete                // the row is deleted
	keyStepRefuse                // the write is refused: 400 provider_change_requires_api_key
)

// providerChangeKeyStep is the one provider-change rule of the default row and
// every purpose override. stored is the scope's rows before the write;
// inheritsDefault is whether the new provider of an override takes the default
// row's bound key (same provider).
func providerChangeKeyStep(stored scopeKeyRows, newProvider string, bodyKey, inheritsDefault bool) keyStep {
	if bodyKey {
		return keyStepWrite
	}
	next := llmorgsettings.NormalizeProvider(newProvider)
	if !stored.keyStored || llmorgsettings.NormalizeProvider(stored.provider) == next {
		return keyStepKeep
	}
	if inheritsDefault {
		return keyStepDelete
	}
	if !llmorgsettings.CredentialsComplete(next, "") {
		return keyStepRefuse
	}
	if llmorgsettings.IsKnownProvider(next) {
		return keyStepDelete
	}
	return keyStepKeep
}

// keyStepFor reads the scope's rows inside the write transaction and answers
// the provider-change rule for a write of provider to purpose ("" = default).
func keyStepFor(ctx context.Context, tx pgExecer, orgID string, purpose llmorgsettings.Purpose, provider string, bodyKey bool) (keyStep, error) {
	stored, err := readScopeKeyRows(ctx, tx, orgID, llmorgsettings.ScopeOf(purpose), true)
	if err != nil {
		return keyStepKeep, err
	}
	inherits := false
	if purpose != "" && !bodyKey && stored.keyStored {
		def, err := readScopeKeyRows(ctx, tx, orgID, llmorgsettings.ScopeOf(""), false)
		if err != nil {
			return keyStepKeep, err
		}
		inherits = def.suppliesKeyFor(provider)
	}
	return providerChangeKeyStep(stored, provider, bodyKey, inherits), nil
}

// suppliesKeyFor is whether the default row's stored key serves an override of
// provider: the same provider, and the key bound to it.
func (s scopeKeyRows) suppliesKeyFor(provider string) bool {
	return s.keyStored && s.keyBound() &&
		llmorgsettings.NormalizeProvider(s.provider) == llmorgsettings.NormalizeProvider(provider)
}

func deleteSettingRow(ctx context.Context, db pgExecer, orgID, key string) error {
	_, err := db.Exec(ctx, `DELETE FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`, orgID, llmCategory, key)
	return err
}

// bindDefaultKey withholds the default row's key from cfg when it was written
// for an earlier provider; the key stays in staleKey for the base_url checks.
func (cfg readinessBYOConfig) bindDefaultKey(bound bool) readinessBYOConfig {
	if !bound {
		cfg.staleKey, cfg.apiKey = cfg.apiKey, secrets.NewHidden("")
	}
	return cfg
}

// defaultKeyBound is keyBound of the default row's stored rows.
func defaultKeyBound(ctx context.Context, db pgExecer, orgID string) (bool, error) {
	rows, err := readScopeKeyRows(ctx, db, orgID, llmorgsettings.ScopeOf(""), false)
	if err != nil {
		return false, err
	}
	return rows.keyBound(), nil
}
