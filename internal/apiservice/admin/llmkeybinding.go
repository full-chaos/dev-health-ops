package admin

import (
	"context"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// A stored key never reaches a provider or base_url it was not written for
// (CHAOS-13096). Every key read here goes through scopeKey
// (llmorgsettings.ReadScopeKey); the writes refuse, keep or delete the key on a
// change of either.

const llmTargetChangeNeedsKey = "provider_or_base_url_change_requires_api_key"

func targetChangeRefusal() *llmRefusal {
	return newLLMRefusal(http.StatusBadRequest, "error", llmTargetChangeNeedsKey, "feature", byoLLMFeature,
		"message", "A new provider or base URL needs its API key entered again.")
}

// scopeKey is the one read of a stored api_key in this package. open=false
// reads presence and binding only; lock takes the scope's row locks (inside a
// write transaction).
func (h *handlers) scopeKey(ctx context.Context, db pgExecer, orgID string, purpose llmorgsettings.Purpose, lock, open bool) (llmorgsettings.ScopeKey, error) {
	var opener llmorgsettings.KeyOpener
	if open {
		opener = func(key string, value *string, encrypted bool) (*string, error) {
			return h.settingValue(&setting{Key: key, Value: value, IsEncrypted: encrypted})
		}
	}
	return llmorgsettings.ReadScopeKey(ctx, db, orgID, purpose, lock, opener)
}

// keyStep is what a write does to the scope's stored api_key row.
type keyStep int

const (
	keyStepKeep   keyStep = iota // the row stays as it is
	keyStepRebind                // the row stays and is re-stamped: provider and base_url did not change
	keyStepWrite                 // the body's api_key replaces it
	keyStepDelete                // the row is deleted
	keyStepRefuse                // the write is refused: 400 provider_or_base_url_change_requires_api_key
)

// keyTarget is the provider and base_url a key is sent to.
type keyTarget struct{ provider, baseURL string }

// storedProvider is a provider name as the writers store it.
func storedProvider(name string) string { return pythonparity.Lower(pythonparity.Strip(name)) }

func (t keyTarget) same(other keyTarget) bool {
	return storedProvider(t.provider) == storedProvider(other.provider) && t.baseURL == other.baseURL
}

// targetChangeKeyStep is the one rule of the default row and every purpose
// override. stored is the scope's key before the write; inheritsDefault is
// whether the new target of an override takes the default row's bound key.
func targetChangeKeyStep(stored llmorgsettings.ScopeKey, next keyTarget, bodyKey, inheritsDefault bool) keyStep {
	if bodyKey {
		return keyStepWrite
	}
	if !stored.Stored() {
		return keyStepKeep
	}
	if next.same(keyTarget{stored.Provider, stored.BaseURL}) {
		if stored.Bound() {
			return keyStepRebind
		}
		return keyStepKeep
	}
	if inheritsDefault {
		return keyStepDelete
	}
	provider := storedProvider(next.provider)
	if !llmorgsettings.CredentialsComplete(provider, "") {
		return keyStepRefuse
	}
	if llmorgsettings.IsKnownProvider(provider) {
		return keyStepDelete
	}
	// No BYO ("", auto, mock, none) or an unknown provider: no reader serves the
	// kept row (its provider or base_url row is newer), and a later change to a
	// provider that needs a key is refused without one.
	return keyStepKeep
}

// suppliesKeyFor is whether the default row's stored key serves an override
// whose target is next: the same provider and base_url, and the key bound.
func suppliesKeyFor(def llmorgsettings.ScopeKey, next keyTarget) bool {
	return def.Bound() && next.same(keyTarget{def.Provider, def.BaseURL})
}

// keyStepFor reads the scope inside the write transaction and answers the rule
// for a write of next to purpose ("" = default).
func (h *handlers) keyStepFor(ctx context.Context, tx pgExecer, orgID string, purpose llmorgsettings.Purpose, next keyTarget, bodyKey bool) (keyStep, error) {
	stored, err := h.scopeKey(ctx, tx, orgID, purpose, true, false)
	if err != nil {
		return keyStepKeep, err
	}
	inherits := false
	if purpose != "" && !bodyKey && stored.Stored() {
		def, err := h.scopeKey(ctx, tx, orgID, "", false, false)
		if err != nil {
			return keyStepKeep, err
		}
		inherits = suppliesKeyFor(def, next)
	}
	return targetChangeKeyStep(stored, next, bodyKey, inherits), nil
}

// applyKeyStep runs the step's row change inside the write transaction, after
// the provider and base_url rows are written.
func (h *handlers) applyKeyStep(ctx context.Context, tx pgExecer, orgID string, purpose llmorgsettings.Purpose, step keyStep) error {
	scope := llmorgsettings.ScopeOf(purpose)
	switch step {
	case keyStepDelete:
		_, err := tx.Exec(ctx, `DELETE FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`, orgID, llmCategory, scope.APIKeyKey)
		return err
	case keyStepRebind:
		// An unchanged provider or base_url row can still move (its description
		// is rewritten); the bound key moves with it.
		_, err := tx.Exec(ctx, `
UPDATE settings AS k SET updated_at = scope.at
FROM (SELECT max(updated_at) AS at FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($4)) AS scope
WHERE k.org_id = $1 AND k.category = $2 AND k.key = $3 AND k.updated_at < scope.at`,
			orgID, llmCategory, scope.APIKeyKey, []string{scope.ProviderKey, scope.BaseURLKey})
		return err
	}
	return nil
}

// bindDefaultKey sets cfg's key from the accessor: served only when bound; the
// stored key stays in staleKey for the base_url checks.
func (cfg readinessBYOConfig) bindDefaultKey(key llmorgsettings.ScopeKey) readinessBYOConfig {
	cfg.apiKey = secrets.NewHidden(key.Served())
	if key.Stored() && !key.Bound() {
		cfg.staleKey = secrets.NewHidden(key.ForChecks())
	}
	return cfg
}
