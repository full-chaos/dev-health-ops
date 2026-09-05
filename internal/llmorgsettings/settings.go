package llmorgsettings

import (
	"context"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const settingsCategoryLLM = "llm"

const (
	keyProvider = "provider"
	keyModel    = "model"
	keyAPIKey   = "api_key"
	keyBaseURL  = "base_url"
)

// Store is the org-scoped LLM settings reader. Pool is the registry
// Postgres pool query-api already constructs (query_route.go's
// RegistryPostgresURI) -- this package never opens its own pool. Decryptor
// must be keyed by the same SETTINGS_ENCRYPTION_KEY/SETTINGS_ENCRYPTION_SALT
// the settings row was written under (providerfoundation.NewFernetDecryptor);
// a zero-value Decryptor makes every encrypted row unreadable (Decrypt
// returns ErrCredentialInvalid, treated the same as a corrupt row -- skipped,
// not fatal, matching _load_org_llm_settings' `except ValueError: continue`).
type Store struct {
	Pool      *pgxpool.Pool
	Decryptor providerfoundation.FernetDecryptor
	// Now is injectable for tests exercising the org-override expiry edge;
	// nil means time.Now().
	Now func() time.Time
}

func (s Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

// loadRawSettings ports _load_org_llm_settings' row-read/decrypt loop
// ONLY (its byo_llm feature-flag gate is loadGatedSettings below, kept
// separate so a caller that only needs the flag decision -- none exist
// yet, but the split mirrors Python's own two-step shape and keeps this
// function unit-testable without a feature_flags fixture).
func (s Store) loadRawSettings(ctx context.Context, orgID string) (map[string]string, error) {
	if s.Pool == nil {
		return nil, fmt.Errorf("llmorgsettings: Store.Pool is nil")
	}
	if orgID == "" {
		return map[string]string{}, nil
	}
	rows, err := s.Pool.Query(ctx, `
SELECT key, value, is_encrypted FROM settings
WHERE org_id = $1 AND category = $2`, orgID, settingsCategoryLLM)
	if err != nil {
		return nil, fmt.Errorf("query org llm settings: %w", err)
	}
	defer rows.Close()

	settings := map[string]string{}
	for rows.Next() {
		var key string
		var value *string
		var isEncrypted bool
		if err := rows.Scan(&key, &value, &isEncrypted); err != nil {
			return nil, fmt.Errorf("scan org llm setting: %w", err)
		}
		if value == nil || *value == "" {
			continue
		}
		resolved := *value
		if isEncrypted {
			plain, err := s.Decryptor.Decrypt(secrets.NewValue(resolved))
			if err != nil {
				// Matches Python: `except ValueError: continue` -- a
				// corrupt/unreadable row is skipped, never fatal to the
				// rest of the org's settings.
				continue
			}
			resolved = string(plain)
		}
		if resolved != "" {
			settings[key] = resolved
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate org llm settings: %w", err)
	}
	return settings, nil
}

// loadGatedSettings ports _load_org_llm_settings in full: an org with no
// BYO settings at all skips the feature-flag lookup entirely (nothing to
// gate, no data-residency concern) and returns an empty map; an org WITH
// settings is gated on byoLLMFlagState -- "disabled" clears the settings
// (falls back to the platform default), "enabled"/"unregistered" pass
// them through unchanged. A flag-lookup FAILURE for a BYO-configured org
// is NOT swallowed: it is returned as an error so every caller in this
// package fails closed and propagates, exactly matching
// _apply_byo_llm_flag_gate's `raise LLMAuthError` -- a genuine licensing-
// store fault must never silently reroute a tenant's BYO traffic to the
// platform LLM.
func (s Store) loadGatedSettings(ctx context.Context, orgID string) (map[string]string, error) {
	settings, err := s.loadRawSettings(ctx, orgID)
	if err != nil {
		return nil, err
	}
	if len(settings) == 0 {
		return settings, nil
	}
	parsedOrgID, err := uuid.Parse(orgID)
	if err != nil {
		return nil, fmt.Errorf(
			"llmorgsettings: org %q has BYO LLM settings but is not a valid UUID; "+
				"refusing to resolve so its BYO traffic is not silently rerouted "+
				"to the platform default: %w", orgID, err)
	}
	state, err := byoLLMFlagState(ctx, s.Pool, parsedOrgID, s.now())
	if err != nil {
		return nil, fmt.Errorf(
			"llmorgsettings: unable to determine the byo_llm feature flag "+
				"state for org %q with BYO LLM settings; refusing to resolve "+
				"so its BYO traffic is not silently rerouted to the platform "+
				"default: %w", orgID, err)
	}
	if state == flagStateDisabled {
		return map[string]string{}, nil
	}
	return settings, nil
}
