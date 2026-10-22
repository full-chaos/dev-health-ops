package llmorgsettings

import (
	"context"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
// A WRONG (present but incorrect) key fails the exact same way, but is no
// longer silent about it -- see telemetry.go for the per-failure and
// first-use-key-validation signals loadRawSettings now emits.
type Store struct {
	Pool      *pgxpool.Pool
	Decryptor providerfoundation.FernetDecryptor
	// Now is injectable for tests exercising the org-override expiry edge;
	// nil means time.Now().
	Now func() time.Time

	purpose Purpose
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
func (s Store) loadRawSettings(ctx context.Context, orgID string) (orgSettings, error) {
	if s.Pool == nil {
		return nil, fmt.Errorf("llmorgsettings: Store.Pool is nil")
	}
	return s.loadRawSettingsFrom(ctx, s.Pool, orgID)
}

// rowQuerier is the read half of a pool or a transaction.
type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// loadRawSettingsFrom is loadRawSettings over an explicit querier, so a caller
// can read the rows and anything that must agree with them in one snapshot.
func (s Store) loadRawSettingsFrom(ctx context.Context, q rowQuerier, orgID string) (orgSettings, error) {
	if orgID == "" {
		return map[string]string{}, nil
	}
	rows, err := q.Query(ctx, `
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
			checkKeyOnFirstDecrypt(ctx, orgID, err)
			if err != nil {
				// Matches Python: `except ValueError: continue` -- a
				// corrupt/unreadable row is skipped, never fatal to the
				// rest of the org's settings. It is no longer SILENT
				// about it, though -- see telemetry.go.
				recordDecryptFailure(ctx, orgID)
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
func (s Store) loadGatedSettings(ctx context.Context, orgID string) (orgSettings, error) {
	settings, err := s.loadRawSettings(ctx, orgID)
	if err != nil {
		return nil, err
	}
	settings, turnedOff := withoutControlRows(settings)
	gated, err := s.applyGate(ctx, orgID, settings)
	if err != nil {
		return nil, err
	}
	// An org admin turned the saved config off: no reader sees an own model.
	if turnedOff {
		return orgSettings{}, nil
	}
	return markKeyInBaseURL(gated, project(gated, s.purpose)), nil
}

// keyInBaseURLMark is the reserved entry markKeyInBaseURL adds to the projected
// settings; it is no stored key and no setting name can equal it.
const keyInBaseURLMark = "\x00key_in_base_url"

// markKeyInBaseURL flags projected settings whose base_url carries any api key
// the org stores (the default row's or a purpose override's), including a key
// the projection dropped; the readers below refuse such a url before a lookup.
func markKeyInBaseURL(stored, projected orgSettings) orgSettings {
	if projected != nil && storedKeyInBaseURL(stored, projected[keyBaseURL]) {
		projected[keyInBaseURLMark] = "1"
	}
	return projected
}

func keyInBaseURLMarked(settings orgSettings) bool { return settings[keyInBaseURLMark] != "" }

// byoGateClosed reports whether the byo_llm gate is closed for an org that holds saved BYO settings.
func (s Store) byoGateClosed(ctx context.Context, orgID string) (bool, error) {
	gated, err := s.applyGate(ctx, orgID, orgSettings{keyProvider: "saved"})
	if err != nil {
		return false, err
	}
	return len(gated) == 0, nil
}

// applyGate is the byo_llm gate half of loadGatedSettings over already-read settings.
func (s Store) applyGate(ctx context.Context, orgID string, settings orgSettings) (orgSettings, error) {
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

// withoutControlRows removes the rows that switch a saved BYO config instead of
// being part of it, so an org holding only those is not read as a BYO org.
func withoutControlRows(settings orgSettings) (rest orgSettings, turnedOff bool) {
	turnedOff = ownModelTurnedOff(settings)
	delete(settings, KeyOwnModelEnabled)
	delete(settings, KeyPromptCache)
	return settings, turnedOff
}
