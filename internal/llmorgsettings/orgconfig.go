package llmorgsettings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// OrgConfig is an org's stored BYO LLM configuration as an internal service
// (acr) reads it: the gated, decrypted settings rows, unvalidated. The
// consumer applies its own usability checks; this is the stored truth.
// APIKey stays a Hidden so no formatter, logger or JSON encoder of this
// struct can print it; the one caller that must serve it calls Reveal.
type OrgConfig struct {
	Provider string
	Model    string
	BaseURL  string
	APIKey   secrets.Hidden
	// Version changes whenever any of the org's llm settings rows is written:
	// the newest updated_at, RFC 3339 with nanoseconds, UTC.
	Version string
}

// ErrStoredSettingUnreadable means a stored llm setting exists but could not
// be read back (undecryptable). The message names the key, never a value.
var ErrStoredSettingUnreadable = errors.New("llmorgsettings: stored org llm setting is unreadable")

// OrgConfig returns the org's BYO config. ok=false means the org has no
// readable BYO settings, or the byo_llm gate is closed for it (the gate
// collapses to an empty map exactly as for every other reader here). err is
// the fail-closed gate-lookup failure or a database fault.
func (s Store) OrgConfig(ctx context.Context, orgID string) (OrgConfig, bool, error) {
	if s.Pool == nil {
		return OrgConfig{}, false, fmt.Errorf("llmorgsettings: Store.Pool is nil")
	}
	// One snapshot for the rows and the version: a settings save landing
	// between two reads must not pair the old values with the new version.
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return OrgConfig{}, false, fmt.Errorf("begin org llm settings snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	raw, err := s.loadRawSettingsFrom(ctx, tx, orgID)
	if err != nil {
		return OrgConfig{}, false, err
	}
	afterRowsRead()
	var newest *time.Time
	var stored []string
	// Only the BYO keys count: the llm category also holds unrelated settings
	// (platform certification profiles) that outlive a BYO delete.
	err = tx.QueryRow(ctx, `
SELECT max(updated_at), coalesce(array_agg(key) FILTER (WHERE value IS NOT NULL AND value <> ''), '{}')
FROM settings WHERE org_id = $1 AND category = $2 AND key = ANY($3)`,
		orgID, settingsCategoryLLM, byoKeys).Scan(&newest, &stored)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return OrgConfig{}, false, fmt.Errorf("query org llm settings version: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return OrgConfig{}, false, fmt.Errorf("end org llm settings snapshot: %w", err)
	}
	// loadRawSettings skips a row it cannot decrypt (wrong key, corrupt row).
	// Every other reader tolerates that; this one serves the key itself, so a
	// stored setting that did not come back is a failure, never a thinner
	// config and never "not configured". A closed byo_llm gate still wins: the
	// org then runs on the platform default whatever is stored.
	for _, key := range stored {
		if raw[key] != "" {
			continue
		}
		parsed, perr := uuid.Parse(orgID)
		if perr != nil {
			return OrgConfig{}, false, fmt.Errorf("%w: setting %q (org id is not a UUID)", ErrStoredSettingUnreadable, key)
		}
		state, gerr := byoLLMFlagState(ctx, s.Pool, parsed, s.now())
		if gerr != nil {
			return OrgConfig{}, false, fmt.Errorf("llmorgsettings: byo_llm flag state for org %q: %w", orgID, gerr)
		}
		if state == flagStateDisabled {
			return OrgConfig{}, false, nil
		}
		return OrgConfig{}, false, fmt.Errorf("%w: setting %q", ErrStoredSettingUnreadable, key)
	}
	byo := orgSettings{}
	for _, key := range byoKeys {
		if raw[key] != "" {
			byo[key] = raw[key]
		}
	}
	settings, err := s.applyGate(ctx, orgID, byo)
	if err != nil {
		return OrgConfig{}, false, err
	}
	if len(settings) == 0 {
		return OrgConfig{}, false, nil
	}
	version := ""
	if newest != nil {
		version = newest.UTC().Format(time.RFC3339Nano)
	}
	return OrgConfig{
		Provider: settings[keyProvider],
		Model:    settings[keyModel],
		BaseURL:  settings[keyBaseURL],
		APIKey:   secrets.NewHidden(settings[keyAPIKey]),
		Version:  version,
	}, true, nil
}

// afterRowsRead runs between the snapshot's row read and its version read; a
// test seam that lets a concurrent save land exactly there.
var afterRowsRead = func() {}

var byoKeys = []string{keyProvider, keyModel, keyAPIKey, keyBaseURL}
