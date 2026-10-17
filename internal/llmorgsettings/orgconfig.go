package llmorgsettings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
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
	// Version is the hex sha256 over the org's BYO rows, sorted by key, each as
	// (key, updated_at, sha256 of the stored value): any write, delete or
	// value change moves it, including a delete of a row that was not the newest.
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
	// Only the BYO keys count: the llm category also holds unrelated settings
	// (platform certification profiles) that outlive a BYO delete.
	candidateKeys := append([]string{}, byoKeys...)
	if s.purpose != "" {
		candidateKeys = append(candidateKeys, PurposeKeys(s.purpose)...)
	}
	versionRows, err := tx.Query(ctx, `
SELECT key, updated_at, coalesce(value, '') FROM settings
WHERE org_id = $1 AND category = $2 AND key = ANY($3) ORDER BY key`,
		orgID, settingsCategoryLLM, candidateKeys)
	if err != nil {
		return OrgConfig{}, false, fmt.Errorf("query org llm settings version: %w", err)
	}
	type versionRow struct {
		key, value string
		updated    time.Time
	}
	var versioned []versionRow
	storedKeys := map[string]bool{}
	for versionRows.Next() {
		var row versionRow
		if err := versionRows.Scan(&row.key, &row.updated, &row.value); err != nil {
			versionRows.Close()
			return OrgConfig{}, false, fmt.Errorf("scan org llm settings version: %w", err)
		}
		versioned = append(versioned, row)
		if row.value != "" {
			storedKeys[row.key] = true
		}
	}
	versionRows.Close()
	if err := versionRows.Err(); err != nil {
		return OrgConfig{}, false, fmt.Errorf("iterate org llm settings version: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return OrgConfig{}, false, fmt.Errorf("end org llm settings snapshot: %w", err)
	}
	// The effective source is the purpose override when one is stored, else the
	// default row; the version covers that source only, so a write to the
	// other one does not move it. The choice rests on the STORED rows, not the
	// decrypted ones: an override whose key cannot be read must fail, never
	// fall back to the default.
	source := sourceKeys(func(key string) bool { return storedKeys[key] }, func(key string) string { return raw[key] }, s.purpose)
	digest := sha256.New()
	var stored []string
	for _, row := range versioned {
		if !slices.Contains(source, row.key) {
			continue
		}
		valueHash := sha256.Sum256([]byte(row.value))
		fmt.Fprintf(digest, "%s\x00%s\x00%x\n", row.key, row.updated.UTC().Format(time.RFC3339Nano), valueHash)
		if row.value != "" {
			stored = append(stored, row.key)
		}
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
	for _, key := range source {
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
	settings = project(settings, s.purpose)
	// The provider the writer stores is already stripped and lower-cased; an
	// empty, "auto", "mock" or "none" provider is "no BYO": the platform default.
	provider := normalizeProvider(settings[keyProvider])
	if provider == "auto" || provider == "mock" || provider == "none" {
		return OrgConfig{}, false, nil
	}
	version := hex.EncodeToString(digest.Sum(nil))
	return OrgConfig{
		Provider: provider,
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
