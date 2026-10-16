package llmorgsettings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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

// OrgConfig returns the org's BYO config. ok=false means the org has no
// readable BYO settings, or the byo_llm gate is closed for it (the gate
// collapses to an empty map exactly as for every other reader here). err is
// the fail-closed gate-lookup failure or a database fault.
func (s Store) OrgConfig(ctx context.Context, orgID string) (OrgConfig, bool, error) {
	settings, err := s.loadGatedSettings(ctx, orgID)
	if err != nil {
		return OrgConfig{}, false, err
	}
	if len(settings) == 0 {
		return OrgConfig{}, false, nil
	}
	var newest *time.Time
	err = s.Pool.QueryRow(ctx, `
SELECT max(updated_at) FROM settings WHERE org_id = $1 AND category = $2`,
		orgID, settingsCategoryLLM).Scan(&newest)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return OrgConfig{}, false, fmt.Errorf("query org llm settings version: %w", err)
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
