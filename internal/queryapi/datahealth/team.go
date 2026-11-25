package datahealth

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
)

// ErrUnknownTeam means a non-empty team id is not an active team of the org.
var ErrUnknownTeam = errors.New("team is not a team of the organization")

// LegacyPseudoTeamReason is the reason of the log line and counter for a legacy pseudo team.
const LegacyPseudoTeamReason = "legacy_pseudo_team"

// Legacy pseudo team kinds, as counted. The raw value is never logged.
const (
	LegacyKindAll     = "all"
	LegacyKindDefault = "default"
	LegacyKindOrgID   = "org_id"
)

const activeTeamSQL = `SELECT id FROM (` + activeteams.IDsSubquery + `) WHERE id = {team:String} LIMIT 1`

// ResolveTeam turns the requested team into the team the reads use. No team
// and "" read org-wide. "ALL", "default" and the caller's own org id are
// legacy pseudo teams that deployed web bundles still send: they read
// org-wide too, and legacyKind names which one it was. Any other value must be
// an active team of the org, else ErrUnknownTeam.
func (r *Reader) ResolveTeam(ctx context.Context, orgID string, requested *string) (team, legacyKind string, err error) {
	if requested == nil || *requested == "" {
		return "", "", nil
	}
	value := *requested
	switch {
	case value == "ALL":
		return "", LegacyKindAll, nil
	case value == "default":
		return "", LegacyKindDefault, nil
	case value == orgID:
		return "", LegacyKindOrgID, nil
	}
	if r.ClickHouse == nil {
		return "", "", errors.New("team lookup: no clickhouse client")
	}
	found := false
	err = runRows(ctx, r.ClickHouse, activeTeamSQL,
		[]clickhouse.Binding{{Name: "org_id", Value: orgID}, {Name: "team", Value: value}},
		func(rows clickhouse.RowScanner) error {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			found = true
			return nil
		})
	if err != nil {
		return "", "", fmt.Errorf("team lookup: %w", err)
	}
	if !found {
		return "", "", ErrUnknownTeam
	}
	return value, "", nil
}
