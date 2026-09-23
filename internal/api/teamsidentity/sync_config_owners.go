package teamsidentity

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

// deriveOwnersFromSyncConfigs ports _derive_owners_from_sync_configs
// (src/dev_health_ops/api/admin/routers/teams.py:55-79) verbatim: an org
// with a working repo sync for this provider can discover teams with no
// extra ?org=/?group= configuration, because repo sync already stores the
// GitHub org / GitLab group in sync_configurations.sync_options (key
// "owner", see the sync-config batch endpoint).
//
// Reads every ACTIVE sync_configurations row for this org+provider (ordered
// by created_at -- the same effectively-insertion order Python's unordered
// SQLAlchemy select() returns for this small, unindexed-by-order set, made
// explicit rather than left to Postgres's default scan order), then for
// each row and each key in optionKeys (checked in the given order, mirroring
// Python's `for key in option_keys` inner loop), collects the first
// non-empty string value seen for that key across every row -- deduped, in
// first-seen order, matching `if value and value not in owners`.
func deriveOwnersFromSyncConfigs(ctx context.Context, pool *pgxpool.Pool, orgID, provider string, optionKeys []string) ([]string, error) {
	rows, err := pool.Query(ctx, `
SELECT sync_options FROM public.sync_configurations
WHERE org_id = $1 AND provider = $2 AND is_active = true
ORDER BY created_at`, orgID, provider)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	owners := []string{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var options map[string]any
		_ = json.Unmarshal(raw, &options)
		for _, key := range optionKeys {
			value, ok := options[key].(string)
			if !ok || value == "" {
				continue
			}
			if _, exists := seen[value]; exists {
				continue
			}
			seen[value] = struct{}{}
			owners = append(owners, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return owners, nil
}

// dedupeDiscoveredTeams ports _dedupe_teams (teams.py:82-92): dedupe
// discovered teams by provider_team_id, keeping the first occurrence --
// used when discovery loops over more than one resolved org/group and
// combines their results.
func dedupeDiscoveredTeams(teams []discoveredTeam) []discoveredTeam {
	seen := map[string]struct{}{}
	unique := make([]discoveredTeam, 0, len(teams))
	for _, team := range teams {
		if _, exists := seen[team.ProviderTeamID]; exists {
			continue
		}
		seen[team.ProviderTeamID] = struct{}{}
		unique = append(unique, team)
	}
	return unique
}
