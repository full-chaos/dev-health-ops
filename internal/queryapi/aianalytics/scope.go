package aianalytics

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/latestrow"
)

// QueryClient is the narrow ClickHouse boundary this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// scope is a request scope after normalisation.
type scope struct {
	repoID     string // canonical repository uuid; empty when none
	teamID     string // empty when none
	workType   string // empty when none
	unresolved bool   // a repository was named but does not resolve
	buckets    []string
}

// normalizeScope resolves the repository reference of a scope. A repository
// is a uuid (any spelling a UUID constructor accepts) or a repository
// full-name looked up in the caller's org.
func normalizeScope(ctx context.Context, client QueryClient, orgID string, in *model.AIScopeInput) (scope, error) {
	var out scope
	if in == nil {
		return out, nil
	}
	if in.TeamID != nil {
		out.teamID = *in.TeamID
	}
	if in.WorkType != nil {
		out.workType = *in.WorkType
	}
	for _, b := range in.Buckets {
		out.buckets = append(out.buckets, strings.ToLower(string(b)))
	}
	if in.RepoID == nil || *in.RepoID == "" {
		return out, nil
	}
	id, ok, err := resolveRepoRef(ctx, client, orgID, *in.RepoID)
	if err != nil {
		return out, err
	}
	if !ok {
		out.unresolved = true
		return out, nil
	}
	out.repoID = id
	return out, nil
}

func resolveRepoRef(ctx context.Context, client QueryClient, orgID, raw string) (string, bool, error) {
	if parsed, err := pythonparity.ParseUUID(raw); err == nil {
		return parsed.String(), true, nil
	}
	rs, err := client.Query(ctx, `SELECT toString(id) AS id
FROM repos
WHERE org_id = {org_id:String}
  AND repo = {slug:String}
ORDER BY toString(id)`, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "slug", Value: raw},
	})
	if err != nil {
		return "", false, fmt.Errorf("aianalytics: repository lookup: %w", err)
	}
	defer rs.Close()
	if !rs.Next() {
		if err := rs.Err(); err != nil {
			return "", false, fmt.Errorf("aianalytics: repository lookup rows: %w", err)
		}
		return "", false, nil
	}
	var id string
	if err := rs.Scan(&id); err != nil {
		return "", false, fmt.Errorf("aianalytics: repository lookup scan: %w", err)
	}
	parsed, err := pythonparity.ParseUUID(id)
	if err != nil {
		return "", false, nil
	}
	return parsed.String(), true, nil
}

// teamRepoIDs resolves a team to the repositories of the org it owns: the one
// ownership rule of teamscope (team_repo_ownership as of now, through
// teamscope.HeldRepoIDs), never teams.repo_patterns. ok=false means the
// repository catalogue or the ownership could not be read; the caller treats
// that as an unavailable scope, never as an empty team.
func teamRepoIDs(ctx context.Context, client QueryClient, orgID, teamID, operation string) (ids []string, ok bool) {
	if orgID == "" || teamID == "" {
		return nil, false
	}
	repos, err := loadRepoNames(ctx, client, orgID)
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.team_repos_unavailable",
			"operation", operation, "error", err.Error())
		return nil, false
	}
	all := make([]string, 0, len(repos))
	for _, r := range repos {
		all = append(all, r.id)
	}
	held, err := teamscope.HeldRepoIDs(ctx, client, orgID, all, []string{teamID}, time.Now().UTC())
	if err != nil {
		slog.WarnContext(ctx, "query_api.ai_analytics.team_repos_unavailable",
			"operation", operation, "error", err.Error())
		return nil, false
	}
	return held, true
}

func loadTeams(ctx context.Context, client QueryClient, orgID string) ([]aiimpact.Team, error) {
	rs, err := client.Query(ctx, fmt.Sprintf(`SELECT toString(id) AS id, coalesce(%s, '') AS name
FROM teams
WHERE org_id = {org_id:String}
  AND id IN (%s)
GROUP BY id`, latestrow.ArgMaxKeepNullBy("name", "updated_at"), activeteams.IDsSubquery), []clickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return nil, fmt.Errorf("teams query: %w", err)
	}
	defer rs.Close()
	var out []aiimpact.Team
	for rs.Next() {
		var t aiimpact.Team
		if err := rs.Scan(&t.ID, &t.Name); err != nil {
			return nil, fmt.Errorf("teams scan: %w", err)
		}
		out = append(out, t)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("teams rows: %w", err)
	}
	return out, nil
}

type repoName struct{ id, fullName string }

func loadRepoNames(ctx context.Context, client QueryClient, orgID string) ([]repoName, error) {
	rs, err := client.Query(ctx, fmt.Sprintf(`SELECT toString(id) AS repo_id, coalesce(%s, '') AS full_name
FROM repos
WHERE org_id = {org_id:String}
GROUP BY id`, latestrow.ArgMaxKeepNullBy("repo", "last_synced")), []clickhouse.Binding{{Name: "org_id", Value: orgID}})
	if err != nil {
		return nil, fmt.Errorf("repos query: %w", err)
	}
	defer rs.Close()
	var out []repoName
	for rs.Next() {
		var r repoName
		if err := rs.Scan(&r.id, &r.fullName); err != nil {
			return nil, fmt.Errorf("repos scan: %w", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("repos rows: %w", err)
	}
	return out, nil
}

// dayBounds are the inclusive window of a date range as the naive UTC
// instants the raw-PR queries compare with: midnight of the first day and
// the last whole second of the last day. The reference binds its end instant
// (the last microsecond of the last day) as second-precision DateTime text, so
// a row stamped inside the last second of the last day is outside the window
// on both planes.
func dayBounds(start, end time.Time) (time.Time, time.Time) {
	s := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	e := time.Date(end.Year(), end.Month(), end.Day(), 23, 59, 59, 0, time.UTC)
	return s, e
}

func warnCatalogue(ctx context.Context, operation string, err error) {
	slog.WarnContext(ctx, "query_api.ai_analytics.catalogue_unavailable",
		"operation", operation, "error", err.Error())
}

// ownerTeams maps each repository id to the team that owns it, by the one
// ownership rule of teamscope (teamscope.OwnerShares as of asOf): of its owners,
// preferTeam when it is one of them (a team scope keeps the rows of the
// repositories it owns), else the owner with the largest linked share, the
// lowest team id on a tie. A repository no team owns maps to nil.
func ownerTeams(ctx context.Context, client QueryClient, orgID string, repoIDs []string, preferTeam string, asOf time.Time) (map[string]*string, error) {
	out := make(map[string]*string, len(repoIDs))
	for _, id := range repoIDs {
		out[id] = nil
	}
	if len(repoIDs) == 0 {
		return out, nil
	}
	source, bindings := teamscope.OwnerShares(orgID, nil, asOf)
	statement := fmt.Sprintf(`SELECT os.owner_repo_id AS owner_repo_id, os.owner_team_id AS owner_team_id
FROM %s AS os
WHERE os.owner_repo_id IN {owner_repo_ids:Array(String)}
ORDER BY os.owner_repo_id, (os.owner_team_id = {owner_prefer_team:String}) DESC, os.owner_share DESC, os.owner_team_id`, source)
	bindings = append([]clickhouse.Binding{
		{Name: "owner_repo_ids", Value: repoIDs},
		{Name: "owner_prefer_team", Value: preferTeam},
	}, bindings...)
	rs, err := client.Query(ctx, statement, bindings)
	if err != nil {
		return nil, fmt.Errorf("repository owners query: %w", err)
	}
	defer rs.Close()
	for rs.Next() {
		var repoID, teamID string
		if err := rs.Scan(&repoID, &teamID); err != nil {
			return nil, fmt.Errorf("repository owners scan: %w", err)
		}
		if out[repoID] != nil || teamID == "" {
			continue
		}
		team := teamID
		out[repoID] = &team
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("repository owners rows: %w", err)
	}
	return out, nil
}
