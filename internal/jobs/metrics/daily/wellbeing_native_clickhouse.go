package daily

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// WellbeingTeam is one team row as read from ClickHouse `teams` -- the same
// shape ClickHouseMetricsSink.get_all_teams (sinks/clickhouse/core.py:109)
// selects. Both the repo-pattern resolver and the membership resolver are
// built from this one shared read, exactly as job_daily.py builds both
// repo_team_resolver and team_resolver from a single primary_sink.get_all_teams()
// call.
type WellbeingTeam struct {
	ID           string
	Name         string
	Members      []string
	RepoPatterns []string
}

// LoadWellbeingTeams ports get_all_teams (sinks/clickhouse/core.py:109) --
// the SAME query, byte for byte, that both job_daily.py's repo_team_resolver
// and team_resolver are built from. This is deliberately not narrowed to
// only the columns team_wellbeing uses (repo_patterns, members): reusing the
// exact production query, rather than a hand-trimmed lookalike, is what
// keeps this reader from drifting out of sync with a column the Python
// selector adds later. conn reuses this package's existing repositoryRows
// capability (clickhouse.go) -- a plain Query method, nothing more.
func LoadWellbeingTeams(ctx context.Context, conn repositoryRows, organizationID string) ([]WellbeingTeam, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	rows, err := conn.Query(ctx,
		"SELECT id, name, members, repo_patterns FROM teams FINAL WHERE org_id = ?",
		organizationID,
	)
	if err != nil {
		return nil, fmt.Errorf("load wellbeing teams: %w", err)
	}
	defer rows.Close()

	var teams []WellbeingTeam
	for rows.Next() {
		var team WellbeingTeam
		if err := rows.Scan(&team.ID, &team.Name, &team.Members, &team.RepoPatterns); err != nil {
			return nil, fmt.Errorf("scan wellbeing team: %w", err)
		}
		teams = append(teams, team)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wellbeing teams: %w", err)
	}
	return teams, nil
}

// repoPatternResolver ports RepoPatternTeamResolver / build_repo_pattern_resolver
// (src/dev_health_ops/providers/teams.py:93,247).
type repoPatternResolver struct {
	exact    map[string][2]string
	prefixes []repoPatternPrefix
}

type repoPatternPrefix struct {
	prefix   string
	teamID   string
	teamName string
}

// NewRepoPatternResolver builds the resolver build_repo_pattern_resolver
// builds: an exact-match map for patterns with no "*", and a prefix list
// (pattern with trailing "*" and "/" stripped) for the rest, sorted longest
// prefix first so the most specific pattern wins when several match.
func NewRepoPatternResolver(teams []WellbeingTeam) numerical.RepoTeamResolver {
	exact := make(map[string][2]string)
	var prefixes []repoPatternPrefix
	for _, team := range teams {
		teamID := strings.TrimSpace(team.ID)
		if teamID == "" || len(team.RepoPatterns) == 0 {
			continue
		}
		teamName := strings.TrimSpace(team.Name)
		for _, raw := range team.RepoPatterns {
			pattern := strings.ToLower(strings.TrimSpace(raw))
			if pattern == "" {
				continue
			}
			if strings.Contains(pattern, "*") {
				prefix := strings.TrimRight(strings.TrimRight(pattern, "*"), "/")
				if prefix != "" {
					prefixes = append(prefixes, repoPatternPrefix{prefix: prefix, teamID: teamID, teamName: teamName})
				}
				continue
			}
			exact[pattern] = [2]string{teamID, teamName}
		}
	}
	sort.SliceStable(prefixes, func(i, j int) bool {
		return len(prefixes[i].prefix) > len(prefixes[j].prefix)
	})
	return &repoPatternResolver{exact: exact, prefixes: prefixes}
}

func (resolver *repoPatternResolver) ResolveRepo(repoName string) (string, string) {
	if resolver == nil || repoName == "" {
		return "", ""
	}
	key := strings.ToLower(strings.TrimSpace(repoName))
	if key == "" {
		return "", ""
	}
	if pair, ok := resolver.exact[key]; ok {
		return pair[0], pair[1]
	}
	for _, candidate := range resolver.prefixes {
		if strings.HasPrefix(key, candidate.prefix) {
			return candidate.teamID, candidate.teamName
		}
	}
	return "", ""
}

// memberResolver ports TeamResolver / _build_member_to_team
// (src/dev_health_ops/providers/teams.py:57,141).
type memberResolver struct {
	memberToTeam map[string][2]string
}

// NewMemberResolver builds the membership resolver
// _build_member_to_team/load_team_resolver_from_store build: a normalized
// (lowercased, whitespace-collapsed) identity -> (team_id, team_name) map.
func NewMemberResolver(teams []WellbeingTeam) numerical.MemberTeamResolver {
	memberToTeam := make(map[string][2]string)
	for _, team := range teams {
		teamID := strings.TrimSpace(team.ID)
		if teamID == "" {
			continue
		}
		teamName := strings.TrimSpace(team.Name)
		if teamName == "" {
			teamName = teamID
		}
		for _, member := range team.Members {
			key := normalizeKey(member)
			if key == "" {
				continue
			}
			memberToTeam[key] = [2]string{teamID, teamName}
		}
	}
	return &memberResolver{memberToTeam: memberToTeam}
}

func (resolver *memberResolver) ResolveMember(identity string) (string, string) {
	if resolver == nil || identity == "" {
		return "", ""
	}
	key := normalizeKey(identity)
	if key == "" {
		return "", ""
	}
	pair, ok := resolver.memberToTeam[key]
	if !ok {
		return "", ""
	}
	return pair[0], pair[1]
}

// normalizeKey ports _norm_key (src/dev_health_ops/providers/identity.py:16):
// strip, lowercase, and collapse internal whitespace runs to single spaces.
func normalizeKey(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

// LoadRepoNames reads the full repo name (`org/repo`, the `repos.repo`
// column) for a set of repository ids, scoped by org_id -- the
// team_wellbeing-native counterpart of job_daily.py's
// `repo_names_by_id = {r.repo_id: r.full_name for r in discovered_repos}`.
// Deduplicated the same way ClickHouseRepositoryDiscoverer.RepositoryIDs
// dedups the same table (argMax by last_synced): `repos` is
// ReplacingMergeTree, so a re-synced repo can have more than one physical
// row.
func LoadRepoNames(ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID) (map[string]string, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidState
	}
	if len(repoIDs) == 0 {
		return map[string]string{}, nil
	}
	rows, err := conn.Query(ctx, `
SELECT id, argMax(repo, last_synced) AS full_name
FROM repos
WHERE org_id = ? AND id IN ?
GROUP BY id`, organizationID, repositoryUUIDStrings(repoIDs))
	if err != nil {
		return nil, fmt.Errorf("load repo names: %w", err)
	}
	defer rows.Close()

	names := make(map[string]string, len(repoIDs))
	for rows.Next() {
		var id uuid.UUID
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("scan repo name: %w", err)
		}
		names[id.String()] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repo names: %w", err)
	}
	return names, nil
}

// LoadWellbeingCommits reads one day's deduplicated commits for team_wellbeing.
//
// This is NOT load_git_rows (metrics/loaders/clickhouse.py:86) -- that query
// LEFT JOINs git_commit_stats for per-file rows, which
// compute_team_wellbeing_metrics_daily immediately collapses back down to one
// row per (repo_id, commit_hash) since it never reads a file-level field.
// Reading git_commits directly, deduplicated via FINAL (ReplacingMergeTree
// keyed on last_synced, ORDER BY (org_id, repo_id, hash) since migration 027),
// produces the identical post-dedup input with no join and no in-process
// dedup step, and is provably equivalent: compute_team_wellbeing_metrics_daily
// only ever reads author_email/author_name/committer_when off a commit, all
// three of which are identical across every duplicate (repo_id, commit_hash)
// row the join produces (they come from git_commits, not git_commit_stats),
// so which duplicate Python's dict-insert dedup happens to keep never affects
// the output.
func LoadWellbeingCommits(
	ctx context.Context, conn repositoryRows, organizationID string, start, end time.Time, repoIDs []uuid.UUID,
) ([]numerical.Commit, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || !start.Before(end) {
		return nil, ErrInvalidState
	}
	query := `
SELECT repo_id, author_email, author_name, committer_when
FROM git_commits FINAL
WHERE org_id = ?
  AND committer_when >= ? AND committer_when < ?`
	arguments := []any{organizationID, start.UTC(), end.UTC()}
	if len(repoIDs) > 0 {
		query += " AND repo_id IN ?"
		arguments = append(arguments, repositoryUUIDStrings(repoIDs))
	}
	rows, err := conn.Query(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("load wellbeing commits: %w", err)
	}
	defer rows.Close()

	var commits []numerical.Commit
	for rows.Next() {
		var (
			repoID        uuid.UUID
			authorEmail   *string
			authorName    *string
			committerWhen time.Time
		)
		if err := rows.Scan(&repoID, &authorEmail, &authorName, &committerWhen); err != nil {
			return nil, fmt.Errorf("scan wellbeing commit: %w", err)
		}
		commits = append(commits, numerical.Commit{
			RepoID:        repoID.String(),
			AuthorEmail:   derefWellbeingString(authorEmail),
			AuthorName:    derefWellbeingString(authorName),
			CommitterWhen: committerWhen,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate wellbeing commits: %w", err)
	}
	return commits, nil
}

// wellbeingBatchConn is the narrow write capability
// WriteTeamMetricsDailyPerRepo needs.
type wellbeingBatchConn interface {
	PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error)
}

// WriteTeamMetricsDailyPerRepo ports the write side of write_team_metrics
// (sinks/clickhouse/work_graph.py:177) -- the same table and column order --
// but stamps each repo GROUP's rows with its own computed_at instead of one
// shared value for the whole write.
//
// CHAOS-4276 codex round-2 finding 1: team_metrics_daily is deduped
// downstream via argMax(<col>, computed_at) per (org_id, team_id, day)
// (cognitive_load.py, clickhouse_dedup.py). When a team spans more than one
// repo in a partition, giving every repo's rows the IDENTICAL computed_at
// makes that argMax tie-break implementation-defined -- ClickHouse gives no
// guarantee which of several rows tied on the max value it returns, and
// that can vary by replica or merge state. Python's real per-repo_id call
// structure never has this problem: job_daily.py captures computed_at fresh
// inside every run_daily_metrics_job call, so distinct repo-scoped calls
// naturally get distinct, monotonically increasing timestamps --
// "whichever repo was processed last wins" is at least a REPRODUCIBLE
// tie-break. perRepoRows and computedAtByRepo must be the same length and
// share index order; every row in perRepoRows[i] is stamped with
// computedAtByRepo[i]. Still ONE PrepareBatch/Send round trip (not one
// write per repo), so this carries no new partial-write risk relative to
// WriteTeamMetricsDaily.
func WriteTeamMetricsDailyPerRepo(
	ctx context.Context, conn wellbeingBatchConn, organizationID string, day time.Time,
	perRepoRows [][]numerical.TeamWellbeingMetric, computedAtByRepo []time.Time,
) (int, error) {
	if len(perRepoRows) != len(computedAtByRepo) {
		return 0, fmt.Errorf("%w: %d repo row-groups but %d timestamps", ErrInvalidState, len(perRepoRows), len(computedAtByRepo))
	}
	total := 0
	for _, rows := range perRepoRows {
		total += len(rows)
	}
	if total == 0 {
		return 0, nil
	}
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return 0, ErrInvalidState
	}
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO team_metrics_daily (
		day, team_id, team_name, commits_count, after_hours_commits_count,
		weekend_commits_count, after_hours_commit_ratio, weekend_commit_ratio,
		computed_at, org_id)`)
	if err != nil {
		return 0, fmt.Errorf("prepare team_metrics_daily batch: %w", err)
	}
	dayValue := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	for groupIndex, rows := range perRepoRows {
		computedAt := computedAtByRepo[groupIndex].UTC()
		for _, row := range rows {
			if err := batch.Append(
				dayValue, row.TeamID, row.TeamName, uint32(row.CommitsCount),
				uint32(row.AfterHoursCommitsCount), uint32(row.WeekendCommitsCount),
				row.AfterHoursCommitRatio, row.WeekendCommitRatio,
				computedAt, organizationID,
			); err != nil {
				return 0, fmt.Errorf("append team_metrics_daily row: %w", err)
			}
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send team_metrics_daily batch: %w", err)
	}
	return total, nil
}

func derefWellbeingString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func repositoryUUIDStrings(ids []uuid.UUID) []string {
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = id.String()
	}
	return result
}
